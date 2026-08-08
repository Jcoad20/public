# ------------------------------------------------------------------------------
# app.py - FastAPI Application Entrypoint for Kubernetes Assistant
# ------------------------------------------------------------------------------
# 功能说明:
#   1. 原有功能: 用户交互式 K8s 查询 (/api/ask)
#   2. 新增功能: 自动诊断接口 (/api/auto-diagnose)
#   3. 启动后台监控调度器
# ------------------------------------------------------------------------------

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Any
from ai import ai_plan, ai_diagnose_anomaly, AIResponse, Step, DiagnosisReport, format_anomalies_for_ai
from k8s import run_kubectl
from monitor import (
    start_monitoring, 
    stop_monitoring, 
    trigger_manual_scan,
    set_scan_callback,
    get_alert_filter,
    alert_filter
)
from alerter import send_aggregated_alert, AlertMessage
import asyncio
import logging
import os

# ------------------------------------------------------------------------------
# Logging Configuration
# ------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# FastAPI App Initialization
# ------------------------------------------------------------------------------
app = FastAPI(
    title="Kubernetes Assistant API",
    description="K8s Assistant Backend with Auto-Diagnosis & Alerting",
    version="2.0.0"
)
app.add_middleware(
    CORSMiddleware, 
    allow_origins=["*"],        # ⚠️ Security note: limit this in production
    allow_methods=["*"], 
    allow_headers=["*"]
)


# ------------------------------------------------------------------------------
# Request Models
# ------------------------------------------------------------------------------

class RequestModel(BaseModel):
    """用户交互式请求模型"""
    user_input: str


class DiagnoseRequest(BaseModel):
    """手动触发诊断请求（可选）"""
    namespaces: Optional[list] = None  # 指定 Namespace，空则全部扫描
    force: bool = False  # 是否强制发送告警（忽略冷却）


class DiagnoseResponse(BaseModel):
    """诊断响应模型"""
    success: bool
    message: str
    anomalies_found: int = 0
    alerts_sent: int = 0
    diagnosis: Optional[Dict[Any, Any]] = None  # AI 诊断报告
    timestamp: str = ""


# ------------------------------------------------------------------------------
# Background Monitoring Callback
# ------------------------------------------------------------------------------

async def on_anomalies_detected(anomalous_pods):
    """
    监控发现异常时的回调处理函数
    
    流程:
    1. 告警过滤去重 -> 2. AI 诊断 -> 3. 发送聚合告警
    """
    if not anomalous_pods:
        return
    
    logger.info(f"Processing {len(anomalous_pods)} detected anomaly(ies)...")
    
    try:
        # Step 1: 告警去重与聚合
        anomalies_dicts = [pod.to_dict() for pod in anomalous_pods]
        aggregated = alert_filter.process_anomalies(anomalies_dicts)
        
        if not aggregated:
            logger.info("All alerts filtered out by cooldown/deduplication.")
            return
        
        # Step 2: AI 诊断（异步执行，不阻塞告警发送）
        try:
            anomalies_for_ai = format_anomalies_for_ai(anomalous_pods)
            diagnosis = await ai_diagnose_anomaly(anomalies_for_ai)
            
            # 将诊断结果附加到告警详情中
            if diagnosis and diagnosis.summary:
                logger.info(f"AI Diagnosis: [{diagnosis.severity}] {diagnosis.summary}")
                
                # 可以选择将诊断信息写入日志或存储
                # 这里简化处理，仅记录
        
        except Exception as e:
            logger.error(f"AI diagnosis failed: {e}")
        
        # Step 3: 发送聚合告警
        alerts_sent = await send_aggregated_alert(aggregated)
        
        if alerts_sent:
            logger.info(f"Successfully sent aggregated alerts for {len(aggregated)} deployment(s).")
        else:
            logger.warning("Failed to send alerts via webhook.")
    
    except Exception as e:
        logger.error(f"Error in anomaly processing callback: {e}", exc_info=True)


# ------------------------------------------------------------------------------
# API Endpoint: Ask Kubernetes Assistant (原有接口)
# ------------------------------------------------------------------------------

@app.post("/api/ask")
async def ask_k8s(request: RequestModel):
    """
    处理用户交互式 K8s 查询请求（原有功能保持不变）
    """

    steps = [Step(step=1, kubectl_command=None, kubectl_output=None)]
    ai_resp = None
    max_steps = 10  # Safety measure

    while not (ai_resp and ai_resp.final_output) and len(steps) <= max_steps:
        ai_resp: AIResponse = await ai_plan(
            request.user_input,
            {"steps": [s.dict() for s in steps]}
        )

        for step in ai_resp.steps:
            if step.kubectl_command and not step.kubectl_output:
                step.kubectl_output = run_kubectl(step.kubectl_command)
        
        if ai_resp.final_output:
            break
        
        steps = ai_resp.steps
        new_step_number = len(steps) + 1
        steps.append(Step(step=new_step_number, kubectl_command=None, kubectl_output=None))

    return ai_resp


# ------------------------------------------------------------------------------
# NEW API Endpoint: Auto Diagnose (新增接口)
# ------------------------------------------------------------------------------

@app.post("/api/auto-diagnose", response_model=DiagnoseResponse)
async def auto_diagnose(request: DiagnoseRequest = None):
    """
    自动诊断接口
    
    功能:
    1. 手动触发一次集群异常扫描
    2. 对发现的异常进行 AI 诊断
    3. 经过去重冷却后发送告警（如果配置了 Webhook）
    
    参数:
    - namespaces: 可选，指定要扫描的 Namespace 列表
    - force: 是否强制发送告警（跳过冷却期）
    
    Returns:
    - 扫描到的异常数量
    - AI 诊断报告
    - 告警发送状态
    """
    
    from datetime import datetime
    
    request = request or DiagnoseRequest()
    
    try:
        # Step 1: 手动触发扫描
        logger.info("Manual scan triggered via /api/auto-diagnose")
        anomalies = trigger_manual_scan()
        
        if not anomalies:
            return DiagnoseResponse(
                success=True,
                message="No anomalies detected in the cluster.",
                anomalies_found=0,
                alerts_sent=0,
                timestamp=datetime.now().isoformat()
            )
        
        logger.info(f"Scan found {len(anomalies)} anomalous pod(s)")
        
        # Step 2: 告警过滤
        anomalies_dicts = [pod.to_dict() for pod in anomalies]
        
        # 如果是强制模式，重置过滤器状态以允许立即发送
        if request.force:
            alert_filter.reset()
        
        aggregated = alert_filter.process_anomalies(anomalies_dicts)
        
        # Step 3: AI 诊断
        diagnosis_dict = None
        try:
            anomalies_for_ai = format_anomalies_for_ai(anomalies)
            diagnosis = await ai_diagnose_anomaly(anomalies_for_ai)
            diagnosis_dict = diagnosis.dict() if diagnosis else None
        except Exception as e:
            logger.error(f"AI diagnosis failed: {e}")
            diagnosis_dict = {"error": str(e)}
        
        # Step 4: 发送告警
        alerts_sent_count = 0
        if aggregated:
            sent = await send_aggregated_alert(aggregated)
            alerts_sent_count = len(aggregated) if sent else 0
        
        return DiagnoseResponse(
            success=True,
            message=(
                f"Diagnosis complete. Found {len(anomalies)} anomaly(ies), "
                f"{alerts_sent_count} alert(s) sent."
            ),
            anomalies_found=len(anomalies),
            alerts_sent=alerts_sent_count,
            diagnosis=diagnosis_dict,
            timestamp=datetime.now().isoformat()
        )
    
    except Exception as e:
        logger.error(f"Auto-diagnose failed: {e}", exc_info=True)
        return DiagnoseResponse(
            success=False,
            message=f"Diagnosis failed: {str(e)}",
            timestamp=datetime.now().isoformat()
        )


@app.get("/api/auto-diagnose/status")
async def get_diagnosis_status():
    """
    获取当前诊断系统状态
    
    Returns:
    - 活跃告警数量
    - 过滤器状态摘要
    - 监控是否运行中
    """
    from monitor import scheduler
    
    filter_summary = alert_filter.get_active_alert_summary()
    
    return {
        "monitoring_running": scheduler.running,
        "active_alerts": filter_summary["total_active"],
        "alert_details": filter_summary["alerts"],
        "webhook_configured": bool(os.environ.get("ALERT_WEBHOOK_URL", "")),
        "alert_enabled": os.environ.get("ALERT_ENABLED", "true").lower() == "true"
    }


# ------------------------------------------------------------------------------
# Lifecycle Events: Startup & Shutdown
# ------------------------------------------------------------------------------

@app.on_event("startup")
async def startup_event():
    """应用启动时初始化"""
    logger.info("Starting up K8s Assistant Backend v2.0...")
    
    # 设置监控回调
    set_scan_callback(on_anomalies_detected)
    
    # 启动后台监控
    start_monitoring()
    
    logger.info("Backend startup complete. Monitoring active.")


@app.on_event("shutdown")
async def shutdown_event():
    """应用关闭时清理"""
    logger.info("Shutting down K8s Assistant Backend...")
    
    stop_monitoring()
    
    logger.info("Shutdown complete.")


# ------------------------------------------------------------------------------
# Health Check Endpoint
# ------------------------------------------------------------------------------

@app.get("/health")
async def health_check():
    """健康检查端点"""
    return {
        "status": "healthy",
        "version": "2.0.0",
        "features": ["interactive_query", "auto_diagnose", "alerting"]
    }
