# ------------------------------------------------------------------------------
# alerter.py - 告警推送模块
# ------------------------------------------------------------------------------
# 功能说明:
#   1. 接收经过去重和聚合后的告警数据
#   2. 通过 Webhook 推送告警消息到外部系统（钉钉、企业微信、Slack 等）
#   3. 支持 HTTP POST 方式推送 JSON 格式告警内容
#   4. 记录推送日志，处理推送失败情况
# ------------------------------------------------------------------------------

import os
import json
import logging
from typing import List, Dict, Optional
from datetime import datetime
import aiohttp

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 从环境变量读取 Webhook 配置
ALERT_WEBHOOK_URL = os.environ.get("ALERT_WEBHOOK_URL", "")  # 告警 Webhook 地址
ALERT_WEBHOOK_TIMEOUT = int(os.environ.get("ALERT_WEBHOOK_TIMEOUT", "10"))  # 超时时间（秒）
ALERT_ENABLED = os.environ.get("ALERT_ENABLED", "true").lower() == "true"


class AlertMessage:
    """告警消息数据结构"""
    
    def __init__(
        self,
        title: str,
        severity: str,
        summary: str,
        details: List[Dict],
        timestamp: datetime = None
    ):
        self.title = title
        self.severity = severity  # critical / warning / info
        self.summary = summary
        self.details = details  # 异常 Pod 详情列表
        self.timestamp = timestamp or datetime.now()
    
    def to_webhook_payload(self) -> Dict:
        """
        转换为通用的 Webhook JSON 格式
        
        兼容多种平台（钉钉、企业微信、Slack、飞书等）
        """
        return {
            "msgtype": "markdown",
            "markdown": {
                "title": f"[{self.severity.upper()}] {self.title}",
                "text": self._format_markdown_content()
            }
        }
    
    def _format_markdown_content(self) -> str:
        """生成 Markdown 格式的告警内容"""
        lines = [
            f"## ⚠️ Kubernetes Pod 异常告警",
            f"",
            f"**级别:** {self.severity.upper()}",
            f"**时间:** {self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}",
            f"**摘要:** {self.summary}",
            f"",
            f"### 异常详情",
            f"| Namespace | Pod | Deployment | 状态 | 重启次数 |",
            f"|-----------|-----|------------|------|----------|",
        ]
        
        for detail in self.details:
            lines.append(
                f"| {detail.get('namespace', '-')} | "
                f"{detail.get('name', '-')} | "
                f"{detail.get('deployment_name', '-')} | "
                f"{detail.get('reason', '-')} | "
                f"{detail.get('restart_count', 0)} |"
            )
        
        lines.append("")
        lines.append("> 此消息由 K8s Assistant 自动诊断系统发出")
        
        return "\n".join(lines)


async def send_alert_webhook(alert: AlertMessage) -> bool:
    """
    发送告警到 Webhook
    
    Args:
        alert: AlertMessage 对象
    
    Returns:
        bool: 是否发送成功
    """
    if not ALERT_ENABLED:
        logger.info("Alert sending is disabled via ALERT_ENABLED env var.")
        return True
    
    if not ALERT_WEBHOOK_URL:
        logger.error("ALERT_WEBHOOK_URL is not configured. Cannot send alert.")
        return False
    
    payload = alert.to_webhook_payload()
    
    try:
        timeout = aiohttp.ClientTimeout(total=ALERT_WEBHOOK_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                ALERT_WEBHOOK_URL,
                json=payload,
                headers={"Content-Type": "application/json"}
            ) as response:
                
                if response.status == 200:
                    logger.info(f"Alert sent successfully to webhook.")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(
                        f"Webhook returned status {response.status}: {error_text}"
                    )
                    return False
    
    except aiohttp.ClientError as e:
        logger.error(f"Failed to send alert via webhook: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error sending alert: {e}", exc_info=True)
        return False


async def send_aggregated_alert(
    aggregated_anomalies: Dict[str, List[Dict]]
) -> bool:
    """
    发送聚合后的告警
    
    Args:
        aggregated_anomalies: 按 Deployment 分组的异常数据
            格式: {deployment_name: [anomaly_dict, ...]}
    
    Returns:
        bool: 是否发送成功
    """
    if not aggregated_anomalies:
        logger.debug("No anomalies to alert.")
        return True
    
    # 统计总数
    total_anomalies = sum(len(v) for v in aggregated_anomalies.values())
    
    # 构建详情列表
    all_details = []
    for deployment_name, anomalies in aggregated_anomalies.items():
        for anomaly in anomalies:
            anomaly["deployment_name"] = deployment_name
            all_details.append(anomaly)
    
    # 创建告警消息
    alert = AlertMessage(
        title=f"K8s Pod 异常检测 ({total_anomalies} 个异常)",
        severity="warning",
        summary=(
            f"检测到 {len(aggregated_anomalies)} 个 Deployment 存在异常，"
            f"共 {total_anomalies} 个异常 Pod"
        ),
        details=all_details
    )
    
    return await send_alert_webhook(alert)


def format_alert_for_console(alert: AlertMessage) -> str:
    """格式化告警用于控制台输出（调试用）"""
    output = [
        "=" * 60,
        f"⚠️  ALERT: {alert.title}",
        f"Severity: {alert.severity}",
        f"Time: {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        alert.summary,
        "",
        "Details:",
    ]
    
    for detail in alert.details:
        output.append(
            f"  - [{detail.get('reason')}] "
            f"{detail.get('namespace')}/{detail.get('name')} "
            f"(restarts: {detail.get('restart_count', 0)})"
        )
    
    output.append("=" * 60)
    return "\n".join(output)


if __name__ == "__main__":
    # 测试模式
    import asyncio
    
    test_alert = AlertMessage(
        title="Test Alert",
        severity="warning",
        summary="This is a test alert message.",
        details=[
            {
                "namespace": "default",
                "name": "test-pod-abc123",
                "deployment_name": "test-deployment",
                "reason": "CrashLoopBackOff",
                "restart_count": 5
            }
        ]
    )
    
    print(format_alert_for_console(test_alert))
    print("\nPayload:")
    print(json.dumps(test_alert.to_webhook_payload(), indent=2, ensure_ascii=False))
