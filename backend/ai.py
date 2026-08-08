import os
import json
import re
import traceback
from pydantic import BaseModel
from typing import Optional
import google.generativeai as genai
import logging

# ------------------------------------------------------------------------------
# Logging Configuration
# ------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Gemini API Configuration
# ------------------------------------------------------------------------------
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY environment variable not set")

genai.configure(api_key=GEMINI_API_KEY)
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash")

# ------------------------------------------------------------------------------
# Data Models (Pydantic for Validation and Type Safety)
# ------------------------------------------------------------------------------

class Step(BaseModel):
    """Represents a single step in the AI-assisted execution flow."""
    step: int
    kubectl_command: Optional[str] = None
    kubectl_output: Optional[str] = None


class AIResponse(BaseModel):
    """Represents the structured response returned from the AI model."""
    intent: str
    steps: list[Step]
    final_output: Optional[str] = None


class DiagnosisReport(BaseModel):
    """
    AI 诊断报告数据结构（用于自动诊断场景）
    
    用于 Pod 异常的智能分析和诊断建议
    """
    severity: str  # critical / warning / info
    summary: str   # 一句话总结
    root_cause_analysis: str  # 根因分析
    recommended_actions: list[str]  # 建议操作列表
    affected_resources: list[dict]  # 受影响资源详情
    confidence: str  # high / medium / low (AI 置信度)


# ------------------------------------------------------------------------------
# Core Function: ai_plan (原有功能保留)
# ------------------------------------------------------------------------------

async def ai_plan(intent_text: str, steps_yaml: dict) -> AIResponse:
    """
    Given a high-level user intent and previous execution steps,
    request a response plan from Google Gemini.
    
    (原有功能保持不变，用于用户交互式查询场景)
    """

    prompt = f"""\
You are a Kubernetes assistant. Your task is to help users manage their Kubernetes clusters by executing kubectl commands and providing a clear, human-readable summary of the results.

The user's high-level intent is:
{intent_text}

You have access to the history of previous steps, which includes the command executed and its output. Your job is to analyze this history and determine the next action.

Previous steps:
{json.dumps(steps_yaml, indent=2)}

Your decision should be one of two things:
1.  **If no command has been run yet**, or if the previous command did not achieve the user's intent, generate the next appropriate 'kubectl' command to execute. Place this command in the 'kubectl_command' field of the next step.
2.  **If a command has been successfully executed and its output is available in 'kubectl_output'**, and you believe the user's intent has been fulfilled, generate a comprehensive, human-readable summary of the results based on the 'kubectl_output'. This summary should be placed in the 'final_output' field. The 'steps' array should remain as it is.

Return a JSON object with the following schema. Do NOT include any other text or markdown formatting outside of the JSON block.

{{
  "intent": "string",
  "steps": [
    {{
      "step": "integer",
      "kubectl_command": "string or null",
      "kubectl_output": "string or null"
    }}
  ],
  "final_output": "string or null"
}}

Here is an example of what to return when you have a final output:
{{
  "intent": "{intent_text}",
  "steps": [
    {{
      "step": 1,
      "kubectl_command": "kubectl get pods -A",
      "kubectl_output": "NAMESPACE NAME...\\ndefault nginx-deployment..."
    }}
  ],
  "final_output": "Here are all the pods across all namespaces:\\nNAMESPACE NAME...\\ndefault nginx-deployment..."
}}
"""
    try:
        model = genai.GenerativeModel(GEMINI_MODEL)

        response = await model.generate_content_async(
            prompt,
            generation_config=genai.GenerationConfig(temperature=0)
        )

        text = response.text if hasattr(response, "text") else response.candidates[0].content.parts[0].text

        if text.startswith("```"):
            text = re.sub(r"^```[a-zA-Z]*\n?", "", text)
            text = re.sub(r"```$", "", text)

        data = json.loads(text)
        return AIResponse(**data)

    except Exception as e:
        traceback.print_exc()
        return AIResponse(
            intent=intent_text,
            steps=steps_yaml["steps"],
            final_output=f"Error: An error occurred while generating the response: {e}"
        )


# ------------------------------------------------------------------------------
# New Function: ai_diagnose_anomaly (新增：异常诊断专用 Prompt)
# ------------------------------------------------------------------------------

async def ai_diagnose_anomaly(anomalies_data: dict) -> DiagnosisReport:
    """
    使用 AI 对 Kubernetes Pod 异常进行智能诊断
    
    此函数专门针对监控发现的 Pod 异常（CrashLoopBackOff、ImagePullBackOff、OOMKilled）
    进行根因分析，生成结构化的诊断报告。
    
    Args:
        anomalies_data: 异常数据字典，包含：
            - anomalies: List[dict] - 异常 Pod 列表
                每项包含 namespace, name, deployment_name, reason, message, restart_count
            - cluster_context: dict - 集群上下文信息（可选）
    
    Returns:
        DiagnosisReport: 结构化的诊断报告
    
    注意事项：
        - 只分析明确异常状态，不猜测 Pending 等复杂状态的原因
        - 输出应准确简洁，避免 AI 幻觉误报
        - 提供可操作的修复建议
    """

    prompt = f"""\
你是一个专业的 Kubernetes 故障诊断专家。请根据以下异常 Pod 信息进行分析和诊断。

## 任务目标
对检测到的 Kubernetes Pod 异常进行根因分析，并提供准确的诊断建议。

## 异常 Pod 数据
{json.dumps(anomalies_data, indent=2, ensure_ascii=False)}

## 分析要求

### 1. 严重程度判断 (severity)
- **critical**: 影响生产流量或核心服务（如多个副本同时 CrashLoopBackOff）
- **warning**: 单个 Pod 异常但存在冗余副本（服务未完全中断）
- **info**: 可自愈或低风险问题

### 2. 根因分析原则
请基于以下已知模式进行分析：

**CrashLoopBackOff 常见原因：**
- 应用程序启动失败（代码错误、配置缺失）
- 健康检查失败（liveness/readiness probe 配置不当）
- 资源不足（CPU/内存限制过低）
- 依赖服务不可用（数据库、缓存等）

**ImagePullBackOff 常见原因：**
- 镜像地址错误或不存在
- 私有仓库认证失败（imagePullSecrets 配置问题）
- 网络问题导致无法访问镜像仓库
- 镜像 tag 不存在

**OOMKilled 常见原因：**
- 容器内存限制设置过低
- 内存泄漏（应用 bug）
- 突发流量导致内存使用激增

### 3. 输出要求
- **只基于提供的数据进行分析**，不要猜测未给出的信息
- 如果信息不足以确定根因，confidence 设为 "low"
- recommended_actions 应具体可执行，避免泛泛而谈
- 使用中文输出（与用户语言一致）

## 返回格式
请返回严格的 JSON 格式：

{{
  "severity": "critical|warning|info",
  "summary": "一句话概括问题和影响",
  "root_cause_analysis": "详细的根因分析（2-5句话）",
  "recommended_actions": [
    "具体的操作步骤 1",
    "具体的操作步骤 2",
    "..."
  ],
  "affected_resources": [
    {{
      "namespace": "命名空间",
      "deployment": "工作负载名称",
      "pods_affected": ["pod-name-1", "pod-name-2"],
      "anomaly_type": "CrashLoopBackOff|ImagePullBackOff|OOMKilled"
    }}
  ],
  "confidence": "high|medium|low"
}}

注意：不要包含任何 JSON 以外的文本或 markdown 标记。
"""

    try:
        model = genai.GenerativeModel(GEMINI_MODEL)

        # 诊断任务使用稍高的 temperature 允许更多创造性分析
        response = await model.generate_content_async(
            prompt,
            generation_config=genai.GenerationConfig(temperature=0.3)
        )

        text = response.text if hasattr(response, "text") else response.candidates[0].content.parts[0].text

        # 清理 markdown 标记
        if text.startswith("```"):
            text = re.sub(r"^```[a-zA-Z]*\n?", "", text)
            text = re.sub(r"```$", "", text)

        data = json.loads(text)
        
        logger.info(f"AI diagnosis completed for {len(anomalies_data.get('anomalies', []))} anomaly(ies)")
        
        return DiagnosisReport(**data)

    except Exception as e:
        logger.error(f"Error in AI diagnosis: {e}", exc_info=True)
        
        # 返回错误状态的诊断报告
        return DiagnosisReport(
            severity="warning",
            summary=f"AI 诊断暂时不可用: {str(e)}",
            root_cause_analysis="无法完成自动诊断，建议人工排查。",
            recommended_actions=[
                "查看 Pod 日志: kubectl logs <pod-name> --previous",
                "检查 Pod 描述: kubectl describe pod <pod-name>",
                "确认最近是否有配置变更"
            ],
            affected_resources=anomalies_data.get("anomalies", []),
            confidence="low"
        )


# ------------------------------------------------------------------------------
# Helper Function: format_anomalies_for_ai (辅助函数)
# ------------------------------------------------------------------------------

def format_anomalies_for_ai(anomalous_pods) -> dict:
    """
    将 AnomalousPod 对象列表转换为 AI 可处理的字典格式
    
    Args:
        anomalous_pods: List[AnomalousPod] 来自 monitor.py 的异常对象
    
    Returns:
        dict: 格式化后的数据
    """
    return {
        "anomalies": [pod.to_dict() for pod in anomalous_pods],
        "scan_timestamp": __import__('datetime').datetime.now().isoformat(),
        "total_count": len(anomalous_pods)
    }
