# ------------------------------------------------------------------------------
# monitor.py - Kubernetes Pod 异常监控扫描模块
# ------------------------------------------------------------------------------
# 功能说明:
#   1. 使用 APScheduler 定时扫描集群中所有 Namespace 的 Pod 状态
#   2. 重点关注 CrashLoopBackOff, ImagePullBackOff, OOMKilled 三种明确异常
#   3. 将异常 Pod 信息传递给告警过滤器和 AI 诊断模块
#   4. 支持配置扫描间隔、监控的 Namespace 范围等参数
# ------------------------------------------------------------------------------

import os
import logging
from datetime import datetime
from typing import List, Dict, Optional
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import kubernetes.client
from kubernetes.client import V1Pod, V1PodStatus, V1ContainerState

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 从环境变量读取配置
SCAN_INTERVAL_SECONDS = int(os.environ.get("MONITOR_SCAN_INTERVAL", "60"))  # 默认60秒扫描一次
MONITOR_NAMESPACES = os.environ.get("MONITOR_NAMESPACES", "").split(",") if os.environ.get("MONITOR_NAMESPACES") else []  # 空表示全部


class AnomalousPod:
    """表示一个异常 Pod 的数据结构"""
    
    def __init__(self, pod: V1Pod):
        self.namespace = pod.metadata.namespace
        self.name = pod.metadata.name
        self.deployment_name = self._extract_deployment_name(pod)
        self.phase = pod.status.phase
        self.reason = self._get_failure_reason(pod)
        self.message = self._get_failure_message(pod)
        self.restart_count = self._get_restart_count(pod)
        self.container_statuses = pod.status.container_statuses or []
        self.timestamp = datetime.now()
    
    def _extract_deployment_name(self, pod: V1Pod) -> str:
        """从 Pod 的 owner references 中提取 Deployment 名称"""
        try:
            for owner in (pod.metadata.owner_references or []):
                if owner.kind == "ReplicaSet":
                    # ReplicaSet 名称格式: deployment-name-xxxxx
                    rs_name = owner.name
                    if rs_name and len(rs_name) > 10:
                        return "-".join(rs_name.split("-")[:-1])
        except Exception as e:
            logger.warning(f"Failed to extract deployment name: {e}")
        return ""
    
    def _get_failure_reason(self, pod: V1Pod) -> Optional[str]:
        """获取 Pod 失败原因"""
        try:
            container_statuses = pod.status.container_statuses or []
            for cs in container_statuses:
                if cs.state and cs.state.waiting:
                    return cs.state.waiting.reason
                elif cs.state and cs.state.terminated:
                    return cs.state.terminated.reason
            return pod.status.reason
        except Exception:
            return None
    
    def _get_failure_message(self, pod: V1Pod) -> Optional[str]:
        """获取 Pod 失败详细消息"""
        try:
            container_statuses = pod.status.container_statuses or []
            for cs in container_statuses:
                if cs.state and cs.state.waiting:
                    return cs.state.waiting.message
                elif cs.state and cs.state.terminated:
                    return cs.state.terminated.message
            return pod.status.message
        except Exception:
            return None
    
    def _get_restart_count(self, pod: V1Pod) -> int:
        """获取容器重启次数"""
        try:
            total_restarts = 0
            for cs in (pod.status.container_statuses or []):
                total_restarts += cs.restart_count or 0
            return total_restarts
        except Exception:
            return 0
    
    def to_dict(self) -> Dict:
        """转换为字典格式"""
        return {
            "namespace": self.namespace,
            "name": self.name,
            "deployment_name": self.deployment_name,
            "phase": self.phase,
            "reason": self.reason,
            "message": self.message,
            "restart_count": self.restart_count,
            "timestamp": self.timestamp.isoformat()
        }


# 定义要关注的明确异常状态（排除 Pending 等复杂状态）
TARGET_ANOMALIES = {"CrashLoopBackOff", "ImagePullBackOff", "OOMKilled"}


def get_k8s_client() -> kubernetes.client.CoreV1Api:
    """获取 Kubernetes API 客户端"""
    try:
        kubernetes.config.load_incluster_config()
    except kubernetes.config.ConfigException:
        kubernetes.config.load_kube_config()
    return kubernetes.client.CoreV1Api()


def scan_pods_for_anomalies() -> List[AnomalousPod]:
    """
    扫描集群中的异常 Pod
    
    Returns:
        List[Anomaly]: 异常 Pod 列表
    """
    anomalous_pods = []
    
    try:
        v1 = get_k8s_client()
        
        # 确定 Namespace 列表
        if MONITOR_NAMESPACES and MONITOR_NAMESPACES[0]:  # 非空列表
            namespaces = MONITOR_NAMESPACES
        else:
            # 获取所有 Namespace
            namespaces = [ns.metadata.name for ns in v1.list_namespace().items]
        
        logger.info(f"Scanning {len(namespaces)} namespaces for anomalies...")
        
        for ns in namespaces:
            try:
                pods = v1.list_namespaced_pod(namespace=ns).items
                
                for pod in pods:
                    anomaly = _check_pod_anomaly(pod)
                    if anomaly:
                        anomalous_pods.append(anomaly)
                        logger.warning(
                            f"Found anomaly: {anomaly.reason} in "
                            f"{ns}/{pod.metadata.name} "
                            f"(deployment: {anomaly.deployment_name})"
                        )
            
            except Exception as e:
                logger.error(f"Error scanning namespace {ns}: {e}")
        
        logger.info(f"Scan complete. Found {len(anomalous_pods)} anomalous pods.")
        
    except Exception as e:
        logger.error(f"Failed to scan Kubernetes cluster: {e}", exc_info=True)
    
    return anomalous_pods


def _check_pod_anomaly(pod: V1Pod) -> Optional[AnomalousPod]:
    """
    检查单个 Pod 是否存在目标异常状态
    
    只关注以下三种明确异常:
    - CrashLoopBackOff: 容器反复崩溃重启
    - ImagePullBackOff: 镜像拉取失败
    - OOMKilled: 内存超限被终止
    
    不分析 Pending 状态（原因复杂，AI容易猜错）
    """
    try:
        container_statuses = pod.status.container_statuses or []
        
        for cs in container_statuses:
            # 检查 Waiting 状态
            if cs.state and cs.state.waiting:
                reason = cs.state.waiting.reason
                if reason in TARGET_ANOMALIES:
                    return AnomalousPod(pod)
            
            # 检查 Terminated 状态
            if cs.state and cs.state.terminated:
                reason = cs.state.terminated.reason
                if reason == "OOMKilled":
                    return AnomalousPod(pod)
        
        # 检查 Pod 级别的 phase 和 reason
        if pod.status.phase == "Failed":
            if pod.status.reason in TARGET_ANOMALIES:
                return AnomalousPod(pod)
        
        return None
        
    except Exception as e:
        logger.error(f"Error checking pod {pod.metadata.name}: {e}")
        return None


# 全局调度器实例
scheduler = BackgroundScheduler()
_scan_callback = None  # 外部注册的回调函数，用于处理发现的异常


def set_scan_callback(callback):
    """
    设置扫描结果回调函数
    
    Args:
        callback: 接收 List[AnomalousPod] 的回调函数
    """
    global _scan_callback
    _scan_callback = callback


def _scheduled_scan_job():
    """定时执行的扫描任务"""
    logger.info("Starting scheduled anomaly scan...")
    
    try:
        anomalies = scan_pods_for_anomalies()
        
        if anomalies and _scan_callback:
            # 调用外部回调处理异常
            _scan_callback(anomalies)
        elif not anomalies:
            logger.debug("No anomalies found in this scan cycle.")
    
    except Exception as e:
        logger.error(f"Error in scheduled scan job: {e}", exc_info=True)


def start_monitoring():
    """启动后台监控调度器"""
    if scheduler.running:
        logger.warning("Monitor scheduler is already running.")
        return
    
    # 添加定时扫描任务
    scheduler.add_job(
        _scheduled_scan_job,
        trigger=IntervalTrigger(seconds=SCAN_INTERVAL_SECONDS),
        id="k8s_anomaly_scanner",
        name="Kubernetes Anomaly Scanner",
        replace_existing=True
    )
    
    scheduler.start()
    logger.info(
        f"Monitoring started. Scan interval: {SCAN_INTERVAL_SECONDS}s"
    )


def stop_monitoring():
    """停止后台监控调度器"""
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Monitoring stopped.")


def trigger_manual_scan() -> List[AnomalousPod]:
    """手动触发一次扫描（用于测试或API调用）"""
    return scan_pods_for_anomalies()


if __name__ == "__main__":
    # 测试模式：直接运行一次扫描
    print("Running manual scan...")
    anomalies = scan_pods_for_anomalies()
    
    if anomalies:
        print(f"\nFound {len(anomalies)} anomalous pod(s):")
        for a in anomalies:
            print(f"  - [{a.reason}] {a.namespace}/{a.name} (deployment: {a.deployment_name})")
    else:
        print("No anomalies detected.")
