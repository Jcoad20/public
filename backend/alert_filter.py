# ------------------------------------------------------------------------------
# alert_filter.py - 告警去重与冷却模块
# ------------------------------------------------------------------------------
# 功能说明:
#   1. 实现告警分组聚合：同一 Deployment 在时间窗口内的多个 Pod 异常合并为一条
#   2. 去重逻辑：相同异常在冷却期内不重复发送
#   3. 冷却机制：可配置的冷却时间窗口（默认5分钟）
#   4. 维护告警历史记录，支持查询最近告警状态
# ------------------------------------------------------------------------------

import time
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from collections import defaultdict
from dataclasses import dataclass, field

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 配置参数
COOLDOWN_SECONDS = int(__import__('os').environ.get("ALERT_COOLDOWN_SECONDS", "300"))  # 默认5分钟
AGGREGATION_WINDOW_SECONDS = int(__import__('os').environ.get("AGGREGATION_WINDOW_SECONDS", "300"))  # 聚合时间窗口


@dataclass
class AlertRecord:
    """单条告警记录"""
    key: str  # 唯一标识: namespace/deployment/reason
    namespace: str
    deployment_name: str
    pod_name: str
    reason: str
    first_seen: datetime
    last_seen: datetime
    count: int = 1
    last_alert_sent: Optional[datetime] = None
    resolved: bool = False


class AlertFilter:
    """
    告警过滤器
    
    负责:
    - 告警去重（相同异常在冷却期内不重复发送）
    - 分组聚合（同一 Deployment 的异常合并）
    - 冷却管理
    """
    
    def __init__(self, cooldown_seconds: int = COOLDOWN_SECONDS):
        self.cooldown_seconds = cooldown_seconds
        # 存储活跃告警: key -> AlertRecord
        self.active_alerts: Dict[str, AlertRecord] = {}
        # 已解决的告警（保留一段时间用于统计）
        self.resolved_alerts: Dict[str, AlertRecord] = {}
    
    def _generate_key(
        self,
        namespace: str,
        deployment_name: str,
        reason: str
    ) -> str:
        """生成告警唯一标识"""
        return f"{namespace}/{deployment_name}/{reason}"
    
    def process_anomalies(self, anomalies: List[Dict]) -> Dict[str, List[Dict]]:
        """
        处理异常列表，返回聚合后的告警数据
        
        Args:
            anomalies: 异常 Pod 字典列表（来自 monitor.py）
        
        Returns:
            Dict[str, List[Dict]]: 按 Deployment 分组的待发送告警
                格式: {deployment_name: [anomaly_dict, ...]}
        """
        now = datetime.now()
        alerts_to_send = defaultdict(list)
        
        # 更新或创建告警记录
        for anomaly in anomalies:
            key = self._generate_key(
                anomaly.get("namespace", ""),
                anomaly.get("deployment_name", "unknown"),
                anomaly.get("reason", "unknown")
            )
            
            if key in self.active_alerts:
                # 更新已有记录
                record = self.active_alerts[key]
                record.last_seen = now
                record.count += 1
                # 如果 Pod 名字不同，更新为最新的
                record.pod_name = anomaly.get("name", record.pod_name)
            else:
                # 创建新记录
                self.active_alerts[key] = AlertRecord(
                    key=key,
                    namespace=anomaly.get("namespace", ""),
                    deployment_name=anomaly.get("deployment_name", "unknown"),
                    pod_name=anomaly.get("name", ""),
                    reason=anomaly.get("reason", "unknown"),
                    first_seen=now,
                    last_seen=now
                )
        
        # 检查哪些告警需要发送（超过冷却期且未在当前批次发送过）
        keys_to_remove = []
        for key, record in self.active_alerts.items():
            should_alert = False
            
            if record.last_alert_sent is None:
                # 首次发现，立即发送
                should_alert = True
            else:
                # 检查是否已过冷却期
                elapsed = (now - record.last_alert_sent).total_seconds()
                if elapsed >= self.cooldown_seconds:
                    should_alert = True
            
            if should_alert:
                # 添加到待发送列表
                alerts_to_send[record.deployment_name].append({
                    "namespace": record.namespace,
                    "name": record.pod_name,
                    "reason": record.reason,
                    "message": "",  # 可扩展
                    "restart_count": 0,  # 可扩展
                    "count_since_last_alert": record.count,
                    "first_seen": record.first_seen.isoformat(),
                    "last_seen": record.last_seen.isoformat()
                })
                
                # 更新最后发送时间
                record.last_alert_sent = now
                record.count = 0  # 重置计数器
        
        logger.info(
            f"Alert filter processed {len(anomalies)} anomalies, "
            f"generated {sum(len(v) for v in alerts_to_send.values())} alert(s) to send."
        )
        
        return dict(alerts_to_send)
    
    def mark_resolved(self, keys: List[str]):
        """
        标记告警为已解决
        
        Args:
            keys: 要标记的告警 key 列表
        """
        for key in keys:
            if key in self.active_alerts:
                record = self.active_alerts.pop(key)
                record.resolved = True
                self.resolved_alerts[key] = record
                logger.info(f"Alert marked as resolved: {key}")
    
    def cleanup_old_records(self, max_age_hours: int = 24):
        """清理过期的已解决记录"""
        cutoff = datetime.now() - timedelta(hours=max_age_hours)
        
        expired_keys = [
            key for key, record in self.resolved_alerts.items()
            if record.last_seen < cutoff
        ]
        
        for key in expired_keys:
            del self.resolved_alerts[key]
        
        if expired_keys:
            logger.info(f"Cleaned up {len(expired_keys)} expired resolved alerts.")
    
    def get_active_alert_summary(self) -> Dict:
        """获取当前活跃告警摘要"""
        return {
            "total_active": len(self.active_alerts),
            "alerts": [
                {
                    "key": r.key,
                    "namespace": r.namespace,
                    "deployment": r.deployment_name,
                    "reason": r.reason,
                    "count": r.count,
                    "first_seen": r.first_seen.isoformat(),
                    "last_seen": r.last_seen.isoformat()
                }
                for r in self.active_alerts.values()
            ]
        }
    
    def reset(self):
        """重置所有状态（主要用于测试）"""
        self.active_alerts.clear()
        self.resolved_alerts.clear()
        logger.info("Alert filter state has been reset.")


# 全局过滤器实例
alert_filter = AlertFilter()


def get_alert_filter() -> AlertFilter:
    """获取全局告警过滤器实例"""
    return alert_filter


if __name__ == "__main__":
    # 测试去重和聚合逻辑
    print("Testing Alert Filter...")
    
    filter_instance = AlertFilter(cooldown_seconds=60)  # 测试用1分钟冷却
    
    # 模拟第一批异常
    anomalies_batch_1 = [
        {"namespace": "default", "name": "pod-1", "deployment_name": "web-app", "reason": "CrashLoopBackOff"},
        {"namespace": "default", "name": "pod-2", "deployment_name": "web-app", "reason": "CrashLoopBackOff"},
        {"namespace": "default", "name": "pod-3", "deployment_name": "api-server", "reason": "OOMKilled"},
    ]
    
    result1 = filter_instance.process_anomalies(anomalies_batch_1)
    print("\nBatch 1 - Alerts to send:")
    for dep, alerts in result1.items():
        print(f"  Deployment '{dep}': {len(alerts)} alert(s)")
    
    # 模拟第二批（相同的异常，应该在冷却期内不重复发送）
    anomalies_batch_2 = [
        {"namespace": "default", "name": "pod-4", "deployment_name": "web-app", "reason": "CrashLoopBackOff"},
    ]
    
    result2 = filter_instance.process_anomalies(anomalies_batch_2)
    print("\nBatch 2 (within cooldown) - Alerts to send:")
    print(f"  Total: {sum(len(v) for v in result2.values())} alert(s)")
    
    print("\n✅ Alert filter test completed.")
