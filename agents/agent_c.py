import logging
import threading
import time

from config import (
    AGENT_A_ID,
    AGENT_C_ID,
    QUEUE_STATUS,
    ROUTING_MONITOR_SUMMARY,
)
from messaging.message import Message
from messaging.queue_manager import QueueManager

logger = logging.getLogger(__name__)


class AgentCMonitor:
    def __init__(self):
        self.id = AGENT_C_ID
        self.queue_manager = QueueManager()
        self.results: dict[str, dict] = {}

    def on_message(self, message: Message):
        corr_id = message.correlation_id
        logger.info(
            "\n  >>> Agent C received: %s from %s (corr_id=%s)",
            message.message_type,
            message.sender_id,
            corr_id[:12],
        )

        if message.message_type == "task.status_update":
            payload = message.payload
            task_id = payload.get("task_id", "unknown")
            pct = payload.get("progress_pct", 0)
            step = payload.get("current_step", "unknown")

            if corr_id not in self.results:
                self.results[corr_id] = {
                    "tasks": [],
                    "start_time": time.time(),
                }

            logger.info(
                "  >>> Progress: %s at %d%% (step: %s)",
                task_id,
                pct,
                step,
            )

        elif message.message_type == "task.completed":
            payload = message.payload
            task_id = payload.get("task_id", "unknown")
            result = payload.get("result_summary", {})
            duration = payload.get("execution_time_ms", 0)

            if corr_id not in self.results:
                self.results[corr_id] = {"tasks": [], "start_time": time.time()}

            self.results[corr_id]["tasks"].append(
                {
                    "task_id": task_id,
                    "from": message.sender_id,
                    "status": "completed",
                    "duration_ms": duration,
                    "result": result,
                }
            )

            logger.info(
                "  >>> %s completed. Extracted: %d fields, %d codes, %d anomalies",
                task_id,
                result.get("fields_extracted", 0),
                result.get("codes_validated", 0),
                result.get("anomalies_flagged", 0),
            )

            self.publish_summary(corr_id)

        elif message.message_type == "task.failed":
            payload = message.payload
            task_id = payload.get("task_id", "unknown")
            error = payload.get("error", "Unknown error")

            logger.error("  >>> %s FAILED: %s", task_id, error)

            if corr_id not in self.results:
                self.results[corr_id] = {"tasks": [], "start_time": time.time()}

            self.results[corr_id]["tasks"].append(
                {
                    "task_id": task_id,
                    "from": message.sender_id,
                    "status": "failed",
                    "error": error,
                }
            )

            self.publish_summary(corr_id)

    def publish_summary(self, corr_id: str):
        data = self.results.get(corr_id, {"tasks": []})
        total_duration = int((time.time() - data.get("start_time", time.time())) * 1000)

        has_failures = any(
            t.get("status") == "failed" for t in data.get("tasks", [])
        )
        has_warnings = any(
            t.get("result", {}).get("anomalies_flagged", 0) > 0
            for t in data.get("tasks", [])
        )

        if has_failures:
            overall_status = "failed"
        elif has_warnings:
            overall_status = "completed_with_warnings"
        else:
            overall_status = "completed"

        summary_msg = Message(
            sender_id=self.id,
            receiver_id=AGENT_A_ID,
            message_type="monitor.summary",
            correlation_id=corr_id,
            priority="high",
            ttl_seconds=3600,
            payload={
                "workflow_id": corr_id,
                "overall_status": overall_status,
                "tasks": data["tasks"],
                "total_duration_ms": total_duration,
                "report_url": f"s3://reports/{corr_id}.md",
            },
        )
        self.queue_manager.publish_message(
            summary_msg, routing_key=ROUTING_MONITOR_SUMMARY
        )
        logger.info(
            "\n  >>> Agent C: Published monitor.summary - Status: %s, Duration: %sms",
            overall_status,
            total_duration,
        )

    def run(self):
        self.queue_manager.connect()
        self.queue_manager.subscribe(QUEUE_STATUS, self.on_message)
        logger.info("Agent C: Monitoring for status updates...")
        self.queue_manager.start_consuming()
