import logging
import threading
import time

from config import (
    AGENT_A_ID,
    AGENT_B_ID,
    AGENT_C_ID,
    QUEUE_MONITOR,
    ROUTING_TASK_ASSIGN,
    ROUTING_MONITOR_SUMMARY,
)
from messaging.message import Message
from messaging.queue_manager import QueueManager

logger = logging.getLogger(__name__)


class AgentAPlanner:
    def __init__(self):
        self.id = AGENT_A_ID
        self.queue_manager = QueueManager()
        self.ack_received = threading.Event()
        self.summary_received = threading.Event()

    def on_message(self, message: Message):
        logger.info(
            "\n  >>> Agent A received: %s from %s (corr_id=%s)",
            message.message_type,
            message.sender_id,
            message.correlation_id[:12],
        )

        if message.message_type == "task.ack":
            progress = message.payload.get("status", "unknown")
            est = message.payload.get("estimated_completion", "N/A")
            logger.info(
                "  >>> Task ACK'd by Agent B. Status: %s, ETA: %s",
                progress,
                est,
            )
            self.ack_received.set()

        elif message.message_type == "monitor.summary":
            status = message.payload.get("overall_status", "unknown")
            duration = message.payload.get("total_duration_ms", "N/A")
            logger.info(
                "  >>> Workflow complete! Status: %s, Duration: %sms",
                status,
                duration,
            )
            logger.info("  >>> Full summary: %s", message.payload)
            self.summary_received.set()

    def assign_task(self, patient_id: str):
        logger.info("\n=== Agent A: Planning task for patient %s ===", patient_id)
        task_msg = Message(
            sender_id=self.id,
            receiver_id=AGENT_B_ID,
            message_type="task.assign",
            correlation_id=f"wf-{patient_id}",
            priority="high",
            ttl_seconds=600,
            payload={
                "task_id": f"task-{patient_id}",
                "task_type": "process_medical_record",
                "parameters": {
                    "patient_id": patient_id,
                    "actions": ["extract_fields", "validate_codes", "flag_anomalies"],
                },
            },
        )
        self.queue_manager.publish_message(
            task_msg, routing_key=ROUTING_TASK_ASSIGN
        )
        logger.info("Agent A: Published task.assign (id=%s)", task_msg.message_id[:8])
        return task_msg.correlation_id

    def run(self, patient_id: str = "4421"):
        self.queue_manager.connect()
        self.queue_manager.subscribe(QUEUE_MONITOR, self.on_message)

        consumer_thread = threading.Thread(
            target=self.queue_manager.start_consuming, daemon=True
        )
        consumer_thread.start()

        time.sleep(0.5)
        corr_id = self.assign_task(patient_id)

        logger.info("\nAgent A: Waiting for ACK from Agent B...")
        if not self.ack_received.wait(timeout=10):
            logger.warning("Agent A: TIMEOUT - No ACK received from Agent B")

        logger.info("\nAgent A: Waiting for summary from Agent C...")
        if not self.summary_received.wait(timeout=30):
            logger.warning("Agent A: TIMEOUT - No summary received from Agent C")

        logger.info("\n=== Agent A: Workflow complete ===")
        self.queue_manager.close()
