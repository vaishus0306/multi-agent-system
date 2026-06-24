import logging
import threading
import time
import random

from config import (
    AGENT_A_ID,
    AGENT_B_ID,
    AGENT_C_ID,
    QUEUE_TASKS,
    ROUTING_TASK_ACK,
    ROUTING_TASK_STATUS,
    ROUTING_TASK_COMPLETED,
    ROUTING_TASK_FAILED,
)
from messaging.message import Message
from messaging.queue_manager import QueueManager

logger = logging.getLogger(__name__)


class AgentBExecutor:
    def __init__(self):
        self.id = AGENT_B_ID
        self.queue_manager = QueueManager()

    def process_task(self, task_msg: Message):
        payload = task_msg.payload
        task_id = payload.get("task_id", "unknown")
        params = payload.get("parameters", {})
        patient_id = params.get("patient_id", "unknown")
        corr_id = task_msg.correlation_id

        logger.info(
            "\n  >>> Agent B: Processing task %s for patient %s <<<",
            task_id,
            patient_id,
        )

        time.sleep(1)

        ack_msg = Message(
            sender_id=self.id,
            receiver_id=AGENT_A_ID,
            message_type="task.ack",
            correlation_id=corr_id,
            priority="low",
            ttl_seconds=60,
            payload={
                "task_id": task_id,
                "status": "received",
                "estimated_completion": "2026-06-23T10:45:00Z",
            },
        )
        self.queue_manager.publish_message(ack_msg, routing_key=ROUTING_TASK_ACK)
        logger.info("  >>> Agent B: Sent task.ack for %s", task_id)

        steps = params.get("actions", ["extract_fields", "validate_codes", "flag_anomalies"])
        for i, step in enumerate(steps):
            logger.info("  >>> Agent B: Executing step %d/%d: %s", i + 1, len(steps), step)
            time.sleep(random.uniform(1.0, 2.5))

            progress_pct = int(((i + 1) / len(steps)) * 100)
            status_msg = Message(
                sender_id=self.id,
                receiver_id=AGENT_C_ID,
                message_type="task.status_update",
                correlation_id=corr_id,
                priority="medium",
                ttl_seconds=120,
                payload={
                    "task_id": task_id,
                    "progress_pct": progress_pct,
                    "current_step": step,
                    "logs": [f"Completed step: {step}"],
                    "errors_warnings": [],
                },
            )
            self.queue_manager.publish_message(
                status_msg, routing_key=ROUTING_TASK_STATUS
            )
            logger.info(
                "  >>> Agent B: Sent status update %d%% for %s",
                progress_pct,
                task_id,
            )

        logger.info("  >>> Agent B: Task %s complete!", task_id)

        result = {
            "fields_extracted": random.randint(10, 15),
            "codes_validated": random.randint(5, 10),
            "anomalies_flagged": random.randint(0, 2),
            "execution_time_ms": random.randint(500000, 800000),
        }

        completed_msg = Message(
            sender_id=self.id,
            receiver_id=AGENT_C_ID,
            message_type="task.completed",
            correlation_id=corr_id,
            priority="high",
            ttl_seconds=300,
            payload={
                "task_id": task_id,
                "result_summary": result,
                "artifacts": [f"s3://results/{patient_id}-summary.json"],
                "execution_time_ms": result["execution_time_ms"],
            },
        )
        self.queue_manager.publish_message(
            completed_msg, routing_key=ROUTING_TASK_COMPLETED
        )
        logger.info("  >>> Agent B: Sent task.completed for %s", task_id)

    def on_message(self, message: Message):
        if message.message_type == "task.assign":
            thread = threading.Thread(target=self.process_task, args=(message,))
            thread.start()

    def run(self):
        self.queue_manager.connect()
        self.queue_manager.subscribe(QUEUE_TASKS, self.on_message)
        logger.info("Agent B: Waiting for tasks...")
        self.queue_manager.start_consuming()
