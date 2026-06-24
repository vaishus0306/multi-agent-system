"""
Simulation mode: tests the full agent workflow without RabbitMQ.
Uses in-memory queues to demonstrate the message flow.
"""

import logging
import threading
import time
import random
from queue import Queue as ThreadQueue
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger("simulation")

from messaging.message import Message


class SimulatedQueueManager:
    """Drop-in replacement for QueueManager that uses in-memory queues."""

    def __init__(self):
        self.queues: dict[str, ThreadQueue] = {
            "tasks": ThreadQueue(),
            "status": ThreadQueue(),
            "monitor": ThreadQueue(),
        }
        self.agent_callbacks: dict[str, object] = {}

    def connect(self):
        logger.info("[Sim] Connected to in-memory broker.")

    def close(self):
        logger.info("[Sim] Connection closed.")

    def publish_message(
        self, message: Message, routing_key: Optional[str] = None
    ):
        key = routing_key or message.message_type

        route_map = {
            "task.assign": "tasks",
            "task.ack": "monitor",
            "task.status_update": "status",
            "task.completed": "status",
            "task.failed": "status",
            "monitor.heartbeat": "monitor",
            "monitor.summary": "monitor",
        }

        target_queue = route_map.get(key)
        if target_queue and target_queue in self.queues:
            self.queues[target_queue].put(message)
            logger.info(
                "[Sim] Published: %s (%s) -> %s queue",
                message.message_type,
                message.message_id[:8],
                target_queue,
            )

    def subscribe(self, queue_name: str, callback, prefetch_count: int = 1):
        self.agent_callbacks[queue_name] = callback
        logger.info("[Sim] Subscribed to queue: %s", queue_name)

    def start_consuming(self):
        pass

    def deliver_one(self, queue_name: str) -> bool:
        if queue_name not in self.queues:
            return False
        try:
            message = self.queues[queue_name].get_nowait()
            cb = self.agent_callbacks.get(queue_name)
            if cb:
                cb(message)
            return True
        except Exception:
            return False

    def deliver_all(self):
        delivered = True
        while delivered:
            delivered = False
            for qname in self.queues:
                if self.deliver_one(qname):
                    delivered = True


class AgentAPlannerSim:
    def __init__(self, qm: SimulatedQueueManager):
        self.qm = qm
        self.id = "agent_a"
        self.ack_received = threading.Event()
        self.summary_received = threading.Event()

    def on_message(self, message: Message):
        logger.info(
            "\n  >>> [Agent A] Received: %s from %s (corr_id=%s)",
            message.message_type,
            message.sender_id,
            message.correlation_id[:12],
        )

        if message.message_type == "task.ack":
            logger.info(
                "  >>> [Agent A] Task ACK'd by Agent B. Status: %s",
                message.payload.get("status"),
            )
            self.ack_received.set()

        elif message.message_type == "monitor.summary":
            logger.info(
                "  >>> [Agent A] Workflow complete! Status: %s, Duration: %sms",
                message.payload.get("overall_status"),
                message.payload.get("total_duration_ms"),
            )
            self.summary_received.set()

    def assign_task(self, patient_id: str):
        logger.info("\n=== [Agent A] Planning task for patient %s ===", patient_id)
        task_msg = Message(
            sender_id=self.id,
            receiver_id="agent_b",
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
        self.qm.publish_message(task_msg)
        logger.info("[Agent A] Published task.assign (id=%s)", task_msg.message_id[:8])

    def run(self, patient_id: str = "4421"):
        self.qm.connect()
        self.qm.subscribe("monitor", self.on_message)
        self.assign_task(patient_id)

        self.qm.deliver_one("tasks")

        logger.info("\n[Agent A] Waiting for ACK...")
        self.qm.deliver_all()
        if self.ack_received.wait(timeout=5):
            logger.info("[Agent A] ACK received!")
        else:
            logger.warning("[Agent A] TIMEOUT on ACK")

        self.qm.deliver_all()

        logger.info("\n[Agent A] Waiting for summary...")
        if self.summary_received.wait(timeout=10):
            logger.info("[Agent A] Summary received!")
        else:
            logger.warning("[Agent A] TIMEOUT on summary")

        logger.info("\n=== [Agent A] Workflow complete ===")


class AgentBExecutorSim:
    def __init__(self, qm: SimulatedQueueManager):
        self.qm = qm
        self.id = "agent_b"

    def on_message(self, message: Message):
        if message.message_type != "task.assign":
            return

        task_id = message.payload.get("task_id", "unknown")
        patient_id = message.payload.get("parameters", {}).get("patient_id", "unknown")
        corr_id = message.correlation_id

        logger.info(
            "\n  >>> [Agent B] Processing %s for patient %s <<<",
            task_id,
            patient_id,
        )

        ack_msg = Message(
            sender_id=self.id,
            receiver_id="agent_a",
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
        self.qm.publish_message(ack_msg)
        logger.info("  >>> [Agent B] Sent task.ack")

        steps = ["extract_fields", "validate_codes", "flag_anomalies"]
        for i, step in enumerate(steps):
            logger.info(
                "  >>> [Agent B] Executing step %d/%d: %s",
                i + 1,
                len(steps),
                step,
            )
            time.sleep(0.3)

            progress_pct = int(((i + 1) / len(steps)) * 100)
            status_msg = Message(
                sender_id=self.id,
                receiver_id="agent_c",
                message_type="task.status_update",
                correlation_id=corr_id,
                payload={
                    "task_id": task_id,
                    "progress_pct": progress_pct,
                    "current_step": step,
                    "logs": [f"Completed: {step}"],
                    "errors_warnings": [],
                },
            )
            self.qm.publish_message(status_msg)
            logger.info("  >>> [Agent B] Status: %d%%", progress_pct)

        result = {
            "fields_extracted": 14,
            "codes_validated": 8,
            "anomalies_flagged": 1,
            "execution_time_ms": 720000,
        }

        completed_msg = Message(
            sender_id=self.id,
            receiver_id="agent_c",
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
        self.qm.publish_message(completed_msg)
        logger.info("  >>> [Agent B] Sent task.completed")


class AgentCMonitorSim:
    def __init__(self, qm: SimulatedQueueManager):
        self.qm = qm
        self.id = "agent_c"
        self.results: dict[str, dict] = {}

    def on_message(self, message: Message):
        corr_id = message.correlation_id
        logger.info(
            "\n  >>> [Agent C] Received: %s from %s",
            message.message_type,
            message.sender_id,
        )

        if message.message_type == "task.status_update":
            pct = message.payload.get("progress_pct", 0)
            logger.info("  >>> [Agent C] Progress: %d%%", pct)

        elif message.message_type == "task.completed":
            result = message.payload.get("result_summary", {})
            logger.info(
                "  >>> [Agent C] Task complete! Extracted: %d fields, %d codes, %d anomalies",
                result.get("fields_extracted", 0),
                result.get("codes_validated", 0),
                result.get("anomalies_flagged", 0),
            )

            summary_msg = Message(
                sender_id=self.id,
                receiver_id="agent_a",
                message_type="monitor.summary",
                correlation_id=corr_id,
                priority="high",
                ttl_seconds=3600,
                payload={
                    "workflow_id": corr_id,
                    "overall_status": "completed_with_warnings",
                    "tasks": [
                        {
                            "task_id": message.payload.get("task_id"),
                            "status": "completed",
                            "duration_ms": message.payload.get("execution_time_ms"),
                        }
                    ],
                    "total_duration_ms": result.get("execution_time_ms", 0),
                    "report_url": f"s3://reports/{corr_id}.md",
                },
            )
            self.qm.publish_message(summary_msg)
            logger.info("  >>> [Agent C] Published monitor.summary")

        elif message.message_type == "task.failed":
            logger.error(
                "  >>> [Agent C] Task FAILED: %s",
                message.payload.get("error"),
            )


def run_simulation():
    logger.info("=" * 60)
    logger.info("MULTI-AGENT SYSTEM SIMULATION")
    logger.info("=" * 60)

    qm = SimulatedQueueManager()

    agent_c = AgentCMonitorSim(qm)
    agent_b = AgentBExecutorSim(qm)
    agent_a = AgentAPlannerSim(qm)

    qm.subscribe("status", agent_c.on_message)
    qm.subscribe("tasks", agent_b.on_message)
    qm.subscribe("monitor", agent_a.on_message)

    agent_a.run("4421")

    logger.info("\n" + "=" * 60)
    logger.info("SIMULATION COMPLETE")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_simulation()
