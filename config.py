import os

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")

EXCHANGE_NAME = "agent_exchange"
EXCHANGE_TYPE = "topic"

QUEUE_TASKS = "tasks"
QUEUE_STATUS = "status"
QUEUE_MONITOR = "monitor"

ROUTING_TASK_ASSIGN = "task.assign"
ROUTING_TASK_ACK = "task.ack"
ROUTING_TASK_STATUS = "task.status_update"
ROUTING_TASK_COMPLETED = "task.completed"
ROUTING_TASK_FAILED = "task.failed"
ROUTING_MONITOR_HEARTBEAT = "monitor.heartbeat"
ROUTING_MONITOR_SUMMARY = "monitor.summary"

AGENT_A_ID = "agent_a"
AGENT_B_ID = "agent_b"
AGENT_C_ID = "agent_c"
