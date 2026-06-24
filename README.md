# Multi-Agent Communication System

A working prototype of 3 AI agents (Planner, Executor, Monitor) communicating via a message queue (RabbitMQ).

## Architecture

```
Agent A (Planner)  ──task.assign──►  RabbitMQ  ──►  Agent B (Executor)
                     ◄──task.ack──                    │
                     ◄──monitor.summary──             ├──task.status_update──► Agent C (Monitor)
                                                      └──task.completed─────► Agent C (Monitor)
```

## Quick Start (Simulation Mode)

No RabbitMQ required — runs entirely in-memory:

```bash
python test_simulation.py
```

## Running with Docker (Full Mode)

```bash
docker compose up --build
```

This starts RabbitMQ + all 3 agents. Check the RabbitMQ management UI at http://localhost:15672 (guest/guest).

## Running Manually

1. Start RabbitMQ (via Docker: `docker run -d -p 5672:5672 rabbitmq:3`)
2. In separate terminals:

```bash
python main.py b   # Agent B (Executor)
python main.py c   # Agent C (Monitor)
python main.py a   # Agent A (Planner)
```

## Message Flow

| Step | From | To | Type | Purpose |
|------|------|----|------|---------|
| 1 | Agent A | Agent B | `task.assign` | Assigns a medical record processing task |
| 2 | Agent B | Agent A | `task.ack` | Acknowledges receipt |
| 3 | Agent B | Agent C | `task.status_update` | Reports progress (33%, 66%, 100%) |
| 4 | Agent B | Agent C | `task.completed` | Reports completion with results |
| 5 | Agent C | Agent A | `monitor.summary` | Final workflow summary |

## Project Structure

```
├── agents/
│   ├── agent_a.py          # Planner - assigns tasks
│   ├── agent_b.py          # Executor - processes tasks
│   └── agent_c.py          # Monitor - tracks progress & reports
├── messaging/
│   ├── message.py           # JSON message schema & validation
│   └── queue_manager.py     # RabbitMQ wrapper (publish/subscribe)
├── config.py                # Shared configuration
├── main.py                  # Entry point (runs all agents)
├── test_simulation.py       # In-memory simulation (no deps)
├── Dockerfile               # Container image
├── docker-compose.yml       # Multi-service orchestration
└── requirements.txt         # Python dependencies
```
