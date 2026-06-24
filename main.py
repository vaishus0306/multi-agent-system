import logging
import sys
import threading

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger("main")


def run_agent_b():
    from agents.agent_b import AgentBExecutor

    agent_b = AgentBExecutor()
    agent_b.run()


def run_agent_c():
    from agents.agent_c import AgentCMonitor

    agent_c = AgentCMonitor()
    agent_c.run()


def run_agent_a(patient_id: str = "4421"):
    from agents.agent_a import AgentAPlanner

    agent_a = AgentAPlanner()
    agent_a.run(patient_id)


def main():
    import time

    mode = sys.argv[1] if len(sys.argv) > 1 else "all"

    if mode == "a":
        run_agent_a()
    elif mode == "b":
        run_agent_b()
    elif mode == "c":
        run_agent_c()
    elif mode == "all":
        logger.info("Starting all agents...")

        thread_b = threading.Thread(target=run_agent_b, daemon=True)
        thread_c = threading.Thread(target=run_agent_c, daemon=True)

        thread_b.start()
        thread_c.start()

        time.sleep(2)

        run_agent_a()
    else:
        print("Usage: python main.py [a|b|c|all]")
        print("  a   - Run Agent A (Planner) only")
        print("  b   - Run Agent B (Executor) only")
        print("  c   - Run Agent C (Monitor) only")
        print("  all - Run all agents together (default)")


if __name__ == "__main__":
    main()
