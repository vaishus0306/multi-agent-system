"""
FastAPI web app - runs simulation and shows output in browser.
For deployment on Render.com / Railway.app
"""

import io
import logging
from contextlib import redirect_stdout
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI(title="Multi-Agent System")

HTML_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
    <title>Multi-Agent System - Simulation</title>
    <style>
        body {{ font-family: 'Courier New', monospace; max-width: 900px; margin: 40px auto; padding: 20px; background: #1a1a2e; color: #e0e0e0; }}
        h1 {{ color: #00d4ff; border-bottom: 2px solid #00d4ff; padding-bottom: 10px; }}
        h2 {{ color: #00d4ff; }}
        pre {{ background: #16213e; padding: 15px; border-radius: 8px; border: 1px solid #0f3460; overflow-x: auto; font-size: 12px; line-height: 1.5; }}
        .btn {{
            background: #00d4ff; color: #1a1a2e; border: none; padding: 12px 30px;
            font-size: 16px; font-weight: bold; border-radius: 6px; cursor: pointer;
            text-decoration: none; display: inline-block; margin: 10px 0;
        }}
        .btn:hover {{ background: #00b4d4; }}
        .status {{ color: #00ff88; }}
        .info {{ background: #16213e; padding: 15px; border-radius: 8px; margin: 20px 0; }}
    </style>
</head>
<body>
    <h1>Multi-Agent Communication System</h1>
    <div class="info">
        <strong>Architecture:</strong> Agent A (Planner) → RabbitMQ → Agent B (Executor) → Agent C (Monitor)
    </div>
    <a href="/simulate" class="btn">Run Simulation</a>
    <h2>Output:</h2>
    <pre>{output}</pre>
</body>
</html>"""


@app.get("/")
def root():
    return HTMLResponse(
        HTML_TEMPLATE.format(
            output='Click "Run Simulation" to see the agents in action.'
        )
    )


@app.get("/simulate")
def simulate():
    buf = io.StringIO()
    logger = logging.getLogger("simulation")
    logger.setLevel(logging.INFO)

    class StreamHandler(logging.Handler):
        def emit(self, record):
            buf.write(self.format(record) + "\n")

    handler = StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)

    from test_simulation import run_simulation
    run_simulation()

    logger.removeHandler(handler)
    output = buf.getvalue()

    return HTMLResponse(HTML_TEMPLATE.format(output=output))
