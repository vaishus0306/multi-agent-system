import io
import logging

from fastapi import FastAPI, Response
from fastapi.responses import HTMLResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

app = FastAPI(title="Multi-Agent System")


class HeadToGetMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.method == "HEAD":
            scope = dict(request.scope)
            scope["method"] = "GET"
            request = Request(scope, receive=request.receive)
            response = await call_next(request)
            response.body = b""
            return response
        return await call_next(request)


app.add_middleware(HeadToGetMiddleware)

HTML_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
    <title>Multi-Agent System - Simulation</title>
    <style>
        body {{ font-family: 'Courier New', monospace; max-width: 900px; margin: 40px auto; padding: 20px; background: #1a1a2e; color: #e0e0e0; }}
        h1 {{ color: #00d4ff; border-bottom: 2px solid #00d4ff; padding-bottom: 10px; }}
        h2 {{ color: #00d4ff; }}
        pre {{ background: #16213e; padding: 15px; border-radius: 8px; border: 1px solid #0f3460; overflow-x: auto; font-size: 12px; line-height: 1.5; }}
        .btn {{ background: #00d4ff; color: #1a1a2e; border: none; padding: 12px 30px; font-size: 16px; font-weight: bold; border-radius: 6px; cursor: pointer; text-decoration: none; display: inline-block; margin: 10px 0; }}
        .btn:hover {{ background: #00b4d4; }}
        .info {{ background: #16213e; padding: 15px; border-radius: 8px; margin: 20px 0; }}
    </style>
</head>
<body>
    <h1>Multi-Agent Communication System</h1>
    <div class="info">
        <strong>Architecture:</strong> Agent A (Planner) &rarr; RabbitMQ &rarr; Agent B (Executor) &rarr; Agent C (Monitor)
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


@app.get("/health")
def health():
    return Response(status_code=200)


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
