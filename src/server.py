"""
MCP server for forex session analysis.
Exposes the analyze_forex_session tool via SSE transport for web hosting.
"""
import json
from typing import Any
from mcp.server import Server
from mcp.server.sse import SseServerTransport
from mcp.types import Tool, TextContent
from tools.session_analyzer import analyze_forex_session
from starlette.applications import Starlette
from starlette.routing import Route
import uvicorn

# Initialize MCP server
app = Server("forex-session-mcp")

@app.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="analyze_forex_session",
            description=(
                "Analyze forex session volatility and generate trading guidance for a given currency pair. "
                "Provides expected deviation in pips, a confidence score (0-1), market drivers, "
                "historical pattern context, and agent-specific trading recommendations based on "
                "historical pattern matching and current market conditions. "
                "Returns a JSON object with fields: pair (string), session (string), "
                "time_window_minutes (integer), volatility_expectation (Low/Medium/High/None), "
                "expected_deviation_pips (number), confidence (0-1 number), "
                "drivers (array of strings), historical_context (object with "
                "similar_conditions_occurrences and expansion_rate), and agent_guidance (string). "
                "On weekends, returns session='Market Closed' with volatility_expectation='None'. "
                "Supported pairs: EUR/USD, GBP/USD, USD/JPY, USD/CHF, AUD/USD, USD/CAD, NZD/USD, "
                "EUR/GBP, EUR/JPY, GBP/JPY and other major/minor pairs."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "pair": {
                        "type": "string",
                        "description": "Currency pair to analyze. Use slash-separated format e.g. 'EUR/USD', 'GBP/USD', 'USD/JPY', 'GBP/JPY', 'AUD/USD'.",
                        "examples": ["EUR/USD", "GBP/JPY", "AUD/USD"]
                    },
                    "target_session": {
                        "type": "string",
                        "description": "Trading session to analyze. Use 'asian' for Asian session (00:00-09:00 UTC), 'london' for London session (08:00-16:00 UTC), 'ny' for New York session (13:00-21:00 UTC), or 'auto' to automatically detect the current or next upcoming session.",
                        "enum": ["asian", "london", "ny", "auto"],
                        "default": "auto"
                    }
                },
                "required": ["pair"]
            }
        )
    ]

@app.call_tool()
async def call_tool(name: str, arguments: Any) -> list[TextContent]:
    if name != "analyze_forex_session":
        raise ValueError(f"Unknown tool: {name}")

    pair = arguments.get("pair")
    if not pair:
        raise ValueError("Missing required argument: pair")

    target_session = arguments.get("target_session", "auto")

    try:
        result = analyze_forex_session(pair, target_session)
        return [TextContent(type="text", text=json.dumps(result, indent=2))]
    except Exception as e:
        error_response = {
            "success": False,
            "error": str(e),
            "pair": pair,
            "target_session": target_session
        }
        return [TextContent(type="text", text=json.dumps(error_response, indent=2))]


# SSE transport setup
sse = SseServerTransport("/messages")

async def handle_sse(request):
    async with sse.connect_sse(request.scope, request.receive, request._send) as streams:
        await app.run(streams[0], streams[1], app.create_initialization_options())

async def handle_messages(request):
    await sse.handle_post_message(request.scope, request.receive, request._send)

# Starlette web app
web_app = Starlette(
    routes=[
        Route("/sse", endpoint=handle_sse),
        Route("/messages", endpoint=handle_messages, methods=["POST"]),
    ]
)

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(web_app, host="0.0.0.0", port=port)