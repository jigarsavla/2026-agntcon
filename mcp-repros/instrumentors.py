"""Load one open-source MCP instrumentor by short name; report what happened."""
import sys
def load(name):
    try:
        if name == "openllmetry":
            from opentelemetry.instrumentation.mcp import McpInstrumentor
            McpInstrumentor().instrument()
        elif name == "openinference":
            from openinference.instrumentation.mcp import MCPInstrumentor
            MCPInstrumentor().instrument()
        elif name == "openlit":
            import openlit
            openlit.init(disable_metrics=True)
        return "loaded"
    except Exception as e:
        print(f"[{name}] failed to load: {type(e).__name__}: {e}", file=sys.stderr)
        return f"failed: {type(e).__name__}"
