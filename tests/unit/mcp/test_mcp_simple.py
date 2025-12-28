"""
Simple MCP tests - just verify the tools exist and work.
"""
import pytest
from mcp import types
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "src"))

import nexus.mcp.mcp_stdio_server as mcp_server


class TestMCPBasics:
    """Basic tests for MCP tools."""

    @pytest.mark.asyncio
    async def test_list_tools(self):
        """Test that we can list all tools."""
        tools = await mcp_server.list_tools()

        # Should have 12 tools
        assert len(tools) == 12

        # Check that tools have required fields
        for tool in tools:
            assert tool.name
            assert tool.description
            assert tool.inputSchema

    @pytest.mark.asyncio
    async def test_call_tool_exists(self):
        """Test that call_tool function exists and handles unknown tools."""
        # Try calling an unknown tool - should return error message
        result = await mcp_server.call_tool("unknown_tool", {})

        assert len(result) > 0
        assert isinstance(result[0], types.TextContent)
        # Should contain error message
        assert "not supported" in result[0].text or "Unknown" in result[0].text

    @pytest.mark.asyncio
    async def test_query_tool_names(self):
        """Test that all expected query tools exist."""
        tools = await mcp_server.list_tools()
        tool_names = [tool.name for tool in tools]

        # Check main query tools exist
        assert "query_tree_cover_loss" in tool_names
        assert "query_primary_forest" in tool_names
        assert "query_carbon_data" in tool_names

        # Check analysis tools exist
        assert "analyze_trend" in tool_names
        assert "compare_countries" in tool_names
        assert "rank_countries" in tool_names

    @pytest.mark.asyncio
    async def test_tool_has_climate_gpt(self):
        """Test that tools use ClimateGPT for responses."""
        # This is a simple check that ClimateGPT function exists
        assert hasattr(mcp_server, 'call_climategpt')

        # Check that format functions exist
        assert hasattr(mcp_server, 'format_data_for_climategpt')
        assert hasattr(mcp_server, 'format_number')