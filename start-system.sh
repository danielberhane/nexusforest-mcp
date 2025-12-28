#!/bin/bash
# Copyright 2025 Daniel Berhane Araya
# SPDX-License-Identifier: Apache-2.0
#
# Daily Startup - Just Start MCP Server

set -e

echo "🚀 Starting Nexus MCP Server..."
echo ""

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

# Check if database exists
if ! docker volume inspect gmu_daen_2025_02_a_processed-data >/dev/null 2>&1; then
    echo "❌ Database not found!"
    echo ""
    echo "Run this first to process data:"
    echo "  ./rebuild-data.sh"
    exit 1
fi

# Start MCP Server
echo -e "${BLUE}Starting MCP server...${NC}"
docker compose --profile production up -d
sleep 3

# Check if server is running
if docker ps | grep -q nexus-mcp-server; then
    echo -e "${GREEN}✅ MCP server is running${NC}"
    echo ""
    docker logs nexus-mcp-server --tail 10
else
    echo "❌ MCP server failed to start"
    docker logs nexus-mcp-server
    exit 1
fi

echo ""
echo "=================================================="
echo "🎉 System is ready!"
echo "=================================================="
echo ""
echo "📊 Status:"
docker ps --filter name=nexus --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo ""
echo "💡 Commands:"
echo "   docker logs nexus-mcp-server -f    # View logs"
echo "   ./stop-system.sh                   # Stop server"
echo ""
