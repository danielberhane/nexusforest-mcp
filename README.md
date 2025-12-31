# NexusForest MCP

> Bridge the gap between AI models and climate data - 24 years of Global Forest Watch insights covering 165+ countries, accessible through natural conversation.

NexusForest Model Context Protocol (MCP) server provides structured access to Global Forest Watch (GFW) deforestation and carbon emission data, enabling AI models to deliver quantified, source-attributed climate intelligence. Developed in partnership with Erasmus.AI to enhance ClimateGPT's capabilities with structured data access.
## Features

- **Comprehensive Coverage**: 165+ countries, 24 years (2001-2024)
- **Optimized Queries**: Indexed SQLite database for efficient data retrieval
- **12 Specialized Tools**: From simple lookups to complex trend analysis
- **Smart Architecture**: Three-table design eliminates 60% data sparsity
- **Security-First**: Parameterized SQL, input validation, read-only access
- **AI Integration**: ClimateGPT API for enhanced climate insights

## Quick Start

### Prerequisites

- Docker Desktop 4.0+
- Claude Desktop (latest)
- Excel data file: `global_05212025.xlsx` (Download from [Global Forest Watch](https://www.globalforestwatch.org/dashboards/global/))

### Installation

```bash
# 1. Clone repository
git clone https://github.com/danielberhane/nexusforest-mcp.git
cd nexusforest-mcp

# 2. Add Excel file to data/raw/
cp /path/to/global_05212025.xlsx data/raw/

# 3. Process data and start server
./rebuild-data.sh
./start-system.sh
```

### Configure MCP Client

**For Claude Desktop (macOS):**
```bash
cat > ~/Library/Application\ Support/Claude/claude_desktop_config.json << 'EOF'
{
  "mcpServers": {
    "nexusforest": {
      "command": "/Applications/Docker.app/Contents/Resources/bin/docker",
      "args": ["exec", "-i", "nexus-mcp-server", "python", "-m", "nexus.mcp.mcp_stdio_server"]
    }
  }
}
EOF
```

**For other MCP clients:** See [Installation Guide](#installation-guide) below

**Windows:** Use `%APPDATA%\Claude\claude_desktop_config.json`
**Linux:** Use `~/.config/Claude/claude_desktop_config.json`

### Verify

Restart your MCP client and ask:
> "What tools do you have available?"

You should see 12 nexusforest tools listed.

## Example Queries

```
"What was Brazil's forest loss in 2023?"
"Compare deforestation in Indonesia vs Malaysia over the last decade"
"Show me the top 10 countries by carbon emissions from forests"
"Calculate the trend in global primary forest loss"
```

## Installation Guide

### macOS/Linux

```bash
# Clone and setup
git clone https://github.com/danielberhane/nexusforest-mcp.git
cd nexusforest-mcp
cp /path/to/global_05212025.xlsx data/raw/

# Build and run
chmod +x *.sh
./rebuild-data.sh  # Builds Docker images, processes data (~2 min)
./start-system.sh  # Starts MCP server

# Configure Claude (see Quick Start above)

# Daily usage
./start-system.sh  # Morning
./stop-system.sh   # Evening
```

### Windows

Using PowerShell:

```powershell
# Clone
git clone https://github.com/danielberhane/nexusforest-mcp.git
cd nexusforest-mcp

# Add data file
copy C:\path\to\global_05212025.xlsx data\raw\

# Build and run
docker-compose build
docker-compose --profile pipeline up
docker-compose --profile production up -d

# Configure Claude
notepad %APPDATA%\Claude\claude_desktop_config.json
# Add configuration (see docs/INSTALLATION.md)
```

## Available Tools

| Tool | Purpose | Example Query |
|------|---------|--------------|
| `query_tree_cover_loss` | Get forest loss for specific country/year | "Brazil's 2023 forest loss" |
| `query_primary_forest` | Query virgin forest loss | "Primary forest in Congo" |
| `query_carbon_data` | Get CO2 emissions | "Carbon from Indonesia forests" |
| `analyze_trend` | Multi-year trends | "Brazil trend 2010-2023" |
| `compare_countries` | Country comparisons | "Brazil vs Indonesia" |
| `rank_countries` | Rankings by metric | "Top 10 by forest loss" |
| `calculate_primary_share` | Primary forest % | "Primary forest share Brazil" |
| `calculate_carbon_intensity` | CO2 per hectare | "Carbon intensity Amazon" |
| `compare_thresholds` | Compare across thresholds | "Compare 30% vs 75% Brazil" |
| `aggregate_global` | Global totals | "Global forest loss 2023" |
| `list_tropical_countries` | List tropical countries | "Show tropical countries" |
| `get_database_summary` | Database statistics | "Database summary" |

See [API Documentation](docs/API.md) for complete tool reference.

## Architecture

```
MCP Client (Claude/ClimateGPT) → MCP Server (Docker) → SQLite Database (43MB)
                                      ↓
                                ClimateGPT API (enhanced analysis)
```

- **Three-table fact design** optimized for query patterns
- **Docker containerization** for consistent deployment
- **Metadata-driven validation** for data quality
- **Index-optimized SQLite** for sub-50ms queries

See [Architecture Documentation](docs/ARCHITECTURE.md) for detailed system diagram and component design.

## Data Updates

When new Global Forest Watch data arrives (annually):

```bash
# Replace Excel file
cp new_data_2026.xlsx data/raw/global_05212025.xlsx

# Rebuild database
./rebuild-data.sh

# Restart server
./start-system.sh
```

## Project Structure

```
nexusforest-mcp/
├── src/nexus/
│   ├── mcp/                 # MCP server implementation
│   ├── data/                # ETL pipeline & database
│   └── config/              # Configuration
├── docker/                  # Container definitions
├── tests/                   # Test suite (49 tests)
├── data/
│   ├── raw/                # Input Excel files
│   └── processed/          # SQLite database (volume)
└── *.sh                    # Management scripts
```

## Testing

```bash
# Run test suite
docker-compose --profile test up

# Expected output
✅ 49 tests passed
```

## Security

- ✅ Parameterized SQL queries (injection-proof)
- ✅ Read-only database access
- ✅ Environment-based secrets
- ✅ Input validation & sanitization


## License

Apache License 2.0 - See [LICENSE](LICENSE) for details.

## Data Attribution

**Source**: Hansen/UMD/Google/USGS/NASA via Global Forest Watch

## Author

**Daniel Berhane Araya**


## Support

- [Documentation](docs/)
- [Report Issues](https://github.com/danielberhane/nexusforest-mcp/issues)
- Contact: [via GitHub](https://github.com/danielberhane)

---

*Source: Hansen/UMD/Google/USGS/NASA*