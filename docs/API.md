# NexusForest MCP - API Reference

## Overview

NexusForest MCP provides 12 specialized tools for querying Global Forest Watch deforestation data through the Model Context Protocol.

## Available Tools

| Tool | Purpose | Parameters |
|------|---------|------------|
| `query_tree_cover_loss` | Get forest loss data | `country`, `year?`, `threshold?` |
| `query_primary_forest` | Get primary forest loss (tropical only) | `country`, `year?` |
| `query_carbon_data` | Get CO2 emissions from deforestation | `country`, `year?`, `threshold?` |
| `analyze_trend` | Analyze multi-year trends | `country`, `start_year`, `end_year`, `metric?` |
| `compare_countries` | Compare metrics across countries | `countries[]`, `year?`, `metric?` |
| `rank_countries` | Rank countries by metric | `metric`, `year`, `top_n?`, `order?` |
| `calculate_primary_share` | Calculate % of primary forest loss | `country`, `year?` |
| `calculate_carbon_intensity` | Calculate CO2 per hectare | `country`, `year?`, `threshold?` |
| `compare_thresholds` | Compare across tree cover thresholds | `country`, `year`, `metric?` |
| `aggregate_global` | Calculate global totals | `year`, `metric?`, `threshold?` |
| `list_tropical_countries` | List countries with tropical forests | None |
| `get_database_summary` | Get database statistics | None |

`?` = optional parameter

## Data Coverage

- **Countries**: 165+
- **Years**: 2001-2024 (2002-2024 for primary forest)
- **Thresholds**: 0%, 10%, 15%, 20%, 25%, 30%, 50%, 75%
- **Carbon Thresholds**: 30%, 50%, 75% only

## Response Format

All tools return consistent JSON:

```json
{
  "status": "success",
  "data": { /* tool-specific results */ },
  "metadata": {
    "query_time_ms": 47,
    "source": "Hansen/UMD/Google/USGS/NASA"
  }
}
```

## Example Usage

### Query Brazil's 2023 forest loss
```json
{
  "tool": "query_tree_cover_loss",
  "arguments": {
    "country": "Brazil",
    "year": 2023,
    "threshold": 30
  }
}
```

### Compare top deforestation countries
```json
{
  "tool": "rank_countries",
  "arguments": {
    "metric": "tree_cover",
    "year": 2023,
    "top_n": 5
  }
}
```

## Performance

- All queries use indexed lookups

## Notes

- Primary forest data only available for 75 tropical countries
- Carbon emissions measured in Megagrams (Mg) CO2 equivalent
- Default threshold is 30% for most analyses
- Data updated annually from Global Forest Watch

---

*Source: Hansen/UMD/Google/USGS/NASA*