# Tableau Cloud And MCP Setup

## Objective
Use Tableau Cloud as the primary presentation surface for the Snowflake-backed OpenDota marts, with Tableau MCP pointed at the published Cloud content.

## Required Repo Environment Variables
Add these values to `.env`:

- `TABLEAU_SERVER`
- `TABLEAU_SITE_NAME`
- `TABLEAU_PAT_NAME`
- `TABLEAU_PAT_VALUE`
- `TABLEAU_PROJECT_NAME`
- `TABLEAU_DATASOURCE_NAME`
- `TABLEAU_WORKBOOK_NAME`

Recommended naming defaults:

- `TABLEAU_PROJECT_NAME=OpenDota LiveOps`
- `TABLEAU_DATASOURCE_NAME=opendota_liveops_curated_marts`
- `TABLEAU_WORKBOOK_NAME=opendota_liveops_meta_stability`

## Publish Order
1. Refresh the Snowflake raw tables and dbt marts.
2. Export the curated marts to `outputs/curated/`.
3. Run `scripts/validate_tableau_exports.py`.
4. In Tableau Cloud or Tableau Desktop, publish one datasource named `TABLEAU_DATASOURCE_NAME`.
5. Build and publish one workbook named `TABLEAU_WORKBOOK_NAME`.
6. Point Tableau MCP at the Tableau Cloud site after the datasource and workbook exist.

## Tableau Cloud Connection Route
Primary route:

1. In Tableau Desktop, connect to Snowflake.
2. Publish a datasource or workbook to the Tableau Cloud project named by `TABLEAU_PROJECT_NAME`.
3. Keep Snowflake as the dynamic system of record.

Fallback route:

1. Upload the CSV exports from `outputs/curated/` to Tableau Public or Tableau Cloud.
2. Use the same datasource and workbook names so the workbook spec stays stable.

## Tableau MCP Mapping
The official Tableau MCP server expects:

- `SERVER`
- `SITE_NAME`
- `PAT_NAME`
- `PAT_VALUE`

This repo keeps the project-level source of truth in `.env` as:

- `TABLEAU_SERVER`
- `TABLEAU_SITE_NAME`
- `TABLEAU_PAT_NAME`
- `TABLEAU_PAT_VALUE`

Use [tableau/tableau_mcp_env.example](../tableau/tableau_mcp_env.example) to map the repo variables into the MCP runtime environment.

## Example MCP Server Configuration
```json
{
  "mcpServers": {
    "tableau": {
      "command": "npx",
      "args": ["-y", "@tableau/mcp-server@latest"],
      "env": {
        "SERVER": "https://your-pod.online.tableau.com",
        "SITE_NAME": "your_site_content_url",
        "PAT_NAME": "opendota_liveops_mcp",
        "PAT_VALUE": "your_tableau_pat_value"
      }
    }
  }
}
```

## Local Prerequisites
- Tableau MCP documents Node.js `22.7.5` or newer.
- Upgrade Node if your local environment is older than `22.7.5` before attempting local MCP setup.
- Tableau MCP targets Tableau Cloud or Tableau Server content. It is not a substitute for local-only Tableau Desktop authoring.

## Validation Commands
```powershell
python scripts/validate_tableau_exports.py
```

If the validation script fails, refresh the historical slice and rerun the export flow before building the workbook.
