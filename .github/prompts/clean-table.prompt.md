---
description: "Generate PySpark data cleaning notebooks for a specific Microsoft Fabric lakehouse table. Use when: clean table, data quality check, validate table data."
agent: "fabric-data-cleaner"
tools: [vscode, execute, read, agent, browser, edit, search, web, 'fabric-mcp/*', todo]
argument-hint: "Workspace, lakehouse, and table to clean"
---

Extract the workspace, lakehouse, and table name from the user's input above. If any are missing, try to discover them using MCP tools. Only ask the user if the results are ambiguous. Then execute all workflow phases.
