---
description: "Create a Power BI semantic model from Microsoft Fabric lakehouse tables. Use when: create semantic model, build data model, star schema, TMDL."
agent: "fabric-semantic-model"
tools: [vscode, execute, read, agent, browser, edit, search, web, 'fabric-mcp/*', todo]
argument-hint: "Workspace, lakehouse, and tables to model"
---

Extract the workspace, lakehouse, and tables from the user's input above. If any are missing, try to discover them using MCP tools. Only ask the user if the results are ambiguous. Then execute all workflow phases.
