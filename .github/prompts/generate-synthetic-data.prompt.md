---
description: "Generate synthetic data and populate a Microsoft Fabric lakehouse. Use when: generate test data, create sample tables, seed lakehouse, synthetic data."
agent: "fabric-synthetic-data"
tools: [vscode, execute, read, agent, browser, edit, search, web, 'fabric-mcp/*', todo]
argument-hint: "Domain and target lakehouse"
---

Extract the domain, volume, and target lakehouse from the user's input above. If any are missing, try to discover the target via MCP tools. Only ask the user if the results are ambiguous. Then execute all workflow phases.
