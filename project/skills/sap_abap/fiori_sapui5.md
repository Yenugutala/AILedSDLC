# Skill: SAP Fiori & SAPUI5

## Overview
Building SAP Fiori applications with SAPUI5 framework, OData services, and Fiori design guidelines.

## Key Patterns

### SAPUI5 MVC Architecture
```
View (XML/JS)  ←→  Controller (JS)  ←→  Model (OData/JSON)
```
- **XML Views** — preferred; declarative, easy to read
- **Controllers** — event handlers and business logic
- **Models**: OData (backend), JSON (local state), Resource (i18n)

### OData Service (Backend)
- Create via SE11 (Data Dictionary) + SEGW (Gateway Service Builder)
- Entity sets map to SAP tables or CDS views
- Operations: `GET_ENTITYSET`, `GET_ENTITY`, `CREATE_ENTITY`, `UPDATE_ENTITY`, `DELETE_ENTITY`
- Use CDS + RAP (RESTful ABAP Programming Model) for modern development

### Fiori Elements (Low-Code)
- **List Report + Object Page** — standard CRUD pattern, annotation-driven
- **Analytical List Page** — for KPI and chart-based UIs
- Annotations in `@UI` namespace drive the UI without custom code:
  ```cds
  @UI.lineItem: [{ position: 10, label: 'Order ID' }]
  OrderID;
  ```
- Overlay with fragments for custom sections where needed

### Routing and Navigation
```js
// manifest.json defines routes
this.getRouter().navTo("detail", { objectId: sId });
```
- Deep links supported via hash-based routing
- Back navigation via `myNavBack()` or History API

### i18n (Internationalisation)
- All UI strings in `i18n/i18n.properties` (and locale variants)
- Access via `this.getResourceBundle().getText("keyName")`
- Never hardcode strings in views or controllers

## Best Practices
- Follow SAP Fiori Design Guidelines for UX consistency
- Use CDS views + RAP for new OData services (replaces SEGW)
- Test with Fiori Client on device for mobile-specific behaviour
- Use `sap.m` controls for mobile-first; avoid `sap.ui.commons` (deprecated)
- Validate OData payloads in the backend — never trust frontend input

## Common Pitfalls
- Business logic in the UI controller instead of the OData service layer
- Not handling OData batch errors — partial success looks like full success
- Ignoring Fiori Design Checklist — custom apps feel inconsistent in Fiori Launchpad
- Direct ABAP function calls from UI — use OData/REST layer only

## Tools
- **SAP Business Application Studio (BAS)** — cloud IDE for Fiori development
- **UI5 Tooling** — build, serve, deploy Fiori apps
- **Fiori Launchpad** — app shell and tile configuration
- **SAP Gateway (SEGW / RAP)** — OData service generation
- **UI5 Inspector** — browser DevTools extension for SAPUI5 debugging
