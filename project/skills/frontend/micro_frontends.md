# Skill: Micro-Frontends

## Overview
Architecture for splitting a frontend monolith into independently deployable UI applications.

## Key Patterns

### Composition Strategies
- **Build-time integration** — publish as npm packages; shell consumes at build time (tight coupling)
- **Run-time via iframes** — strong isolation; limited communication; UX drawbacks
- **Run-time via JavaScript** — Module Federation or single-spa; shared dependencies
- **Edge-side composition** — server assembles fragments at CDN level (Fastly, Cloudflare Workers)

### Module Federation (Webpack 5 / Vite)
- Host app consumes `remoteEntry.js` from each micro-frontend at runtime
- Share common libraries (React, React-DOM) to avoid duplication
- Versioning contracts — pin shared library versions to avoid incompatibility
- Async loading — remote modules loaded on demand

### single-spa
- Framework-agnostic orchestrator — each app registers as a single-spa parcel
- Route-based activation: show/hide apps based on URL
- Lifecycle hooks: `bootstrap`, `mount`, `unmount`

### Shared State and Communication
- **Custom events** — `window.dispatchEvent` for cross-app messaging (loose coupling)
- **Shared store** — expose via Module Federation; use carefully
- **URL / query params** — state in URL for deep-linking
- **Auth token** — pass via cookie or shared LocalStorage key

## Best Practices
- Each micro-frontend owns its own CI/CD pipeline and deployment
- Define a design system / shared component library as a federated remote
- Use a shell app for global concerns (auth, nav, routing)
- Test each micro-frontend independently and integration-test the shell
- Agree on versioning contracts before sharing modules

## Common Pitfalls
- Multiple React instances causing hooks errors — ensure shared React
- Coupling micro-frontends via shared global state (defeats independence)
- Inconsistent UX across teams without a shared design system
- Deployment coordination drift when teams release at different cadences

## Tools
- **Webpack Module Federation** — runtime sharing
- **Vite Plugin Federation** — Vite-based MF
- **single-spa** — framework-agnostic orchestrator
- **Nx** — monorepo tooling for MFE projects
- **Bit** — component-level versioning and sharing
