# Skill: Angular Patterns

## Overview
Angular architecture, modules, services, RxJS patterns, and performance for enterprise SPAs.

## Key Patterns

### Architecture
- **Feature Modules** — group related components, services, routes into standalone modules
- **Standalone Components** (Angular 14+) — remove NgModule boilerplate, import directly
- **Smart / Dumb Components** — smart containers manage state; dumb components are pure presentational
- **Lazy Loading** — `loadChildren` in routes to reduce initial bundle size

### Dependency Injection
- Provide services at root (`providedIn: 'root'`) for singletons
- Feature-scoped providers — provide at module/component level to scope lifetime
- Use injection tokens for non-class dependencies (config objects, primitives)

### RxJS Patterns
- Prefer `async` pipe over manual subscriptions — auto-unsubscribes on destroy
- `combineLatest` / `forkJoin` for parallel streams
- `switchMap` for cancellable HTTP requests (e.g. search autocomplete)
- `takeUntilDestroyed()` (Angular 16+) to clean up subscriptions
- Avoid nested subscribes — use higher-order mapping operators

### Change Detection
- `ChangeDetectionStrategy.OnPush` — only checks on input change or Observable emit
- `markForCheck()` to trigger check manually when using OnPush
- `trackBy` in `*ngFor` to avoid full list re-render

## Best Practices
- Use Angular CLI for consistent project structure
- Enforce linting with ESLint + angular-eslint
- Keep templates simple — move logic to component class or pipes
- Use `signal`-based reactivity (Angular 17+) for simpler state
- Separate HTTP logic into dedicated service classes
- Use environment files for config; never hardcode URLs

## Common Pitfalls
- Subscribing manually without unsubscribing (memory leaks)
- Deeply nested route configurations without lazy loading
- Importing `FormsModule` / `HttpClientModule` in every module instead of root
- Overloading `ngOnInit` with too much logic

## Tools & Libraries
- **Angular CLI** — scaffolding and build
- **NgRx** — Redux-style state management
- **Angular Material / CDK** — UI component library
- **Spectator** — testing utilities
- **Nx** — monorepo tooling
