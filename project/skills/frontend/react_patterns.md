# Skill: React Patterns

## Overview
React component patterns, hooks, performance optimisation, and architecture for scalable SPAs.

## Key Patterns

### Component Design
- **Compound Components** — parent shares state with children via Context (e.g. `<Tabs>/<Tab>`)
- **Render Props** — pass a function as a prop to customise rendering
- **Higher-Order Components (HOC)** — wrap component to inject behaviour (use sparingly; prefer hooks)
- **Controlled vs Uncontrolled** — prefer controlled inputs for predictable state

### Hooks
- `useState` / `useReducer` — local state; use reducer when state logic is complex
- `useEffect` — side effects; always declare deps array; clean up subscriptions
- `useMemo` / `useCallback` — memoise expensive computations and stable function references
- `useContext` — consume context; avoid over-use (causes re-renders on every context change)
- Custom hooks — extract reusable stateful logic (`useFetch`, `useDebounce`, `useLocalStorage`)

### Performance
- **Code splitting** — `React.lazy` + `Suspense` for route-level splitting
- **Virtualisation** — `react-window` / `react-virtual` for large lists
- **React.memo** — skip re-render when props are shallowly equal
- **Avoid anonymous functions in JSX** — stabilise with `useCallback`

## Best Practices
- Co-locate component, styles, and tests in one folder
- Keep components small and single-purpose
- Lift state only as high as necessary
- Use TypeScript for all prop interfaces
- Prefer composition over inheritance
- Use Error Boundaries around async / data-fetching trees

## Common Pitfalls
- Missing deps in `useEffect` causing stale closures
- Overusing `useContext` — splits into smaller contexts or use Zustand/Jotai
- Mutating state directly instead of returning new objects
- Not cleaning up effects (timers, event listeners, subscriptions)
- Over-memoising — measure before optimising

## Tools & Libraries
- **Vite** — build tool
- **React Query / SWR** — server state management
- **Zustand / Jotai** — client state
- **React Hook Form** — form management
- **Storybook** — component development
- **React Testing Library** — component testing
