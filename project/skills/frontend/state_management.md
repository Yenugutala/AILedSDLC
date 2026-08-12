# Skill: Frontend State Management

## Overview
Choosing and implementing the right state management strategy based on scope and complexity.

## Key Patterns

### State Categories
| Type | Where it lives | Tool |
|---|---|---|
| Server state | Remote — fetched and cached | React Query, SWR, Apollo |
| Global client state | App-wide shared | Zustand, Redux, Pinia |
| Local component state | Single component | useState, ref |
| Form state | Form lifecycle | React Hook Form, Formik |
| URL state | Browser address bar | useSearchParams, nuxt-router |

### Context API (React)
- Good for low-frequency updates (theme, auth user, locale)
- Split contexts by domain to avoid unnecessary re-renders
- Combine with `useReducer` for complex transitions

### Zustand
- Minimal boilerplate; no Provider needed
- Define store as a hook: `const useStore = create(set => ({ ... }))`
- Supports middleware: devtools, persist, immer
- Select slices to avoid unnecessary re-renders

### Redux Toolkit
- `createSlice` generates actions and reducers together
- `createAsyncThunk` for async actions
- RTK Query replaces React Query for Redux-centric apps
- DevTools for time-travel debugging

### Jotai / Recoil (Atomic State)
- State as independent atoms — components subscribe to only what they use
- Derived state via selectors / derived atoms
- Fine-grained reactivity without Provider boilerplate

## Best Practices
- Separate server state (React Query) from client state (Zustand)
- Keep global state minimal — prefer local state first
- Normalise collections in global state (keyed by ID)
- Use selectors to derive computed values
- Never store derived data — compute it

## Common Pitfalls
- Putting everything in global state (over-engineering)
- Storing server responses in Redux when React Query handles it better
- Mutating Redux state directly instead of returning new objects
- Context causing full tree re-renders on every update

## Tools
- **Redux Toolkit** — opinionated Redux
- **Zustand** — lightweight global state
- **Jotai** — atomic state
- **React Query / TanStack Query** — server state
- **XState** — finite state machines for complex flows
