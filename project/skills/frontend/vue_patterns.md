# Skill: Vue 3 Patterns

## Overview
Vue 3 Composition API, reactivity system, Pinia state management, and Vue Router patterns.

## Key Patterns

### Composition API
- `setup()` or `<script setup>` — co-locate logic by feature, not by option type
- `ref()` — reactive primitive wrapper; access value via `.value`
- `reactive()` — reactive object; destructuring loses reactivity (use `toRefs`)
- `computed()` — derived reactive values with caching
- `watch` / `watchEffect` — side effects on reactive data
- Composables (`useXxx`) — reusable logic extracted into functions

### Component Patterns
- **Defineprops / Defineemits** — typed props and events in `<script setup>`
- **Provide / Inject** — dependency injection across deep component trees
- **Teleport** — render content outside the component hierarchy (modals, toasts)
- **Suspense** — async component loading with fallback

### Routing (Vue Router 4)
- `createRouter` with `createWebHistory` for HTML5 history mode
- Lazy routes: `component: () => import('./views/Home.vue')`
- Navigation guards (`beforeEach`) for auth checks
- Route meta fields for permission flags

### State Management (Pinia)
- Define stores with `defineStore`; use `setup` syntax for composition
- Actions are async-friendly — no extra middleware needed
- Stores are devtools-friendly and TypeScript-first
- `storeToRefs` to destructure state reactively

## Best Practices
- Use `<script setup>` for concise single-file components
- Keep composables pure and side-effect-free where possible
- Use `v-model` with custom components via `defineModel` (Vue 3.4+)
- Validate props with types and validators
- Use `shallowRef` / `shallowReactive` for large non-reactive objects

## Common Pitfalls
- Destructuring `reactive()` objects without `toRefs` loses reactivity
- Using `watch` with shallow comparison on objects (use `{ deep: true }` carefully)
- Mutating props directly — emit events instead
- Forgetting `v-bind="$attrs"` on root element of non-fragment components

## Tools & Libraries
- **Vite** — build tool (official Vue template)
- **Pinia** — state management
- **VueUse** — collection of composition utilities
- **Vitest** — unit testing
- **Nuxt 3** — SSR / SSG meta-framework
