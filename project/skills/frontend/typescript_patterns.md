# Skill: TypeScript Patterns

## Overview
Advanced TypeScript patterns for type safety, maintainability, and developer productivity.

## Key Patterns

### Generics
```ts
function first<T>(arr: T[]): T | undefined { return arr[0]; }
type ApiResponse<T> = { data: T; error: string | null; };
```
- Constrain with `extends`: `<T extends object>`
- Default type params: `<T = string>`

### Discriminated Unions
```ts
type Result<T> =
  | { status: 'success'; data: T }
  | { status: 'error'; message: string };
```
- Exhaustive checks with `never` in switch default branches
- Replace boolean flags with explicit state unions

### Utility Types
| Type | Purpose |
|---|---|
| `Partial<T>` | All fields optional |
| `Required<T>` | All fields required |
| `Pick<T, K>` | Select subset of keys |
| `Omit<T, K>` | Exclude subset of keys |
| `Readonly<T>` | Immutable version |
| `Record<K, V>` | Typed dictionary |
| `ReturnType<F>` | Infer function return type |
| `Parameters<F>` | Infer function param types |

### Type Guards
```ts
function isError(val: unknown): val is Error {
  return val instanceof Error;
}
```
- `in` operator narrowing: `if ('name' in obj)`
- `typeof` narrowing: `if (typeof x === 'string')`

### Template Literal Types
```ts
type EventName = `on${Capitalize<string>}`;
type ApiPath = `/api/${string}`;
```

### Mapped Types
```ts
type Nullable<T> = { [K in keyof T]: T[K] | null };
```

## Best Practices
- Enable `strict: true` in `tsconfig.json`
- Prefer `interface` for extendable shapes; `type` for unions/intersections
- Avoid `any` — use `unknown` and narrow explicitly
- Export types separately from runtime code (`export type`)
- Use `satisfies` operator to validate without widening types

## Common Pitfalls
- Type assertions (`as`) hiding runtime errors
- Overusing `any` in third-party integrations — use `@types/` or write declaration files
- Not using `const` enums (use union of string literals instead)
- Ignoring TypeScript errors with `// @ts-ignore`

## Tools
- **ts-reset** — sensible overrides for TypeScript DOM types
- **zod** — runtime schema validation with type inference
- **tsd** — type testing
- **typescript-eslint** — TypeScript-aware linting rules
