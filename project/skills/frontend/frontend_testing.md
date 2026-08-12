# Skill: Frontend Testing

## Overview
Testing pyramid for frontend: unit, component, integration, and end-to-end tests.

## Key Patterns

### Testing Pyramid
- **Unit** — pure functions, hooks, utilities (fast, isolated)
- **Component** — render a component, interact, assert DOM output
- **Integration** — multiple components working together with real data flows
- **E2E** — full user journey through the browser

### React Testing Library (RTL)
- Query by user-visible attributes: `getByRole`, `getByLabelText`, `getByText`
- Avoid querying by `data-testid` unless no semantic alternative exists
- `userEvent` over `fireEvent` — simulates real browser interactions
- `screen` object for queries without destructuring
- Wrap async interactions in `waitFor` or use `findBy` queries

### Jest
- Unit test utilities, hooks, and business logic
- Mock modules: `jest.mock('./api')` for external dependencies
- `jest.spyOn` for method tracking without replacing implementation
- Snapshot testing for stable UI output (use sparingly)
- Coverage thresholds in `jest.config.js`

### Playwright / Cypress (E2E)
- Test critical user journeys end-to-end against a real server
- Use `data-testid` sparingly; prefer accessible selectors
- Playwright: multi-browser, multi-tab, network interception
- Cypress: time-travel debugging, component testing mode
- Run E2E in CI against staging; not production

### MSW (Mock Service Worker)
- Intercept HTTP requests at the network level in tests and dev
- Single mock definition reused across unit, component, and E2E tests
- Prefer over manually mocking `fetch`/`axios`

## Best Practices
- Follow the testing trophy: more integration tests than unit or E2E
- Test behaviour, not implementation details
- Keep test files co-located with source files
- Run unit/component tests on every commit; E2E on PR merge

## Common Pitfalls
- Testing internal state instead of user-visible output
- Over-mocking — tests pass but production still breaks
- Flaky E2E tests due to timing — use explicit waits, not `sleep`
- No coverage for error states and loading states

## Tools
- **Vitest** — fast Vite-native unit test runner
- **React Testing Library** — component tests
- **Playwright** — E2E browser automation
- **MSW** — API mocking at network level
- **Storybook Interaction Tests** — visual + interaction testing
