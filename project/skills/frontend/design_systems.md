# Skill: Design Systems

## Overview
Building and maintaining shared component libraries, design tokens, and documentation for consistent UIs.

## Key Patterns

### Token Architecture
- **Primitive tokens** — raw values (`color-blue-500: #3B82F6`)
- **Semantic tokens** — intent-based (`color-action-primary: {color-blue-500}`)
- **Component tokens** — component-specific (`button-background: {color-action-primary}`)
- Expressed as CSS custom properties; consumed by all platforms

### Component Library Structure
```
packages/
  ui/
    src/
      components/Button/
        Button.tsx
        Button.stories.tsx
        Button.test.tsx
        index.ts
      tokens/
      hooks/
    index.ts
```
- Export components, hooks, and tokens from a single package entry point
- Peer-depend on React/framework; do not bundle it

### Storybook
- Stories document component variants, states, and edge cases
- Use `args` and `argTypes` for interactive controls
- Interaction tests with `@storybook/test` for accessibility and behaviour
- Publish Storybook as static site for designer-developer collaboration

### Versioning and Publishing
- Semantic versioning — breaking changes = major bump
- Changesets (`@changesets/cli`) for automated CHANGELOG and publish
- Publish to private npm registry (Verdaccio, GitHub Packages, npm private)
- Each component versioned independently or as a monorepo batch

### Documentation
- Component purpose, props API, accessibility notes in Storybook
- Do / Don't examples for usage guidance
- Figma kit kept in sync with code tokens

## Best Practices
- Design tokens are the contract between design and engineering
- Consume the design system as a dependency, not a fork
- Establish a contribution process for teams adding new components
- Automate visual regression testing (Chromatic)
- Support dark mode and high-contrast themes from day one

## Common Pitfalls
- Building a design system before product patterns are stable
- No governance — components diverge because no review process
- Skipping accessibility in the design system (all consumers inherit the issue)
- Not documenting when NOT to use a component

## Tools
- **Storybook** — component documentation and dev environment
- **Chromatic** — visual regression testing
- **Changesets** — versioning and CHANGELOG automation
- **Style Dictionary** — transform tokens for all platforms
