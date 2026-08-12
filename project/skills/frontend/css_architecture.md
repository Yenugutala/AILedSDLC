# Skill: CSS Architecture

## Overview
Scalable CSS methodologies, component-scoped styles, and utility-first approaches for maintainable UIs.

## Key Patterns

### BEM (Block Element Modifier)
- `.block__element--modifier` naming convention
- Blocks are standalone components; elements are children; modifiers are variants
- Reduces specificity conflicts; predictable class names
- Best for global stylesheet approaches without scoping

### CSS Modules
- Styles are locally scoped to the component by default
- Import as object: `import styles from './Button.module.css'`
- Composes via `composes` keyword for shared styles
- Zero runtime cost — compiled to unique class names at build time

### Tailwind CSS (Utility-First)
- Compose styles from atomic utility classes directly in HTML
- JIT compiler generates only used classes — minimal bundle
- Use `@apply` to extract repeated utility combinations into components
- `tailwind.config.js` for design tokens (colours, spacing, fonts)

### CSS-in-JS
- **styled-components / Emotion** — co-locate styles with components; dynamic theming
- Runtime cost — styles injected at render time; use with care for SSR
- **vanilla-extract** — zero-runtime CSS-in-JS with TypeScript types

### Design Tokens
- Define brand values (colour, spacing, typography) as CSS custom properties
- `--color-primary: #0066cc;` in `:root`
- Token tiers: primitive → semantic → component

## Best Practices
- Establish a consistent spacing and typography scale
- Avoid deep nesting (max 3 levels in Sass/Less)
- Use `rem` for font sizes, `px` for borders, `%` or `fr` for layouts
- Separate layout (Grid/Flex) from component styles
- Use CSS Grid for two-dimensional layouts; Flexbox for one-dimensional

## Common Pitfalls
- Overusing `!important` to fight specificity wars
- Global styles leaking into component-scoped systems
- Inconsistent spacing values outside the design token system
- Unused CSS shipped to production (use PurgeCSS / Tailwind's purge)

## Tools
- **PostCSS** — transform CSS with plugins
- **Sass/SCSS** — variables, mixins, nesting
- **Stylelint** — CSS/SCSS linting
- **PurgeCSS** — remove unused styles
