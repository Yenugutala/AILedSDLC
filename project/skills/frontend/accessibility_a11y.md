# Skill: Accessibility (a11y)

## Overview
Building inclusive web interfaces that work for all users including those using assistive technologies.

## Key Patterns

### WCAG 2.1 Principles (POUR)
- **Perceivable** — content presentable in multiple ways (text alt for images, captions for video)
- **Operable** — all functionality accessible via keyboard; no seizure-inducing animations
- **Understandable** — clear language, consistent navigation, error identification
- **Robust** — content parseable by assistive technologies; valid semantic HTML

### Semantic HTML
- Use native elements before ARIA: `<button>` not `<div role="button">`
- Landmark roles: `<header>`, `<nav>`, `<main>`, `<aside>`, `<footer>`
- Heading hierarchy: one `<h1>` per page; don't skip levels
- `<label>` associated with every form input via `for` / `id`

### ARIA
- `aria-label` / `aria-labelledby` — name elements without visible text
- `aria-describedby` — link additional description to an element
- `aria-live` regions — announce dynamic content changes to screen readers
- `aria-expanded`, `aria-haspopup` — state for interactive widgets
- Rule: no ARIA is better than bad ARIA

### Keyboard Navigation
- All interactive elements must be focusable and operable with Enter/Space
- Logical tab order matches visual order
- Visible focus indicator — never `outline: none` without replacement
- Focus trapping in modals; return focus on close
- Skip navigation link as first focusable element

### Colour and Contrast
- WCAG AA: 4.5:1 contrast ratio for normal text, 3:1 for large text
- WCAG AAA: 7:1 for normal text
- Never use colour as the sole means of conveying information

## Best Practices
- Test with keyboard only (no mouse)
- Test with VoiceOver (macOS/iOS) and NVDA (Windows)
- Use axe DevTools browser extension for automated checks
- Include a11y in PR checklists and CI pipelines

## Common Pitfalls
- Icon buttons without accessible labels
- Modal dialogs that don't trap focus
- Custom dropdowns missing keyboard support
- Form errors not announced to screen readers

## Tools
- **axe-core** — automated accessibility testing
- **WAVE** — visual a11y feedback in browser
- **Deque University** — training and guidelines
- **React Aria** — accessible component primitives
