# Skill: Frontend Performance

## Overview
Techniques for optimising load time, runtime performance, and Core Web Vitals scores.

## Key Patterns

### Core Web Vitals
- **LCP (Largest Contentful Paint)** — target < 2.5s; optimise hero images, fonts, critical CSS
- **INP (Interaction to Next Paint)** — target < 200ms; defer non-critical JS, avoid long tasks
- **CLS (Cumulative Layout Shift)** — target < 0.1; set explicit width/height on images and ads

### Bundle Optimisation
- **Code splitting** — route-level splits with `React.lazy` / dynamic `import()`
- **Tree shaking** — import named exports only; avoid barrel files for large libs
- **Minification** — Terser for JS, cssnano for CSS
- **Compression** — Brotli > gzip; configure at CDN/server level
- **Module federation** — share dependencies across micro-frontends at runtime

### Image Optimisation
- Use modern formats: WebP, AVIF
- Responsive images: `srcset` + `sizes`
- Lazy load below-fold images: `loading="lazy"`
- Use a CDN with on-the-fly resizing (Cloudinary, Imgix)

### Caching
- **Cache-Control** headers — `immutable` for hashed assets, `no-cache` for HTML
- **Service Workers** — cache static assets; stale-while-revalidate strategy
- **HTTP/2** — multiplexed requests; no need for spriting/concatenation

### Runtime Performance
- Avoid layout thrashing — batch DOM reads before writes
- Debounce scroll/resize handlers
- Use `requestAnimationFrame` for visual updates
- Web Workers for CPU-intensive tasks off the main thread
- Virtualise long lists with `react-window` / `@tanstack/virtual`

## Best Practices
- Measure first — use Lighthouse, WebPageTest, Chrome DevTools
- Preload critical resources: `<link rel="preload">`
- Prefetch next routes: `<link rel="prefetch">`
- Remove unused dependencies regularly (`depcheck`)

## Common Pitfalls
- Shipping polyfills for modern browsers unnecessarily
- Not setting `width`/`height` on images causing CLS
- Importing entire lodash instead of individual functions
- Synchronous third-party scripts blocking render

## Tools
- **Lighthouse** — automated performance auditing
- **WebPageTest** — real-browser waterfall analysis
- **Bundle Analyzer** — visualise bundle composition
- **Sentry Performance** — real-user monitoring
