# Skill: Next.js Patterns

## Overview
Next.js App Router architecture, rendering strategies, Server Components, and deployment patterns.

## Key Patterns

### Rendering Strategies
- **SSR (Server-Side Rendering)** — fetch data per request; always fresh; use for auth-gated pages
- **SSG (Static Site Generation)** — build-time fetch; fastest; use for marketing / docs pages
- **ISR (Incremental Static Regeneration)** — revalidate static pages on a schedule or on-demand
- **CSR (Client-Side Rendering)** — fetch in browser; use for highly dynamic or user-specific data

### App Router (Next.js 13+)
- All components are Server Components by default — no client JS
- `'use client'` directive opts component into client rendering
- Layouts (`layout.tsx`) persist across navigations without re-mounting
- `loading.tsx` — instant loading UI via React Suspense
- `error.tsx` — error boundary per route segment

### Server Components
- Fetch data directly in the component — no `useEffect` or API routes needed
- Access secrets safely (env vars never sent to browser)
- No state, no effects, no browser APIs
- Pass serialisable data to Client Components via props

### Data Fetching
- `fetch` with `{ cache: 'no-store' }` for SSR
- `fetch` with `{ next: { revalidate: 3600 } }` for ISR
- `generateStaticParams` to enumerate dynamic SSG routes
- Route Handlers (`app/api/`) replace `pages/api/` in App Router

### Caching
- Automatic Request Deduplication — same `fetch` URL called once per render
- `unstable_cache` — cache arbitrary async functions (DB queries)
- `revalidatePath` / `revalidateTag` — on-demand cache purge

## Best Practices
- Start with Server Components; add `'use client'` only when needed
- Co-locate route segments with their data-fetching logic
- Use `<Image>` component for automatic WebP and responsive images
- Use `<Link>` for client navigation with prefetching
- Set `NEXT_PUBLIC_` prefix only for env vars needed on the client

## Common Pitfalls
- Putting secrets in `NEXT_PUBLIC_` variables (exposed to browser)
- Using Client Components everywhere (misses SSR/SSG benefits)
- Fetching inside `useEffect` when a Server Component would suffice
- Not handling `loading.tsx` states (causes content shift)

## Tools
- **Vercel** — native deployment platform for Next.js
- **next-auth** — authentication
- **next-intl** — internationalisation
- **Contentlayer** — type-safe content for MDX/CMS
