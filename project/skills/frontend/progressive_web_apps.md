# Skill: Progressive Web Apps (PWA)

## Overview
Enhancing web apps with native-like capabilities: offline support, installability, and push notifications.

## Key Patterns

### Web App Manifest
```json
{
  "name": "My App",
  "short_name": "App",
  "start_url": "/",
  "display": "standalone",
  "background_color": "#ffffff",
  "icons": [{ "src": "/icon-192.png", "sizes": "192x192", "type": "image/png" }]
}
```
- Enables Add to Home Screen (A2HS)
- Controls splash screen, display mode, orientation

### Service Workers
- Intercept network requests — serve from cache when offline
- Background sync — retry failed requests when connection restores
- Push notifications — receive server messages even when app is closed
- Registration: `navigator.serviceWorker.register('/sw.js')`

### Caching Strategies
| Strategy | Use Case |
|---|---|
| Cache First | Static assets (JS, CSS, fonts) |
| Network First | API calls needing freshness |
| Stale While Revalidate | Non-critical content (news, blog) |
| Network Only | Auth endpoints, payments |
| Cache Only | Pre-cached offline fallbacks |

### Push Notifications
- Server sends notification via Web Push Protocol
- Browser decrypts and displays notification even when tab is closed
- User must grant `Notification` permission
- VAPID keys for server authentication

### App Shell Architecture
- Cache the minimal HTML/CSS/JS shell on first load
- Dynamic content fetched separately and rendered into shell
- Shell loads instantly; content fills in from network or cache

## Best Practices
- Use Workbox library for service worker management
- Always provide an offline fallback page
- Test offline behaviour in Chrome DevTools (Offline checkbox)
- Update service workers gracefully — prompt user to reload
- Audit with Lighthouse PWA checklist

## Common Pitfalls
- Service worker caching stale API responses indefinitely
- Not handling service worker update lifecycle (users stuck on old version)
- Push notification spam driving users to revoke permission
- PWA features requiring HTTPS — must use SSL even in development (mkcert)

## Tools
- **Workbox** — service worker toolkit by Google
- **vite-plugin-pwa** — PWA integration for Vite
- **Lighthouse** — PWA audit
- **Web Push libraries** — web-push (Node), push.js (browser)
