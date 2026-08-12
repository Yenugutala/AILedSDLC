# Skill: Frontend Web Security

## Overview
Protecting web applications from client-side attacks: XSS, CSRF, clickjacking, and data exposure.

## Key Patterns

### Cross-Site Scripting (XSS)
- **Reflected XSS** — malicious script in URL parameter echoed into page
- **Stored XSS** — malicious script persisted in database
- **DOM XSS** — script injected via browser DOM manipulation
- Mitigations: escape all user input in templates; use `textContent` not `innerHTML`; Content Security Policy

### Content Security Policy (CSP)
```
Content-Security-Policy: default-src 'self'; script-src 'self' cdn.example.com; object-src 'none'
```
- Whitelist trusted script/style/image sources
- `nonce`-based CSP for inline scripts
- Report-only mode for testing before enforcing

### CSRF (Cross-Site Request Forgery)
- `SameSite=Strict` or `SameSite=Lax` on session cookies
- CSRF tokens for state-changing form submissions
- Double-submit cookie pattern for SPAs
- Custom request headers (automatically prevent CORS simple requests)

### Clickjacking
- `X-Frame-Options: DENY` or `Content-Security-Policy: frame-ancestors 'none'`
- Prevents embedding your page in a malicious iframe

### Sensitive Data
- Never store secrets, tokens, or PII in `localStorage` — XSS can read it
- Use `HttpOnly`, `Secure`, `SameSite` cookies for session tokens
- Clear sensitive data from memory after use
- Avoid logging PII in browser console

### HTTPS
- Enforce HTTPS with `Strict-Transport-Security: max-age=31536000; includeSubDomains`
- Certificate pinning for mobile WebViews
- Mixed content — all sub-resources must be HTTPS

## Best Practices
- Apply CSP headers on all responses
- Sanitise HTML with DOMPurify before inserting user content
- Validate and sanitise on the server — never trust client-side validation alone
- Use `rel="noopener noreferrer"` on external links with `target="_blank"`

## Common Pitfalls
- Using `dangerouslySetInnerHTML` in React with unsanitised input
- Storing JWTs in localStorage (XSS-accessible)
- Relying solely on client-side input validation
- Open redirects via unchecked URL parameters

## Tools
- **DOMPurify** — HTML sanitisation
- **Helmet.js** — security headers middleware (Node)
- **OWASP ZAP** — automated security scanning
- **CSP Evaluator** — validate CSP policies
