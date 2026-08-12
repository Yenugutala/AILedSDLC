# Skill: Authentication & OAuth 2.0

## Overview
Implementing secure authentication and authorisation using OAuth 2.0, OIDC, and JWT.

## Key Patterns

### OAuth 2.0 Grant Types
| Flow | Use Case |
|---|---|
| Authorization Code + PKCE | Web apps, SPAs, mobile apps (recommended) |
| Client Credentials | Machine-to-machine (no user) |
| Device Code | Smart TVs, CLI tools |
| Refresh Token | Obtain new access tokens without re-authentication |

### Authorization Code + PKCE Flow
```
1. App generates code_verifier (random string) and code_challenge (SHA256 hash)
2. Redirect user to /authorize with code_challenge
3. User authenticates at IdP; IdP redirects to callback with auth code
4. App exchanges code + code_verifier for access_token + refresh_token
5. App uses access_token to call API; refresh when expired
```

### JWT (JSON Web Token)
- Header.Payload.Signature — base64url encoded, dot-separated
- Verify signature with public key before trusting claims
- Keep access tokens short-lived (5–15 minutes); use refresh tokens for renewal
- Never store sensitive data in JWT payload — it is base64 encoded, not encrypted
- Use `exp`, `iat`, `iss`, `aud` standard claims

### OIDC (OpenID Connect)
- Layer on top of OAuth 2.0 for identity; adds `id_token` (JWT with user claims)
- UserInfo endpoint for additional profile data
- Standard claims: `sub`, `email`, `name`, `picture`

### Session vs Token Strategy
| | Session Cookies | JWTs |
|---|---|---|
| Storage | Server-side session store | Client-side (cookie or memory) |
| Revocation | Immediate | Difficult (short expiry + refresh) |
| Scale | Requires sticky sessions or shared store | Stateless — scales horizontally |

## Best Practices
- Always use PKCE for public clients (SPAs, mobile)
- Store tokens in memory or `HttpOnly` cookies — never in `localStorage`
- Validate `iss`, `aud`, `exp` on every JWT
- Implement token rotation — invalidate refresh token on use
- Use a certified IdP (Auth0, Keycloak, Okta, AWS Cognito) — don't build your own

## Common Pitfalls
- Implicit flow (deprecated) — use Authorization Code + PKCE instead
- Storing JWTs in `localStorage` — vulnerable to XSS
- Long-lived access tokens without refresh token rotation
- Not validating token audience (`aud`) — token accepted by wrong service

## Tools
- **Auth0 / Okta** — managed identity platform
- **Keycloak** — open-source IdP
- **AWS Cognito** — AWS managed auth
- **NextAuth.js / Lucia** — auth libraries for Node/Next.js
- **jose / jsonwebtoken** — JWT verification libraries
