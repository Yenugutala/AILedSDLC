# Skill: REST API Design

## Overview
Designing consistent, intuitive, and evolvable REST APIs following industry standards.

## Key Patterns

### Resource Naming
- Plural nouns for collections: `GET /users`, `POST /users`
- Nested for relationships: `GET /users/{id}/orders`
- Actions as sub-resources: `POST /orders/{id}/cancel`
- Avoid verbs in URLs: NOT `/getUser` — use `GET /users/{id}`

### HTTP Methods
| Method | Operation | Idempotent | Safe |
|---|---|---|---|
| GET | Read | Yes | Yes |
| POST | Create | No | No |
| PUT | Full replace | Yes | No |
| PATCH | Partial update | No | No |
| DELETE | Delete | Yes | No |

### HTTP Status Codes
- `200 OK` — successful GET/PUT/PATCH
- `201 Created` — successful POST with `Location` header
- `204 No Content` — successful DELETE
- `400 Bad Request` — validation error (include error details)
- `401 Unauthorized` — not authenticated
- `403 Forbidden` — authenticated but not authorised
- `404 Not Found` — resource does not exist
- `409 Conflict` — state conflict (duplicate, optimistic lock failure)
- `422 Unprocessable Entity` — semantic validation failure
- `429 Too Many Requests` — rate limited (include `Retry-After` header)
- `500 Internal Server Error` — unexpected server error

### Request / Response Design
- Use JSON with `Content-Type: application/json`
- Consistent error envelope: `{ "error": { "code": "VALIDATION_ERROR", "message": "...", "details": [...] } }`
- Consistent timestamps: ISO 8601 (`2024-01-15T09:30:00Z`)
- Pagination: cursor-based for large datasets; offset for small
- Field filtering: `?fields=id,name,email` to reduce payload

### Versioning
- URL versioning: `/api/v1/users` (visible, simple)
- Header versioning: `Accept: application/vnd.api+json;version=1`
- Deprecate with `Sunset` header before removing

## Best Practices
- Return the created/updated resource in the response body
- Use `ETag` and `Last-Modified` for caching and optimistic locking
- Document with OpenAPI 3.x — generate client SDKs automatically
- Validate all inputs at the API boundary with a schema
- Always paginate list endpoints — never return unbounded collections

## Common Pitfalls
- Inconsistent naming (camelCase vs snake_case — pick one and stick to it)
- 200 OK with an error body inside
- No pagination on list endpoints (causes timeouts at scale)
- Breaking changes in minor versions

## Tools
- **OpenAPI / Swagger** — API specification and documentation
- **Stoplight / Redocly** — API design and docs platform
- **Postman / Insomnia** — API testing
- **Spectral** — OpenAPI linting
