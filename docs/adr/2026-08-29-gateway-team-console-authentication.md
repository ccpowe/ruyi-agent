# Gateway Team Console Browser Authentication

Date: 2026-08-29

Status: Accepted

## Context

The Gateway Task HTTP API requires one configured Bearer token, but the debug
team console shell and its JavaScript and CSS were previously served without
server-side authentication. The shell did not embed the configured token, yet
it exposed an administrative surface and asked the browser to persist the
Gateway credential in `localStorage`.

Adding the existing Bearer dependency directly to `/debug/team` is not a usable
browser flow. A normal top-level browser navigation cannot attach an arbitrary
`Authorization` header, and asset requests would have the same problem. Token
query parameters are not acceptable because URLs are routinely copied and
recorded in history, access logs, referrers, and monitoring systems.

The console remains a debug surface for the current single Gateway principal.
This decision does not add users, tenants, scopes, or per-Task authorization.

## Decision

### Login and session

`GET /debug/team/login` is the only anonymous console document. It contains a
minimal form and no script, third-party resource, or Gateway data. The form
posts the token in a size-limited `application/x-www-form-urlencoded` body to
`POST /debug/team/login`; no console endpoint accepts credentials or redirect
targets in query parameters.

The server compares the submitted value exactly and in constant time with the
configured Gateway Bearer token. A successful login returns a fixed `303`
redirect and a short-lived cookie with these properties:

- an opaque, versioned ticket containing issue time, expiry, and a 32-byte
  nonce;
- an HMAC-SHA256 over the canonical ticket fields;
- a domain-separated HMAC key derived from the configured Gateway token;
- an eight-hour maximum lifetime;
- `HttpOnly`, `SameSite=Strict`, `Path=/`, no `Domain`, and `Secure` whenever
  the trusted ASGI scheme is HTTPS.

The derivation makes tickets verifiable by multiple workers that share the same
Gateway token, and rotating that token invalidates old tickets. The Gateway
token must therefore be a high-entropy secret. The nonce prevents identical
tickets but does not make a weak human-chosen token resistant to offline
guessing. A future product that accepts weak credentials must use a separately
configured high-entropy session secret instead.

The ticket is stateless. `POST /debug/team/logout` deletes the browser cookie,
but cannot revoke a copied ticket before its expiry. A rolling token rotation
can also produce a brief interval where workers configured with old and new
tokens disagree about a ticket.

### Transport and same-origin policy

The console permits HTTPS on any valid host. Plain HTTP login and session use
are restricted to `localhost` or an IP loopback host. The implementation uses
only the ASGI `scheme` and exactly one strictly parsed raw `Host` header. It does
not trust `X-Forwarded-Proto` directly.

A TLS-terminating reverse proxy must be configured as a trusted proxy so the
ASGI server receives the corrected HTTPS scheme. If it is not, non-loopback
console login fails closed instead of issuing a non-`Secure` cookie. The public
Bearer API contract is unchanged by this console-only transport rule.

Every request that creates, uses for API access, or destroys a console session
must pass the browser same-origin policy:

- a present `Sec-Fetch-Site` must be the single value `same-origin`;
- if Fetch Metadata is absent, at least one `Origin` or `Referer` is required;
- every supplied `Origin` and `Referer` must be unique, syntactically valid,
  and exactly match the target scheme, normalized host, and effective port;
- missing, duplicate, malformed, or contradictory signals fail closed.

The login and console documents use `Referrer-Policy: same-origin` so older
browsers without Fetch Metadata retain a same-origin fallback.

### Gateway API use from the console

Bearer authentication remains the public API contract and has strict
precedence. If an `Authorization` header is present but missing, duplicated, or
incorrect, the request receives `401`; it never falls back to a console cookie.

Only when `Authorization` is absent may the same-origin console use its cookie.
That fallback additionally requires exactly one valid session cookie and the
exact custom header `X-Ruyi-Team-Console: 1`. The custom header is not a secret;
it makes browser requests non-simple and subject to the same-origin/CORS
boundary. Fetch Metadata and origin checks remain mandatory defense in depth.

All API responses authenticated with the console cookie, including error
responses and streams, receive `Cache-Control: no-store` through a non-buffering
ASGI middleware. Bearer-authenticated API behavior is otherwise unchanged.

### Protected resources

`/debug/team`, `/debug/team/app.css`, and `/debug/team/app.js` require a valid
console session. The HTML route redirects an unauthenticated browser to the
login page; assets return `401`. Console documents and assets use `no-store`,
`nosniff`, same-origin resource policy, frame denial, and a self-only Content
Security Policy. The console no longer loads Google Fonts, and its JavaScript
actively removes the legacy `ruyi.gatewayToken` local-storage entry.

Unauthorized API responses include a Bearer `WWW-Authenticate` challenge.

## Consequences

- The debug console is no longer anonymously loadable and no longer persists
  the Gateway credential in JavaScript-readable storage.
- Browser users log in once and then use an HttpOnly short-lived session.
- Credentialed console use fails on public plaintext HTTP and on proxies that
  do not provide a trusted HTTPS ASGI scheme.
- Cookie authentication is deliberately limited to the same Gateway principal;
  it is not a substitute for the future user-level authorization model.
- Operators must avoid permissive credentialed CORS configurations. The marker,
  Fetch Metadata, and origin checks make accidental exposure fail closed.

## Rejected alternatives

- Requiring Bearer headers on the HTML and assets breaks ordinary navigation.
- Putting the token in a query parameter leaks it through common URL handling.
- Keeping the token in `localStorage` leaves the long-lived Gateway credential
  readable by any script executing in the origin.
- Mirroring every Gateway route below `/debug/team/api/*` creates a second API
  surface and duplicated routing semantics for no additional principal model.
- Accepting Basic authentication would silently broaden the public Gateway API
  authentication contract.

## References

- [OWASP Session Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html)
- [OWASP Cross-Site Request Forgery Prevention Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html)
- [MDN Set-Cookie](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Set-Cookie)
