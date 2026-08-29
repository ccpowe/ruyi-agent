from __future__ import annotations

import socket

import httpx


TELEGRAM_API_HOST = "api.telegram.org"
TELEGRAM_FALLBACK_SEED_IPS = ["149.154.167.220", "149.154.167.99", "149.154.167.50"]
NETWORK_ERROR_PATTERNS = (
    "temporary failure in name resolution",
    "name or service not known",
    "nodename nor servname provided",
    "getaddrinfo failed",
)


class TelegramAPIError(Exception):
    pass


class TelegramNetworkError(TelegramAPIError):
    pass


class UnsupportedTelegramChatTypeError(ValueError):
    pass
def _looks_like_network_error(exc: BaseException) -> bool:
    if isinstance(
        exc,
        (
            httpx.ConnectError,
            httpx.ConnectTimeout,
            httpx.ReadError,
            httpx.ReadTimeout,
            httpx.RemoteProtocolError,
            httpx.PoolTimeout,
        ),
    ):
        return True
    text = str(exc).lower()
    return any(pattern in text for pattern in NETWORK_ERROR_PATTERNS)


class TelegramFallbackResolver:
    def __init__(
        self,
        *,
        fallback_ips: list[str] | None = None,
        timeout: float = 5.0,
    ) -> None:
        self._configured_ips = fallback_ips or []
        self._timeout = timeout
        self._discovered_ips: list[str] = []
        self._sticky_ip: str | None = None
        self._loaded = False

    def mark_success(self, ip: str) -> None:
        self._sticky_ip = ip

    def mark_failure(self, ip: str) -> None:
        if self._sticky_ip == ip:
            self._sticky_ip = None

    async def get_fallback_ips(self) -> list[str]:
        if not self._loaded:
            self._discovered_ips = await self._discover_fallback_ips()
            self._loaded = True
        ordered: list[str] = []
        if self._sticky_ip:
            ordered.append(self._sticky_ip)
        for ip in [
            *self._configured_ips,
            *self._discovered_ips,
            *TELEGRAM_FALLBACK_SEED_IPS,
        ]:
            if ip not in ordered:
                ordered.append(ip)
        return ordered

    async def _discover_fallback_ips(self) -> list[str]:
        discovered: list[str] = []
        doh_urls = [
            "https://cloudflare-dns.com/dns-query",
            "https://dns.google/resolve",
        ]
        async with httpx.AsyncClient(
            timeout=self._timeout,
            headers={"Accept": "application/dns-json"},
        ) as client:
            for url in doh_urls:
                try:
                    response = await client.get(
                        url,
                        params={"name": TELEGRAM_API_HOST, "type": "A"},
                    )
                    response.raise_for_status()
                    payload = response.json()
                except Exception:
                    continue
                answers = payload.get("Answer")
                if not isinstance(answers, list):
                    continue
                for answer in answers:
                    if not isinstance(answer, dict):
                        continue
                    value = answer.get("data")
                    if (
                        isinstance(value, str)
                        and _is_ipv4(value)
                        and value not in discovered
                    ):
                        discovered.append(value)
        return discovered


def _is_ipv4(value: str) -> bool:
    try:
        socket.inet_aton(value)
    except OSError:
        return False
    return value.count(".") == 3


class _AsyncBytesStream(httpx.AsyncByteStream):
    def __init__(self, content: bytes) -> None:
        self._content = content

    async def __aiter__(self):
        yield self._content


class TelegramFallbackTransport(httpx.AsyncBaseTransport):
    def __init__(
        self,
        *,
        resolver: TelegramFallbackResolver,
        base_transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._resolver = resolver
        self._base_transport = base_transport or httpx.AsyncHTTPTransport(retries=1)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        body = await request.aread()
        primary_request = self._build_request(request, request.url, body)
        try:
            return await self._base_transport.handle_async_request(primary_request)
        except Exception as exc:
            if request.url.host != TELEGRAM_API_HOST or not _looks_like_network_error(
                exc
            ):
                raise

        fallback_ips = await self._resolver.get_fallback_ips()
        last_exc: BaseException | None = None
        for ip in fallback_ips:
            fallback_request = self._build_fallback_request(request, ip, body)
            try:
                response = await self._base_transport.handle_async_request(
                    fallback_request
                )
            except Exception as exc:
                last_exc = exc
                self._resolver.mark_failure(ip)
                continue
            self._resolver.mark_success(ip)
            return response
        if last_exc is not None:
            raise last_exc
        raise

    def _build_request(
        self,
        request: httpx.Request,
        url: httpx.URL,
        body: bytes,
    ) -> httpx.Request:
        return httpx.Request(
            request.method,
            url,
            headers=request.headers,
            content=body,
            extensions=dict(request.extensions),
        )

    def _build_fallback_request(
        self,
        request: httpx.Request,
        ip: str,
        body: bytes,
    ) -> httpx.Request:
        url = request.url.copy_with(host=ip)
        fallback_request = self._build_request(request, url, body)
        fallback_request.headers["Host"] = TELEGRAM_API_HOST
        fallback_request.extensions["sni_hostname"] = TELEGRAM_API_HOST
        return fallback_request

    async def aclose(self) -> None:
        await self._base_transport.aclose()
