"""Reset-aware Ollama Cloud quota gate.

The settings-page parser and cookie validation follow the design of CodexBar's
Ollama connector (MIT, Copyright 2026 Peter Steinberger). See
THIRD_PARTY_NOTICES.md for attribution. This module intentionally stores and
exposes only sanitized quota data; the session cookie never enters logs or the
knowledge database.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import unescape
import logging
import re
import threading
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import (HTTPRedirectHandler, Request, build_opener)


LOGGER = logging.getLogger(__name__)
SETTINGS_URL = "https://ollama.com/settings"
SESSION_LABELS = ("Session usage", "Hourly usage")
USAGE_LABELS = (*SESSION_LABELS, "Weekly usage")
SESSION_COOKIE_NAMES = {
    "session",
    "__Secure-session",
    "ollama_session",
    "__Host-ollama_session",
    "wos-session",
    "__Secure-next-auth.session-token",
    "next-auth.session-token",
}


class OllamaQuotaError(RuntimeError):
    """A sanitized quota-probe failure safe to persist and display."""

    def __init__(self, message: str, *, kind: str = "check_failed"):
        super().__init__(message)
        self.kind = kind


@dataclass(frozen=True)
class OllamaQuotaSnapshot:
    plan_name: str | None
    session_used_percent: float | None
    weekly_used_percent: float | None
    session_resets_at: datetime | None
    weekly_resets_at: datetime | None
    checked_at: datetime


@dataclass(frozen=True)
class QuotaDecision:
    allowed: bool
    status: str
    blocked_until: datetime | None = None
    message: str | None = None


def _utc(value: datetime | None = None) -> datetime:
    value = value or datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return _utc(value).isoformat() if value else None


def _recognized_cookie_name(name: str) -> bool:
    if name in SESSION_COOKIE_NAMES:
        return True
    return (
        name.startswith("__Secure-next-auth.session-token.")
        or name.startswith("next-auth.session-token.")
    )


def normalize_cookie_header(raw: str | None) -> str:
    """Validate a manual Cookie header without ever returning it in an error."""
    value = (raw or "").strip()
    if value.lower().startswith("cookie:"):
        value = value.split(":", 1)[1].strip()
    if not value:
        raise OllamaQuotaError(
            "OLLAMA_COOKIE_HEADER is not configured; AI analysis is paused.",
            kind="configuration_error",
        )
    if "\r" in value or "\n" in value:
        raise OllamaQuotaError(
            "OLLAMA_COOKIE_HEADER contains invalid line breaks; AI analysis is paused.",
            kind="configuration_error",
        )
    names = {
        part.strip().split("=", 1)[0]
        for part in value.split(";")
        if "=" in part and part.strip().split("=", 1)[0]
    }
    if not any(_recognized_cookie_name(name) for name in names):
        raise OllamaQuotaError(
            "OLLAMA_COOKIE_HEADER has no recognized Ollama session cookie; AI analysis is paused.",
            kind="configuration_error",
        )
    return value


def _parse_date(text: str) -> datetime | None:
    match = re.search(r'data-time=["\']([^"\']+)', text)
    if not match:
        return None
    raw = match.group(1).strip()
    try:
        return _utc(datetime.fromisoformat(raw.replace("Z", "+00:00")))
    except ValueError:
        return None


def _usage_window(label: str, html: str) -> tuple[float, datetime | None] | None:
    start = html.find(label)
    if start < 0:
        return None
    tail = html[start + len(label):]
    boundaries = [tail.find(other) for other in USAGE_LABELS if other != label]
    boundaries = [position for position in boundaries if position >= 0]
    window = tail[:min(boundaries)] if boundaries else tail[:4000]
    window = window[:4000]
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*%\s*used", window, re.IGNORECASE)
    if not match:
        match = re.search(r"width:\s*([0-9]+(?:\.[0-9]+)?)%", window, re.IGNORECASE)
    if not match:
        return None
    return min(100.0, max(0.0, float(match.group(1)))), _parse_date(window)


def _looks_signed_out(html: str) -> bool:
    lower = html.lower()
    sign_in_heading = "sign in to ollama" in lower or "log in to ollama" in lower
    auth_route = "/api/auth/signin" in lower or "/auth/signin" in lower
    login_route = any(
        marker in lower
        for marker in (
            'action="/login"', "action='/login'", 'href="/login"', "href='/login'",
            'action="/signin"', "action='/signin'", 'href="/signin"', "href='/signin'",
        )
    )
    password = any(marker in lower for marker in (
        'type="password"', "type='password'", 'name="password"', "name='password'",
    ))
    email = any(marker in lower for marker in (
        'type="email"', "type='email'", 'name="email"', "name='email'",
    ))
    form = "<form" in lower
    endpoint = auth_route or login_route
    return bool(
        (sign_in_heading and form and (email or password or endpoint))
        or (form and endpoint)
        or (form and password and email)
    )


def parse_ollama_usage(html: str, *, now: datetime | None = None) -> OllamaQuotaSnapshot:
    """Parse the plan and both quota windows from Ollama's settings HTML."""
    plan_match = re.search(
        r"Cloud Usage\s*</span>\s*<span[^>]*>([^<]+)</span>",
        html,
        re.DOTALL,
    )
    plan = unescape(plan_match.group(1)).strip() if plan_match else None
    session = next(
        (parsed for label in SESSION_LABELS if (parsed := _usage_window(label, html))),
        None,
    )
    weekly = _usage_window("Weekly usage", html)
    if session is None and weekly is None:
        if _looks_signed_out(html):
            raise OllamaQuotaError(
                "The Ollama browser session is signed out or expired; AI analysis is paused.",
                kind="configuration_error",
            )
        raise OllamaQuotaError(
            "Ollama's settings page did not contain recognizable usage data; AI analysis is paused."
        )
    return OllamaQuotaSnapshot(
        plan_name=plan or None,
        session_used_percent=session[0] if session else None,
        weekly_used_percent=weekly[0] if weekly else None,
        session_resets_at=session[1] if session else None,
        weekly_resets_at=weekly[1] if weekly else None,
        checked_at=_utc(now),
    )


def _trusted_ollama_url(url: str) -> bool:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    return parsed.scheme.lower() == "https" and (
        host == "ollama.com" or host == "www.ollama.com" or host.endswith(".ollama.com")
    )


def _sign_in_url(url: str) -> bool:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    path = parsed.path.lower()
    if parsed.scheme.lower() != "https":
        return False
    if host in {"ollama.com", "www.ollama.com"}:
        return path == "/signin"
    if host == "signin.ollama.com":
        return True
    return host.endswith(".workos.com") and path.startswith("/user_management/authorize")


class _SafeCookieRedirectHandler(HTTPRedirectHandler):
    """Keep the session cookie on HTTPS Ollama redirects and strip it elsewhere."""

    def __init__(self, cookie_header: str):
        super().__init__()
        self.cookie_header = cookie_header

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if redirected is None:
            return None
        redirected.remove_header("Cookie")
        if _trusted_ollama_url(newurl):
            redirected.add_header("Cookie", self.cookie_header)
        return redirected


class OllamaQuotaClient:
    """Retrieve Ollama quota from the authenticated settings page."""

    def __init__(self, cookie_header: str | None, *, timeout_seconds: int = 30):
        self._raw_cookie_header = cookie_header
        self.timeout_seconds = max(1, timeout_seconds)

    def fetch(self, *, now: datetime | None = None) -> OllamaQuotaSnapshot:
        cookie = normalize_cookie_header(self._raw_cookie_header)
        request = Request(
            SETTINGS_URL,
            headers={
                "Cookie": cookie,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Origin": "https://ollama.com",
                "Referer": SETTINGS_URL,
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "Chrome/143.0.0.0 Safari/537.36"
                ),
            },
            method="GET",
        )
        opener = build_opener(_SafeCookieRedirectHandler(cookie))
        try:
            with opener.open(request, timeout=self.timeout_seconds) as response:  # nosec B310
                final_url = response.geturl()
                if _sign_in_url(final_url):
                    raise OllamaQuotaError(
                        "The Ollama browser session is signed out or expired; AI analysis is paused.",
                        kind="configuration_error",
                    )
                body = response.read(5_000_001)
        except HTTPError as exc:
            if exc.code in {401, 403}:
                raise OllamaQuotaError(
                    "The Ollama browser session is unauthorized or expired; AI analysis is paused.",
                    kind="configuration_error",
                ) from exc
            raise OllamaQuotaError(
                f"Ollama quota check returned HTTP {exc.code}; AI analysis is paused."
            ) from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise OllamaQuotaError(
                "Ollama quota check could not reach the settings page; AI analysis is paused."
            ) from exc
        if len(body) > 5_000_000:
            raise OllamaQuotaError(
                "Ollama's settings response exceeded the safety limit; AI analysis is paused."
            )
        return parse_ollama_usage(body.decode("utf-8", errors="replace"), now=now)


class OllamaQuotaGuard:
    """Fail-closed, cached admission control for Ollama model requests."""

    def __init__(
        self,
        client: OllamaQuotaClient,
        *,
        session_limit_percent: float = 95.0,
        weekly_limit_percent: float = 95.0,
        poll_seconds: int = 60,
        max_requests_between_checks: int = 20,
        reset_grace_seconds: int = 30,
        failure_retry_seconds: int = 300,
        state_sink: Callable[[dict], None] | None = None,
        clock: Callable[[], datetime] | None = None,
    ):
        self.client = client
        self.session_limit_percent = session_limit_percent
        self.weekly_limit_percent = weekly_limit_percent
        self.poll_seconds = max(10, poll_seconds)
        self.max_requests_between_checks = max(1, max_requests_between_checks)
        self.reset_grace_seconds = max(0, reset_grace_seconds)
        self.failure_retry_seconds = max(30, failure_retry_seconds)
        self.state_sink = state_sink
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.RLock()
        self._snapshot: OllamaQuotaSnapshot | None = None
        self._next_check_at: datetime | None = None
        self._blocked_until: datetime | None = None
        self._requests_since_check = 0
        self._status = "unchecked"
        self._message: str | None = None
        self._publish()

    def _now(self) -> datetime:
        return _utc(self.clock())

    def _state(self) -> dict:
        snapshot = self._snapshot
        return {
            "enabled": True,
            "status": self._status,
            "message": self._message,
            "plan": snapshot.plan_name if snapshot else None,
            "session_used_percent": snapshot.session_used_percent if snapshot else None,
            "weekly_used_percent": snapshot.weekly_used_percent if snapshot else None,
            "session_resets_at": _iso(snapshot.session_resets_at) if snapshot else None,
            "weekly_resets_at": _iso(snapshot.weekly_resets_at) if snapshot else None,
            "checked_at": _iso(snapshot.checked_at) if snapshot else None,
            "next_check_at": _iso(self._next_check_at),
            "blocked_until": _iso(self._blocked_until),
            "requests_since_check": self._requests_since_check,
            "session_limit_percent": self.session_limit_percent,
            "weekly_limit_percent": self.weekly_limit_percent,
        }

    def _publish(self) -> None:
        if not self.state_sink:
            return
        try:
            self.state_sink(self._state())
        except Exception:
            LOGGER.exception("Could not persist sanitized Ollama quota state")

    def public_state(self) -> dict:
        with self._lock:
            return self._state()

    def _evaluate(self, now: datetime) -> QuotaDecision:
        assert self._snapshot is not None
        exceeded: list[tuple[str, datetime | None]] = []
        if (
            self._snapshot.session_used_percent is not None
            and self._snapshot.session_used_percent >= self.session_limit_percent
        ):
            exceeded.append(("session", self._snapshot.session_resets_at))
        if (
            self._snapshot.weekly_used_percent is not None
            and self._snapshot.weekly_used_percent >= self.weekly_limit_percent
        ):
            exceeded.append(("weekly", self._snapshot.weekly_resets_at))
        if not exceeded:
            self._blocked_until = None
            self._status = "available"
            self._message = "Ollama Cloud quota is below the configured safety thresholds."
            self._publish()
            return QuotaDecision(True, self._status, message=self._message)

        reset_times = [reset for _, reset in exceeded if reset and reset > now]
        if len(reset_times) == len(exceeded):
            self._blocked_until = max(reset_times) + timedelta(seconds=self.reset_grace_seconds)
            windows = " and ".join(name for name, _ in exceeded)
            self._message = f"Ollama {windows} quota reached the safety threshold."
        else:
            # Without a trustworthy reset timestamp, poll sparingly and remain closed.
            self._blocked_until = now + timedelta(seconds=self.poll_seconds)
            self._message = "Ollama quota reached the safety threshold; reset time is unavailable."
        self._next_check_at = self._blocked_until
        self._status = "paused"
        self._publish()
        return QuotaDecision(False, self._status, self._blocked_until, self._message)

    def before_request(self) -> QuotaDecision:
        with self._lock:
            now = self._now()
            if self._blocked_until and now < self._blocked_until:
                return QuotaDecision(False, self._status, self._blocked_until, self._message)
            should_check = (
                self._snapshot is None
                or self._next_check_at is None
                or now >= self._next_check_at
                or self._requests_since_check >= self.max_requests_between_checks
            )
            if not should_check:
                return self._evaluate(now)
            try:
                self._snapshot = self.client.fetch(now=now)
            except OllamaQuotaError as exc:
                self._status = exc.kind
                self._message = str(exc)
                self._blocked_until = now + timedelta(seconds=self.failure_retry_seconds)
                self._next_check_at = self._blocked_until
                self._requests_since_check = 0
                self._publish()
                return QuotaDecision(False, self._status, self._blocked_until, self._message)
            except Exception:
                LOGGER.exception("Unexpected Ollama quota check failure")
                self._status = "check_failed"
                self._message = "Unexpected Ollama quota-check failure; AI analysis is paused."
                self._blocked_until = now + timedelta(seconds=self.failure_retry_seconds)
                self._next_check_at = self._blocked_until
                self._requests_since_check = 0
                self._publish()
                return QuotaDecision(False, self._status, self._blocked_until, self._message)
            self._requests_since_check = 0
            self._blocked_until = None
            self._next_check_at = now + timedelta(seconds=self.poll_seconds)
            return self._evaluate(now)

    def record_request(self) -> None:
        """Count an actual model HTTP attempt for the between-check budget."""
        with self._lock:
            self._requests_since_check += 1
            self._publish()

    def record_rate_limit(self, retry_after_seconds: int | None = None) -> None:
        """Honor an API 429 immediately, even if the settings snapshot was stale."""
        with self._lock:
            now = self._now()
            delay = max(
                self.poll_seconds,
                retry_after_seconds or self.failure_retry_seconds,
            ) + self.reset_grace_seconds
            self._blocked_until = now + timedelta(seconds=delay)
            self._next_check_at = self._blocked_until
            self._status = "rate_limited"
            self._message = "Ollama returned a rate limit; AI analysis will resume after a quota recheck."
            self._publish()
