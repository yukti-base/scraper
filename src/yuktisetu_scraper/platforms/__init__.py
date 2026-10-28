"""Adapter registry.

A platform absent from this map is not an error -- CodingPlatform.OTHER and the
HTML platforms that have no adapter yet both land here. The worker marks such a
job DONE with nothing written rather than retrying it forever, because no amount
of retrying will conjure an adapter.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import httpx

from ..models import Platform
from .base import Adapter, Blocked, HandleNotFound
from .codechef import CodeChefAdapter
from .codeforces import CodeforcesAdapter
from .leetcode import LeetCodeAdapter

ADAPTERS: dict[Platform, type[Adapter]] = {
    Platform.CODEFORCES: CodeforcesAdapter,
    Platform.LEETCODE: LeetCodeAdapter,
    Platform.CODECHEF: CodeChefAdapter,
    # GEEKSFORGEEKS, HACKERRANK, HACKEREARTH and ATCODER are not built.
}


def adapter_for(
    platform: Platform,
    client: httpx.AsyncClient,
    throttle: Callable[[], Awaitable[None]] | None = None,
) -> Adapter | None:
    # Belt and braces: even if a lane for a blocked platform existed, no
    # request could be made for it.
    from ..config import settings
    if platform in settings.blocked_platforms:
        return None
    cls = ADAPTERS.get(platform)
    return cls(client, throttle) if cls else None


__all__ = ["ADAPTERS", "adapter_for", "Adapter", "Blocked", "HandleNotFound"]
