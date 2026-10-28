"""Logging configuration.

Separated out because the default is unusable: at DEBUG, httpx and httpcore
print every header of every request, which buries our own narration about a
hundred lines deep. The project's standing expectation is that DEBUG narrates
every step of OUR process -- that only works if the transport layer underneath
is not shouting over it.
"""

from __future__ import annotations

import logging

from .config import settings

# Switched on individually when a request itself is the thing under
# investigation -- a suspected header or TLS problem. Never on by default.
NOISY = ("httpx", "httpcore", "hpack", "urllib3")


def configure(level: str | None = None) -> None:
    logging.basicConfig(
        level=level or settings.log_level,
        format="%(asctime)s %(levelname)-5s [%(name)s] %(message)s",
        force=True,
    )
    for name in NOISY:
        logging.getLogger(name).setLevel(logging.WARNING)
