"""CodeChef is blocked (2026-10-28, terms of service): no adapter, no lane, no
manual refresh -- nothing in the scraper may make a request to it."""
import httpx
import pytest

from yuktisetu_scraper.config import settings
from yuktisetu_scraper.models import Platform
from yuktisetu_scraper.platforms import adapter_for


def test_codechef_is_blocked_by_default():
    assert Platform.CODECHEF in settings.blocked_platforms


@pytest.mark.asyncio
async def test_no_adapter_is_handed_out_for_a_blocked_platform():
    async with httpx.AsyncClient() as client:
        assert adapter_for(Platform.CODECHEF, client) is None
        assert adapter_for(Platform.CODEFORCES, client) is not None
        assert adapter_for(Platform.LEETCODE, client) is not None


def test_manual_refresh_refuses_a_blocked_platform():
    from fastapi import HTTPException
    from yuktisetu_scraper import api
    # The handler directly: the refusal happens before any database access.
    with pytest.raises(HTTPException) as e:
        api.refresh(api.EnqueueRequest(student_user_id=1, platform=Platform.CODECHEF, handle="x"))
    assert "blocked" in str(e.value.detail)
