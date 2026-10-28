"""A paused lane must sleep, whatever the clock says.

What went wrong: while CodeChef was paused, the lane slept "until the next job
is due". The waiting jobs were already due, so that time was in the past, the
sleep was zero, and the lane logged "paused until ..." about 3000 times a second.
"""
from datetime import datetime, timedelta, timezone

from yuktisetu_scraper.lane import MIN_PAUSED_SLEEP_SECONDS, paused_sleep_seconds

NOW = datetime(2026, 10, 28, 17, 48, 50, tzinfo=timezone.utc)
CAP = 300.0


def test_sleeps_until_the_pause_ends():
    assert paused_sleep_seconds(NOW + timedelta(minutes=4), None, NOW, CAP) == 240


def test_wakes_earlier_for_a_job_reaching_its_limit():
    assert paused_sleep_seconds(NOW + timedelta(minutes=4), NOW + timedelta(minutes=1), NOW, CAP) == 60


def test_a_time_in_the_past_never_becomes_a_busy_loop():
    past = NOW - timedelta(minutes=3)
    assert paused_sleep_seconds(past, past, NOW, CAP) == MIN_PAUSED_SLEEP_SECONDS
    assert paused_sleep_seconds(None, past, NOW, CAP) == MIN_PAUSED_SLEEP_SECONDS


def test_capped_and_defaults_to_the_cap():
    assert paused_sleep_seconds(NOW + timedelta(hours=2), None, NOW, CAP) == CAP
    assert paused_sleep_seconds(None, None, NOW, CAP) == CAP
