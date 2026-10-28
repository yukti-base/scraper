"""After a block, the platform is read more slowly, and the pace recovers only
after a run of clean answers. Plus the lane's spin guard constants stay sane."""
from yuktisetu_scraper import lane
from yuktisetu_scraper.models import Platform
from yuktisetu_scraper.ratelimit import RateLimiter


def limiter():
    return RateLimiter(lambda p: 0.5)  # one request every 2 s


def test_each_block_doubles_the_gap_up_to_eight_times():
    r = limiter()
    assert r.interval_for(Platform.CODECHEF) == 2.0
    assert [r.slow_down(Platform.CODECHEF) for _ in range(5)] == [2, 4, 8, 8, 8]
    assert r.interval_for(Platform.CODECHEF) == 16.0


def test_slowdown_is_per_platform():
    r = limiter()
    r.slow_down(Platform.CODECHEF)
    assert r.slowdown(Platform.LEETCODE) == 1
    assert r.interval_for(Platform.LEETCODE) == 2.0


def test_recovers_by_halving_after_ten_clean_answers():
    r = limiter()
    r.slow_down(Platform.CODECHEF)
    r.slow_down(Platform.CODECHEF)  # 4x
    for _ in range(9):
        assert r.record_clean(Platform.CODECHEF) == 4
    assert r.record_clean(Platform.CODECHEF) == 2
    for _ in range(10):
        r.record_clean(Platform.CODECHEF)
    assert r.slowdown(Platform.CODECHEF) == 1


def test_a_block_mid_recovery_restarts_the_count():
    r = limiter()
    r.slow_down(Platform.CODECHEF)  # 2x
    for _ in range(9):
        r.record_clean(Platform.CODECHEF)
    r.slow_down(Platform.CODECHEF)  # 4x, count reset
    for _ in range(9):
        r.record_clean(Platform.CODECHEF)
    assert r.slowdown(Platform.CODECHEF) == 4


def test_no_sleep_path_can_be_zero():
    assert lane.MIN_SLEEP_SECONDS > 0
    assert lane.MIN_PAUSED_SLEEP_SECONDS > 0
    assert lane.SPIN_BACKOFF_SECONDS >= 1
