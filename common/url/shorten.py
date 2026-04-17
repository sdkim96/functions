"""URL 단축 모듈 (da.gd → ulvis.net → 원본 URL 폴백)"""

import asyncio
import logging

from common.http.http import get_raw

logger = logging.getLogger(__name__)

MAX_RETRIES = 3


async def shorten(url: str) -> str:
    """URL 단축. 429 시 backoff 재시도. 실패 시 원본 반환."""
    if not url:
        return url

    for attempt in range(MAX_RETRIES):
        short = await _try_dagd(url)
        if short:
            return short
        if attempt < MAX_RETRIES - 1:
            wait = 2 ** (attempt + 1)
            logger.info("da.gd 재시도 대기: %d초", wait)
            await asyncio.sleep(wait)

    short = await _try_ulvis(url)
    if short:
        return short

    logger.warning("URL 단축 모두 실패, 원본 사용: %s", url[:80])
    return url


async def shorten_batch(urls: list[str], delay: float = 1.5) -> list[str]:
    """여러 URL 일괄 단축"""
    results = []
    for i, url in enumerate(urls, 1):
        short = await shorten(url)
        results.append(short)
        if i % 20 == 0:
            logger.info("URL 단축 진행: %d/%d", i, len(urls))
        if delay > 0 and i < len(urls):
            await asyncio.sleep(delay)
    return results


async def _try_dagd(url: str) -> str | None:
    """da.gd API로 단축"""
    try:
        resp = await get_raw(
            url="https://da.gd/s",
            params={"url": url},
        )
        if resp.status_code == 200 and resp.text.strip().startswith("http"):
            return resp.text.strip()
    except Exception as e:
        logger.debug("da.gd 실패: %s", e)
    return None


async def _try_ulvis(url: str) -> str | None:
    """ulvis.net API로 단축"""
    try:
        resp = await get_raw(
            url="https://ulvis.net/API/write/get",
            params={"url": url},
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("success") and data.get("data", {}).get("url", "").startswith("http"):
                return data["data"]["url"]
    except Exception as e:
        logger.debug("ulvis.net 실패: %s", e)
    return None
