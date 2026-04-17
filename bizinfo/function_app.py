"""Azure Function - Bizinfo 공고 수집기

하루에 한번씩 Bizinfo에서 공고를 가져와서 DB에 저장함.
"""

import logging
import os
import sys
import typing as t
from datetime import datetime

import azure.functions as func
import pydantic

_dir = os.path.dirname(__file__)
sys.path.insert(0, _dir)
sys.path.insert(0, os.path.join(_dir, ".."))

from common.http.http import get
from common.db.db import Session, engine, sessionmaker
from common.url.shorten import shorten_batch
from models import migrate, upsert, get_cached_urls, cache_urls

app = func.FunctionApp()

BIZINFO_URL = os.environ.get("BIZINFO_URL", "")
BIZINFO_API_KEY = os.environ.get("BIZINFO_API_KEY", "")
DB_CONNECTION_STRING = os.environ.get("DB_CONNECTION_STRING", "")


class BizinfoItem(pydantic.BaseModel):
    pblancNm: str
    pblancId: str
    pblancUrl: str
    jrsdInsttNm: t.Optional[str] = None
    excInsttNm: t.Optional[str] = None
    pldirSportRealmLclasCodeNm: t.Optional[str] = None
    trgetNm: t.Optional[str] = None
    bsnsSumryCn: t.Optional[str] = None
    creatPnttm: t.Optional[str] = None
    reqstBeginEndDe: t.Optional[str] = None
    hashtags: t.Optional[str] = None
    inqireCo: t.Optional[int] = None
    totCnt: t.Optional[int] = None
    flpthNm: t.Optional[str] = None
    fileNm: t.Optional[str] = None
    printFlpthNm: t.Optional[str] = None
    printFileNm: t.Optional[str] = None
    reqstMthPapersCn: t.Optional[str] = None
    refrncNm: t.Optional[str] = None
    updtPnttm: t.Optional[str] = None
    pldirSportRealmMlsfcCodeNm: t.Optional[str] = None


class BizinfoResponse(pydantic.BaseModel):
    jsonArray: t.List[BizinfoItem] = []


async def _run() -> int:
    """공고 수집 핵심 로직. 저장 건수를 반환."""
    data = await get(
        BizinfoResponse,
        url=BIZINFO_URL,
        params={
            "crtfcKey": BIZINFO_API_KEY,
            "dataType": "json",
        },
        headers={},
    )
    items = data.jsonArray
    logging.info("Bizinfo API 응답 수신 완료: %d건", len(items))

    today = datetime.now().strftime("%Y-%m-%d")
    eng = engine(DB_CONNECTION_STRING)
    migrate(eng)
    sm = sessionmaker(eng)

    all_urls = [i.pblancUrl for i in items]

    with Session(sm) as session:
        cached = get_cached_urls(session, all_urls)
    logging.info("캐시 히트: %d건, 미스: %d건", len(cached), len(all_urls) - len(cached))

    uncached_urls = [u for u in all_urls if u not in cached]
    BATCH_SIZE = 20
    for i in range(0, len(uncached_urls), BATCH_SIZE):
        batch = uncached_urls[i:i + BATCH_SIZE]
        short_results = await shorten_batch(batch, delay=1.5)
        new_mappings = {orig: short for orig, short in zip(batch, short_results)}
        with Session(sm) as session:
            cache_urls(session, new_mappings)
        cached.update(new_mappings)
        logging.info("캐시 저장: %d/%d건", min(i + BATCH_SIZE, len(uncached_urls)), len(uncached_urls))

    rows = []
    for item in items:
        rows.append(dict(
            pblanc_id=item.pblancId,
            inst=item.jrsdInsttNm or "",
            type=item.pldirSportRealmLclasCodeNm or "",
            title=item.pblancNm,
            url=cached.get(item.pblancUrl, item.pblancUrl),
            hashtags=item.hashtags or "",
            created_at=(item.creatPnttm or today)[:10],
        ))

    with Session(sm) as session:
        upsert(session, rows)
    logging.info("DB 저장 완료: %d건", len(items))
    return len(items)



@app.timer_trigger(
    schedule="0 0 * * * *",
    arg_name="timer",
    run_on_startup=True,
)
async def fetch_bizinfo(timer: func.TimerRequest) -> None:
    logging.info("Bizinfo 공고 수집 시작")
    try:
        count = await _run()
        logging.info("Bizinfo 공고 수집 완료: %d건", count)
    except Exception:
        logging.exception("Bizinfo 공고 수집 실패")
    if timer.past_due:
        logging.warning("타이머가 지연되어 실행됨")
