import typing as t

import httpx
import pydantic

ResponseT = t.TypeVar("ResponseT", bound=pydantic.BaseModel)

async def get(
    schema: t.Type[ResponseT],
    *,
    url: str,
    params: dict,
    headers: dict,
) -> ResponseT:
    req = _request(
        method="GET",
        url=url,
        params=params,
        headers=headers,
    )
    resp = await _send(req)
    json_ = resp.json()
    return schema.model_validate(json_)


async def get_raw(
    *,
    url: str,
    params: dict | None = None,
    headers: dict | None = None,
    timeout: int = 10,
) -> httpx.Response:
    req = _request(
        method="GET",
        url=url,
        params=params or {},
        headers=headers or {},
    )
    return await _send(req, timeout=timeout)


async def _send(req: httpx.Request, timeout: int = 30) -> httpx.Response:
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.send(req)
        resp.raise_for_status()
        return resp
    
def _request(
    method: str,
    url: str,
    params: dict,
    headers: dict,
) -> httpx.Request:
    return httpx.Request(
        method=method,
        url=url,
        params=params,
        headers=headers,
    )