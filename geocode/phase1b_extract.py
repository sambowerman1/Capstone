"""Phase 1b: LLM extraction for Tier 3 legal descriptions via MiMo-V2-Flash."""

import asyncio
import json

import aiohttp

import config
import db
from phase1_normalize import normalize_route

_SYSTEM_PROMPT = (
    "You are a data extraction assistant. Extract structured highway location "
    "data from legal descriptions. Respond with ONLY a JSON object, no other text."
)

_USER_TEMPLATE = """Extract from this highway description:
- route_number: the highway/route number (e.g. "US 322", "SR 9A", "I-10", "State Route 66")
- from_location: the starting point, landmark, or beginning boundary
- to_location: the ending point, landmark, or ending boundary
- county: the county name if mentioned

State: {state}
Highway name: {highway_name}
Description: {description}

Respond with ONLY this JSON format:
{{"route_number": "...", "from_location": "...", "to_location": "...", "county": "..."}}
Use null for any field that cannot be determined."""


async def _call_mimo(session: aiohttp.ClientSession, row: dict) -> dict | None:
    """Send one row to MiMo and return the parsed JSON or None."""
    description = row.get("from_location") or row.get("to_location") or ""
    if not description.strip():
        return None

    user_msg = _USER_TEMPLATE.format(
        state=row["state"],
        highway_name=row["highway_name"],
        description=description,
    )

    payload = {
        "model": config.MIMO_MODEL,
        "temperature": 0.3,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ],
    }
    headers = {
        "Authorization": f"Bearer {config.MIMO_API_KEY}",
        "Content-Type": "application/json",
    }

    try:
        async with session.post(
            config.MIMO_API_URL, json=payload, headers=headers,
            timeout=aiohttp.ClientTimeout(total=60),
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
    except Exception:
        return None

    try:
        content = data["choices"][0]["message"]["content"]
        # Strip markdown fences if present
        content = content.strip()
        if content.startswith("```"):
            content = content.split("\n", 1)[-1]
            if content.endswith("```"):
                content = content[:-3]
            content = content.strip()
        return json.loads(content)
    except (KeyError, IndexError, json.JSONDecodeError):
        return None


async def _process_batch(rows: list[dict]):
    """Process all Tier 3 rows with bounded concurrency."""
    sem = asyncio.Semaphore(config.MIMO_CONCURRENT)
    connector = aiohttp.TCPConnector(limit=config.MIMO_CONCURRENT)
    async with aiohttp.ClientSession(connector=connector) as session:

        async def process_one(row: dict, idx: int, total: int):
            async with sem:
                result = await _call_mimo(session, row)

            if idx % 20 == 0 or idx == total:
                print(f"  [{idx}/{total}]")

            if result is None:
                db.append_error(row["id"], "phase1b: LLM returned no parseable JSON")
                return

            route_number = result.get("route_number")
            from_loc = result.get("from_location")
            to_loc = result.get("to_location")
            county = result.get("county")

            updates: dict = {}

            if route_number:
                updates["normalized_routes"] = normalize_route(route_number, row["state"])
            if from_loc:
                updates["parsed_from"] = from_loc
            if to_loc:
                updates["parsed_to"] = to_loc
            if county:
                updates["parsed_county"] = county

            # Reclassify tier based on what we extracted
            if from_loc and to_loc:
                updates["tier"] = 1
            elif from_loc and not to_loc:
                updates["tier"] = 2
            elif route_number and not from_loc and not to_loc:
                updates["tier"] = 4

            if updates:
                db.update_row(row["id"], **updates)

        tasks = [
            process_one(row, i, len(rows))
            for i, row in enumerate(rows, 1)
        ]
        await asyncio.gather(*tasks)


def run():
    rows = db.get_rows_by_tier(3, status="normalized")
    total = len(rows)
    if total == 0:
        print("Phase 1b: No Tier 3 rows to process.")
        return

    if not config.MIMO_API_KEY:
        print("Phase 1b: MIMO_API_KEY not set — skipping LLM extraction.")
        return

    print(f"Phase 1b: Extracting locations from {total} legal descriptions via MiMo...")
    asyncio.run(_process_batch(rows))
    stats = db.get_stats()
    print(f"Phase 1b complete. Updated tier distribution: {stats['by_tier']}")
