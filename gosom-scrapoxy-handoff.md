# Google Maps Scraper + Scrapoxy Handoff for Niche-Sieve

## Context
We are replacing the DataForSEO maps integration in Phase 3 of `niche-sieve` with a standalone `google-maps-scraper` (gosom) service. This service uses Scrapoxy for proxy rotation to handle rate limiting and blocking.

This document outlines how to integrate the new scraper client into `niche-sieve`.

## Prerequisites

### 1. Run the Maps Scraper Service
The `google-maps-scraper` must be running and accessible to `niche-sieve`. It requires the `SCRAPOXY_PROXY_URL` environment variable to be set for proxy rotation.

```bash
# Example: Running the scraper locally
export SCRAPOXY_PROXY_URL="http://user:pass@scrapoxy-host:8888"
./google-maps-scraper -web -addr :8080 -data-folder webdata
```

### 2. Dependencies
The client requires `aiohttp`. Ensure it is added to `niche-sieve`'s dependencies (e.g., `pyproject.toml` or `requirements.txt`).

```bash
pip install aiohttp
```

## Integration Steps

### 1. Add the Client Code
Copy the following Python code into a new file in `niche-sieve`, for example: `niche_sieve/clients/maps_scraper.py`.

```python
import asyncio
import csv
import io
import json
import logging
import time
from dataclasses import dataclass
from typing import List, Optional, Dict, Any

try:
    import aiohttp
except ImportError:
    aiohttp = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class MapsResult:
    input_id: str
    link: str
    cid: str
    title: str
    category: str
    address: str
    open_hours: Dict[str, List[str]]
    popular_times: Dict[str, Dict[int, int]]
    web_site: str
    phone: str
    plus_code: str
    review_count: int
    review_rating: float
    reviews_per_rating: Dict[int, int]
    latitude: float
    longitude: float
    status: str
    description: str
    reviews_link: str
    thumbnail: str
    timezone: str
    price_range: str
    data_id: str
    place_id: str
    images: List[Dict[str, str]]
    reservations: List[Dict[str, str]]
    order_online: List[Dict[str, str]]
    menu: Dict[str, str]
    owner: Dict[str, str]
    complete_address: Dict[str, str]
    about: List[Dict[str, Any]]
    user_reviews: List[Dict[str, Any]]
    emails: List[str]

    @classmethod
    def from_csv_row(cls, row: Dict[str, str]) -> 'MapsResult':
        def parse_json(field_name: str, default=None):
            val = row.get(field_name, '')
            if not val:
                return default
            try:
                return json.loads(val)
            except json.JSONDecodeError:
                return default

        def parse_float(field_name: str, default=0.0):
            val = row.get(field_name, '')
            if not val:
                return default
            try:
                return float(val)
            except ValueError:
                return default

        def parse_int(field_name: str, default=0):
            val = row.get(field_name, '')
            if not val:
                return default
            try:
                return int(float(val))
            except ValueError:
                return default
        
        return cls(
            input_id=row.get('input_id', ''),
            link=row.get('link', ''),
            title=row.get('title', ''),
            category=row.get('category', ''),
            address=row.get('address', ''),
            open_hours=parse_json('open_hours', {}),
            popular_times=parse_json('popular_times', {}),
            web_site=row.get('website', ''),
            phone=row.get('phone', ''),
            plus_code=row.get('plus_code', ''),
            review_count=parse_int('review_count'),
            review_rating=parse_float('review_rating'),
            reviews_per_rating=parse_json('reviews_per_rating', {}),
            latitude=parse_float('latitude'),
            longitude=parse_float('longitude'),
            cid=row.get('cid', ''),
            status=row.get('status', ''),
            description=row.get('descriptions', ''),
            reviews_link=row.get('reviews_link', ''),
            thumbnail=row.get('thumbnail', ''),
            timezone=row.get('timezone', ''),
            price_range=row.get('price_range', ''),
            data_id=row.get('data_id', ''),
            place_id=row.get('place_id', ''),
            images=parse_json('images', []),
            reservations=parse_json('reservations', []),
            order_online=parse_json('order_online', []),
            menu=parse_json('menu', {}),
            owner=parse_json('owner', {}),
            complete_address=parse_json('complete_address', {}),
            about=parse_json('about', []),
            user_reviews=parse_json('user_reviews', []),
            emails=parse_json('emails', []) if row.get('emails') and row.get('emails').startswith('[') else ([x.strip() for x in row.get('emails', '').split(',')] if row.get('emails') else [])
        )

class MapsScraperClient:
    def __init__(self, base_url: str, timeout_s: int = 300) -> None:
        self.base_url = base_url.rstrip('/')
        self.timeout_s = timeout_s

    async def submit_job(
        self,
        query: str,
        *,
        lang: str = "en",
        zoom: int = 15,
        radius: int = 5000,
        depth: int = 1,
        fast_mode: bool = False,
        lat: str = "0",
        lon: str = "0",
        max_time: int = 180,
        proxies: Optional[List[str]] = None
    ) -> str:
        url = f"{self.base_url}/api/v1/jobs"
        real_payload = {
            "name": f"Job for {query}",
            "keywords": [query],
            "lang": lang,
            "zoom": zoom,
            "radius": radius,
            "depth": depth,
            "fast_mode": fast_mode,
            "lat": lat,
            "lon": lon,
            "max_time": max_time,
            "email": False,
        }
        
        if proxies:
            real_payload["proxies"] = proxies
            
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=real_payload, timeout=30) as response:
                if response.status != 201:
                    text = await response.text()
                    raise RuntimeError(f"Failed to submit job: {response.status} {text}")
                data = await response.json()
                return data['id']

    async def wait_for_job(self, job_id: str, *, poll_s: int = 5) -> Dict[str, Any]:
        url = f"{self.base_url}/api/v1/jobs/{job_id}"
        start_time = time.time()
        
        async with aiohttp.ClientSession() as session:
            while True:
                if time.time() - start_time > self.timeout_s:
                    raise TimeoutError(f"Job {job_id} timed out")
                
                async with session.get(url) as response:
                    if response.status == 404:
                         pass
                    elif response.status != 200:
                         logger.warning(f"Error checking job status: {response.status}")
                    else:
                        data = await response.json()
                        status = data.get('status')
                        if status == 'finished' or status == 'ok':
                             return data
                        if status == 'failed' or status == 'error':
                            raise RuntimeError(f"Job {job_id} failed: {data.get('error')}")
                
                await asyncio.sleep(poll_s)

    async def download_csv(self, job_id: str) -> str:
        url = f"{self.base_url}/api/v1/jobs/{job_id}/download"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.text()

    async def fetch_places(self, query: str, **kwargs) -> List[MapsResult]:
        """
        High-level method to submit a job, wait for it, and return parsed results.
        """
        job_id = await self.submit_job(query, **kwargs)
        logger.info(f"Job submitted: {job_id}")
        
        await self.wait_for_job(job_id)
        logger.info(f"Job finished")
        
        csv_content = await self.download_csv(job_id)
        
        results = []
        reader = csv.DictReader(io.StringIO(csv_content))
        for row in reader:
            results.append(MapsResult.from_csv_row(row))
            
        return results
```

### 2. Configure Environment Variables
Update your configuration (e.g., `.env`) to include:

*   `MAPS_SCRAPER_URL`: URL of the `google-maps-scraper` service (e.g., `http://localhost:8080`).

### 3. Update Phase 3 Logic
In `niche-sieve`, locate the Phase 3 logic (where DataForSEO is currently called). Replace it with the `MapsScraperClient`:

```python
# Example integration
from niche_sieve.clients.maps_scraper import MapsScraperClient

async def get_maps_results(query: str, config):
    client = MapsScraperClient(base_url=config.MAPS_SCRAPER_URL)
    
    results = await client.fetch_places(
        query=query,
        lang="en",
        zoom=15,
        radius=5000,
        depth=1,
        fast_mode=False  # Set to True if detailed data isn't needed
    )
    return results
```

## Verification
1.  Ensure the `google-maps-scraper` service is running with `SCRAPOXY_PROXY_URL` set.
2.  Run the `niche-sieve` process (or a specific test script) that triggers Phase 3.
3.  Verify that jobs are created on the scraper (check scraper logs).
4.  Verify that results are returned and parsed correctly into `MapsResult` objects.

## Proxy Reliability Features (High Concurrency)
Recent updates add adaptive proxy selection and health tracking to improve reliability when proxy quality is mixed or poor.

### New CLI Flags
- `-proxy-selection` selects proxies using `random`, `round_robin`, or `least_failed` (recommended: `least_failed` for bad proxies).
- `-proxy-validation-rate` samples connectivity checks per attempt (0-1).
- `-proxy-cooldown-base` and `-proxy-cooldown-max` apply exponential backoff after failures.
- `-proxy-reselect-on-validation-failure` and `-proxy-reselect-attempts` reselect proxies when validation fails.
- `-proxy-refresh-on-failure` and `-proxy-refresh-failure-threshold` trigger on-demand refresh of the proxy list.

### Recommended High-Concurrency Scrapoxy Settings
```bash
export SCRAPOXY_PROXY_URL="http://project-user:project-pass@scrapoxy-host:8888"
./google-maps-scraper \
  -web \
  -addr :8080 \
  -data-folder webdata \
  -c 20 \
  -proxy-selection least_failed \
  -proxy-validation-rate 0.25 \
  -proxy-cooldown-base 2s \
  -proxy-cooldown-max 2m \
  -proxy-refresh-on-failure \
  -proxy-refresh-failure-threshold 10
```

### Notes
- If `SCRAPOXY_PROXY_URL` points to a single Scrapoxy entrypoint, each new connection will rotate upstream even if the local proxy list is a single entry.
- For Scrapoxy lists, the selector will rotate away from failing proxies, rather than always using the first entry.
