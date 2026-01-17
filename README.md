# Google Maps Scraper (Educational Fork)

> **This fork exists for educational and research purposes only.**

This is a fork of [gosom/google-maps-scraper](https://github.com/gosom/google-maps-scraper) with fixes for review extraction issues.

## Disclaimer

This software is provided for **educational and research purposes only**. Users are responsible for ensuring their use complies with applicable laws, regulations, and terms of service. The authors do not condone or encourage any use that violates third-party terms of service.

## Why This Fork Exists

The upstream scraper has issues with extracting reviews (see issues [#205](https://github.com/gosom/google-maps-scraper/issues/205), [#195](https://github.com/gosom/google-maps-scraper/issues/195), [#185](https://github.com/gosom/google-maps-scraper/issues/185), [#156](https://github.com/gosom/google-maps-scraper/issues/156)). This fork includes fixes submitted as PRs:

- **[PR #207](https://github.com/gosom/google-maps-scraper/pull/207)**: Fix `-extra-reviews` by using browser-based RPC requests
- **[PR #208](https://github.com/gosom/google-maps-scraper/pull/208)**: Fix inline review parsing

---

## Quick Start

### Docker

```bash
docker pull ghcr.io/dredozubov/google-maps-scraper:latest

echo "pizza manhattan" > queries.txt
docker run -v $(pwd):/data ghcr.io/dredozubov/google-maps-scraper:latest \
  -input /data/queries.txt \
  -results /data/results.json \
  -json \
  -extra-reviews
```

### Build from Source

```bash
git clone https://github.com/dredozubov/google-maps-scraper.git
cd google-maps-scraper
go build
./google-maps-scraper -input queries.txt -json -extra-reviews
```

### Web UI

```bash
./google-maps-scraper -web -addr :8080 -data-folder webdata
```

Then open http://localhost:8080 in your browser.

---

## Configuration

### Command Line Options

```
Usage: google-maps-scraper [options]

Core Options:
  -input string       Path to input file with queries (one per line)
  -results string     Output file path (default: stdout)
  -json              Output JSON instead of CSV
  -depth int         Max scroll depth in results (default: 10)
  -c int             Concurrency level (default: half of CPU cores)

Email & Reviews:
  -email             Extract emails from business websites
  -extra-reviews     Collect extended reviews (up to ~300)

Location Settings:
  -lang string       Language code, e.g., 'de' for German (default: "en")
  -geo string        Coordinates for search, e.g., '37.7749,-122.4194'
  -zoom int          Zoom level 0-21 (default: 15)
  -radius float      Search radius in meters (default: 10000)

Web Server:
  -web               Run web server mode
  -addr string       Server address (default: ":8080")
  -data-folder       Data folder for web runner (default: "webdata")

Database:
  -dsn string        PostgreSQL connection string
  -produce           Produce seed jobs only (requires -dsn)

Proxy:
  -proxies string                      Comma-separated proxy list
                                       Format: protocol://user:pass@host:port
  -proxy-selection string              Proxy selection strategy: random, round_robin, least_failed (default: random)
  -proxy-validation-rate float         Fraction of attempts to validate proxy connectivity, 0-1 (default: 0)
  -proxy-cooldown-base duration        Base cooldown after proxy failure (default: 2s)
  -proxy-cooldown-max duration         Max cooldown after proxy failure (default: 2m)
  -proxy-reselect-on-validation-failure bool  Reselect proxy when validation fails (default: true)
  -proxy-reselect-attempts int         Max reselections after proxy validation failure (default: 2)
  -proxy-refresh-interval duration     Interval to refresh proxy list from URL, 0 to disable (default: 0)
  -proxy-refresh-on-failure bool       Refresh proxy list on repeated proxy failures (default: false)
  -proxy-refresh-failure-threshold int Proxy failures before triggering on-demand refresh (default: 10)

Advanced:
  -exit-on-inactivity duration    Exit after inactivity (e.g., '5m')
  -fast-mode                      Quick mode with reduced data
  -debug                          Show browser window
```

Run `./google-maps-scraper -h` for the complete list.

### Using Proxies

```bash
./google-maps-scraper \
  -input queries.txt \
  -results results.csv \
  -proxies 'socks5://user:pass@host:port,http://host2:port2' \
  -depth 1 -c 2
```

**Scrapoxy**

Scrapoxy uses a **single gateway model** where the proxy URL is the endpoint itself (not a URL that serves a list of proxies). Scrapoxy handles upstream proxy rotation and health management internally.

```bash
export SCRAPOXY_PROXY_URL="http://project-user:project-pass@scrapoxy-host:8888"
./google-maps-scraper -input queries.txt -results results.csv -depth 1 -c 20
```

> **Note:** Most proxy management flags (`-proxy-selection`, `-proxy-validation-rate`, `-proxy-refresh-*`, etc.) have no effect with Scrapoxy since there's only one endpoint and Scrapoxy manages upstream health. These flags are designed for multi-proxy setups with a dedicated proxy list service.

**Multi-Proxy Setup**

If you have a dedicated proxy list service that returns multiple proxy URLs:

```bash
export SCRAPOXY_PROXY_URL="http://proxy-list-service.example/api/proxies"
./google-maps-scraper \
  -input queries.txt \
  -results results.csv \
  -depth 1 \
  -c 20 \
  -proxy-selection least_failed \
  -proxy-validation-rate 0.25 \
  -proxy-refresh-interval 5m
```

---

## REST API

When running the web server, a REST API is available:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/jobs` | POST | Create a new scraping job |
| `/api/v1/jobs` | GET | List all jobs |
| `/api/v1/jobs/{id}` | GET | Get job details |
| `/api/v1/jobs/{id}` | DELETE | Delete a job |
| `/api/v1/jobs/{id}/download` | GET | Download results as CSV |

API documentation available at http://localhost:8080/api/docs

---

## Extracted Data Points

| Field | Description |
|-------|-------------|
| `title` | Business name |
| `category` | Business type |
| `address` | Street address |
| `phone` | Contact phone number |
| `website` | Official business website |
| `review_count` | Total number of reviews |
| `review_rating` | Average star rating |
| `latitude` / `longitude` | GPS coordinates |
| `open_hours` | Operating hours |
| `user_reviews` | Customer reviews (with `-extra-reviews`) |
| `emails` | Extracted emails (with `-email` flag) |

See upstream documentation for the complete list of 33+ data points.

---

## PostgreSQL for Distributed Scraping

For distributed scraping across multiple machines:

```bash
# 1. Start PostgreSQL
docker-compose -f docker-compose.dev.yaml up -d

# 2. Seed jobs
./google-maps-scraper \
  -dsn "postgres://postgres:postgres@localhost:5432/postgres" \
  -produce \
  -input queries.txt

# 3. Run scrapers (on multiple machines)
./google-maps-scraper \
  -c 2 \
  -depth 1 \
  -dsn "postgres://postgres:postgres@localhost:5432/postgres"
```

---

## References

- [Upstream repository](https://github.com/gosom/google-maps-scraper)
- [scrapemate](https://github.com/gosom/scrapemate) - The underlying web crawling framework

---

## License

This project is licensed under the [MIT License](LICENSE).

---

## Legal Notice

This software is provided for **educational and research purposes only**. Users are solely responsible for ensuring their use complies with all applicable laws, regulations, and third-party terms of service. The authors make no representations regarding the legality of any particular use case.
