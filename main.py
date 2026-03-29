"""
Serif — PostHog No-Trial Signup → Instantly Campaign Sync
Runs every 4 hours via Railway cron. Finds signups in the last 4 hours
who haven't started a trial, and routes them into the right Instantly campaign
based on email domain (business vs personal).
"""
import os
import re
import sys
import time
import logging
import requests
from datetime import datetime, timezone

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config (set as Railway environment variables) ─────────────────────────────
POSTHOG_API_KEY      = os.environ["POSTHOG_API_KEY"]
POSTHOG_PROJECT_ID   = os.environ.get("POSTHOG_PROJECT_ID", "41680")
INSTANTLY_API_KEY    = os.environ["INSTANTLY_API_KEY"]
BUSINESS_CAMPAIGN_ID = os.environ.get("BUSINESS_CAMPAIGN_ID", "a8c19c1e-9d5a-456b-b4a6-c05be83257b4")
PERSONAL_CAMPAIGN_ID = os.environ.get("PERSONAL_CAMPAIGN_ID", "cb0bb24c-4607-4b56-bfc5-3c95952a361a")
LOOKBACK_HOURS       = int(os.environ.get("LOOKBACK_HOURS", "4"))
POSTHOG_TIMEOUT      = int(os.environ.get("POSTHOG_TIMEOUT", "60"))

# ── Constants ─────────────────────────────────────────────────────────────────
PERSONAL_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com",
    "live.com", "aol.com", "proton.me", "protonmail.com", "me.com",
    "mac.com", "googlemail.com", "yahoo.co.uk", "hotmail.co.uk", "msn.com",
    "ymail.com", "mail.com", "inbox.com", "zohomail.com", "fastmail.com",
    "hey.com", "pm.me",
}
BLOCKED_DOMAINS = {"fyxer.com", "fyxer.ai"}
BOT_PATTERN = re.compile(r"test|bot|spam|fake|noreply|no-reply", re.IGNORECASE)

# ── PostHog queries ───────────────────────────────────────────────────────────
#
# Previous approach used a single HogQL query with two correlated NOT IN
# subqueries scanning the ENTIRE events table with no time bounds — this caused
# consistent 504 Gateway Timeout errors on every run.
#
# Fix: two independent flat queries joined in Python (no correlated subqueries).
# ClickHouse handles simple bounded flat scans efficiently.
#
# Query 1: People who have converted (trial or CC added) in the last 90 days.
# Bounded — flat scan, no subquery. Anyone converted >90 days ago is not a
# relevant new-signup prospect.
_CONVERTED_QUERY = """
SELECT DISTINCT person.properties.email AS email
FROM events
WHERE event IN ('StripeV2 Trial Started', 'StripeV2 Credit Card Added')
  AND person.properties.email IS NOT NULL
  AND person.properties.email != ''
  AND timestamp >= now() - INTERVAL 90 DAY
""".strip()

# Query 2: Signups in the last N hours (matches cron interval exactly).
# Flat scan bounded to the lookback window — fast.
_SIGNUPS_QUERY = """
SELECT
    person.properties.email AS email,
    person.properties.name  AS name
FROM events
WHERE event = 'User Signup'
  AND person.properties.email IS NOT NULL
  AND person.properties.email != ''
  AND timestamp >= now() - INTERVAL {hours} HOUR
GROUP BY email, name
ORDER BY min(timestamp) DESC
LIMIT 500
""".strip()


def _run_hogql(query: str) -> list:
    """Execute a HogQL query with up to 3 retries on transient 5xx / timeout."""
    url = f"https://us.posthog.com/api/projects/{POSTHOG_PROJECT_ID}/query"
    payload = {"query": {"kind": "HogQLQuery", "query": query}}
    headers = {
        "Authorization": f"Bearer {POSTHOG_API_KEY}",
        "Content-Type": "application/json",
    }
    last_exc = None
    for attempt in range(3):
        try:
            if attempt:
                wait = attempt * 10
                log.info("Retrying PostHog query in %ds (attempt %d/3)...", wait, attempt + 1)
                time.sleep(wait)
            resp = requests.post(url, json=payload, headers=headers, timeout=POSTHOG_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            if data.get("error"):
                raise RuntimeError(f"PostHog query error: {data['error']}")
            return data.get("results", [])
        except (requests.Timeout, requests.HTTPError) as exc:
            last_exc = exc
            log.warning("PostHog attempt %d/%d failed: %s", attempt + 1, 3, exc)
        except requests.RequestException as exc:
            raise  # Non-transient network failure — propagate immediately
    raise last_exc  # All retries exhausted


def fetch_notrial_signups(hours: int) -> list:
    """
    Returns [email, name] rows for users who signed up in the last `hours`
    hours and have NOT started a trial or added a CC in the last 90 days.
    Two flat queries, joined in Python — no correlated subqueries, no timeouts.
    """
    # Step 1: get converted user emails (flat, bounded to 90 days)
    log.info("Fetching converted users (last 90 days)...")
    converted_rows = _run_hogql(_CONVERTED_QUERY)
    converted_emails = {row[0].strip().lower() for row in converted_rows if row and row[0]}
    log.info("Converted users found: %d", len(converted_emails))

    # Step 2: get recent signups (flat, bounded to lookback window)
    log.info("Fetching signups in last %dh...", hours)
    signup_rows = _run_hogql(_SIGNUPS_QUERY.format(hours=hours))
    log.info("Raw signups found: %d", len(signup_rows))

    # Step 3: Python set subtraction — exclude anyone who already converted
    filtered = [
        row for row in signup_rows
        if row and row[0] and row[0].strip().lower() not in converted_emails
    ]
    log.info("After removing converted users: %d to process", len(filtered))
    return filtered


# ── Categorisation ────────────────────────────────────────────────────────────
def categorise(rows: list) -> tuple:
    """Returns (business_leads, personal_leads, bot_count)."""
    business, personal, bots = [], [], 0
    for row in rows:
        email, name = row[0], row[1] or ""
        email = email.strip().lower()
        if not email or BOT_PATTERN.search(email) or "+" in email:
            bots += 1
            continue
        parts = email.split("@")
        if len(parts) != 2:
            bots += 1
            continue
        domain = parts[1]
        if domain in BLOCKED_DOMAINS:
            log.info("Skipping blocked domain: %s", email)
            continue
        name_parts = name.strip().split() if name.strip() else []
        first = name_parts[0] if name_parts else email.split("@")[0]
        last  = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
        lead = {
            "email":        email,
            "first_name":   first,
            "last_name":    last,
            "company_name": domain,
        }
        if domain in PERSONAL_DOMAINS:
            personal.append(lead)
        else:
            business.append(lead)
    return business, personal, bots


# ── Instantly ─────────────────────────────────────────────────────────────────
INSTANTLY_HEADERS = {"Content-Type": "application/json"}


def push_to_instantly(campaign_id: str, leads: list) -> dict:
    """Push leads one at a time via the v2 single-lead endpoint."""
    if not leads:
        return {"uploaded": 0, "skipped": 0}
    uploaded = skipped = 0
    for lead in leads:
        payload = {**lead, "campaign_id": campaign_id, "skip_if_in_workspace": True}
        try:
            resp = requests.post(
                "https://api.instantly.ai/api/v2/leads",
                json=payload,
                headers={**INSTANTLY_HEADERS, "Authorization": f"Bearer {INSTANTLY_API_KEY}"},
                timeout=30,
            )
            if resp.ok:
                uploaded += 1
            elif resp.status_code == 409:
                skipped += 1  # Already in workspace — not an error, expected
            else:
                log.warning(
                    "Failed to add %s: HTTP %s – %s",
                    lead["email"], resp.status_code, resp.text[:200],
                )
        except requests.RequestException as exc:
            log.warning("Network error adding %s: %s", lead["email"], exc)
    return {"uploaded": uploaded, "skipped": skipped}


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("=== Serif No-Trial Sync starting (lookback: %dh) ===", LOOKBACK_HOURS)
    start = datetime.now(timezone.utc)

    # 1. Fetch — exit 0 on transient PostHog failures so Railway doesn't alert
    try:
        rows = fetch_notrial_signups(LOOKBACK_HOURS)
    except Exception as exc:
        log.error("PostHog fetch failed — will retry next run: %s", exc)
        logging.shutdown()
        sys.exit(0)

    if not rows:
        log.info("No new no-trial signups in last %dh — nothing to do.", LOOKBACK_HOURS)
        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        log.info("=== Done in %.1fs | Business: +0 | Personal: +0 ===", elapsed)
        logging.shutdown()
        return

    # 2. Categorise
    business, personal, bots = categorise(rows)
    log.info(
        "Categorised → business: %d, personal: %d, bots/skipped: %d",
        len(business), len(personal), bots,
    )

    # 3. Push to Instantly
    biz_result = per_result = {"uploaded": 0, "skipped": 0}

    if business:
        log.info("Pushing %d business leads...", len(business))
        try:
            biz_result = push_to_instantly(BUSINESS_CAMPAIGN_ID, business)
            log.info(
                "Business → uploaded: %d, skipped: %d",
                biz_result["uploaded"], biz_result["skipped"],
            )
        except Exception as exc:
            log.error("Business push failed: %s", exc)

    if personal:
        log.info("Pushing %d personal leads...", len(personal))
        try:
            per_result = push_to_instantly(PERSONAL_CAMPAIGN_ID, personal)
            log.info(
                "Personal → uploaded: %d, skipped: %d",
                per_result["uploaded"], per_result["skipped"],
            )
        except Exception as exc:
            log.error("Personal push failed: %s", exc)

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    log.info(
        "=== Done in %.1fs | Business: +%d | Personal: +%d ===",
        elapsed,
        biz_result["uploaded"],
        per_result["uploaded"],
    )
    logging.shutdown()


if __name__ == "__main__":
    main()
