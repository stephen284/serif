"""
Serif — PostHog No-Trial Signup → Instantly Campaign Sync
Runs every 4 hours via Railway cron. Finds signups in the last 25 hours
who never started a trial, and routes them into the right Instantly campaign
based on email domain (business vs personal).
"""

import os
import re
import sys
import json
import logging
import requests
from datetime import datetime

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config (set as Railway environment variables) ─────────────────────────────
POSTHOG_API_KEY       = os.environ["POSTHOG_API_KEY"]
POSTHOG_PROJECT_ID    = os.environ.get("POSTHOG_PROJECT_ID", "41680")
INSTANTLY_API_KEY     = os.environ["INSTANTLY_API_KEY"]
BUSINESS_CAMPAIGN_ID  = os.environ.get("BUSINESS_CAMPAIGN_ID",  "a8c19c1e-9d5a-456b-b4a6-c05be83257b4")
PERSONAL_CAMPAIGN_ID  = os.environ.get("PERSONAL_CAMPAIGN_ID",  "cb0bb24c-4607-4b56-bfc5-3c95952a361a")
LOOKBACK_HOURS        = int(os.environ.get("LOOKBACK_HOURS", "25"))

# ── Constants ─────────────────────────────────────────────────────────────────
PERSONAL_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com",
    "live.com", "aol.com", "proton.me", "protonmail.com", "me.com", "mac.com",
    "googlemail.com", "yahoo.co.uk", "hotmail.co.uk", "msn.com", "ymail.com",
    "mail.com", "inbox.com", "zohomail.com", "fastmail.com", "hey.com", "pm.me",
}

BLOCKED_DOMAINS = {"fyxer.com", "fyxer.ai"}

BOT_PATTERN = re.compile(r"test|bot|spam|fake|noreply|no-reply", re.IGNORECASE)

POSTHOG_QUERY = """
SELECT
    person.properties.email  AS email,
    person.properties.name   AS name
FROM events
WHERE
    event = 'User Signup'
    AND person.properties.email IS NOT NULL
    AND person.properties.email != ''
    AND timestamp >= now() - INTERVAL {hours} HOUR
    AND person.id NOT IN (
        SELECT DISTINCT person_id FROM events WHERE event = 'StripeV2 Trial Started'
    )
    AND person.id NOT IN (
        SELECT DISTINCT person_id FROM events WHERE event = 'StripeV2 Credit Card Added'
    )
GROUP BY email, name
ORDER BY min(timestamp) DESC
LIMIT 500
""".strip()


# ── PostHog ───────────────────────────────────────────────────────────────────
def fetch_notrial_signups(hours: int) -> list[dict]:
    url = f"https://us.posthog.com/api/projects/{POSTHOG_PROJECT_ID}/query"
    payload = {
        "query": {
            "kind": "HogQLQuery",
            "query": POSTHOG_QUERY.format(hours=hours),
        }
    }
    resp = requests.post(
        url,
        json=payload,
        headers={
            "Authorization": f"Bearer {POSTHOG_API_KEY}",
            "Content-Type": "application/json",
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("error"):
        raise RuntimeError(f"PostHog query error: {data['error']}")
    return data.get("results", [])


# ── Categorisation ────────────────────────────────────────────────────────────
def categorise(rows: list) -> tuple[list, list, int]:
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
INSTANTLY_HEADERS = {
    "Content-Type": "application/json",
}

def push_to_instantly(campaign_id: str, leads: list[dict]) -> dict:
    """Push leads in batches of 100. Returns aggregate stats."""
    if not leads:
        return {"uploaded": 0, "skipped": 0}

    total_uploaded = 0
    total_skipped  = 0
    batch_size     = 100

    for i in range(0, len(leads), batch_size):
        batch = leads[i : i + batch_size]
        payload = {
            "campaign_id":          campaign_id,
            "skip_if_in_workspace": True,
            "leads":                batch,
        }
        resp = requests.post(
            "https://api.instantly.ai/api/v2/leads/bulk-assign",
            json=payload,
            headers={**INSTANTLY_HEADERS, "Authorization": f"Bearer {INSTANTLY_API_KEY}"},
            timeout=30,
        )

        if not resp.ok:
            # Fall back to single-lead endpoint if bulk isn't available
            log.warning("Bulk endpoint returned %s, falling back to single-lead adds", resp.status_code)
            uploaded, skipped = _push_leads_individually(campaign_id, batch)
            total_uploaded += uploaded
            total_skipped  += skipped
        else:
            result = resp.json()
            total_uploaded += result.get("leads_uploaded", len(batch))
            total_skipped  += result.get("skipped_count", 0) + result.get("duplicated_leads", 0)

    return {"uploaded": total_uploaded, "skipped": total_skipped}


def _push_leads_individually(campaign_id: str, leads: list[dict]) -> tuple[int, int]:
    uploaded = skipped = 0
    for lead in leads:
        payload = {**lead, "campaign_id": campaign_id, "skip_if_in_workspace": True}
        resp = requests.post(
            "https://api.instantly.ai/api/v2/leads",
            json=payload,
            headers={**INSTANTLY_HEADERS, "Authorization": f"Bearer {INSTANTLY_API_KEY}"},
            timeout=15,
        )
        if resp.ok:
            uploaded += 1
        elif resp.status_code == 409:
            skipped += 1
        else:
            log.warning("Failed to add %s: %s %s", lead["email"], resp.status_code, resp.text[:120])
    return uploaded, skipped


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("=== Serif No-Trial Sync starting (lookback: %dh) ===", LOOKBACK_HOURS)
    start = datetime.utcnow()

    # 1. Fetch
    try:
        rows = fetch_notrial_signups(LOOKBACK_HOURS)
    except Exception as exc:
        log.error("PostHog fetch failed: %s", exc)
        sys.exit(1)

    log.info("PostHog returned %d rows", len(rows))

    if len(rows) < 1:
        log.info("Nothing to do — exiting cleanly.")
        return

    # 2. Categorise
    business, personal, bots = categorise(rows)
    log.info(
        "Categorised → business: %d, personal: %d, bots/skipped: %d",
        len(business), len(personal), bots,
    )

    # 3. Push
    biz_result = per_result = {"uploaded": 0, "skipped": 0}

    if business:
        log.info("Pushing %d business leads…", len(business))
        try:
            biz_result = push_to_instantly(BUSINESS_CAMPAIGN_ID, business)
            log.info("Business → uploaded: %d, skipped: %d", biz_result["uploaded"], biz_result["skipped"])
        except Exception as exc:
            log.error("Business push failed: %s", exc)

    if personal:
        log.info("Pushing %d personal leads…", len(personal))
        try:
            per_result = push_to_instantly(PERSONAL_CAMPAIGN_ID, personal)
            log.info("Personal → uploaded: %d, skipped: %d", per_result["uploaded"], per_result["skipped"])
        except Exception as exc:
            log.error("Personal push failed: %s", exc)

    elapsed = (datetime.utcnow() - start).total_seconds()
    log.info(
        "=== Done in %.1fs | Business: +%d | Personal: +%d ===",
        elapsed, biz_result["uploaded"], per_result["uploaded"],
    )


if __name__ == "__main__":
    main()
