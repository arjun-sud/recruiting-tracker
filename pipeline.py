#!/usr/bin/env python3
"""
Recruiting tracker pipeline.

Stages, in order:  fetch -> normalize -> filter -> dedupe -> flag-new -> verify -> write

Hard rule: a listing is written to jobs.json ONLY if it was re-fetched at publish
time and confirmed live (HTTP 200 + posting still present). Nothing is invented.
If a source returns nothing, it contributes nothing.

Run modes:
  python pipeline.py --live                              # fetch real APIs (GitHub / open internet)
  python pipeline.py --from-file data/raw_captured.json  # run the logic offline on saved data
"""

import argparse, json, sys, urllib.request, urllib.error
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).parent
DATA = ROOT / "data"
OUT_FILE = DATA / "jobs.json"
SOURCES_FILE = ROOT / "sources.json"
UA = {"User-Agent": "recruiting-tracker/0.1 (personal project)"}

# ---------- filters ----------
INTERN_TERMS = ["intern", "internship", "externship", "extern", "co-op", "coop",
    "summer analyst", "summer associate", "early career", "early talent",
    "student program", "campus", "university program", "fellowship", "apprentice", "trainee"]
BUSINESS_TRACK = ["business", "bizops", "biz ops", "strategy", "strategic",
    "corporate development", "corp dev", "finance", "financial", "fp&a", "growth",
    "operations", "chief of staff", "investment", "analyst", "consulting"]
NON_BUSINESS = ["engineer", "engineering", "software", "developer", "designer",
    "design ", "product manager", "data scientist", "machine learning", "backend",
    "frontend", "devops", "security engineer"]

def _has(text, terms):
    t = (text or "").lower()
    return any(term in t for term in terms)

def is_internship(title): return _has(title, INTERN_TERMS)
def is_business_track(title): return _has(title, BUSINESS_TRACK) and not _has(title, NON_BUSINESS)

def is_us_or_remote(location):
    loc = (location or "").lower()
    if not loc: return False
    if "remote" in loc: return True
    non_us = ["spain","germany","switzerland"," uk","united kingdom","france","india",
              "emirates","dubai","singapore","canada","brazil","netherlands","ireland",
              "australia","japan","mexico","poland","israel"]
    if any(m in loc for m in non_us): return False
    us = ["usa","united states",", ca",", ny",", tx",", ma",", il",", wa",", dc",
          "washington","new york","san francisco","boston","chicago","los angeles",
          "seattle","austin","atlanta","denver","miami","remote"]
    return any(m in loc for m in us)

# ---------- adapters ----------
def _get_json(url, data=None):
    body = json.dumps(data).encode() if data is not None else None
    headers = dict(UA)
    if data is not None: headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)

def fetch_greenhouse(src):
    url = f"https://boards-api.greenhouse.io/v1/boards/{src['token']}/jobs?content=false"
    data = _get_json(url)
    return [{"title": j.get("title",""), "company": src["firm"],
             "location": (j.get("location") or {}).get("name",""),
             "url": j.get("absolute_url",""), "posted": j.get("updated_at",""),
             "source_type": "greenhouse", "confidence": src["confidence"]}
            for j in data.get("jobs", [])]

def fetch_lever(src):
    url = f"https://api.lever.co/v0/postings/{src['token']}?mode=json"
    data = _get_json(url)
    out = []
    for j in data:
        out.append({"title": j.get("text",""), "company": src["firm"],
                    "location": (j.get("categories") or {}).get("location",""),
                    "url": j.get("hostedUrl",""),
                    "posted": datetime.fromtimestamp((j.get("createdAt") or 0)/1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if j.get("createdAt") else "",
                    "source_type": "lever", "confidence": src["confidence"]})
    return out

def fetch_getro(src):
    """Best-effort. Getro serves listings from a search API keyed by collection id.
    If the endpoint/shape is off, this raises and the run logs it (non-fatal)."""
    url = f"https://api.getro.com/api/v2/collections/{src['collection_id']}/search/jobs"
    out, page = [], 0
    while page < 40:  # cap pages
        payload = {"hitsPerPage": 100, "page": page, "query": "intern"}
        data = _get_json(url, payload)
        hits = data.get("results") or data.get("hits") or data.get("jobs") or []
        if not hits: break
        for j in hits:
            out.append({"title": j.get("title") or j.get("name",""),
                        "company": (j.get("organization") or {}).get("name") or j.get("company",""),
                        "location": j.get("locations") and ", ".join(j.get("locations")) or j.get("location",""),
                        "url": j.get("url") or j.get("apply_url",""),
                        "posted": j.get("created_at",""),
                        "source_type": "getro", "confidence": src["confidence"]})
        page += 1
    return out

ADAPTERS = {"greenhouse": fetch_greenhouse, "lever": fetch_lever, "getro": fetch_getro}

# ---------- transform ----------
def normalize(raw):
    return {"title": (raw.get("title") or "").strip(), "company": (raw.get("company") or "").strip(),
            "location": (raw.get("location") or "").strip(), "url": (raw.get("url") or "").strip(),
            "posted": raw.get("posted",""), "source_type": raw.get("source_type",""),
            "confidence": raw.get("confidence","best-effort")}

def passes_filters(j):
    if not is_internship(j["title"]): return False
    if not is_us_or_remote(j["location"]): return False
    if j["source_type"] in ("getro","consider") and not is_business_track(j["title"]): return False
    return True

def dedupe(jobs):
    seen, out = set(), []
    for j in jobs:
        k = j["url"] or (j["company"], j["title"], j["location"])
        if k in seen: continue
        seen.add(k); out.append(j)
    return out

def load_prev_urls():
    if OUT_FILE.exists():
        return {m["url"] for m in json.loads(OUT_FILE.read_text()).get("matches", [])}
    return set()

def verify_live(url):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=30) as r:
            if r.status != 200: return False
            final = r.geturl().lower()
            if "error=true" in final or "not-found" in final: return False
            body = r.read(60000).decode("utf-8","ignore").lower()
            return not ("no longer accepting" in body or "position closed" in body)
    except Exception:
        return False

# ---------- runners ----------
def _write(matches, live_sample, sources_checked, counts, mode):
    out = {"generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "run_mode": mode, "sources_checked": sources_checked,
           "stage_counts": counts, "matches": matches, "live_sample": live_sample}
    DATA.mkdir(exist_ok=True)
    OUT_FILE.write_text(json.dumps(out, indent=2))
    print(json.dumps(counts, indent=2)); print("wrote", OUT_FILE)

def run_live():
    reg = json.loads(SOURCES_FILE.read_text())["sources"]
    prev = load_prev_urls()
    raw, sources_checked, sample_raw = [], [], []
    for src in reg:
        try:
            got = ADAPTERS[src["type"]](src)
            sources_checked.append({"firm": src["firm"], "source_type": src["type"],
                                    "confidence": src["confidence"], "count": len(got), "status": "ok"})
            raw += got
            sample_raw += [g for g in got if is_us_or_remote(g.get("location"))][:8]
        except Exception as e:
            # log failure visibly, keep going (never hide a broken source)
            print(f"[warn] source {src['firm']} ({src['type']}) failed: {e}", file=sys.stderr)
            sources_checked.append({"firm": src["firm"], "source_type": src["type"],
                                    "confidence": src["confidence"], "count": 0, "status": f"failed: {e}"})
    normalized = [normalize(r) for r in raw]
    filtered = dedupe([j for j in normalized if passes_filters(j)])
    for j in filtered: j["is_new"] = j["url"] not in prev
    matches = []
    for j in filtered:
        if verify_live(j["url"]):
            j["verified"] = True; j["verified_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            matches.append(j)
        else:
            print(f"[verify-drop] {j['company']} - {j['title']}", file=sys.stderr)
    live_sample = [normalize(r) for r in sample_raw]
    for j in live_sample: j["is_new"] = j["url"] not in prev
    _write(matches, live_sample[:12], sources_checked,
           {"raw": len(raw), "after_filters": len(filtered), "verified_live": len(matches)},
           "live")

def run_from_file(path):
    payload = json.loads(Path(path).read_text())
    raw, sources_checked = [], []
    for src in payload["sources"]:
        sources_checked.append({"firm": src["firm"], "source_type": src["source_type"],
            "confidence": src["confidence"], "count": len(src["jobs"]),
            "board_total_reported": src.get("board_total_reported")})
        for j in src["jobs"]:
            j = dict(j); j["source_type"] = src["source_type"]; j["confidence"] = src["confidence"]
            j.setdefault("company", src["firm"]); raw.append(j)
    prev = load_prev_urls()
    normalized = [normalize(r) for r in raw]
    filtered = dedupe([j for j in normalized if passes_filters(j)])
    for j in filtered: j["is_new"] = j["url"] not in prev
    live_sample = [normalize(r) for r in raw if is_us_or_remote(r.get("location"))]
    for j in live_sample: j["is_new"] = j["url"] not in prev
    _write([], live_sample, sources_checked,
           {"raw": len(raw), "after_filters": len(filtered), "verified_live": 0},
           "from-file (offline, no live re-verify)")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-file"); ap.add_argument("--live", action="store_true")
    a = ap.parse_args()
    if a.live: run_live()
    elif a.from_file: run_from_file(a.from_file)
    else: ap.print_help()

if __name__ == "__main__":
    main()
