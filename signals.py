#!/usr/bin/env python3
"""Strategic Radar signals layer: non-news evidence for the desks.

Runs after collect.py in the same workflow and writes, under --data:
  signals.json          the current picture (thermal anomalies by watch box, official releases)
  signals_state.json    rolling memory (which 0.1-degree cells burn every day, which items were seen)
  signals/<date>.jsonl  one line per new signal, kept 30 days

Sources (all public, no key):
  NASA FIRMS active-fire files (VIIRS NOAA-20 and MODIS, last 24 h, global CSV)
  US Department of War / Defense news RSS
  official lists in OFFICIAL: Turkish MFA (RSS), QNA, ONA, OFAC recent actions, SPA

Thermal anomalies are not strikes. A hotspot is a heat source seen from orbit: fires, flares, industry,
farming, and sometimes explosions or burning ships. The layer drops cells that burn on most days
(gas flares, refineries, steel plants) and reports what is new, so the desks can check it against claims.
Standard library only.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import hashlib
import io
import json
import re
import ssl
import sys
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

UA = {"User-Agent": "Mozilla/5.0 (StrategicRadar signals; +https://github.com/muratyesiltas-blip/strategic-radar-collector)",
      "Accept-Language": "en"}

FIRMS = {
    "viirs_n20": "https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-20-viirs-c2/csv/J1_VIIRS_C2_Global_24h.csv",
    "modis": "https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/csv/MODIS_C6_1_Global_24h.csv",
}

# Watch boxes: (lat_min, lat_max, lon_min, lon_max), file id(s), plain label
BOXES = {
    "hormuz": ((24.5, 27.8, 54.5, 58.5), ["iran", "gulf"], "Strait of Hormuz and approaches"),
    "gulf_north": ((24.0, 30.5, 47.0, 52.5), ["gulf", "iran"], "Kuwait, Bahrain, Qatar and the Saudi east coast"),
    "iran_south": ((26.0, 31.5, 48.0, 56.0), ["iran"], "Iran's Gulf coast and Khuzestan"),
    "red_sea_yemen": ((12.0, 17.5, 41.0, 46.0), ["mideast", "gulf"], "Hodeidah, the Yemeni Red Sea coast and Bab al-Mandeb"),
    "iraq_kurdistan": ((35.8, 37.5, 42.5, 45.8), ["pkk", "mideast"], "Qandil, Duhok and the Iraq-Türkiye border belt"),
    "syria_ne": ((35.0, 37.3, 38.0, 42.5), ["syria", "pkk"], "Northeast Syria"),
    "levant": ((31.0, 34.8, 34.2, 36.8), ["israel", "mideast", "syria"], "Gaza, Israel, Lebanon and southwest Syria"),
    "black_sea_crimea": ((44.0, 46.5, 32.0, 38.0), ["russia"], "Crimea, Novorossiysk and the Kerch Strait"),
    "ukraine_east": ((46.5, 50.5, 35.0, 40.5), ["russia"], "The front line in eastern and southern Ukraine"),
}

DEFENSE_RSS = "https://www.defense.gov/DesktopModules/ArticleCS/RSS.ashx?ContentType=9&Site=945&max=20"
KEYWORDS = re.compile(r"\b(iran|irgc|houthi|yemen|red sea|hormuz|centcom|iraq|syria|t(u|ü)rk|ankara|black sea|israel|gaza|"
                      r"lebanon|hezbollah|ukrain|russia|crimea|nato|gulf|bahrain|kuwait|qatar|saudi|emirat|oman)\w*", re.I)

# For a country's own news agency its country name says nothing; look for security and foreign-policy words instead
AGENCY_EN = re.compile(r"\b(iran|israel|gaza|palestin|lebanon|syria|iraq|yemen|houthi|red sea|hormuz|t(u|ü)rk|ankara|security|defen[cs]e|"
                       r"military|armed forces|missile|drone|attack|strike|navy|naval|foreign minister|minister of foreign|"
                       r"strategic dialogue|joint statement|ceasefire|sanction|united states|russia|ukrain|nato|gcc)\w*", re.I)
AGENCY_AR = re.compile(r"(إيران|الإيراني|إسرائيل|الإسرائيلي|غزة|فلسطين|لبنان|سوريا|العراق|اليمن|الحوثي|البحر الأحمر|هرمز|تركيا|التركي|"
                       r"أمن|الأمن|عسكري|العسكري|القوات المسلحة|صاروخ|مسيّرة|مسيرة|هجوم|البحرية|وزير الخارجية|الخارجية|وقف إطلاق النار|عقوبات|"
                       r"الولايات المتحدة|روسيا|أوكرانيا|الناتو|مجلس التعاون)")
# Agency stories that name a place but carry no security content (museums, festivals, national-day parties, pensions)
AGENCY_SKIP = re.compile(r"(museum|heritage|festival|exhibition|tourism|tourist|cultur|\bart\b|handicraft|cuisine|\bseason|\bsport|football|"
                         r"marathon|weather|\brain(fall)?\b|retirement|social insurance|pension|national day|celebrat|anniversary of|"
                         r"متحف|تراث|مهرجان|معرض|سياح|ثقاف|الحرف اليدوية|طقس|أمطار|تقاعد|التأمينات|اليوم الوطني|تحتفل|يحتفل|احتفال)", re.I)

PERSIST_DAYS = 7      # memory window for burning cells
PERSIST_MIN = 3       # a cell seen on at least this many of the window's days counts as persistent
HIGH_FRP = 50.0       # MW; a single new point this hot is worth a look on its own
WARMUP_DAYS = 3       # no alerts until the persistent-cell memory has this many days


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def fetch(url: str, timeout: int = 40) -> bytes:
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout, context=ssl.create_default_context()) as r:
        data = r.read()
        if r.headers.get("Content-Encoding") == "gzip":
            data = gzip.decompress(data)
        return data


def in_box(lat: float, lon: float, b) -> bool:
    return b[0] <= lat <= b[1] and b[2] <= lon <= b[3]


def cell(lat: float, lon: float) -> str:
    return f"{round(lat, 1):.1f},{round(lon, 1):.1f}"


def read_firms(name: str, url: str, status: dict) -> list[dict]:
    try:
        raw = fetch(url, timeout=60).decode("utf-8", "ignore")
    except Exception as e:  # noqa: BLE001
        status[name] = f"error: {str(e)[:120]}"
        return []
    rows = []
    for r in csv.DictReader(io.StringIO(raw)):
        try:
            lat, lon = float(r["latitude"]), float(r["longitude"])
        except (KeyError, ValueError):
            continue
        conf = (r.get("confidence") or "").strip().lower()
        if name == "modis":
            try:
                if int(conf) < 50:
                    continue
            except ValueError:
                continue
        elif conf in ("l", "low"):
            continue
        box = next((k for k, (b, _, _) in BOXES.items() if in_box(lat, lon, b)), None)
        if not box:
            continue
        try:
            frp = float(r.get("frp") or 0)
        except ValueError:
            frp = 0.0
        rows.append({"lat": lat, "lon": lon, "box": box, "frp": frp, "sensor": name,
                     "date": r.get("acq_date"), "time": (r.get("acq_time") or "").zfill(4),
                     "daynight": r.get("daynight"), "cell": cell(lat, lon)})
    status[name] = f"ok: {len(rows)} points in watch boxes"
    return rows


def thermal(points: list[dict], state: dict, today: str) -> dict:
    """Group today's points by box, drop persistent cells, report the new ones."""
    hist = state.setdefault("cells", {})           # cell -> list of dates seen
    persistent = {c for c, ds in hist.items() if len(set(ds)) >= PERSIST_MIN}
    out = {}
    for key, (b, files, label) in BOXES.items():
        pts = [p for p in points if p["box"] == key]
        new = [p for p in pts if p["cell"] not in persistent]
        cells_new = sorted({p["cell"] for p in new})
        top = sorted(new, key=lambda p: -p["frp"])[:8]
        out[key] = {"label": label, "files": files, "points": len(pts), "persistentDropped": len(pts) - len(new),
                    "newPoints": len(new), "newCells": len(cells_new), "frpNew": round(sum(p["frp"] for p in new), 1),
                    "top": [{"lat": p["lat"], "lon": p["lon"], "frp": p["frp"], "sensor": p["sensor"],
                             "time": f'{p["date"]} {p["time"][:2]}:{p["time"][2:]}Z', "cell": p["cell"],
                             "map": f'https://firms.modaps.eosdis.nasa.gov/map/#d:24hrs;@{p["lon"]},{p["lat"]},11z'} for p in top]}
    # update memory: today's cells, then trim to the window
    cutoff = (dt.date.fromisoformat(today) - dt.timedelta(days=PERSIST_DAYS)).isoformat()
    for p in points:
        ds = hist.setdefault(p["cell"], [])
        if today not in ds:
            ds.append(today)
    for c in list(hist):
        hist[c] = [d for d in hist[c] if d > cutoff]
        if not hist[c]:
            del hist[c]
    return out


def thermal_alerts(boxes: dict, state: dict) -> list[dict]:
    """New strong heat sources (FRP of at least HIGH_FRP in a cell that does not burn most days).
    Silent for the first WARMUP_DAYS days, while the memory of persistent cells fills."""
    seen = state.setdefault("alerted", {})
    first = state.setdefault("firstRun", now_utc().date().isoformat())
    if (now_utc().date() - dt.date.fromisoformat(first)).days < WARMUP_DAYS:
        return []
    alerts = []
    for key, v in boxes.items():
        for p in v["top"]:
            if p["frp"] < HIGH_FRP:
                continue
            sig = hashlib.sha1(f'{key}{p["cell"]}{p["time"][:10]}'.encode()).hexdigest()[:12]
            if sig in seen:
                continue
            seen[sig] = p["time"][:10]
            alerts.append({"id": f"T-{sig}", "kind": "thermal", "box": key, "label": v["label"], "files": v["files"],
                           "what": f'New heat source of {p["frp"]} MW near {p["lat"]:.2f}, {p["lon"]:.2f} ({p["sensor"]}, {p["time"]})',
                           "lat": p["lat"], "lon": p["lon"], "frp": p["frp"], "map": p["map"],
                           "caution": "A thermal anomaly is not a confirmed strike; check it against reporting and claims."})
    # forget alerts older than 3 days
    cut = (now_utc().date() - dt.timedelta(days=3)).isoformat()
    for k in list(seen):
        if seen[k] < cut:
            del seen[k]
    return alerts


def defense_rss(state: dict, status: dict) -> list[dict]:
    try:
        root = ET.fromstring(fetch(DEFENSE_RSS))
    except Exception as e:  # noqa: BLE001
        status["defense_rss"] = f"error: {str(e)[:120]}"
        return []
    seen = state.setdefault("officialSeen", {})
    items = []
    for it in root.iter("item"):
        title = (it.findtext("title") or "").strip()
        link = (it.findtext("link") or "").strip()
        desc = re.sub(r"<[^>]+>", " ", it.findtext("description") or "")
        pub = it.findtext("pubDate") or ""
        if not title or not link:
            continue
        relevant = bool(KEYWORDS.search(title + " " + desc))
        uid = hashlib.sha1(link.encode()).hexdigest()[:12]
        items.append({"id": f"O-{uid}", "kind": "official", "source": "US Department of War", "domain": "defense.gov",
                      "rel": "A", "title": title, "url": link, "published": pub, "summary": re.sub(r"\s+", " ", desc).strip()[:400],
                      "relevant": relevant, "new": uid not in seen})
        seen.setdefault(uid, now_utc().date().isoformat())
    cut = (now_utc().date() - dt.timedelta(days=30)).isoformat()
    for k in list(seen):
        if seen[k] < cut:
            del seen[k]
    status["defense_rss"] = f"ok: {len(items)} items, {sum(i['relevant'] for i in items)} relevant"
    return items


# Official sources whose lists can be read without a browser. kind: rss | links | nextdata
OFFICIAL = [
    {"key": "mfa_tr", "source": "Türkiye Ministry of Foreign Affairs", "domain": "mfa.gov.tr", "files": ["turkey"],
     "kind": "rss", "url": "https://www.mfa.gov.tr/rss.en.mfa", "all": True, "base": "https://www.mfa.gov.tr/",
     "fallback": "https://www.mfa.gov.tr/default.en.mfa",
     "fallbackPattern": r'<a[^>]+href="((?:https://www\.mfa\.gov\.tr/|/)?[\w\-]+\.en\.mfa)"[^>]*>\s*([^<]{25,250})</a>'},
    {"key": "qna", "agency": True, "source": "Qatar News Agency", "domain": "qna.org.qa", "files": ["gulf"], "kind": "links",
     "url": "https://www.qna.org.qa/en", "pattern": r'<a[^>]+href="(https://qna\.org\.qa/en/News-Area/News/\d{4}-\d{2}/\d{2}/[^"]+)"[^>]*>\s*([^<]{15,200})</a>'},
    {"key": "ona", "source": "Oman News Agency", "domain": "omannews.gov.om", "files": ["gulf"], "kind": "links", "lang": "ar",
     "url": "https://omannews.gov.om/", "base": "https://omannews.gov.om/", "insecure": True,
     "pattern": r'<a[^>]+href="(topics/ar/\d+/show/\d+/ona)"[^>]*>\s*([^<]{10,200})</a>'},
    {"key": "ofac", "source": "US Treasury OFAC", "domain": "ofac.treasury.gov", "files": ["iran", "russia", "gulf"], "kind": "links", "all": True,
     "url": "https://ofac.treasury.gov/recent-actions", "base": "https://ofac.treasury.gov",
     "pattern": r'<a[^>]+href="(/recent-actions/\d{8}[^"]*)"[^>]*>\s*([^<]{8,200})</a>'},
    {"key": "spa", "agency": True, "source": "Saudi Press Agency", "domain": "spa.gov.sa", "files": ["gulf"], "kind": "nextdata",
     "url": "https://www.spa.gov.sa/en", "base": "https://www.spa.gov.sa/en/"},
]


def _get_text(url: str, insecure: bool = False) -> str:
    req = urllib.request.Request(url, headers=UA)
    ctx = ssl._create_unverified_context() if insecure else ssl.create_default_context()  # noqa: S323 (ONA has a broken chain)
    with urllib.request.urlopen(req, timeout=30, context=ctx) as r:
        return r.read().decode("utf-8", "ignore")


def _nextdata_items(h: str, base: str) -> list[tuple[str, str, str]]:
    """Walk a Next.js page's embedded JSON for news-like objects: a title plus an id or slug."""
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', h, re.S)
    if not m:
        return []
    out, seen = [], set()
    def walk(o):
        if isinstance(o, dict):
            title = o.get("title") or o.get("headline")
            ident = o.get("slug") or o.get("uuid") or o.get("id") or o.get("newsId")
            date = o.get("publishedAt") or o.get("publishDate") or o.get("date") or o.get("createdAt") or ""
            if isinstance(title, str) and len(title) > 25 and ident is not None and str(ident) not in seen:
                seen.add(str(ident))
                link = o.get("url") or o.get("link") or f"{base}{o.get('slug') or ident}"
                if isinstance(link, str) and link.startswith("/"):
                    link = base.split("/en/")[0] + link
                out.append((str(link), title.strip(), str(date)))
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    try:
        walk(json.loads(m.group(1)))
    except ValueError:
        return []
    return out[:30]


def official_pages(state: dict, status: dict) -> list[dict]:
    seen = state.setdefault("officialSeen", {})
    items = []
    for src in OFFICIAL:
        try:
            h = _get_text(src["url"], src.get("insecure", False))
        except Exception as e:  # noqa: BLE001
            status[src["key"]] = f"error: {str(e)[:120]}"
            continue
        rows = []
        if src["kind"] == "rss":
            try:
                root = ET.fromstring(h.encode("utf-8"))
                for it in root.iter("item"):
                    rows.append(((it.findtext("link") or "").strip(), (it.findtext("title") or "").strip(), it.findtext("pubDate") or ""))
            except ET.ParseError:
                # some ministry feeds are not well-formed XML; read the items with a pattern instead
                for blk in re.findall(r"<item>(.*?)</item>", h, re.S | re.I):
                    lk = re.search(r"<link>\s*(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?\s*</link>", blk, re.S | re.I)
                    tt = re.search(r"<title>\s*(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?\s*</title>", blk, re.S | re.I)
                    pd = re.search(r"<pubDate>(.*?)</pubDate>", blk, re.S | re.I)
                    if lk and tt:
                        rows.append((lk.group(1).strip(), re.sub(r"<[^>]+>", "", tt.group(1)).strip(), pd.group(1).strip() if pd else ""))
            if not rows:
                # Atom feeds use <entry> and <link href="...">
                for blk in re.findall(r"<entry[\s>](.*?)</entry>", h, re.S | re.I):
                    lk = re.search(r'<link[^>]+href="([^"]+)"', blk, re.I)
                    tt = re.search(r"<title[^>]*>\s*(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?\s*</title>", blk, re.S | re.I)
                    pd = re.search(r"<(?:updated|published)>(.*?)</", blk, re.S | re.I)
                    if lk and tt:
                        rows.append((lk.group(1).strip(), re.sub(r"<[^>]+>", "", tt.group(1)).strip(), pd.group(1).strip() if pd else ""))
            if not rows and src.get("fallback"):
                # the feed came back empty or as an HTML page: read the ministry's own list page instead
                diag = re.sub(r"\s+", " ", h[:160])
                try:
                    h2 = _get_text(src["fallback"])
                    for u, ttl in re.findall(src["fallbackPattern"], h2):
                        u = u if u.startswith("http") else src.get("base", "") + u.lstrip("/")
                        rows.append((u, re.sub(r"\s+", " ", ttl).strip(), ""))
                except Exception as e:  # noqa: BLE001
                    diag += f" | fallback error: {str(e)[:80]}"
                if not rows:
                    status[src["key"] + "_diag"] = f"feed {len(h)} chars, starts: {diag}"
        elif src["kind"] == "links":
            for u, ttl in re.findall(src["pattern"], h):
                u = u if u.startswith("http") else src.get("base", "") + u
                rows.append((u, re.sub(r"\s+", " ", ttl).strip(), ""))
        elif src["kind"] == "nextdata":
            rows = _nextdata_items(h, src.get("base", ""))
        dedup, n = set(), 0
        for link, title, pub in rows:
            if not link or not title or link in dedup:
                continue
            dedup.add(link)
            import html as _h
            title = _h.unescape(title)
            if src.get("all"):
                relevant = True
            elif (src.get("agency") or src.get("lang") == "ar") and AGENCY_SKIP.search(title):
                relevant = False
            elif src.get("lang") == "ar":
                relevant = bool(AGENCY_AR.search(title))
            elif src.get("agency"):
                relevant = bool(AGENCY_EN.search(title))
            else:
                relevant = bool(KEYWORDS.search(title))
            uid = hashlib.sha1(link.encode()).hexdigest()[:12]
            items.append({"id": f"O-{uid}", "kind": "official", "source": src["source"], "domain": src["domain"], "rel": "A",
                          "files": src["files"], "lang": src.get("lang", "en"), "title": title, "url": link, "published": pub,
                          "relevant": bool(relevant), "new": uid not in seen})
            seen.setdefault(uid, now_utc().date().isoformat())
            n += 1
            if n >= 25:
                break
        status[src["key"]] = f"ok: {n} items" if n else "ok but no items parsed"
    return items


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    a = ap.parse_args()
    out = Path(a.data)
    out.mkdir(parents=True, exist_ok=True)
    sp = out / "signals_state.json"
    state = json.loads(sp.read_text()) if sp.exists() else {}
    status: dict = {}
    t = now_utc()
    today = t.date().isoformat()

    points = []
    for name, url in FIRMS.items():
        points += read_firms(name, url, status)
    boxes = thermal(points, state, today) if points else {}
    if "firstRun" not in state:
        state["recentAlerts"] = []   # drop alerts raised before the warm-up rule existed
    alerts = thermal_alerts(boxes, state) if boxes else []
    # keep the last 48 hours of alerts in the picture, so a task that reads later still sees them
    recent = [x for x in state.get("recentAlerts", []) if x.get("seenAt", "") >= (t - dt.timedelta(hours=48)).isoformat(timespec="seconds")]
    recent += [dict(x, seenAt=t.isoformat(timespec="seconds")) for x in alerts]
    state["recentAlerts"] = recent[-60:]
    official = defense_rss(state, status) + official_pages(state, status)

    sig = {"generatedAt": t.isoformat(timespec="seconds"), "version": 1,
           "note": "Machine-collected signals. Thermal anomalies are heat sources seen from orbit, not confirmed strikes; "
                   "persistent cells (seen on at least %d of the last %d days) are dropped as flares or industry." % (PERSIST_MIN, PERSIST_DAYS),
           "thermal": boxes, "alerts": state["recentAlerts"], "newAlerts": len(alerts), "official": official, "status": status}
    (out / "signals.json").write_text(json.dumps(sig, ensure_ascii=False, indent=1))
    sp.write_text(json.dumps(state, ensure_ascii=False))

    arch = out / "signals"
    arch.mkdir(exist_ok=True)
    new_lines = alerts + [o for o in official if o["new"] and o["relevant"]]
    if new_lines:
        with open(arch / f"{today}.jsonl", "a", encoding="utf-8") as f:
            for x in new_lines:
                f.write(json.dumps(dict(x, seenAt=t.isoformat(timespec="seconds")), ensure_ascii=False) + "\n")
    cut = (t.date() - dt.timedelta(days=30)).isoformat()
    for f in arch.glob("*.jsonl"):
        if f.stem < cut:
            f.unlink()
    print(f"signals: {len(points)} box points, {len(alerts)} thermal alerts, {len(official)} official items; {status}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
