#!/usr/bin/env python3
"""Strategic Radar collector.

Pulls RSS/Atom feeds and the GDELT DOC API, keeps credible sources only,
sorts items into the nine Strategic Radar files, grades the source, removes
duplicates and writes compact JSON that the Claude tasks read in one request:

  data/latest.json      every item of the last 48 hours, grouped by file
  data/files/<id>.json  the same, one file per Strategic Radar file
  data/thinktanks.json  think tank publications of the last 5 days
  data/status.json      health of every feed and of GDELT
  data/archive/<date>.jsonl  every new item, one line each, kept for 30 days

State between runs lives in data/state.json and data/store.json.
Runs in GitHub Actions every two hours; it also runs locally:
  python collector/collect.py [--data data] [--config collector/config] [--offline-fixtures tests/fixtures]
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import feedparser
import requests
import yaml

UA = "Mozilla/5.0 (compatible; StrategicRadarCollector/1.0; +https://github.com/)"
FILES = ["turkey", "syria", "iran", "israel", "gulf", "pkk", "mideast", "russia", "europe"]
NEWS_WINDOW_H = 48
TT_WINDOW_D = 5
STORE_KEEP_H = 120
ARCHIVE_KEEP_D = 30
PER_FILE_CAP = 120
REL_W = {"A": 3, "B": 2, "C": 1, "D": 0, "E": -1, "F": 0}
TRACKING = re.compile(r"^(utm_|fbclid|gclid|mc_|ocid|cmpid|at_|ref$|src$)", re.I)

NOW = datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def load_json(p: Path, default):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def dump_json(p: Path, obj, compact=False):
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        if compact:
            json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
        else:
            json.dump(obj, f, ensure_ascii=False, indent=1)
    tmp.replace(p)


# ---------- text and url helpers ----------
TAG = re.compile(r"<[^>]+>")
WS = re.compile(r"\s+")


def clean(s: str | None, limit: int | None = None) -> str:
    s = html.unescape(TAG.sub(" ", s or ""))
    s = WS.sub(" ", s).strip()
    if limit and len(s) > limit:
        s = s[: limit - 1].rsplit(" ", 1)[0] + "…"
    return s


def canon_url(u: str) -> str:
    try:
        sp = urlsplit(u.strip())
    except ValueError:
        return u.strip()
    q = [(k, v) for k, v in parse_qsl(sp.query, keep_blank_values=True) if not TRACKING.match(k)]
    host = sp.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    path = sp.path.rstrip("/") or "/"
    return urlunsplit(("https", host, path, urlencode(q), ""))


def strip_tracking(u: str) -> str:
    try:
        sp = urlsplit(u.strip())
    except ValueError:
        return u.strip()
    q = [(k, v) for k, v in parse_qsl(sp.query, keep_blank_values=True) if not TRACKING.match(k)]
    return urlunsplit((sp.scheme or "https", sp.netloc, sp.path, urlencode(q), ""))


def domain_of(u: str) -> str:
    try:
        h = urlsplit(u).netloc.lower()
    except ValueError:
        return ""
    return h[4:] if h.startswith("www.") else h


def domain_match(dom: str, base: str) -> bool:
    return dom == base or dom.endswith("." + base)


def norm_title(t: str) -> set[str]:
    t = re.sub(r"[^\w\s]", " ", t.lower())
    stop = {"the", "a", "an", "of", "to", "in", "on", "and", "for", "with", "as", "at", "by", "is", "after", "says", "over", "from"}
    return {w for w in t.split() if len(w) > 2 and w not in stop}


def jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


# ---------- classifier ----------
class Classifier:
    def __init__(self, cfg: dict):
        self.rx = {}
        for fid, spec in cfg["files"].items():
            words = sorted(set(spec.get("keywords", [])), key=len, reverse=True)
            self.rx[fid] = re.compile(r"(?<![\w])(" + "|".join(re.escape(w) for w in words) + r")(?![\w])", re.I)
        self.boost = re.compile(r"(?<![\w])(" + "|".join(re.escape(w) for w in cfg.get("turkey_boost", [])) + r")(?![\w])", re.I)
        neg = cfg.get("negative", [])
        self.neg = re.compile(r"(?<![\w])(" + "|".join(re.escape(w) for w in neg) + r")(?![\w])", re.I) if neg else None
        self.min_hits = cfg.get("min_hits_general", 1)

    def noise(self, title: str) -> bool:
        return bool(self.neg and self.neg.search(title))

    def hits(self, text: str) -> dict[str, int]:
        out = {}
        for fid, rx in self.rx.items():
            n = len(rx.findall(text))
            if n:
                out[fid] = n
        return out

    def turkey(self, text: str) -> bool:
        return bool(self.boost.search(text))

    def assign(self, text: str, feed_files: list[str], feed_type: str) -> tuple[list[str], dict]:
        h = self.hits(text)
        files = {f for f, n in h.items() if n >= self.min_hits}
        # A single-file national or official feed keeps its own file even without a keyword hit.
        if len(feed_files) == 1 and feed_type in ("local", "official", "party", "diaspora"):
            files.add(feed_files[0])
        else:
            files |= {f for f in feed_files if f in h}
        return [f for f in FILES if f in files], h


# ---------- feed fetching ----------
DISCOVER_PATHS = ["/feed/", "/rss", "/rss.xml", "/feed.xml", "/feeds/all.atom.xml", "/rss/", "/index.xml", "/atom.xml"]
ALT = re.compile(r"<link[^>]+rel=[\"']alternate[\"'][^>]*>", re.I)


def http_get(session, url, timeout):
    r = session.get(url, timeout=timeout, headers={"User-Agent": UA, "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, text/html;q=0.8, */*;q=0.5"})
    r.raise_for_status()
    return r


def looks_like_feed(content: bytes) -> bool:
    head = content[:2000].lower()
    return b"<rss" in head or b"<feed" in head or b"<rdf" in head


def discover(session, home: str, timeout: int) -> str | None:
    if not home:
        return None
    try:
        r = http_get(session, home, timeout)
        for tag in ALT.findall(r.text):
            if re.search(r"(rss|atom)\+xml", tag, re.I):
                m = re.search(r"href=[\"']([^\"']+)[\"']", tag, re.I)
                if m:
                    return urljoin(r.url, html.unescape(m.group(1)))
    except requests.RequestException:
        pass
    base = "{0.scheme}://{0.netloc}".format(urlsplit(home))
    prefix = urlsplit(home).path.rstrip("/")
    for path in ([prefix + p for p in DISCOVER_PATHS] if prefix else []) + DISCOVER_PATHS:
        try:
            r = http_get(session, base + path, timeout)
            if looks_like_feed(r.content):
                return r.url
        except requests.RequestException:
            continue
    return None


def entries_from_feed(content: bytes, feed: dict, max_items: int) -> list[dict]:
    parsed = feedparser.parse(content)
    out = []
    for e in parsed.entries[:max_items]:
        link = e.get("link") or ""
        if not link:
            continue
        t = None
        for k in ("published_parsed", "updated_parsed", "created_parsed"):
            if e.get(k):
                t = datetime(*e[k][:6], tzinfo=timezone.utc)
                break
        summary = e.get("summary") or e.get("description") or ""
        out.append({"title": clean(e.get("title"), 300), "url": link, "time": t, "summary": clean(summary, 480)})
    return out


# ---------- GDELT ----------
GDELT_API = "https://api.gdeltproject.org/api/v2/doc/doc"


def gdelt_query(session, query: str, timespan: str, maxrecords: int, timeout: int) -> list[dict]:
    params = {"query": query, "mode": "artlist", "format": "json", "timespan": timespan, "maxrecords": str(maxrecords), "sort": "datedesc"}
    r = session.get(GDELT_API, params=params, timeout=timeout, headers={"User-Agent": UA})
    r.raise_for_status()
    txt = r.text.strip()
    if not txt.startswith("{"):
        raise ValueError("GDELT answered with non-JSON: " + txt[:120])
    return json.loads(txt).get("articles", [])


def gdelt_time(s: str) -> datetime | None:
    try:
        return datetime.strptime(s, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


# ---------- main ----------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="data")
    ap.add_argument("--config", default=str(Path(__file__).parent / "config"))
    ap.add_argument("--offline-fixtures", default="", help="read feeds and GDELT from this folder (tests)")
    ap.add_argument("--no-gdelt", action="store_true")
    args = ap.parse_args()

    data = Path(args.data)
    cfgdir = Path(args.config)
    sources = yaml.safe_load((cfgdir / "sources.yaml").read_text(encoding="utf-8"))
    cls_cfg = yaml.safe_load((cfgdir / "classify.yaml").read_text(encoding="utf-8"))
    clf = Classifier(cls_cfg)
    blocked = [d.lower() for d in sources.get("blocked_domains", [])]
    timeout = sources.get("defaults", {}).get("timeout", 20)
    max_items = sources.get("defaults", {}).get("max_items_per_feed", 40)

    state = load_json(data / "state.json", {"feeds": {}, "runs": []})
    store = load_json(data / "store.json", {"items": {}})
    items: dict = store.get("items", {})
    session = requests.Session()
    fixtures = Path(args.offline_fixtures) if args.offline_fixtures else None

    status = {"generatedAt": iso(NOW), "feeds": [], "gdelt": []}
    new_ids: list[str] = []

    def is_blocked(dom: str) -> bool:
        return any(domain_match(dom, b) for b in blocked)

    def add(raw: dict, src: dict) -> bool:
        url = strip_tracking(raw["url"])
        key = canon_url(raw["url"])
        dom = domain_of(url)
        if not raw.get("title") or is_blocked(dom) or clf.noise(raw["title"]):
            return False
        text = raw["title"] + " " + raw.get("summary", "")
        if src["kind"] == "thinktank":
            files = [f for f in FILES if f in clf.hits(text)] or list(src.get("files", []))
            hits = clf.hits(text)
        else:
            files, hits = clf.assign(text, src.get("files", []), src["type"])
            if not files:
                return False
        iid = hashlib.sha1(key.encode()).hexdigest()[:16]
        t = raw.get("time") or NOW
        if t > NOW + timedelta(minutes=10):
            t = NOW
        if iid in items:
            it = items[iid]
            it["files"] = sorted(set(it["files"]) | set(files), key=FILES.index)
            if src["via"] == "rss" and it.get("via") == "gdelt":
                it.update(outlet=src["name"], type=src["type"], rel=src["rel"], via="rss")
                if raw.get("summary"):
                    it["summary"] = raw["summary"]
            return False
        tr = clf.turkey(text)
        score = sum(hits.values()) * 2 + (3 if tr else 0) + REL_W.get(src["rel"], 0) + (1 if src["type"] == "official" else 0)
        items[iid] = {
            "id": iid, "kind": src["kind"], "title": raw["title"], "url": url, "domain": dom,
            "outlet": src["name"], "type": src["type"], "lang": src.get("lang", "en"), "rel": src["rel"],
            "time": iso(t), "firstSeen": iso(NOW), "summary": raw.get("summary", ""),
            "files": files, "hits": hits, "tr": tr, "score": score, "via": src["via"], "feed": src.get("id", "gdelt"),
        }
        new_ids.append(iid)
        return True

    # RSS / Atom
    for feed in sources["feeds"]:
        fid = feed["id"]
        st = state["feeds"].setdefault(fid, {"fails": 0})
        url = feed.get("url") or st.get("discovered") or ""
        rec = {"id": fid, "name": feed["name"], "url": url, "ok": False, "items": 0, "new": 0}
        try:
            if fixtures:
                p = fixtures / f"{fid}.xml"
                if not p.exists():
                    raise FileNotFoundError("no fixture")
                content = p.read_bytes()
            else:
                if not url or st.get("fails", 0) >= 3:
                    found = discover(session, feed.get("home", ""), timeout)
                    if found:
                        url = found
                        st["discovered"] = found
                        st["fails"] = 0
                        rec["url"] = found
                        rec["discovered"] = True
                if not url:
                    raise ValueError("no feed url and auto-discovery found none")
                content = http_get(session, url, timeout).content
                if not looks_like_feed(content):
                    raise ValueError("response is not a feed")
            entries = entries_from_feed(content, feed, max_items)
            src = dict(feed, via="rss")
            n_new = sum(1 for e in entries if add(e, src))
            rec.update(ok=True, items=len(entries), new=n_new)
            st["fails"] = 0
            st["lastOk"] = iso(NOW)
        except Exception as ex:  # noqa: BLE001 - one bad feed must not stop the run
            st["fails"] = st.get("fails", 0) + 1
            rec["error"] = f"{type(ex).__name__}: {str(ex)[:160]}"
            rec["fails"] = st["fails"]
        rec["lastOk"] = st.get("lastOk")
        status["feeds"].append(rec)

    # GDELT
    g = sources.get("gdelt", {})
    if g.get("enabled") and not args.no_gdelt:
        doms = g.get("domains", {})
        queries = [(fid, spec["gdelt"] + " sourcelang:english") for fid, spec in cls_cfg["files"].items() if spec.get("gdelt")]
        if g.get("turkish_query"):
            queries.append(("turkey", g["turkish_query"].strip() + " sourcelang:turkish"))
        for i, (fid, q) in enumerate(queries):
            rec = {"file": fid, "query": q, "ok": False, "items": 0, "kept": 0, "new": 0}
            try:
                if fixtures:
                    p = fixtures / f"gdelt-{fid}.json"
                    arts = json.loads(p.read_text())["articles"] if p.exists() else []
                else:
                    if i:
                        time.sleep(g.get("pause_seconds", 6))
                    arts = gdelt_query(session, q, g.get("timespan", "4h"), g.get("maxrecords", 120), timeout + 10)
                rec["items"] = len(arts)
                for a in arts:
                    dom = domain_of(a.get("url", ""))
                    meta = next((m for base, m in doms.items() if domain_match(dom, base)), None)
                    if not meta:
                        continue
                    rec["kept"] += 1
                    lang = "tr" if "turkish" in q else "en"
                    src = {"name": meta["name"], "type": meta["type"], "rel": meta["rel"], "files": [fid], "kind": "news", "via": "gdelt", "lang": lang, "id": "gdelt"}
                    if add({"title": clean(a.get("title"), 300), "url": a["url"], "time": gdelt_time(a.get("seendate")), "summary": ""}, src):
                        rec["new"] += 1
                rec["ok"] = True
            except Exception as ex:  # noqa: BLE001
                rec["error"] = f"{type(ex).__name__}: {str(ex)[:160]}"
            status["gdelt"].append(rec)

    # near-duplicate titles: keep the better-graded / earlier item, record the others as alsoReportedBy
    by_time = sorted(items.values(), key=lambda x: x["time"], reverse=True)
    recent = [x for x in by_time if parse_iso(x["time"]) and parse_iso(x["time"]) > NOW - timedelta(hours=NEWS_WINDOW_H) and x["kind"] == "news"]
    toks = {x["id"]: norm_title(x["title"]) for x in recent}
    dropped = set()
    for i, a in enumerate(recent):
        if a["id"] in dropped:
            continue
        for b in recent[i + 1 : i + 80]:
            if b["id"] in dropped or b["domain"] == a["domain"]:
                continue
            if jaccard(toks[a["id"]], toks[b["id"]]) >= 0.72:
                keep, lose = (a, b) if REL_W.get(a["rel"], 0) >= REL_W.get(b["rel"], 0) else (b, a)
                keep.setdefault("alsoReportedBy", [])
                if lose["outlet"] not in keep["alsoReportedBy"]:
                    keep["alsoReportedBy"].append(lose["outlet"])
                keep["files"] = sorted(set(keep["files"]) | set(lose["files"]), key=FILES.index)
                lose["dupOf"] = keep["id"]
                dropped.add(lose["id"])
                if lose is a:
                    break

    # prune store
    cutoff = NOW - timedelta(hours=STORE_KEEP_H)
    items = {k: v for k, v in items.items() if (parse_iso(v["time"]) or NOW) > cutoff or (parse_iso(v["firstSeen"]) or NOW) > NOW - timedelta(hours=12)}

    # outputs
    def public(x):
        out = {k: x[k] for k in ("id", "title", "url", "outlet", "domain", "type", "lang", "rel", "time", "firstSeen", "summary", "files", "tr", "score") if k in x}
        if x.get("alsoReportedBy"):
            out["alsoReportedBy"] = x["alsoReportedBy"]
        return out

    news = [x for x in items.values() if x["kind"] == "news" and not x.get("dupOf") and (parse_iso(x["time"]) or NOW) > NOW - timedelta(hours=NEWS_WINDOW_H)]
    per_file = {}
    for fid in FILES:
        L = [x for x in news if fid in x["files"]]
        L.sort(key=lambda x: (x["time"], x["score"]), reverse=True)
        per_file[fid] = [public(x) for x in L[:PER_FILE_CAP]]
        dump_json(data / "files" / f"{fid}.json", {"generatedAt": iso(NOW), "file": fid, "windowHours": NEWS_WINDOW_H, "items": per_file[fid]}, compact=True)
    latest = {
        "generatedAt": iso(NOW), "windowHours": NEWS_WINDOW_H,
        "counts": {f: len(per_file[f]) for f in FILES},
        "newThisRun": len([i for i in new_ids if items.get(i, {}).get("kind") == "news"]),
        "note": "Credible-source items of the last 48 hours, grouped by Strategic Radar file, newest first. rel = Admiralty source-reliability letter; the analyst sets the credibility number. firstSeen = when the collector first saw the item (use it to pick items since the last sweep). Summaries come from the feeds; open the url before citing a detail that is not in the title or summary.",
        "files": per_file,
    }
    dump_json(data / "latest.json", latest, compact=True)

    tts = [x for x in items.values() if x["kind"] == "thinktank" and (parse_iso(x["time"]) or NOW) > NOW - timedelta(days=TT_WINDOW_D)]
    tts.sort(key=lambda x: x["time"], reverse=True)
    dump_json(data / "thinktanks.json", {"generatedAt": iso(NOW), "windowDays": TT_WINDOW_D, "items": [public(x) for x in tts]}, compact=True)

    # archive of new items
    if new_ids:
        day = NOW.strftime("%Y-%m-%d")
        ap_path = data / "archive" / f"{day}.jsonl"
        ap_path.parent.mkdir(parents=True, exist_ok=True)
        with ap_path.open("a", encoding="utf-8") as f:
            for i in new_ids:
                if i in items:
                    f.write(json.dumps(public(items[i]), ensure_ascii=False) + "\n")
    for p in (data / "archive").glob("*.jsonl"):
        try:
            if datetime.strptime(p.stem, "%Y-%m-%d").replace(tzinfo=timezone.utc) < NOW - timedelta(days=ARCHIVE_KEEP_D):
                p.unlink()
        except ValueError:
            pass

    ok = sum(1 for f in status["feeds"] if f["ok"])
    status["summary"] = {
        "feedsOk": ok, "feedsTotal": len(status["feeds"]),
        "gdeltOk": sum(1 for g_ in status["gdelt"] if g_["ok"]), "gdeltTotal": len(status["gdelt"]),
        "newItems": len(new_ids), "newsItems48h": len(news), "thinktankItems": len(tts),
        "failing": [f["id"] for f in status["feeds"] if not f["ok"]],
    }
    dump_json(data / "status.json", status)
    state["runs"] = (state.get("runs", []) + [{"at": iso(NOW), "new": len(new_ids), "feedsOk": ok}])[-50:]
    state["lastRun"] = iso(NOW)
    dump_json(data / "state.json", state)
    dump_json(data / "store.json", {"items": items}, compact=True)
    print(json.dumps(status["summary"], ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
