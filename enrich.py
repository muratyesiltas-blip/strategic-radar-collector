"""AI enrichment for the Strategic Radar collector (Claude API).

Optional layer. Without ANTHROPIC_API_KEY in the environment it does nothing and
the collector publishes the plain feed as before.

Layer 1a  every run   Haiku scores, tags and summarises new items (item["ai"])
Layer 1b  once a day  Sonnet clusters the last 24 hours into stories (stories.json)
Layer 2   every run   Haiku summarises think tank pieces from their full text (item["brief"])

A spending guard (ai_usage.json) stops all calls once the daily or monthly
estimate passes the caps in sources.yaml -> ai.
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

API = "https://api.anthropic.com/v1"
FILES = ["turkey", "syria", "iran", "israel", "gulf", "pkk", "mideast", "russia", "europe"]
DESKS = ["milpol", "geo", "fp", "stratint"]
# USD per million tokens (input, output); cache reads cost 10% of input.
PRICES = {"haiku": (1.0, 5.0), "sonnet": (3.0, 15.0), "opus": (5.0, 25.0)}
# 5.x generation list prices (prompts under 100k tokens; Haiku 5.5 is 5x above 100k).
PRICES_V5 = {"haiku": (0.10, 0.50), "sonnet": (2.0, 10.0), "opus": (5.0, 25.0)}


def prices(model: str, prompt_tokens: int, override: dict | None = None) -> tuple:
    fam = family(model)
    if override and override.get(fam):
        return tuple(override[fam])
    m = re.search(r"-(\d+)(?:-\d+)?", model)
    if m and int(m.group(1)) >= 5:
        pin, pout = PRICES_V5[fam]
        if fam == "haiku" and prompt_tokens > 100_000:
            pin, pout = pin * 5, pout * 5
        return pin, pout
    return PRICES[fam]
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"


def iso(d: datetime) -> str:
    return d.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(s):
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def family(model: str) -> str:
    for f in PRICES:
        if f in model:
            return f
    return "sonnet"


class BudgetExceeded(Exception):
    pass


class Claude:
    def __init__(self, key: str, cfg: dict, usage: dict, now: datetime, mock: bool = False):
        self.key, self.cfg, self.usage, self.now, self.mock = key, cfg, usage, now, mock
        self.lock = threading.Lock()
        self.run_usd = 0.0
        self.calls = 0
        self.errors: list[str] = []
        self.err_counts: dict = {}
        self.thinking_param = True  # send thinking: disabled until the API says it does not accept it
        self.truncated = 0
        self.models = self._pick_models()

    # ----- models -----
    def _pick_models(self) -> dict:
        want = dict(self.cfg.get("models", {}) or {})
        if self.mock:
            return {"haiku": want.get("haiku") or "mock-haiku", "sonnet": want.get("sonnet") or "mock-sonnet"}
        if want.get("haiku") and want.get("sonnet"):
            return want
        try:
            r = requests.get(f"{API}/models", params={"limit": 100}, headers=self._headers(), timeout=20)
            r.raise_for_status()
            ms = sorted(r.json().get("data", []), key=lambda m: m.get("created_at", ""), reverse=True)
            for fam in ("haiku", "sonnet"):
                if not want.get(fam):
                    hit = next((m["id"] for m in ms if fam in m["id"]), None)
                    if hit:
                        want[fam] = hit
        except Exception as ex:  # noqa: BLE001
            self.errors.append(f"models: {type(ex).__name__}: {str(ex)[:120]}")
        want.setdefault("haiku", "claude-haiku-4-5")
        want.setdefault("sonnet", "claude-sonnet-4-5")
        return want

    def _headers(self) -> dict:
        return {"x-api-key": self.key, "anthropic-version": "2023-06-01", "content-type": "application/json"}

    # ----- budget -----
    def _check_budget(self):
        day = self.now.strftime("%Y-%m-%d")
        if self.usage.get("usd", 0.0) >= float(self.cfg.get("monthly_usd", 45)):
            raise BudgetExceeded("monthly cap reached")
        if self.usage.get("days", {}).get(day, 0.0) >= float(self.cfg.get("daily_usd", 2.5)):
            raise BudgetExceeded("daily cap reached")

    def _book(self, model: str, u: dict, task: str):
        ptok = u.get("input_tokens", 0) + u.get("cache_creation_input_tokens", 0) + u.get("cache_read_input_tokens", 0)
        pin, pout = prices(model, ptok, self.cfg.get("prices"))
        inp = u.get("input_tokens", 0) + 1.25 * u.get("cache_creation_input_tokens", 0) + 0.1 * u.get("cache_read_input_tokens", 0)
        usd = (inp * pin + u.get("output_tokens", 0) * pout) / 1e6
        day = self.now.strftime("%Y-%m-%d")
        with self.lock:
            self.run_usd += usd
            self.calls += 1
            U = self.usage
            U["usd"] = round(U.get("usd", 0.0) + usd, 4)
            U["calls"] = U.get("calls", 0) + 1
            U.setdefault("days", {})[day] = round(U["days"].get(day, 0.0) + usd, 4)
            t = U.setdefault("byTask", {}).setdefault(task, {"usd": 0.0, "in": 0, "out": 0, "calls": 0})
            t["usd"] = round(t["usd"] + usd, 4)
            t["in"] += u.get("input_tokens", 0) + u.get("cache_read_input_tokens", 0) + u.get("cache_creation_input_tokens", 0)
            t["out"] += u.get("output_tokens", 0)
            t["calls"] += 1

    # ----- call -----
    def call(self, fam: str, system: str, user: str, max_tokens: int, task: str) -> str:
        with self.lock:
            self._check_budget()
        model = self.models[fam]
        if self.mock:
            self._book(model, {"input_tokens": len(system + user) // 4, "output_tokens": max_tokens // 4}, task)
            return mock_answer(task, user)
        last = None
        for attempt in range(5):
            body = {
                "model": model, "max_tokens": max_tokens if self.thinking_param else max_tokens * 3,
                "system": [{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
                "messages": [{"role": "user", "content": user}],
            }
            if self.thinking_param:
                body["thinking"] = {"type": "disabled"}
            r = requests.post(f"{API}/messages", headers=self._headers(), json=body, timeout=240)
            if r.status_code in (429, 500, 502, 503, 529):
                last = f"HTTP {r.status_code}"
                wait = float(r.headers.get("retry-after", 0) or 0) or 8 * (attempt + 1)
                time.sleep(min(wait, 40))
                continue
            if r.status_code == 400 and self.thinking_param and "thinking" in r.text.lower():
                self.thinking_param = False
                self.note("api: thinking parameter refused, retrying without it")
                continue
            if r.status_code >= 400:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
            d = r.json()
            self._book(model, d.get("usage", {}), task)
            if d.get("stop_reason") == "max_tokens":
                with self.lock:
                    self.truncated += 1
            return "".join(b.get("text", "") for b in d.get("content", []) if b.get("type") == "text")
        raise RuntimeError(f"gave up after retries ({last})")

    def note(self, msg: str):
        key = msg.split(":")[0] + ":" + msg.split(":")[1][:40] if msg.count(":") >= 1 else msg[:40]
        with self.lock:
            self.err_counts[key] = self.err_counts.get(key, 0) + 1
            if len(self.errors) < 12:
                self.errors.append(msg[:220])


def parse_json(txt: str):
    txt = re.sub(r"^```(?:json)?\s*|\s*```$", "", txt.strip())
    try:
        return json.loads(txt)
    except json.JSONDecodeError:
        pass
    starts = sorted((i, o, c) for o, c in (("[", "]"), ("{", "}")) if (i := txt.find(o)) != -1)
    for i, _o, c in starts:
        b = txt.rfind(c)
        if b > i:
            try:
                return json.loads(txt[i : b + 1])
            except json.JSONDecodeError:
                continue
    objs = salvage_objects(txt)
    if objs:
        return objs
    raise ValueError("no JSON in model answer (%d chars): %s" % (len(txt), txt[:80]))


def salvage_objects(txt: str) -> list:
    """Recover the complete objects of a truncated JSON array (the model ran out of tokens)."""
    i = txt.find("[")
    if i == -1:
        return []
    dec = json.JSONDecoder()
    out, pos = [], i + 1
    while True:
        while pos < len(txt) and txt[pos] in " \n\r\t,":
            pos += 1
        if pos >= len(txt) or txt[pos] != "{":
            break
        try:
            obj, pos = dec.raw_decode(txt, pos)
        except json.JSONDecodeError:
            break
        out.append(obj)
    return out


# ---------------------------------------------------------------- prompts
FILE_GUIDE = """Files (theatres) of Strategic Radar, a Türkiye-centred strategic intelligence service:
turkey = Türkiye's foreign, security and defence policy, domestic politics that bear on them, defence industry
syria = Syria (government, SDF, Israel and Türkiye in Syria, reconstruction)
iran = Iran (regime, nuclear file, IRGC, Hormuz, proxies seen from Tehran)
israel = Israel (government, IDF operations, Israel's regional moves)
gulf = Saudi Arabia, UAE, Qatar, Kuwait, Bahrain, Oman, GCC, Yemen and the Houthis as a Gulf issue
pkk = PKK and its affiliates, the process to end PKK violence, KRG and Iraqi Kurdish politics, YPG/SDF as a PKK issue
mideast = Lebanon, Hezbollah, Gaza, Hamas, West Bank, Iraq, Jordan, Egypt, Red Sea
russia = Russia, Ukraine and the war, Black Sea
europe = EU, NATO, European capitals, Greece, Cyprus, European defence
Desks: milpol = military operations, force posture, defence industry, deterrence; geo = geopolitics, energy, corridors, great-power competition; fp = diplomacy, alliances, foreign policy decisions; stratint = warning signals, surprises, events that break a trend."""

ITEM_SYSTEM = FILE_GUIDE + """

You triage news items for analysts. For EACH input item return one object:
{"id": same id, "score": 0-10, "files": [file ids from the list, most relevant first, 1-3], "desk": one desk id,
 "sum": one plain English sentence of at most 30 words stating what happened (who did what, where), no opinion,
 "title_en": English translation of the title when the item is not in English, else "",
 "actors": up to 4 named states, organisations or people, "event": one of statement|meeting|agreement|military|attack|incident|election|legal|economic|energy|report|other,
 "place": main country or "" }
Score rubric: 9-10 a strategic shift or major escalation; 7-8 a significant development for Türkiye or a tracked theatre; 4-6 routine but relevant;
1-3 marginal; 0 off-topic (sport, lifestyle, markets noise, local crime with no political angle).
Add 1 point when the item bears directly on Türkiye's interests. Use only the title and summary; never invent facts.
Answer with a compact JSON array only (no line breaks inside objects), same order as the input, no commentary."""

BRIEF_SYSTEM = FILE_GUIDE + """

You summarise think tank publications for a Turkish strategic analyst. Return one JSON object:
{"summary": 110-160 words in plain English on what the piece argues and how, "argument": the central claim in one sentence,
 "recommendations": up to 4 short policy recommendations the authors make (empty list if none),
 "turkey": one sentence on what the piece means for Türkiye, or "" if it has no bearing,
 "files": 1-3 file ids, "type": report|commentary|analysis|event|podcast|other}
Report the authors' views as theirs ("the authors argue"); do not add your own judgement; never quote more than 12 words.
Answer with the JSON object only."""

STORY_SYSTEM = FILE_GUIDE + """

You receive the last 24 hours of credible-source news items, one per line: id | outlet | files | score | text.
Group items that report the same event or the same developing story. Return a JSON object:
{"stories": [{"headline": at most 14 words, "summary": 2-3 plain sentences on what happened and what is new, sourced only from the items,
  "files": 1-3 file ids, "desk": one desk id, "importance": 1-5 (5 = strategic shift), "ids": [item ids in the story],
  "turkey": one sentence on the stake for Türkiye, or ""}]}
Rules: 25 to 45 stories, most important first; a single-item story is fine when the item matters (score 6+);
drop trivia; never merge different events; never invent facts beyond the items. JSON only."""


# ---------------------------------------------------------------- mock (offline tests)
def mock_answer(task: str, user: str) -> str:
    if task == "items":
        arr = json.loads(user[user.find("[") :])
        return json.dumps([{"id": x["id"], "score": 6, "files": x.get("files") or ["turkey"], "desk": "fp", "sum": "Mock: " + x["title"][:80],
                            "title_en": "", "actors": [], "event": "other", "place": ""} for x in arr])
    if task == "brief":
        return json.dumps({"summary": "Mock summary.", "argument": "Mock argument.", "recommendations": [], "turkey": "", "files": ["turkey"], "type": "analysis"})
    ids = re.findall(r"^([0-9a-f]{16}) \|", user, flags=re.M)
    return json.dumps({"stories": [{"headline": "Mock story", "summary": "Mock.", "files": ["turkey"], "desk": "fp", "importance": 3, "ids": ids[:3], "turkey": ""}]})


# ---------------------------------------------------------------- layer 1a
def enrich_items(items: dict, cl: Claude, cfg: dict, now: datetime, deadline: float) -> dict:
    news_cut = now - timedelta(hours=48)
    tt_cut = now - timedelta(days=5)
    cands = [x for x in items.values() if "ai" not in x and not x.get("dupOf") and x.get("aiTries", 0) < 2
             and (parse_iso(x["time"]) or now) > (news_cut if x["kind"] == "news" else tt_cut)]
    cands.sort(key=lambda x: (x.get("firstSeen", ""), x.get("score", 0)), reverse=True)
    cands = cands[: int(cfg.get("max_items_per_run", 300))]
    size = int(cfg.get("batch_size", 20))
    chunks = [cands[i : i + size] for i in range(0, len(cands), size)]
    done = 0

    def work(chunk):
        if time.monotonic() > deadline:
            return 0
        payload = [{"id": x["id"], "title": x["title"], "summary": (x.get("summary") or "")[:400], "outlet": x["outlet"],
                    "lang": x.get("lang", "en"), "files": x.get("files", [])} for x in chunk]
        for x in chunk:
            x["aiTries"] = x.get("aiTries", 0) + 1
        ans = parse_json(cl.call("haiku", ITEM_SYSTEM, "Items:\n" + json.dumps(payload, ensure_ascii=False), 220 * len(chunk) + 400, "items"))
        by_id = {x["id"]: x for x in chunk}
        n = 0
        for a in ans if isinstance(ans, list) else []:
            x = by_id.get(a.get("id"))
            if not x:
                continue
            files = [f for f in a.get("files", []) if f in FILES][:3]
            ai = {"score": max(0, min(10, int(a.get("score", 0) or 0))), "desk": a.get("desk") if a.get("desk") in DESKS else "",
                  "sum": str(a.get("sum", ""))[:300], "actors": [str(s)[:60] for s in (a.get("actors") or [])][:4],
                  "event": str(a.get("event", ""))[:20], "place": str(a.get("place", ""))[:40], "at": iso(now)}
            if a.get("title_en") and x.get("lang", "en") != "en":
                ai["title_en"] = str(a["title_en"])[:300]
            if files:
                ai["files"] = files
                if x["kind"] == "news":
                    x["files"] = sorted(set(files), key=FILES.index)
            x["ai"] = ai
            n += 1
        return n

    with ThreadPoolExecutor(max_workers=int(cfg.get("workers", 4))) as pool:
        for fut in [pool.submit(work, c) for c in chunks]:
            try:
                done += fut.result()
            except BudgetExceeded:
                raise
            except Exception as ex:  # noqa: BLE001
                cl.note(f"items: {type(ex).__name__}: {str(ex)[:160]}")
    return {"candidates": len(cands), "enriched": done}


# ---------------------------------------------------------------- layer 2
def page_text(url: str, timeout: int = 20) -> str:
    try:
        import trafilatura
    except ImportError:
        trafilatura = None
    r = requests.get(url, timeout=timeout, headers={"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})
    r.raise_for_status()
    html = r.text
    txt = trafilatura.extract(html, include_comments=False, include_tables=False) if trafilatura else None
    if not txt:
        html = re.sub(r"(?is)<(script|style|nav|header|footer|aside)[^>]*>.*?</\1>", " ", html)
        paras = re.findall(r"(?is)<p[^>]*>(.*?)</p>", html)
        txt = "\n".join(re.sub(r"<[^>]+>", " ", p) for p in paras)
    return re.sub(r"[ \t]+", " ", txt or "").strip()


def brief_thinktanks(items: dict, cl: Claude, cfg: dict, now: datetime, deadline: float) -> dict:
    cut = now - timedelta(days=5)
    cands = [x for x in items.values() if x["kind"] == "thinktank" and "brief" not in x and x.get("briefTries", 0) < 2
             and (parse_iso(x["time"]) or now) > cut]
    cands.sort(key=lambda x: x["time"], reverse=True)
    cands = cands[: int(cfg.get("max_briefs_per_run", 25))]
    stats = {"candidates": len(cands), "briefed": 0, "fulltext": 0}
    max_chars = int(cfg.get("brief_max_chars", 24000))

    def work(x):
        if time.monotonic() > deadline:
            return None
        x["briefTries"] = x.get("briefTries", 0) + 1
        src, text = "abstract", ""
        try:
            text = page_text(x["url"])
        except Exception:  # noqa: BLE001 - blocked page: fall back to the feed abstract
            text = ""
        if len(text) >= 1200:
            src = "fulltext"
        else:
            text = (x.get("summary") or "")
            if len(text) < 200:
                return None
        user = f"Title: {x['title']}\nPublisher: {x['outlet']}\nDate: {x['time'][:10]}\n\nText:\n{text[:max_chars]}"
        a = parse_json(cl.call("haiku", BRIEF_SYSTEM, user, 1400, "brief"))
        if not isinstance(a, dict) or not a.get("summary"):
            return None
        x["brief"] = {"summary": str(a["summary"])[:1400], "argument": str(a.get("argument", ""))[:300],
                      "recommendations": [str(s)[:200] for s in (a.get("recommendations") or [])][:4],
                      "turkey": str(a.get("turkey", ""))[:300], "type": str(a.get("type", ""))[:20],
                      "files": [f for f in (a.get("files") or []) if f in FILES][:3], "source": src, "at": iso(now)}
        return src

    with ThreadPoolExecutor(max_workers=int(cfg.get("workers", 4))) as pool:
        for fut in [pool.submit(work, x) for x in cands]:
            try:
                src = fut.result()
            except BudgetExceeded:
                raise
            except Exception as ex:  # noqa: BLE001
                cl.note(f"brief: {type(ex).__name__}: {str(ex)[:160]}")
                continue
            if src:
                stats["briefed"] += 1
                stats["fulltext"] += src == "fulltext"
    return stats


# ---------------------------------------------------------------- layer 1b
def cluster_stories(items: dict, cl: Claude, cfg: dict, now: datetime, data: Path, force: bool = False) -> dict:
    path = data / "stories.json"
    old = {}
    if path.exists():
        try:
            old = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            old = {}
    last = parse_iso(old.get("generatedAt"))
    hours = cfg.get("cluster_hours_utc", [2])
    due = force or last is None or (now - last > timedelta(hours=20) and now.hour in hours) or now - last > timedelta(hours=30)
    if not due:
        return {"skipped": "not due", "last": old.get("generatedAt")}
    cut = now - timedelta(hours=24)
    pool = [x for x in items.values() if x["kind"] == "news" and not x.get("dupOf") and not x.get("aiDrop")
            and (parse_iso(x["time"]) or now) > cut]
    pool.sort(key=lambda x: (x.get("ai", {}).get("score", x.get("score", 0) / 3), x["time"]), reverse=True)
    pool = pool[: int(cfg.get("cluster_max_items", 420))]
    if len(pool) < 10:
        return {"skipped": "too few items"}
    lines = []
    for x in pool:
        ai = x.get("ai", {})
        text = ai.get("sum") or ai.get("title_en") or x["title"]
        lines.append(f"{x['id']} | {x['outlet']} | {','.join(x.get('files', []))} | {ai.get('score', '-')} | {text[:220]}")
    ans = parse_json(cl.call("sonnet", STORY_SYSTEM, "\n".join(lines), int(cfg.get("cluster_max_tokens", 16000)), "stories"))
    by_id = {x["id"]: x for x in pool}
    stories = []
    raw = ans.get("stories", []) if isinstance(ans, dict) else (ans if isinstance(ans, list) else [])
    for i, s in enumerate(raw):
        ids = [k for k in s.get("ids", []) if k in by_id]
        if not ids:
            continue
        its = sorted((by_id[k] for k in ids), key=lambda x: x["time"])
        stories.append({
            "id": f"S{now.strftime('%Y%m%d')}-{i + 1:02d}", "headline": str(s.get("headline", ""))[:160],
            "summary": str(s.get("summary", ""))[:900], "files": [f for f in s.get("files", []) if f in FILES][:3],
            "desk": s.get("desk") if s.get("desk") in DESKS else "", "importance": max(1, min(5, int(s.get("importance", 3) or 3))),
            "turkey": str(s.get("turkey", ""))[:300], "itemIds": ids, "outlets": sorted({x["outlet"] for x in its}),
            "firstSeen": its[0]["time"], "lastSeen": its[-1]["time"],
            "links": [{"outlet": x["outlet"], "title": x["title"], "url": x["url"], "rel": x["rel"]}
                      for x in sorted(its, key=lambda x: "ABCDEF".find(x.get("rel", "F")))[:4]],
        })
        for k in ids:
            by_id[k]["story"] = stories[-1]["id"]
    out = {"generatedAt": iso(now), "windowHours": 24, "model": cl.models["sonnet"], "itemsIn": len(pool),
           "note": "Stories cluster the last 24 hours of collector items. Summaries come only from item titles and feed summaries; open the links before citing details.",
           "stories": stories}
    path.write_text(json.dumps(out, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    return {"stories": len(stories), "itemsIn": len(pool)}


# ---------------------------------------------------------------- entry point
def run(items: dict, data: Path, now: datetime, cfg: dict) -> dict:
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    mock = bool(os.environ.get("AI_MOCK"))
    if not cfg.get("enabled", True):
        return {"enabled": False, "reason": "disabled in sources.yaml"}
    if not key and not mock:
        return {"enabled": False, "reason": "no ANTHROPIC_API_KEY secret"}
    upath = data / "ai_usage.json"
    usage = {}
    if upath.exists():
        try:
            usage = json.loads(upath.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            usage = {}
    month = now.strftime("%Y-%m")
    if usage.get("month") != month:
        usage = {"month": month, "usd": 0.0, "calls": 0, "days": {}, "byTask": {}, "previous": {k: usage.get(k) for k in ("month", "usd", "calls")} if usage else None}
    cl = Claude(key, cfg, usage, now, mock=mock)
    deadline = time.monotonic() + float(cfg.get("time_budget_seconds", 420))
    rep = {"enabled": True, "mock": mock, "models": cl.models}
    try:
        rep["items"] = enrich_items(items, cl, cfg, now, deadline)
        drop = int(cfg.get("drop_below", 2))
        for x in items.values():
            if x.get("ai") and x["kind"] == "news" and x["ai"].get("score", 10) < drop:
                x["aiDrop"] = True
        rep["briefs"] = brief_thinktanks(items, cl, cfg, now, deadline)
        try:
            rep["stories"] = cluster_stories(items, cl, cfg, now, data, force=bool(os.environ.get("AI_FORCE_STORIES")))
        except BudgetExceeded:
            raise
        except Exception as ex:  # noqa: BLE001
            rep["stories"] = {"error": f"{type(ex).__name__}: {str(ex)[:200]}"}
    except BudgetExceeded as ex:
        rep["stopped"] = str(ex)
    except Exception as ex:  # noqa: BLE001 - AI trouble must never break the feed
        cl.note(f"run: {type(ex).__name__}: {str(ex)[:200]}")
    days = usage.get("days", {})
    usage["days"] = {d: days[d] for d in sorted(days)[-40:]}
    upath.write_text(json.dumps(usage, ensure_ascii=False, indent=1), encoding="utf-8")
    rep.update(calls=cl.calls, usdRun=round(cl.run_usd, 4), usdToday=usage.get("days", {}).get(now.strftime("%Y-%m-%d"), 0.0),
               usdMonth=usage.get("usd", 0.0), caps={"daily": cfg.get("daily_usd", 2.5), "monthly": cfg.get("monthly_usd", 45)},
               errors=cl.errors[:12], errorCounts=cl.err_counts, truncated=cl.truncated, thinkingParam=cl.thinking_param)
    return rep
