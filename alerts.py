"""Strategic Radar warning layer (layer 3): alerts and phone push.

Every collector run, after the Haiku triage (enrich.py layer 1a):
  1. picks fresh, high-scoring news items that no alert has covered yet,
  2. asks Claude Haiku which of them deserve an alert (FLASH or PRIORITY) and merges repeats,
  3. stores the alerts in data/alerts.json (7 days),
  4. pushes them to the owner's phone through ntfy (https://ntfy.sh) when the NTFY_TOPIC secret is set.

Quiet hours (default 21:00-04:00 UTC = 00:00-07:00 Istanbul): nothing is pushed; alerts wait in a queue and the
first run after the quiet window sends one morning digest. Caps: at most 3 single pushes per run and 12 per day;
the rest go out as one "N more" message.
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path

import requests

ALERT_SYSTEM_TAIL = """

You are the warning officer of a Türkiye-centred strategic intelligence watch floor. You receive fresh news items
(ref | time | outlet | files | score | text) and the alerts already sent in the last 48 hours.
Decide which items deserve an alert to the analyst's phone. Return one JSON object:
{"alerts": [{"refs": ["r3", "r7"], "level": "flash" | "priority", "headline": at most 12 words, plain and factual,
  "summary": two sentences on what happened and what is new, only from the items,
  "turkey": one sentence on the stake for Türkiye,
  "files": 1-3 file ids, "repeat_of": the id of an alert already sent that covers the same event, or ""}]}
FLASH: a development that changes a file's threat picture now: a strike or major attack, a significant deployment,
a ceasefire breakdown or a deal that ends fighting, a leader-level decision with immediate security effect,
an attack on Turkish personnel, citizens, ships or territory, a closure or reopening of a strait or major corridor.
PRIORITY: a significant development the analyst should read within hours: a major diplomatic move involving Türkiye,
a sharp escalation step short of FLASH, an official threat naming Türkiye, a sudden policy turn in a tracked file.
Leave out everything else (routine statements, analysis, follow-ups that add nothing, market moves, sport).
Group items about the same event into one alert. If an event was already alerted, return it only when it adds a
major new fact, and set repeat_of. Expect zero alerts in most runs; never more than 4. JSON only."""

LEVEL_PRI = {"flash": 5, "priority": 4}

_STOP = set("the a an of to in on at for and or by with from as is are was were be its it this that after over into amid new says say said will".split())


def _toks(s: str) -> set:
    return {w for w in re.findall(r"[a-z0-9]+", (s or "").lower()) if len(w) > 2 and w not in _STOP}


def _sim(a: str, b: str) -> float:
    A, B = _toks(a), _toks(b)
    if not A or not B:
        return 0.0
    return len(A & B) / min(len(A), len(B))


def _dup_of(rec: dict, recent: list) -> str:
    """Second check after the model: same event as an alert of the last 48 h? (headline overlap, or shared items)."""
    for a in recent:
        if set(rec.get("itemIds") or []) & set(a.get("itemIds") or []):
            return a["id"]
        if _sim(rec["headline"], a.get("headline", "")) >= 0.5:
            return a["id"]
        if _sim(rec["headline"] + " " + rec["summary"], a.get("headline", "") + " " + a.get("summary", "")) >= 0.45:
            return a["id"]
    return ""


def _load(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    return {"alerts": [], "queue": [], "pushLog": [], "lastDigest": "", "hello": ""}


def _quiet(now: datetime, cfg: dict) -> bool:
    q = cfg.get("alert_quiet_utc", [21, 4])
    a, b = int(q[0]), int(q[1])
    h = now.hour
    return (a <= h or h < b) if a > b else (a <= h < b)


def _ntfy(payload: dict) -> tuple[bool, str]:
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not topic:
        return False, "no NTFY_TOPIC secret"
    server = os.environ.get("NTFY_SERVER", "https://ntfy.sh").rstrip("/")
    body = dict(payload, topic=topic)
    headers = {}
    tok = os.environ.get("NTFY_TOKEN", "").strip()
    if tok:
        headers["Authorization"] = f"Bearer {tok}"
    try:
        r = requests.post(server + "/", data=json.dumps(body, ensure_ascii=False).encode("utf-8"), headers=headers, timeout=15)
        return r.ok, f"HTTP {r.status_code}"
    except Exception as ex:  # noqa: BLE001
        return False, f"{type(ex).__name__}: {str(ex)[:120]}"


def _push_alert(a: dict) -> tuple[bool, str]:
    files = ", ".join(a.get("fileNames") or a.get("files") or [])
    msg = a["summary"] + (("\n\nTürkiye: " + a["turkey"]) if a.get("turkey") else "")
    links = a.get("links") or []
    p = {"title": f"{'FLASH' if a['level'] == 'flash' else 'PRIORITY'} · {files}: {a['headline']}"[:250],
         "message": msg[:3800], "priority": LEVEL_PRI.get(a["level"], 3),
         "tags": ["rotating_light"] if a["level"] == "flash" else ["warning"]}
    if links and links[0].get("url"):
        p["click"] = links[0]["url"]
        p["actions"] = [{"action": "view", "label": (links[0].get("outlet") or "Source")[:40], "url": links[0]["url"]}]
    return _ntfy(p)


def _push_digest(alerts: list, title: str) -> tuple[bool, str]:
    lines = [f"{'FLASH' if a['level'] == 'flash' else 'PRIORITY'} · {a['headline']}" for a in alerts]
    return _ntfy({"title": title[:250], "message": "\n".join(lines)[:3800],
                  "priority": 4 if any(a["level"] == "flash" for a in alerts) else 3, "tags": ["newspaper"]})


FILE_NAMES = {"turkey": "Türkiye", "syria": "Syria", "iran": "Iran", "israel": "Israel", "gulf": "Gulf", "pkk": "PKK",
              "mideast": "Middle East", "russia": "Russia-Ukraine", "europe": "Europe"}


def run_alerts(items: dict, cl, cfg: dict, now: datetime, data: Path) -> dict:
    from enrich import FILE_GUIDE, FILES, iso, parse_iso, parse_json  # lazy: enrich imports this module

    if not cfg.get("alerts_enabled", True):
        return {"enabled": False}
    path = data / "alerts.json"
    st = _load(path)
    rep = {"candidates": 0, "new": 0, "pushed": 0, "queued": 0}
    min_score = int(cfg.get("alert_min_score", 8))
    fresh_h = float(cfg.get("alert_fresh_hours", 3))
    cut = now - timedelta(hours=fresh_h)
    cands = []
    for x in items.values():
        if x.get("kind") != "news" or x.get("dupOf") or x.get("aiDrop") or x.get("alerted") or x.get("alertTried"):
            continue
        ai = x.get("ai") or {}
        sc = ai.get("score", -1)
        if sc < min_score and not (sc >= min_score - 1 and x.get("tr")):
            continue
        fs = parse_iso(x.get("firstSeen")) or now
        if fs < cut or (parse_iso(x.get("time")) or now) < now - timedelta(hours=30):
            continue
        cands.append(x)
    cands.sort(key=lambda x: (x.get("ai", {}).get("score", 0), x.get("time", "")), reverse=True)
    cands = cands[: int(cfg.get("alert_max_candidates", 30))]
    rep["candidates"] = len(cands)
    recent = [a for a in st["alerts"] if (parse_iso(a.get("at")) or now) > now - timedelta(hours=48)]
    new_alerts = []
    if cands and cl is not None:
        ref = {}
        lines = []
        for n, x in enumerate(cands, 1):
            ref[f"r{n}"] = x
            ai = x.get("ai", {})
            text = ai.get("sum") or ai.get("title_en") or x["title"]
            lines.append(f"r{n} | {x.get('time', '')[:16]} | {x['outlet']} | {','.join(x.get('files', []))} | {ai.get('score', '-')} | {text[:240]}")
        sent = "\n".join(f"{a['id']} | {a['at'][:16]} | {a['level']} | {a['headline']}" for a in recent) or "(none)"
        user = f"ALERTS ALREADY SENT (48 h):\n{sent}\n\nFRESH ITEMS:\n" + "\n".join(lines)
        for x in cands:
            x["alertTried"] = iso(now)
        ans = parse_json(cl.call("haiku", FILE_GUIDE + ALERT_SYSTEM_TAIL, user, 1600, "alerts"))
        raw = ans.get("alerts", []) if isinstance(ans, dict) else (ans if isinstance(ans, list) else [])
        seq = len([a for a in st["alerts"] if a["id"].startswith("A" + now.strftime("%Y%m%d"))])
        for a in raw[:4]:
            if not isinstance(a, dict) or a.get("level") not in ("flash", "priority"):
                continue
            its = [ref[r] for r in (a.get("refs") or []) if isinstance(r, str) and r in ref]
            if not its:
                continue
            rpt = str(a.get("repeat_of") or "")
            seq += 1
            files = [f for f in (a.get("files") or []) if f in FILES][:3] or its[0].get("files", [])[:2]
            rec = {"id": f"A{now.strftime('%Y%m%d%H%M')}-{seq}", "at": iso(now), "level": a["level"],
                   "headline": str(a.get("headline", ""))[:140], "summary": str(a.get("summary", ""))[:600],
                   "turkey": str(a.get("turkey", ""))[:300], "files": files, "fileNames": [FILE_NAMES.get(f, f) for f in files],
                   "repeatOf": rpt if rpt.startswith("A") else "", "itemIds": [x["id"] for x in its],
                   "links": [{"outlet": x["outlet"], "title": x["title"], "url": x["url"], "rel": x.get("rel", "")}
                             for x in sorted(its, key=lambda x: "ABCDEF".find(x.get("rel", "F") or "F"))[:3]],
                   "push": ""}
            for x in its:
                x["alerted"] = rec["id"]
            dup = _dup_of(rec, recent + new_alerts)
            if dup or rec["repeatOf"]:
                # same event as an earlier alert: keep it as an update, never push it again
                rec["repeatOf"] = rec["repeatOf"] or dup
                rec["push"] = "repeat"
                rep["repeats"] = rep.get("repeats", 0) + 1
            new_alerts.append(rec)
    rep["new"] = len([a for a in new_alerts if a["push"] != "repeat"])

    # push
    topic = bool(os.environ.get("NTFY_TOPIC", "").strip())
    rep["ntfy"] = topic
    today = now.strftime("%Y-%m-%d")
    pushed_today = sum(1 for p in st["pushLog"] if p.get("at", "").startswith(today) and p.get("ok"))
    if topic and not st.get("hello"):
        ok, why = _ntfy({"title": "Strategic Radar alerts connected", "priority": 3, "tags": ["white_check_mark"],
                         "message": "This phone now receives FLASH and PRIORITY alerts from the collector, 07:00-24:00 Istanbul time, with a morning digest of anything that came in overnight."})
        st["pushLog"].append({"at": iso(now), "kind": "hello", "ok": ok, "why": why})
        if ok:
            st["hello"] = iso(now)
    if _quiet(now, cfg):
        fresh = [a for a in new_alerts if a["push"] != "repeat"]
        for a in fresh:
            a["push"] = "queued"
            st["queue"].append(a["id"])
        rep["queued"] = len(fresh)
    else:
        by_id = {a["id"]: a for a in st["alerts"] + new_alerts}
        q = [by_id[i] for i in st.get("queue", []) if i in by_id]
        if q and topic:
            ok, why = _push_digest(q, f"Strategic Radar · overnight: {len(q)} alert{'s' if len(q) != 1 else ''}")
            st["pushLog"].append({"at": iso(now), "kind": "digest", "ids": [a["id"] for a in q], "ok": ok, "why": why})
            if ok:
                for a in q:
                    a["push"] = "digest"
                st["queue"] = []
                st["lastDigest"] = iso(now)
                rep["pushed"] += 1
        elif q and not topic:
            st["queue"] = []
        singles = sorted([a for a in new_alerts if a["push"] != "repeat"], key=lambda a: a["level"] != "flash")
        cap_run, cap_day = int(cfg.get("alert_push_per_run", 3)), int(cfg.get("alert_push_per_day", 12))
        rest = []
        for a in singles:
            if not topic:
                a["push"] = "off"
                continue
            if rep["pushed"] >= cap_run or pushed_today + rep["pushed"] >= cap_day:
                rest.append(a)
                continue
            ok, why = _push_alert(a)
            a["push"] = "sent" if ok else f"failed: {why}"
            st["pushLog"].append({"at": iso(now), "kind": "alert", "ids": [a["id"]], "ok": ok, "why": why})
            rep["pushed"] += 1 if ok else 0
        if rest and topic and pushed_today + rep["pushed"] < cap_day + 1:
            ok, why = _push_digest(rest, f"Strategic Radar · {len(rest)} more alert{'s' if len(rest) != 1 else ''}")
            for a in rest:
                a["push"] = "grouped" if ok else f"failed: {why}"
            st["pushLog"].append({"at": iso(now), "kind": "more", "ids": [a["id"] for a in rest], "ok": ok, "why": why})
        elif rest:
            for a in rest:
                a["push"] = "capped"

    keep = now - timedelta(days=int(cfg.get("alert_keep_days", 7)))
    st["alerts"] = [a for a in st["alerts"] + new_alerts if (parse_iso(a.get("at")) or now) > keep][-300:]
    st["pushLog"] = st["pushLog"][-80:]
    st["updatedAt"] = iso(now)
    st["note"] = ("Machine alerts from the collector's warning layer (Claude Haiku over high-scoring items). FLASH = changes a file's "
                  "threat picture; PRIORITY = read within hours. Open the links before citing; the analyst grades sources.")
    path.write_text(json.dumps(st, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    rep["alerts7d"] = len(st["alerts"])
    rep["queue"] = len(st.get("queue", []))
    return rep
