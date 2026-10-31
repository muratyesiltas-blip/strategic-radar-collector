"""Strategic Radar knowledge base.

Builds the event archive the dashboard's Knowledge tab reads.

  Collector side (called from collect.py after enrich.run):
    data/kb/events.json         every story event the collector has built, kept for 400 days
    data/kb/db/ev-YYYY-MM-x.json  database-shaped 5-day buckets of those events (a = 1-5, b = 6-10 ... f = 26-end)
    data/kb/db/manifest.json    bucket list with content hashes
    data/kb/kb.py               a copy of this file, so the daily Claude task can run the sync below

  Daily task side (python kb.py sync ...):
    merges the analyst desk's file-day developments into dk-YYYY-MM-x buckets, compares every bucket with
    the copy already in the dashboard database and prints the documents that need writing.

Actor names are normalised with the alias table below; desk developments get actors by dictionary match.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

KEEP_DAYS = 400
FILES = ["turkey", "syria", "iran", "israel", "gulf", "pkk", "mideast", "russia", "europe"]

# canonical name: (kind, [aliases]); kind = state | person | org | group
ACTORS = {
    # states
    "Türkiye": ("state", ["Turkey", "Turkiye", "Türkiye", "Ankara", "Turkish government", "Turkish"]),
    "United States": ("state", ["US", "U.S.", "USA", "United States", "Washington", "American", "US administration", "Trump administration", "White House", "Pentagon"]),
    "Russia": ("state", ["Russia", "Moscow", "Kremlin", "Russian Federation", "Russian"]),
    "Ukraine": ("state", ["Ukraine", "Kyiv", "Kiev", "Ukrainian"]),
    "Iran": ("state", ["Iran", "Tehran", "Islamic Republic", "Iranian"]),
    "Israel": ("state", ["Israel", "Israeli", "Jerusalem government"]),
    "Syria": ("state", ["Syria", "Damascus", "Syrian government", "Syrian"]),
    "Iraq": ("state", ["Iraq", "Baghdad", "Iraqi government", "Iraqi"]),
    "Saudi Arabia": ("state", ["Saudi Arabia", "Riyadh", "Saudi", "KSA"]),
    "United Arab Emirates": ("state", ["UAE", "United Arab Emirates", "Abu Dhabi", "Emirati"]),
    "Qatar": ("state", ["Qatar", "Doha", "Qatari"]),
    "Kuwait": ("state", ["Kuwait", "Kuwaiti"]),
    "Oman": ("state", ["Oman", "Muscat", "Omani"]),
    "Bahrain": ("state", ["Bahrain", "Manama", "Bahraini"]),
    "Egypt": ("state", ["Egypt", "Cairo", "Egyptian"]),
    "Jordan": ("state", ["Jordan", "Amman", "Jordanian"]),
    "Lebanon": ("state", ["Lebanon", "Beirut", "Lebanese"]),
    "Yemen": ("state", ["Yemen", "Yemeni"]),
    "Libya": ("state", ["Libya", "Tripoli", "Libyan"]),
    "Greece": ("state", ["Greece", "Athens", "Greek"]),
    "Cyprus": ("state", ["Cyprus", "Nicosia", "Republic of Cyprus", "Cypriot"]),
    "Azerbaijan": ("state", ["Azerbaijan", "Baku", "Azerbaijani"]),
    "Armenia": ("state", ["Armenia", "Yerevan", "Armenian"]),
    "Georgia": ("state", ["Tbilisi", "Georgian government"]),
    "Pakistan": ("state", ["Pakistan", "Islamabad", "Pakistani"]),
    "China": ("state", ["China", "Beijing", "Chinese", "PRC"]),
    "India": ("state", ["India", "New Delhi", "Indian government"]),
    "France": ("state", ["France", "Paris", "French", "Elysee", "Élysée"]),
    "Germany": ("state", ["Germany", "Berlin", "German", "Bundeswehr"]),
    "United Kingdom": ("state", ["UK", "United Kingdom", "Britain", "British", "London", "Downing Street"]),
    "Italy": ("state", ["Italy", "Rome", "Italian"]),
    "Poland": ("state", ["Poland", "Warsaw", "Polish"]),
    "Romania": ("state", ["Romania", "Bucharest", "Romanian"]),
    "Bulgaria": ("state", ["Bulgaria", "Sofia", "Bulgarian"]),
    "Hungary": ("state", ["Hungary", "Budapest", "Hungarian"]),
    "Serbia": ("state", ["Serbia", "Belgrade", "Serbian"]),
    "Moldova": ("state", ["Moldova", "Chisinau", "Moldovan"]),
    "Belarus": ("state", ["Belarus", "Minsk", "Belarusian"]),
    "Sudan": ("state", ["Sudan", "Khartoum", "Sudanese"]),
    "Somalia": ("state", ["Somalia", "Mogadishu", "Somali"]),
    "Afghanistan": ("state", ["Afghanistan", "Kabul", "Afghan"]),
    "Palestine": ("state", ["Palestine", "Palestinian Authority", "Ramallah", "PA"]),
    "Japan": ("state", ["Japan", "Tokyo", "Japanese"]),
    "North Korea": ("state", ["North Korea", "Pyongyang", "DPRK"]),
    "Kazakhstan": ("state", ["Kazakhstan", "Astana", "Kazakh"]),
    "Uzbekistan": ("state", ["Uzbekistan", "Tashkent", "Uzbek"]),
    "Turkmenistan": ("state", ["Turkmenistan", "Ashgabat"]),
    # organisations
    "NATO": ("org", ["NATO", "North Atlantic Treaty Organization", "Alliance"]),
    "European Union": ("org", ["EU", "European Union", "Brussels", "European Commission", "European Council", "European Parliament"]),
    "United Nations": ("org", ["UN", "United Nations", "UN Security Council", "UNSC", "Security Council", "UN General Assembly"]),
    "IAEA": ("org", ["IAEA", "International Atomic Energy Agency"]),
    "Gulf Cooperation Council": ("org", ["GCC", "Gulf Cooperation Council"]),
    "Arab League": ("org", ["Arab League"]),
    "Organization of Turkic States": ("org", ["Organization of Turkic States", "OTS"]),
    "OPEC+": ("org", ["OPEC", "OPEC+"]),
    "IMF": ("org", ["IMF", "International Monetary Fund"]),
    "OSCE": ("org", ["OSCE"]),
    "ICC": ("org", ["ICC", "International Criminal Court"]),
    "ICJ": ("org", ["ICJ", "International Court of Justice"]),
    "Turkish Armed Forces": ("org", ["TSK", "Turkish Armed Forces", "Turkish military", "Turkish army"]),
    "MİT": ("org", ["MIT", "MİT", "Turkish intelligence", "National Intelligence Organization"]),
    "TBMM": ("org", ["TBMM", "Turkish parliament", "Grand National Assembly"]),
    "IDF": ("org", ["IDF", "Israel Defense Forces", "Israeli military", "Israeli army"]),
    "IRGC": ("org", ["IRGC", "Revolutionary Guards", "Islamic Revolutionary Guard Corps", "Revolutionary Guard"]),
    "CENTCOM": ("org", ["CENTCOM", "US Central Command"]),
    "Knesset": ("org", ["Knesset"]),
    "Verkhovna Rada": ("org", ["Verkhovna Rada", "Rada"]),
    # parties and armed or political groups
    "AK Parti": ("group", ["AK Parti", "AKP", "AK Party", "Justice and Development Party"]),
    "CHP": ("group", ["CHP", "Republican People's Party"]),
    "MHP": ("group", ["MHP", "Nationalist Movement Party"]),
    "DEM Party": ("group", ["DEM Party", "DEM Parti", "DEM"]),
    "İYİ Parti": ("group", ["İYİ Parti", "IYI Party", "Good Party"]),
    "PKK": ("group", ["PKK", "Kurdistan Workers' Party", "Kurdistan Workers Party"]),
    "SDF": ("group", ["SDF", "Syrian Democratic Forces", "QSD"]),
    "YPG": ("group", ["YPG", "People's Protection Units"]),
    "PYD": ("group", ["PYD"]),
    "KRG": ("group", ["KRG", "Kurdistan Regional Government", "Erbil"]),
    "KDP": ("group", ["KDP", "Kurdistan Democratic Party"]),
    "PUK": ("group", ["PUK", "Patriotic Union of Kurdistan"]),
    "Hamas": ("group", ["Hamas"]),
    "Hezbollah": ("group", ["Hezbollah", "Hizbollah", "Hizballah"]),
    "Houthis": ("group", ["Houthis", "Houthi", "Ansar Allah", "Ansarallah"]),
    "Islamic State": ("group", ["ISIS", "ISIL", "Islamic State", "Daesh", "DAESH", "IS"]),
    "HTS": ("group", ["HTS", "Hayat Tahrir al-Sham"]),
    "Popular Mobilization Forces": ("group", ["PMF", "Popular Mobilization Forces", "Hashd al-Shaabi", "Hashd"]),
    "Islamic Jihad": ("group", ["Palestinian Islamic Jihad", "Islamic Jihad", "PIJ"]),
    "Wagner / Africa Corps": ("group", ["Wagner", "Africa Corps"]),
    "Likud": ("group", ["Likud"]),
    "AfD": ("group", ["AfD", "Alternative for Germany"]),
    "RSF": ("group", ["RSF", "Rapid Support Forces"]),
    "Southern Transitional Council": ("group", ["STC", "Southern Transitional Council"]),
    "LNA (Haftar)": ("group", ["LNA", "Libyan National Army", "Haftar forces"]),
    # people
    "Recep Tayyip Erdoğan": ("person", ["Erdoğan", "Erdogan", "Recep Tayyip Erdoğan", "Recep Tayyip Erdogan", "Tayyip Erdoğan"]),
    "Hakan Fidan": ("person", ["Hakan Fidan", "Fidan"]),
    "Yaşar Güler": ("person", ["Yaşar Güler", "Yasar Guler"]),
    "İbrahim Kalın": ("person", ["İbrahim Kalın", "Ibrahim Kalin", "Kalın", "Kalin"]),
    "Cevdet Yılmaz": ("person", ["Cevdet Yılmaz", "Cevdet Yilmaz"]),
    "Mehmet Şimşek": ("person", ["Mehmet Şimşek", "Mehmet Simsek", "Şimşek", "Simsek"]),
    "Devlet Bahçeli": ("person", ["Devlet Bahçeli", "Devlet Bahceli", "Bahçeli", "Bahceli"]),
    "Özgür Özel": ("person", ["Özgür Özel", "Ozgur Ozel"]),
    "Ekrem İmamoğlu": ("person", ["Ekrem İmamoğlu", "Ekrem Imamoglu", "İmamoğlu", "Imamoglu"]),
    "Mansur Yavaş": ("person", ["Mansur Yavaş", "Mansur Yavas"]),
    "Abdullah Öcalan": ("person", ["Abdullah Öcalan", "Abdullah Ocalan", "Öcalan", "Ocalan"]),
    "Selahattin Bayraktaroğlu": ("person", ["Bayraktaroğlu", "Bayraktaroglu"]),
    "Ali Yerlikaya": ("person", ["Ali Yerlikaya", "Yerlikaya"]),
    "Numan Kurtulmuş": ("person", ["Numan Kurtulmuş", "Numan Kurtulmus", "Kurtulmuş"]),
    "Donald Trump": ("person", ["Trump", "Donald Trump", "Donald J. Trump", "President Trump"]),
    "JD Vance": ("person", ["Vance", "JD Vance", "J.D. Vance"]),
    "Marco Rubio": ("person", ["Rubio", "Marco Rubio"]),
    "Pete Hegseth": ("person", ["Hegseth", "Pete Hegseth"]),
    "Steve Witkoff": ("person", ["Witkoff", "Steve Witkoff"]),
    "Tom Barrack": ("person", ["Barrack", "Tom Barrack", "Thomas Barrack"]),
    "Vladimir Putin": ("person", ["Putin", "Vladimir Putin"]),
    "Sergei Lavrov": ("person", ["Lavrov", "Sergei Lavrov", "Sergey Lavrov"]),
    "Volodymyr Zelensky": ("person", ["Zelensky", "Zelenskyy", "Volodymyr Zelensky", "Volodymyr Zelenskyy", "Zelenskiy"]),
    "Andrii Sybiha": ("person", ["Sybiha", "Andrii Sybiha", "Andriy Sybiha"]),
    "Ali Khamenei": ("person", ["Khamenei", "Ali Khamenei", "Supreme Leader"]),
    "Masoud Pezeshkian": ("person", ["Pezeshkian", "Masoud Pezeshkian"]),
    "Abbas Araghchi": ("person", ["Araghchi", "Abbas Araghchi", "Araqchi"]),
    "Ali Larijani": ("person", ["Larijani", "Ali Larijani"]),
    "Benjamin Netanyahu": ("person", ["Netanyahu", "Benjamin Netanyahu", "Bibi"]),
    "Israel Katz": ("person", ["Israel Katz"]),
    "Gideon Saar": ("person", ["Gideon Saar", "Sa'ar", "Saar"]),
    "Eyal Zamir": ("person", ["Eyal Zamir", "Zamir"]),
    "Bezalel Smotrich": ("person", ["Smotrich", "Bezalel Smotrich"]),
    "Itamar Ben-Gvir": ("person", ["Ben-Gvir", "Ben Gvir", "Itamar Ben-Gvir"]),
    "Gadi Eisenkot": ("person", ["Eisenkot", "Gadi Eisenkot"]),
    "Naftali Bennett": ("person", ["Bennett", "Naftali Bennett"]),
    "Yair Lapid": ("person", ["Lapid", "Yair Lapid"]),
    "Ahmed al-Sharaa": ("person", ["al-Sharaa", "Ahmed al-Sharaa", "Sharaa", "al-Shara", "Jolani", "al-Jolani"]),
    "Asaad al-Shaibani": ("person", ["Shaibani", "al-Shaibani", "Asaad al-Shaibani"]),
    "Mazloum Abdi": ("person", ["Mazloum Abdi", "Mazlum Abdi", "Mazloum Kobani", "Mazlum Kobani"]),
    "Mohammed bin Salman": ("person", ["Mohammed bin Salman", "MBS", "Crown Prince Mohammed"]),
    "Khalid bin Salman": ("person", ["Khalid bin Salman"]),
    "Faisal bin Farhan": ("person", ["Faisal bin Farhan"]),
    "Mohamed bin Zayed": ("person", ["Mohamed bin Zayed", "Mohammed bin Zayed", "MBZ"]),
    "Tahnoon bin Zayed": ("person", ["Tahnoon bin Zayed", "Tahnoun bin Zayed"]),
    "Tamim bin Hamad": ("person", ["Tamim bin Hamad", "Sheikh Tamim", "Emir Tamim"]),
    "Abdel Fattah al-Sisi": ("person", ["Sisi", "el-Sisi", "al-Sisi", "Abdel Fattah al-Sisi"]),
    "King Abdullah II": ("person", ["King Abdullah"]),
    "Mohammed Shia al-Sudani": ("person", ["Sudani", "al-Sudani", "Mohammed Shia al-Sudani"]),
    "Masrour Barzani": ("person", ["Masrour Barzani"]),
    "Nechirvan Barzani": ("person", ["Nechirvan Barzani"]),
    "Masoud Barzani": ("person", ["Masoud Barzani"]),
    "Bafel Talabani": ("person", ["Bafel Talabani"]),
    "Shehbaz Sharif": ("person", ["Shehbaz Sharif"]),
    "Asim Munir": ("person", ["Asim Munir"]),
    "Ilham Aliyev": ("person", ["Aliyev", "Ilham Aliyev"]),
    "Nikol Pashinyan": ("person", ["Pashinyan", "Nikol Pashinyan"]),
    "Kyriakos Mitsotakis": ("person", ["Mitsotakis", "Kyriakos Mitsotakis"]),
    "Nikos Christodoulides": ("person", ["Christodoulides", "Nikos Christodoulides"]),
    "Tufan Erhürman": ("person", ["Erhürman", "Erhurman", "Tufan Erhürman"]),
    "Emmanuel Macron": ("person", ["Macron", "Emmanuel Macron"]),
    "Friedrich Merz": ("person", ["Merz", "Friedrich Merz"]),
    "Keir Starmer": ("person", ["Starmer", "Keir Starmer"]),
    "Giorgia Meloni": ("person", ["Meloni", "Giorgia Meloni"]),
    "Viktor Orbán": ("person", ["Orbán", "Orban", "Viktor Orbán", "Viktor Orban"]),
    "Aleksandar Vučić": ("person", ["Vučić", "Vucic", "Aleksandar Vučić", "Aleksandar Vucic"]),
    "Ursula von der Leyen": ("person", ["von der Leyen", "Ursula von der Leyen"]),
    "Kaja Kallas": ("person", ["Kallas", "Kaja Kallas"]),
    "António Costa": ("person", ["António Costa", "Antonio Costa"]),
    "Mark Rutte": ("person", ["Rutte", "Mark Rutte"]),
    "António Guterres": ("person", ["Guterres", "António Guterres", "Antonio Guterres"]),
    "Rafael Grossi": ("person", ["Grossi", "Rafael Grossi"]),
    "Xi Jinping": ("person", ["Xi Jinping", "Xi"]),
    "Narendra Modi": ("person", ["Modi", "Narendra Modi"]),
    "Khalifa Haftar": ("person", ["Haftar", "Khalifa Haftar"]),
    "Abdul Hamid Dbeibah": ("person", ["Dbeibah", "Dbeibeh", "Abdul Hamid Dbeibah"]),
    "Mahmoud Abbas": ("person", ["Mahmoud Abbas", "Abu Mazen"]),
    "Naim Qassem": ("person", ["Naim Qassem", "Qassem"]),
    "Joseph Aoun": ("person", ["Joseph Aoun"]),
    "Nawaf Salam": ("person", ["Nawaf Salam"]),
    "Abdul-Malik al-Houthi": ("person", ["Abdul-Malik al-Houthi", "Abdulmalik al-Houthi"]),
}

TITLES = re.compile(
    r"^(?:(?:the|former|ex-|acting|interim|outgoing|incoming|deputy|vice|chief|senior)\s+)*"
    r"(?:(?:turkish|us|u\.s\.|american|russian|iranian|israeli|syrian|iraqi|saudi|emirati|qatari|greek|french|german|british|ukrainian|egyptian|"
    r"chinese|eu|european|nato|un)\s+)?"
    r"(?:president|prime minister|pm|foreign minister|defen[cs]e minister|interior minister|finance minister|minister|secretary of state|"
    r"defen[cs]e secretary|secretary|chancellor|king|emir|crown prince|sheikh|supreme leader|ayatollah|general|gen\.|lt\. gen\.|admiral|"
    r"ambassador|envoy|special envoy|speaker|leader|spokesperson|spokesman|mr\.?|mrs\.?|ms\.?|dr\.?)\s+",
    re.I,
)

_LOOKUP: dict[str, str] = {}
for _canon, (_kind, _al) in ACTORS.items():
    for _a in [_canon] + _al:
        _LOOKUP[_a.casefold()] = _canon


def norm_actor(name: str) -> str:
    """Map a free-text actor name to its canonical form (or a tidied version of itself)."""
    s = re.sub(r"\s+", " ", str(name or "")).strip(" .,;:'\"")
    if not s:
        return ""
    k = s.casefold()
    if k in _LOOKUP:
        return _LOOKUP[k]
    t = TITLES.sub("", s).strip()
    if t.casefold() in _LOOKUP:
        return _LOOKUP[t.casefold()]
    k2 = re.sub(r"['’]s$", "", t).strip()
    if k2.casefold() in _LOOKUP:
        return _LOOKUP[k2.casefold()]
    # "Russian forces", "Iranian officials" -> the state
    m = re.match(r"^(\w[\w\-]*)\s+(?:forces|troops|officials|government|army|military|navy|authorities|delegation|leadership|drones?)$", t, re.I)
    if m and m.group(1).casefold() in _LOOKUP:
        return _LOOKUP[m.group(1).casefold()]
    return t[:60]


# ambiguous short aliases only count when written exactly (case-sensitive) and never as dictionary hits in lower case
_STRICT = {"US", "UN", "EU", "UK", "IS", "PA", "DEM", "MIT", "Xi", "OTS", "STC", "LNA", "Alliance", "Rada", "Hashd", "Saar", "Qassem",
           "Zamir", "Bennett", "Lapid", "Barrack", "Kalin", "Fidan", "Vance", "Merz", "Grossi", "Modi", "Turkish", "American", "Russian",
           "Iranian", "Israeli", "Syrian", "Iraqi", "Saudi", "Greek", "French", "German", "British", "Polish", "Indian government", "Chinese",
           "Japanese", "Italian", "Afghan", "Somali", "Jordan", "Georgian government", "Xi Jinping", "Supreme Leader", "King Abdullah",
           "Security Council", "Washington", "London", "Paris", "Rome", "Berlin", "Sofia", "Brussels", "Erbil", "Doha", "Cairo", "Athens"}
_MATCHERS = []
for _canon, (_kind, _al) in ACTORS.items():
    words = sorted({a for a in [_canon] + _al}, key=len, reverse=True)
    loose = [w for w in words if w not in _STRICT and len(w) > 3]
    strict = [w for w in words if w in _STRICT or len(w) <= 3]
    pats = []
    if loose:
        pats.append(re.compile(r"(?<![\w\-])(?:" + "|".join(re.escape(w) for w in loose) + r")(?![\w])", re.I))
    if strict:
        pats.append(re.compile(r"(?<![\w\-])(?:" + "|".join(re.escape(w) for w in strict) + r")(?![\w\-])"))
    _MATCHERS.append((_canon, pats))

# capital-city aliases are too noisy for dictionary tagging of free text
_NO_TAG = {"Ankara", "Washington", "Moscow", "Kyiv", "Kiev", "Tehran", "Damascus", "Baghdad", "Riyadh", "Abu Dhabi", "Doha", "Muscat",
           "Manama", "Cairo", "Amman", "Beirut", "Tripoli", "Athens", "Nicosia", "Baku", "Yerevan", "Tbilisi", "Islamabad", "Beijing",
           "New Delhi", "Paris", "Berlin", "London", "Rome", "Warsaw", "Bucharest", "Sofia", "Budapest", "Belgrade", "Chisinau", "Minsk",
           "Khartoum", "Mogadishu", "Kabul", "Ramallah", "Tokyo", "Pyongyang", "Astana", "Tashkent", "Ashgabat", "Brussels", "Erbil",
           "Alliance", "Downing Street", "Elysee", "Élysée", "Kremlin", "White House", "Pentagon"}
_MATCHERS_TAG = []
for _canon, (_kind, _al) in ACTORS.items():
    words = sorted({a for a in [_canon] + _al if a not in _NO_TAG}, key=len, reverse=True)
    loose = [w for w in words if w not in _STRICT and len(w) > 3]
    strict = [w for w in words if w in _STRICT or len(w) <= 3]
    pats = []
    if loose:
        pats.append(re.compile(r"(?<![\w\-])(?:" + "|".join(re.escape(w) for w in loose) + r")(?![\w])", re.I))
    if strict:
        pats.append(re.compile(r"(?<![\w\-])(?:" + "|".join(re.escape(w) for w in strict) + r")(?![\w\-])"))
    _MATCHERS_TAG.append((_canon, pats))


def tag_actors(text: str, limit: int = 8) -> list[str]:
    """Dictionary match of known actors in free text, most mentioned first."""
    if not text:
        return []
    hits = []
    for canon, pats in _MATCHERS_TAG:
        n = 0
        first = 10 ** 9
        for p in pats:
            for m in p.finditer(text):
                n += 1
                first = min(first, m.start())
        if n:
            hits.append((-n, first, canon))
    hits.sort()
    return [h[2] for h in hits[:limit]]


def actor_kinds() -> dict:
    return {k: v[0] for k, v in ACTORS.items()}


def bucket_of(day: str) -> str:
    y, m, d = day[:4], day[5:7], int(day[8:10])
    return f"{y}-{m}-" + "abcdef"[min(5, (d - 1) // 5)]


def canon_json(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(obj) -> str:
    return hashlib.sha1(canon_json(obj).encode("utf-8")).hexdigest()[:16]


def merge_actors(*lists, limit: int = 8) -> list[str]:
    out = []
    for L in lists:
        for a in L or []:
            c = norm_actor(a)
            if c and c not in out:
                out.append(c)
    return out[:limit]


# ---------------------------------------------------------------- collector side
def story_event(s: dict, day: str) -> dict:
    text = f"{s.get('headline', '')}. {s.get('summary', '')}"
    actors = merge_actors(s.get("actors") or [], tag_actors(text, 6), limit=8)
    return {
        "id": s["id"], "d": day, "h": s.get("headline", "")[:160], "s": s.get("summary", "")[:600],
        "f": s.get("files", [])[:3], "k": s.get("desk", ""), "i": s.get("importance", 3), "t": s.get("turkey", "")[:300],
        "a": actors, "p": str(s.get("place", "") or "")[:60], "y": str(s.get("type", "") or "")[:20],
        "n": len(s.get("outlets", [])), "fs": s.get("firstSeen", ""), "ls": s.get("lastSeen", ""),
        "l": [[x.get("outlet", ""), x.get("url", ""), x.get("rel", "")] for x in (s.get("links") or [])[:3]],
    }


def load_events(data: Path) -> dict:
    p = data / "kb" / "events.json"
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    return {"events": {}, "storyBuilds": {}, "backfilled": []}


def add_stories(kbs: dict, stories: list, day: str, build_at: str) -> int:
    """Replace the events of one build day with this build's stories."""
    evs = kbs["events"]
    for k in [k for k, e in evs.items() if e.get("d") == day and not e.get("desk")]:
        del evs[k]
    for s in stories:
        e = story_event(s, day)
        evs[e["id"]] = e
    kbs["storyBuilds"][day] = build_at
    return len(stories)


def write_outputs(kbs: dict, data: Path, now: datetime) -> dict:
    root = data / "kb"
    db = root / "db"
    db.mkdir(parents=True, exist_ok=True)
    cut = (now - timedelta(days=KEEP_DAYS)).strftime("%Y-%m-%d")
    kbs["events"] = {k: e for k, e in kbs["events"].items() if e.get("d", "") >= cut}
    kbs["updatedAt"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    (root / "events.json").write_text(json.dumps(kbs, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    buckets: dict[str, list] = {}
    for e in kbs["events"].values():
        buckets.setdefault(bucket_of(e["d"]), []).append(e)
    for old in db.glob("ev-*.json"):
        old.unlink()
    manifest = {"updatedAt": kbs["updatedAt"], "docs": {}}
    for b, L in sorted(buckets.items()):
        L.sort(key=lambda e: (e["d"], -e.get("i", 3), e["id"]))
        doc = {"bucket": b, "kind": "collector", "count": len(L), "events": L}
        (db / f"ev-{b}.json").write_text(json.dumps(doc, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        manifest["docs"][f"ev-{b}"] = {"hash": digest(L), "count": len(L), "bytes": (db / f"ev-{b}.json").stat().st_size}
    (db / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")
    try:
        shutil.copyfile(Path(__file__), root / "kb.py")
    except OSError:
        pass
    return {"events": len(kbs["events"]), "buckets": len(buckets), "storyDays": len(kbs.get("storyBuilds", {})),
            "backfilled": kbs.get("backfilled", [])[-12:]}


# ---------------------------------------------------------------- desk developments (daily task side)
def desk_events(day_doc: dict, file_id: str, day: str) -> list:
    out = []
    for n, dv in enumerate(day_doc.get("developments") or [], 1):
        h = str(dv.get("headline", "")).strip()
        t = str(dv.get("text", "") or dv.get("analysis", "")).strip()
        if not h and not t:
            continue
        src = [[s.get("outlet", ""), s.get("url", ""), s.get("grade", "")] for s in (dv.get("sources") or []) if isinstance(s, dict)][:3]
        out.append({"id": f"D{day.replace('-', '')}-{file_id}-{n}", "d": day, "h": h[:160],
                    "s": (t[:700] + ("…" if len(t) > 700 else "")), "f": [file_id],
                    "a": tag_actors(f"{h}. {t}", 8), "l": src, "desk": 1})
    return out


def _load_doc(path: Path) -> dict:
    d = json.loads(path.read_text(encoding="utf-8"))
    return d.get("data", d) if isinstance(d, dict) else {}


def cmd_sync(args) -> int:
    """Print the kb/* database documents that need writing.

    --base     collector data URL (meta/config.collectorBase); collector buckets come from <base>/kb/db/
    --kbdb     folder holding the current database copies of collection kb (ArtifactData list kb with out_dir; may be empty)
    --days     optional folder of today's file-day documents, one JSON per file named <file>.json or <file>-<date>.json,
               or a folder tree files/<file>/days/<date>.json (ArtifactData list output)
    --out      folder for the documents to write
    """
    import urllib.request

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    kbdb = Path(args.kbdb) if args.kbdb else None
    current = {}
    if kbdb and kbdb.exists():
        for p in kbdb.rglob("*.json"):
            try:
                current[p.stem] = _load_doc(p)
            except Exception:  # noqa: BLE001
                pass
    writes = []
    meta = current.get("meta") or {}
    meta = {"updatedAt": "", "ev": dict(meta.get("ev") or {}), "dk": dict(meta.get("dk") or {}), "kinds": actor_kinds()}

    # 1. collector buckets
    if args.base:
        base = args.base.rstrip("/")

        def get(url):
            with urllib.request.urlopen(url, timeout=60) as r:  # noqa: S310
                return json.loads(r.read().decode("utf-8"))
        try:
            man = get(f"{base}/kb/db/manifest.json")
        except Exception as ex:  # noqa: BLE001
            print(f"WARN collector manifest unavailable: {ex}", file=sys.stderr)
            man = {"docs": {}}
        for doc_id, info in sorted(man.get("docs", {}).items()):
            cur = current.get(doc_id)
            if cur and digest(cur.get("events", [])) == info.get("hash"):
                meta["ev"][doc_id] = {"count": info.get("count"), "hash": info.get("hash")}
                continue
            doc = get(f"{base}/kb/db/{doc_id}.json")
            (out / f"{doc_id}.json").write_text(json.dumps(doc, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
            writes.append(doc_id)
            meta["ev"][doc_id] = {"count": doc.get("count"), "hash": info.get("hash")}

        # 1b. warning-layer alerts (alerts.json -> kb/alerts)
        try:
            al = get(f"{base}/alerts.json")
            doc = {"updatedAt": al.get("updatedAt", ""), "note": al.get("note", ""), "queue": len(al.get("queue") or []),
                   "lastDigest": al.get("lastDigest", ""), "connected": bool(al.get("hello")),
                   "alerts": [{k: v for k, v in a.items() if k != "itemIds"} for a in (al.get("alerts") or [])][-200:]}
            cur = current.get("alerts")
            if not cur or digest(cur.get("alerts", [])) != digest(doc["alerts"]) or cur.get("connected") != doc["connected"]:
                (out / "alerts.json").write_text(json.dumps(doc, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
                writes.append("alerts")
        except Exception as ex:  # noqa: BLE001
            print(f"WARN alerts.json unavailable: {ex}", file=sys.stderr)

    # 2. desk developments
    if args.days:
        found = []
        root = Path(args.days)
        for p in root.rglob("*.json"):
            parts = p.parts
            fid, day = None, None
            if "days" in parts and parts.index("days") >= 1:
                fid = parts[parts.index("days") - 1]
                day = p.stem
            else:
                m = re.match(r"^([a-z]+)(?:[-_](\d{4}-\d{2}-\d{2}))?$", p.stem)
                if m:
                    fid, day = m.group(1), m.group(2)
            if fid not in FILES:
                continue
            try:
                doc = _load_doc(p)
            except Exception:  # noqa: BLE001
                continue
            day = day or str(doc.get("date", ""))[:10]
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", day or ""):
                continue
            found.append((fid, day, doc))
        touched = {}
        for fid, day, doc in found:
            b = f"dk-{bucket_of(day)}"
            if b not in touched:
                base_doc = current.get(b) or {"bucket": bucket_of(day), "kind": "desk", "events": []}
                touched[b] = {e["id"]: e for e in base_doc.get("events", [])}
            evs = touched[b]
            for k in [k for k, e in evs.items() if e.get("d") == day and fid in e.get("f", [])]:
                del evs[k]
            for e in desk_events(doc, fid, day):
                evs[e["id"]] = e
        for b, evs in touched.items():
            L = sorted(evs.values(), key=lambda e: (e["d"], FILES.index(e["f"][0]) if e["f"] and e["f"][0] in FILES else 99, e["id"]))
            doc = {"bucket": b[3:], "kind": "desk", "count": len(L), "events": L}
            cur = current.get(b)
            if cur and digest(cur.get("events", [])) == digest(L):
                meta["dk"][b] = {"count": len(L)}
                continue
            (out / f"{b}.json").write_text(json.dumps(doc, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
            writes.append(b)
            meta["dk"][b] = {"count": len(L)}

    if [w for w in writes if w != "alerts"] or not current.get("meta"):
        meta["updatedAt"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        meta["total"] = sum(v.get("count") or 0 for v in meta["ev"].values()) + sum(v.get("count") or 0 for v in meta["dk"].values())
        (out / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        writes.append("meta")
    big = [w for w in writes if (out / f"{w}.json").stat().st_size > 700_000]
    print(json.dumps({"write": [{"collection": "kb", "doc_id": w, "file_path": str((out / f"{w}.json").resolve()),
                                 "bytes": (out / f"{w}.json").stat().st_size} for w in writes], "tooBig": big}, ensure_ascii=False, indent=1))
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd")
    s = sub.add_parser("sync")
    s.add_argument("--base", default="")
    s.add_argument("--kbdb", default="")
    s.add_argument("--days", default="")
    s.add_argument("--out", required=True)
    t = sub.add_parser("tag")
    t.add_argument("text")
    a = ap.parse_args(argv)
    if a.cmd == "sync":
        return cmd_sync(a)
    if a.cmd == "tag":
        print(tag_actors(a.text))
        return 0
    ap.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
