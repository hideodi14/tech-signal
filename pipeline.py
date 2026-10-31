#!/usr/bin/env python3
"""테크 시그널 데일리 파이프라인

수집 → 중복·기처리 제거 → 1차 선별(점수) → 구조화(근거 검증) → data/latest.json

  python pipeline.py           # 실제 실행 (환경변수 LLM_API_KEY 필요)
  python pipeline.py --sample  # 네트워크 없이 관리자 화면 확인용 샘플 데이터 생성

핵심 원칙
  1. LLM이 '사실'로 내놓는 값은 원문 구절(evidence)이 있어야 하고, 파이썬이 그 구절이
     실제 원문에 있는지 검사한다. 없으면 버린다.
  2. '추정'은 추정이라고 표시하고, 근거가 없으면 '판단불가'로 둔다.
  3. 운영자 코멘트(사람의 판단)는 LLM이 절대 대신 쓰지 않는다. 대신 쌓인 코멘트를
     few-shot 예시로 넣어 선별 기준과 질문 방향이 운영자를 닮아가게 한다.
"""
from __future__ import annotations

import calendar
import datetime as dt
import hashlib
import html
import json
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
KST = dt.timezone(dt.timedelta(hours=9))

PROVIDER = os.getenv("LLM_PROVIDER", "anthropic").lower()  # anthropic | gemini
DEFAULT_MODELS = {"anthropic": "claude-haiku-5-5", "gemini": "gemini-3.5-flash-lite"}
MODEL = os.getenv("LLM_MODEL") or DEFAULT_MODELS.get(PROVIDER, "")
TOP_N = int(os.getenv("TOP_N", "10"))            # 하루에 구조화할 기사 수
FRESH_HOURS = int(os.getenv("FRESH_HOURS", "36"))  # 이 시간보다 오래된 기사는 제외
FEWSHOT_N = 8                                      # 프롬프트에 넣을 과거 코멘트 수
SAME_CAT_MAX = 5                                   # 그중 같은 카테고리에서 가져올 최대 수

# admin.html의 LENSES·CATEGORIES와 반드시 같게 유지하세요.
CATEGORIES = ["AI기기·가전", "엣지AI·칩", "로봇", "부품·공급망", "외주생산·EMS", "HW스타트업", "정책·인증", "기타"]
# 렌즈 이름과 정의는 사람(검수 화면)과 AI(프롬프트)가 똑같이 본다. 정의가 같아야 일치율이 의미 있다.
LENS_DEFS = {
    "부품 수급": "부품을 제때 구하나 — 리드타임, 단종(EOL), 품귀, 특정 공급사 의존, 대체품 유무",
    "외주 조립·납기": "외주 공장(EMS·ODM)이 자삽·수삽으로 제때 만들어 내나 — 생산 이관, 업체 생산 능력, 납기 지연",
    "불량·수율": "만든 것 중 얼마나 제대로 나오나 — 초기 불량, 수율, 로스, 리콜·반품",
    "설계 양산성": "시제품 설계 그대로 수천 대를 만들 수 있나 — 부품 선택, 조립·검사하기 쉬운 구조, 설계 변경",
    "양산 단가": "한 대 만드는 데 얼마 드나 — BOM 원가, 금형·초기 투자, 물량에 따른 단가",
    "수요·시장성": "누가 왜 사나 — 실제 문제를 푸나, 값만큼 쓸모 있나, 오버엔지니어링은 아닌가",
    "인증·규제": "팔기 전에 통과해야 하나 — KC·전파·안전 인증, 수출 규제, 정부 지원 요건",
}
LENSES = list(LENS_DEFS)
MAX_LENSES = 3


# ─────────────────────────────── 1. 수집 ───────────────────────────────

def gnews(q: str, lang: str = "ko") -> str:
    if lang == "ko":
        return f"https://news.google.com/rss/search?q={quote(q)}&hl=ko&gl=KR&ceid=KR:ko"
    return f"https://news.google.com/rss/search?q={quote(q)}&hl=en-US&gl=US&ceid=US:en"


# 니치에 맞춰 자유롭게 고치세요. (이름, RSS 주소)
SOURCES = [
    ("구글뉴스", gnews("하드웨어 스타트업 양산 OR 출시 연기 OR 폐업")),
    ("구글뉴스", gnews("AI 가전 OR 스마트홈 기기 출시")),
    ("구글뉴스", gnews("온디바이스 AI 칩 OR NPU 모듈")),
    ("구글뉴스", gnews("전자부품 수급 OR 리드타임 OR 단종")),
    ("구글뉴스", gnews("EMS 위탁생산 OR PCB 실장 OR SMT")),
    ("구글뉴스", gnews("전자제품 리콜 OR 불량 OR 수율")),
    ("구글뉴스", gnews("로봇 양산")),
    ("Google News", gnews("hardware startup production delay OR shuts down", "en")),
    ("Google News", gnews("AI device shipping delay OR recall", "en")),
    ("Google News", gnews("contract manufacturer electronics OR EMS", "en")),
    ("Google News", gnews("component shortage lead time", "en")),
]


def clean(text: str | None) -> str:
    text = re.sub(r"<[^>]+>", " ", text or "")
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


def collect() -> list[dict]:
    import feedparser

    cutoff = time.time() - FRESH_HOURS * 3600
    items, seen_titles = [], set()
    for name, url in SOURCES:
        feed = feedparser.parse(url)
        for e in feed.entries[:30]:
            title = clean(e.get("title"))
            link = e.get("link", "")
            if not title or not link:
                continue
            pp = e.get("published_parsed")
            if pp and calendar.timegm(pp) < cutoff:
                continue
            key = re.sub(r"[^0-9a-z가-힣]", "", title.lower())[:60]
            if key in seen_titles:
                continue
            seen_titles.add(key)
            src = e.get("source") or {}
            items.append({
                "id": hashlib.sha1(link.encode()).hexdigest()[:12],
                "title": title,
                "link": link,
                "source": clean(src.get("title")) or name,
                "published": e.get("published", ""),
                # 저작권: 원문 전체가 아니라 RSS 요약만 저장한다.
                "snippet": clean(e.get("summary"))[:600],
            })
    return items


# ─────────────────────────────── 2. LLM 호출 ───────────────────────────────

def _post(url: str, **kw):
    """무료 사용량은 분당 요청 수가 제한돼 있어서, 너무 많다(429)거나 잠깐 바쁘다(5xx)고 하면 기다렸다 다시 보낸다."""
    import requests

    for attempt in range(5):
        r = requests.post(url, **kw)
        if r.status_code not in (429, 500, 502, 503, 504):
            break
        wait = int(r.headers.get("retry-after") or 0) or 15 * (attempt + 1)
        print(f"[대기] {r.status_code} 응답, {wait}초 뒤 다시 시도", file=sys.stderr)
        time.sleep(min(wait, 90))
    r.raise_for_status()
    return r


def llm(system: str, user: str, max_tokens: int = 2000) -> str:
    key = os.environ["LLM_API_KEY"]
    if PROVIDER == "anthropic":
        r = _post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": key, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": MODEL, "max_tokens": max_tokens, "temperature": 0,
                  "system": system,
                  "messages": [{"role": "user", "content": user}]},
            timeout=120,
        )
        return "".join(b.get("text", "") for b in r.json()["content"])
    if PROVIDER == "gemini":
        r = _post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}:generateContent",
            headers={"x-goog-api-key": key, "content-type": "application/json"},
            json={"systemInstruction": {"parts": [{"text": system}]},
                  "contents": [{"role": "user", "parts": [{"text": user}]}],
                  "generationConfig": {"temperature": 0, "maxOutputTokens": max_tokens,
                                       "responseMimeType": "application/json"}},
            timeout=120,
        )
        parts = r.json()["candidates"][0].get("content", {}).get("parts", [])
        text = "".join(p.get("text", "") for p in parts if not p.get("thought"))
        if not text:
            raise ValueError("Gemini 응답이 비어 있음 (출력 길이 한도에 걸렸을 수 있음)")
        return text
    raise ValueError(f"지원하지 않는 LLM_PROVIDER: {PROVIDER}")


def parse_json(text: str):
    text = re.sub(r"^```(?:json)?|```$", "", text.strip(), flags=re.M).strip()
    start = min([i for i in (text.find("["), text.find("{")) if i >= 0], default=0)
    return json.loads(text[start:])


# ─────────────────────────────── 3. 프롬프트 ───────────────────────────────

SCORE_PROMPT = """너는 '테크 시그널'의 1차 선별기다.
테크 시그널의 정체성: "시제품과 양산 사이를 아는 사람이 읽는 AI 하드웨어".
운영자는 가전 제조사 자재과에서 부품 키팅·수량 관리와 외주 조립업체(자삽·수삽)의 납기·불량 대응을 했다.
독자: AI 기기·로봇·엣지 디바이스가 데모를 넘어 실제로 수천 대씩 만들어질 수 있는지 궁금한 사람.

각 기사에 0~10점을 매겨라.
가점
- 시제품에서 양산으로 넘어가는 이야기: 양산 시작, 출시 연기, 생산 이관, 생산 물량
- 부품 수급: 리드타임, 단종, 품귀, 특정 공급사 의존
- 외주 생산: EMS·ODM 위탁, PCB 실장, 조립 공장 이전
- 불량·수율·리콜·반품
- AI가 들어가는 생활가전·IoT 기기, 엣지 AI 칩·모듈의 실제 탑재 사례
- 하드웨어 스타트업의 실패·피벗, 특히 양산 단계에서 막힌 사례
- 인증·규제가 출시 일정에 영향을 주는 이야기
감점
- 빅테크의 일반적인 모델 발표, 소프트웨어만의 이야기, 주가·시황 기사, 행사 홍보 기사, 같은 내용의 중복 보도

아래는 운영자가 과거에 직접 코멘트를 남긴 기사들이다. 운영자가 어떤 관점을 중요하게 보는지 참고해
비슷한 관점의 기사에 가점을 줘라.
{fewshot}

각 기사의 카테고리도 하나 고른다: {categories}

출력은 JSON 배열만. 다른 말은 쓰지 마라.
[{"id": "기사 id", "score": 0~10 숫자, "why": "15자 이내 이유", "category": "카테고리"}]"""

STRUCT_PROMPT = """너는 '테크 시그널'의 기사 구조화 도우미다.
너의 결과물은 가전 제조사 자재과에서 부품 키팅과 외주 조립업체 납기·불량 대응을 해 본 운영자가 검수한다. 틀린 정보 하나가 서비스 신뢰를 무너뜨린다.
그러니 모르면 비워 두는 것이 정답이다.

## 절대 규칙
1. extracted의 모든 값에는 evidence를 붙인다. evidence는 입력된 title 또는 snippet에서
   글자 그대로 복사한 구절(5~40자)이어야 한다. 그대로 복사할 구절이 없으면 그 값을 넣지 마라.
2. 원문에 없는 스펙, 가격, 수량, 성능 수치, ROI를 절대 만들어내지 마라.
3. estimated는 원문에 판단 근거가 있을 때만 채우고, 없으면 value를 "판단불가"로 둔다.
4. 운영자 코멘트를 대신 쓰지 마라. review_question은 운영자가 코멘트를 쓰기 쉽도록
   현장 관점에서 던지는 질문 1개다. (예: "이 물량을 외주로 돌리면 부품 수급과 조립 중 어디가 먼저 막힐까?")
5. program_ids는 아래 '지원사업 목록'에 있는 id만 쓸 수 있다. 관련 있는 게 없으면 빈 배열.
6. summary는 원문 문장을 베끼지 말고 너의 말로 2문장 이내로 쓴다.
7. category는 입력의 category_hint를 기본으로 하되, 명백히 틀렸으면 고친다. 목록: {categories}

## 관점 렌즈
ai_lenses에는 운영자가 이 기사를 볼 때 가장 따져볼 관점을 아래 목록에서 0~{max_lenses}개 고른다.
{lenses}
- 이름이 아니라 정의를 기준으로 고른다. 두 렌즈 사이에서 애매하면 정의에 더 가까운 쪽 하나만.
- 늦어지는 원인이 부품이 안 와서면 '부품 수급', 공장이 못 만들어서면 '외주 조립·납기'.
- 불량이 결과로 드러난 이야기면 '불량·수율', 원인이 설계에 있으면 '설계 양산성'.
- 렌즈 이름은 위에 적힌 그대로 쓴다. 목록 밖의 말은 쓰지 마라. 억지로 채우지 말고, 맞는 게 없으면 빈 배열 [].
- 아래 과거 코멘트에서 운영자가 어떤 렌즈를 골랐는지가 가장 중요한 단서다.

## 운영자의 관점 (과거 코멘트 예시, 같은 카테고리 우선)
아래 코멘트에서 운영자가 무엇을 따지는지 읽고, ai_lenses·estimated·review_question을 그 관점에 맞춰라.
{fewshot}

## 지원사업 목록
{programs}

## 출력 형식 (JSON 객체만, 다른 말 금지)
{
  "summary": "2문장 이내 요약",
  "category": "목록 중 하나",
  "ai_lenses": ["렌즈 목록 중 0~3개"],
  "extracted": {
    "companies": [{"value": "기업명", "evidence": "원문 그대로 구절"}],
    "stage": {"value": "연구|시제품|파일럿|양산|상용판매 중 하나", "evidence": "원문 그대로 구절"} 또는 null,
    "numbers": [{"value": "가격·수량·성능 등 수치", "evidence": "원문 그대로 구절"}]
  },
  "estimated": {
    "production_difficulty": {"value": "낮음|중간|높음|판단불가 (시제품을 수천 대 단위로 만들기 어려운 정도)", "reason": "근거 1문장", "confidence": "낮음|중간|높음"},
    "likely_bottleneck": {"value": "예상 병목 한 구절 또는 판단불가", "reason": "근거 1문장", "confidence": "낮음|중간|높음"}
  },
  "program_ids": [],
  "failure_case": {"cause_tags": ["양산전환", "부품수급", "외주·조립", "불량·리콜", "납기지연", "인증", "자금", "수요부족", "기술한계" 등], "evidence": "원문 그대로 구절"} 또는 null,
  "review_question": "현장 관점 질문 1개"
}"""


# 대조군: 운영자 코멘트 없이 고른 렌즈. 개인화 효과(일치율 차이)를 재기 위한 기준선이다.
BASELINE_PROMPT = """너는 제조 현장 기술 기사를 읽고 현장 엔지니어가 따져볼 관점을 고르는 분류기다.
아래 렌즈 목록에서 이 기사에 해당하는 것을 0~{max_lenses}개 고른다.
{lenses}
이름이 아니라 정의를 기준으로 고르고, 렌즈 이름은 위에 적힌 그대로 쓴다.
늦어지는 원인이 부품이 안 와서면 '부품 수급', 공장이 못 만들어서면 '외주 조립·납기'.
불량이 결과로 드러난 이야기면 '불량·수율', 원인이 설계에 있으면 '설계 양산성'.
목록 밖의 말은 쓰지 마라. 맞는 게 없으면 빈 배열.
출력은 JSON 객체만: {"lenses": []}"""


def fill(prompt: str, **kw: str) -> str:
    """중괄호가 많은 JSON 예시가 있어 str.format 대신 지정한 자리만 바꾼다."""
    base = {"categories": " / ".join(CATEGORIES),
            "lenses": "\n".join(f"- {k}: {v}" for k, v in LENS_DEFS.items()),
            "max_lenses": str(MAX_LENSES)}
    for k, v in {**base, **kw}.items():
        prompt = prompt.replace("{" + k + "}", v)
    return prompt


def fewshot_block(comments: list[dict], category: str | None = None) -> str:
    """같은 카테고리 코멘트를 최대 SAME_CAT_MAX개 먼저 고르고, 나머지는 다른 카테고리로 채운다.
    각 묶음 안에서는 ⭐(학습 예시) → 최신 순. 한 관점에 쏠리지 않도록 다른 카테고리도 일부 남긴다."""
    usable = [c for c in comments if (c.get("comment") or "").strip() and not c.get("skipped")]
    key = lambda c: (bool(c.get("starred")), c.get("date", ""), c.get("saved_at", ""))  # noqa: E731
    same = sorted([c for c in usable if category and c.get("category") == category],
                  key=key, reverse=True)[:SAME_CAT_MAX]
    same_ids = {id(c) for c in same}
    others = sorted([c for c in usable if id(c) not in same_ids],
                    key=key, reverse=True)[:FEWSHOT_N - len(same)]
    picked = others[::-1] + same[::-1]  # 같은 카테고리를 프롬프트 끝(가장 가까운 자리)에
    if not picked:
        return "(아직 쌓인 코멘트 없음 — 위 기준만 따른다)"
    lines = []
    for c in picked:
        lens = c.get("my_lenses")
        lens_txt = ", ".join(lens) if lens else ("해당 없음" if lens == [] else "기록 없음")
        lines.append(f"- [{c.get('category', '기타')}] 기사: {c['title']}\n"
                     f"  운영자 렌즈: {lens_txt}\n  운영자 코멘트: {c['comment']}")
    return "\n".join(lines)


def clean_lenses(v) -> list[str]:
    if not isinstance(v, list):
        return []
    return [x for x in dict.fromkeys(str(i).strip() for i in v) if x in LENSES][:MAX_LENSES]


# ─────────────────────────────── 4. 선별 · 구조화 · 검증 ───────────────────────────────

def score(items: list[dict], fewshot: str) -> list[dict]:
    system = fill(SCORE_PROMPT, fewshot=fewshot)
    scored: dict[str, dict] = {}
    for i in range(0, len(items), 40):
        chunk = [{"id": x["id"], "title": x["title"], "snippet": x["snippet"][:200]}
                 for x in items[i:i + 40]]
        try:
            for r in parse_json(llm(system, json.dumps(chunk, ensure_ascii=False), 3000)):
                scored[str(r.get("id"))] = r
        except Exception as e:  # 한 묶음이 실패해도 나머지는 진행
            print(f"[경고] 점수 매기기 실패: {e}", file=sys.stderr)
    for x in items:
        r = scored.get(x["id"], {})
        try:
            x["score"] = float(r.get("score", 0))
        except (TypeError, ValueError):
            x["score"] = 0.0
        x["why"] = str(r.get("why", ""))[:30]
        x["category_hint"] = r.get("category") if r.get("category") in CATEGORIES else "기타"
    return sorted(items, key=lambda x: x["score"], reverse=True)


def _norm(s: str | None) -> str:
    return re.sub(r"\s+", "", s or "").lower()


def verify(data: dict, src: dict, valid_program_ids: set[str]) -> int:
    """evidence가 원문에 실제로 있는지 검사하고, 없는 값은 제거한다. 제거한 개수를 돌려준다."""
    text = _norm(src["title"] + " " + src["snippet"])
    dropped = 0

    def ok(f) -> bool:
        return isinstance(f, dict) and bool(f.get("value")) and \
            len(_norm(f.get("evidence"))) >= 2 and _norm(f.get("evidence")) in text

    ex = data.get("extracted") or {}
    for k in ("companies", "numbers"):
        vals = ex.get(k) or []
        kept = [v for v in vals if ok(v)]
        dropped += len(vals) - len(kept)
        ex[k] = kept
    if ex.get("stage") is not None and not ok(ex.get("stage")):
        ex["stage"] = None
        dropped += 1
    data["extracted"] = ex

    fc = data.get("failure_case")
    if fc and not ok({"value": "x", "evidence": fc.get("evidence")}):
        data["failure_case"] = None
        dropped += 1

    ids = [str(p) for p in (data.get("program_ids") or [])]
    data["program_ids"] = [p for p in ids if p in valid_program_ids]
    dropped += len(ids) - len(data["program_ids"])

    data["ai_lenses"] = clean_lenses(data.get("ai_lenses"))
    if data.get("category") not in CATEGORIES:
        data["category"] = src.get("category_hint", "기타")

    est = data.get("estimated") or {}
    for k in ("production_difficulty", "likely_bottleneck"):
        if not isinstance(est.get(k), dict) or not est[k].get("value"):
            est[k] = {"value": "판단불가", "reason": "", "confidence": "낮음"}
    data["estimated"] = est
    return dropped


def baseline_lenses(item: dict) -> list[str] | None:
    """few-shot 없이 고른 렌즈. 실패하면 None (통계에서 제외)."""
    try:
        user = json.dumps({k: item[k] for k in ("title", "snippet")}, ensure_ascii=False)
        return clean_lenses(parse_json(llm(fill(BASELINE_PROMPT), user, 200)).get("lenses"))
    except Exception as e:
        print(f"[경고] 기준선 렌즈 실패 {item['id']}: {e}", file=sys.stderr)
        return None


def structure(item: dict, comments: list[dict], programs: list[dict]) -> dict:
    prog = json.dumps([{k: p.get(k) for k in ("id", "title", "keywords")} for p in programs],
                      ensure_ascii=False) if programs else "(현재 등록된 지원사업 없음)"
    fewshot = fewshot_block(comments, item.get("category_hint"))
    system = fill(STRUCT_PROMPT, fewshot=fewshot, programs=prog)
    user = json.dumps({k: item.get(k, "") for k in ("title", "source", "snippet", "category_hint")},
                      ensure_ascii=False)
    data = parse_json(llm(system, user, 1500))
    if not isinstance(data, dict):
        raise ValueError("구조화 결과가 JSON 객체가 아님")
    data.pop("id", None)
    data["dropped"] = verify(data, item, {p["id"] for p in programs})
    by_id = {p["id"]: p for p in programs}
    data["programs"] = [{k: by_id[i].get(k) for k in ("id", "title", "agency", "deadline", "url")}
                        for i in data["program_ids"]]
    data["ai_lenses_baseline"] = baseline_lenses(item)
    return data


# ─────────────────────────────── 5. 저장 ───────────────────────────────

def load_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def save_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=1), encoding="utf-8")


def write_meta() -> None:
    """공개 페이지(index.html)가 렌즈 정의·카테고리를 여기서 읽는다. 정의를 파이썬 한 곳에서만 관리하기 위해."""
    save_json(DATA / "meta.json", {"lenses": LENS_DEFS, "categories": CATEGORIES})


def active_programs(programs: list[dict], today: str) -> list[dict]:
    return [p for p in programs if p.get("id") and (not p.get("deadline") or p["deadline"] >= today)]


def main() -> None:
    if "--sample" in sys.argv:
        write_sample()
        return
    if not os.getenv("LLM_API_KEY"):
        sys.exit("환경변수 LLM_API_KEY가 필요합니다.")
    if PROVIDER not in DEFAULT_MODELS:
        sys.exit(f"LLM_PROVIDER는 anthropic 또는 gemini여야 합니다 (지금: {PROVIDER})")

    now = dt.datetime.now(KST)
    today = now.date().isoformat()
    seen_list: list[str] = load_json(DATA / "seen.json", [])
    seen = set(seen_list)
    comments = load_json(DATA / "comments.json", [])
    programs = active_programs(load_json(DATA / "programs.json", []), today)
    fewshot = fewshot_block(comments)

    items = [x for x in collect() if x["id"] not in seen]
    print(f"새 기사 {len(items)}건 수집")

    ranked = score(items, fewshot)[:TOP_N] if items else []
    for x in ranked:
        try:
            x.update(structure(x, comments, programs))
        except Exception as e:
            x["error"] = str(e)[:200]
            print(f"[경고] 구조화 실패 {x['id']}: {e}", file=sys.stderr)
        time.sleep(0.5)

    payload = {"date": today, "generated_at": now.isoformat(timespec="minutes"),
               "model": f"{PROVIDER}/{MODEL}", "items": ranked}
    save_json(DATA / "latest.json", payload)
    save_json(DATA / "archive" / f"{today}.json", payload)
    write_meta()

    seen_list += [x["id"] for x in items]
    save_json(DATA / "seen.json", seen_list[-5000:])
    print(f"완료: {len(ranked)}건 → data/latest.json")


# ─────────────────────────────── 샘플 데이터 ───────────────────────────────

def write_sample() -> None:
    today = dt.datetime.now(KST).date().isoformat()
    est = lambda v, r, c: {"value": v, "reason": r, "confidence": c}  # noqa: E731
    items = [
        {
            "id": "sample000001", "title": "[샘플] A사, AI 음성비서 탑재 소형 가전 출시 3개월 연기",
            "link": "https://example.com/1", "source": "샘플뉴스", "published": "", "score": 9, "why": "양산 단계 지연",
            "snippet": "A사는 AI 음성비서를 탑재한 소형 가전의 출시를 3개월 연기한다고 밝혔다. 외주 생산을 맡은 업체의 초기 불량률이 예상보다 높았다고 설명했다.",
            "summary": "AI 기능을 넣은 소형 가전이 양산 단계에서 출시가 밀렸다. 회사는 외주 생산 초기 불량을 이유로 들었다.",
            "category": "AI기기·가전",
            "extracted": {"companies": [{"value": "A사", "evidence": "A사는 AI 음성비서를"}],
                          "stage": None,
                          "numbers": [{"value": "출시 3개월 연기", "evidence": "출시를 3개월 연기"}]},
            "estimated": {"production_difficulty": est("중간", "기능 자체보다 외주 생산 초기 불량이 일정을 잡고 있다.", "중간"),
                          "likely_bottleneck": est("외주 생산 초기 불량", "원문에 외주 업체 초기 불량률이 직접 언급됨.", "높음")},
            "program_ids": [], "programs": [], "failure_case": None, "dropped": 1,
            "review_question": "초기 불량이 부품 탓인지, 조립 탓인지, 설계 탓인지 어떻게 가를 수 있을까?",
        },
        {
            "id": "sample000002", "title": "[샘플] B스타트업, 가정용 로봇 양산 지연 끝에 사업 중단",
            "link": "https://example.com/2", "source": "샘플뉴스", "published": "", "score": 8, "why": "HW 실패 사례",
            "snippet": "B스타트업은 부품 수급 문제로 양산이 1년 넘게 지연되자 사업 중단을 발표했다.",
            "summary": "부품 조달이 막혀 양산이 장기간 밀린 하드웨어 스타트업이 결국 사업을 접었다.",
            "category": "HW스타트업",
            "extracted": {"companies": [{"value": "B스타트업", "evidence": "B스타트업은 부품 수급"}],
                          "stage": None, "numbers": [{"value": "1년 이상 지연", "evidence": "1년 넘게 지연"}]},
            "estimated": {"production_difficulty": est("판단불가", "", "낮음"),
                          "likely_bottleneck": est("부품 수급", "원문에 부품 수급 문제가 직접 언급됨.", "높음")},
            "program_ids": [], "programs": [],
            "failure_case": {"cause_tags": ["부품수급", "양산"], "evidence": "부품 수급 문제로 양산이"},
            "dropped": 0, "review_question": "설계 단계에서 어떤 부품 선택이 이런 지연을 막을 수 있었을까?",
        },
        {
            "id": "sample000003", "title": "[샘플] C사, 저전력 NPU 보드 공개",
            "link": "https://example.com/3", "source": "샘플뉴스", "published": "", "score": 7, "why": "엣지 하드웨어",
            "snippet": "C사가 저전력 NPU를 탑재한 개발 보드를 공개했다.",
            "summary": "C사가 저전력 신경망 가속기를 얹은 개발 보드를 내놓았다. 구체적인 성능 수치는 공개되지 않았다.",
            "category": "엣지AI·칩",
            "extracted": {"companies": [{"value": "C사", "evidence": "C사가 저전력"}], "stage": None, "numbers": []},
            "estimated": {"production_difficulty": est("판단불가", "", "낮음"), "likely_bottleneck": est("판단불가", "", "낮음")},
            "program_ids": [], "programs": [], "failure_case": None, "dropped": 2,
            "review_question": "이 보드를 가전에 넣어 수천 대를 만든다면 무엇부터 확인하겠는가?",
        },
    ]
    for i in range(4, 8):
        items.append({"id": f"sample00000{i}", "title": f"[샘플] 선택 검수 기사 {i}", "link": "https://example.com",
                      "source": "샘플뉴스", "published": "", "score": 10 - i, "why": "샘플", "snippet": "",
                      "summary": "선택 검수 영역 확인용 샘플입니다.", "category": "기타",
                      "extracted": {"companies": [], "stage": None, "numbers": []},
                      "estimated": {"production_difficulty": est("판단불가", "", "낮음"),
                                    "likely_bottleneck": est("판단불가", "", "낮음")},
                      "program_ids": [], "programs": [], "failure_case": None, "dropped": 0,
                      "review_question": "현장 관점에서 한 줄 남긴다면?"})
    sample_lenses = {"sample000001": (["외주 조립·납기", "불량·수율"], ["외주 조립·납기"]),
                     "sample000002": (["부품 수급", "설계 양산성"], ["부품 수급"]),
                     "sample000003": (["설계 양산성"], ["양산 단가", "설계 양산성"])}
    for it in items:
        it["category_hint"] = it["category"]
        it["ai_lenses"], it["ai_lenses_baseline"] = sample_lenses.get(it["id"], ([], []))
    save_json(DATA / "latest.json", {"date": today, "generated_at": "sample", "model": "sample", "items": items})
    write_meta()
    print("샘플 데이터 → data/latest.json")


if __name__ == "__main__":
    main()
