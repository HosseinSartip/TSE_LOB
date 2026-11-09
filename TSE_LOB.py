#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TSE_LOB.py  —  دریافت تابلو و تاریخچه دفتر سفارشات بازار سرمایه ایران (TSETMC)

برنامه مستقل TSE_LOB: سرویس محلی Flask، رابط وب تعبیه‌شده و پایگاه داده SQLite.

اجرا در ویندوز:   RUN_TSE_LOB.bat
  • فایل BAT محیط مجازی .venv را در همین پوشه می‌سازد و پیش‌نیازها را نصب می‌کند.
آزمون داخلی:   python TSE_LOB.py --self-test        (بدون اینترنت)
آزمون واقعی:   python TSE_LOB.py --live-check فولاد   (نیازمند دسترسی به TSETMC)
نیازمندی: Python 3.10 یا بالاتر

اصل طراحی: صحت داده مهم‌تر از ظاهر برنامه است. هیچ داده ساختگی، refID جعلی یا پوشش تاریخی ادعایی
تولید نمی‌شود؛ هرجا منبع داده‌ای ندهد، خانه خالی می‌ماند و در گزارش پوشش ثبت می‌شود.
"""
from __future__ import annotations

# کنترل اجرای ویندوز؛ راه‌اندازی استاندارد از طریق فایل BAT انجام می‌شود.
import os as _guard_os
import sys as _guard_sys
if (__name__ == "__main__" and _guard_os.name == "nt"
        and _guard_os.environ.get("TSE_LOB_LAUNCHED") != "1"):
    try:
        _guard_sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    print("برای نصب پیش‌نیازها و اجرای برنامه، فایل RUN_TSE_LOB.bat را اجرا کنید.")
    raise SystemExit(3)

import argparse
import collections
import csv
import datetime as _dt
import hashlib
import importlib.util
import io
import json
import logging
import math
import os
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import urllib.parse
import webbrowser
import zipfile
import zlib
from pathlib import Path

APP_VERSION = "2.0.0"
OUTPUT_ALGO_VERSION = 3  # افزایش این مقدار پس از هر تغییر اثرگذار بر محتوا/ساختار خروجی
INSTALL_CMD = "RUN_TSE_LOB.bat"


# ─────────────────── بررسی و نصب خودکار وابستگی‌ها (فقط هنگام اجرای مستقیم) ───────────────────
REQUIRED_PACKAGES = {"flask": "flask", "requests": "requests", "jdatetime": "jdatetime"}
TEST_PACKAGES = {"pyarrow": "pyarrow"}        # فقط برای --self-test (خواندن Parquet)


def _missing_packages(required=None):
    req = REQUIRED_PACKAGES if required is None else required
    return [pkg for mod, pkg in req.items() if importlib.util.find_spec(mod) is None]


def _bootstrap_deps():
    """فقط پیش‌نیازها را بررسی می‌کند؛ نصب بسته از داخل برنامه انجام نمی‌شود."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    if sys.version_info < (3, 10):
        print("Python 3.10 or newer is required / نیازمند Python 3.10 یا بالاتر است.")
        sys.exit(2)
    required = dict(REQUIRED_PACKAGES)
    if "--self-test" in sys.argv:
        required.update(TEST_PACKAGES)
    missing = _missing_packages(required)
    if missing:
        print("وابستگی‌های لازم نصب نیستند: %s" % ", ".join(missing))
        print("اجرا کنید: RUN_TSE_LOB.bat  (نصب محلی در محیط مجازی همین پوشه)")
        sys.exit(2)


if __name__ == "__main__":
    _bootstrap_deps()

# ───────────────────────────── وابستگی‌ها ─────────────────────────────
_MISSING = []
try:
    import requests
except ImportError:  # pragma: no cover
    requests = None
    _MISSING.append("requests")
try:
    import jdatetime
except ImportError:  # pragma: no cover
    jdatetime = None
    _MISSING.append("jdatetime")
try:
    import flask
    from flask import Flask, Response, jsonify, request, send_from_directory, stream_with_context
except ImportError:  # pragma: no cover
    flask = None
    _MISSING.append("flask")


def _missing_deps_message(need_flask=True):
    miss = [m for m in _MISSING if need_flask or m != "flask"]
    if not miss:
        return None
    return ("کتابخانه(های) زیر نصب نیستند: %s\nبرای نصب این دستور را اجرا کنید:\n    %s\n"
            "(Missing libraries: %s — install with: %s)" % ("، ".join(miss), INSTALL_CMD, ", ".join(miss), INSTALL_CMD))


# ───────────────────────────────── ثابت‌ها ─────────────────────────────────────
DEFAULT_API_BASE = "https://cdn.tsetmc.com/api"
DEFAULT_WINDOW = ("08:45:00", "12:30:59")      # وقت محلی بازار تهران؛ قابل تغییر در رابط
DEFAULT_DELAY = 0.6                              # ثانیه بین درخواست‌های متوالی
MAX_RANGE_DAYS = 1827                            # پنج سال شمسی (حداکثر با دو روز کبیسه)
SCHEMA_VER = 1                                   # نسخه ساختار کش؛ تغییر آن کش قدیمی را نامعتبر می‌کند
MAX_CONSEC_FAIL = 5                              # توقف پس از این تعداد روز ناموفق متوالی
MAX_CONSEC_BLOCKED = 2                           # توقف سریع‌تر هنگام مسدودشدن
BUNDLE_MAX_SYMBOLS = 3000                        # سقف تعداد نماد در یک دریافت دسته‌ای
MAX_RESPONSE_BYTES = 150 * 1024 * 1024
BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
TEHRAN_TZ = _dt.timezone(_dt.timedelta(hours=3, minutes=30))   # از ۱۴۰۱ ساعت تابستانی لغو شده است

# ستون‌های دفتر سفارشات: نام انگلیسی برای Parquet، عنوان فارسی برای CSV و نوع داده.
OB_COLS = [
    ("symbol", "نام نماد", "str"), ("company", "نام شرکت", "str"), ("industry", "نوع صنعت", "str"),
    ("date_jalali", "تاریخ شمسی", "str"), ("date_gregorian", "تاریخ میلادی", "date"), ("event_time", "ساعت", "str"),
    ("ref_id", "refID", "int"), ("level", "سطح", "int"),
    ("ask_price", "قیمت فروش", "int"), ("ask_volume", "حجم فروش", "int"), ("ask_orders", "تعداد فروش", "int"),
    ("bid_orders", "تعداد خرید", "int"), ("bid_volume", "حجم خرید", "int"), ("bid_price", "قیمت خرید", "int")]
OB_HEADERS = [c[1] for c in OB_COLS]
FORMAT_EXT = {"csv": "csv", "parquet": "parquet"}
FORMAT_ORDER = ("parquet", "csv")

# نگاشت‌های فیلد: «primary» در سه منبع مستقل (orbo، tsetmc-mcp، tse-market-data) آمده است؛
# «alias_short» فقط در tsetmc-skill مستند شده و تأییدنشده است. تشخیص نهایی بر اساس پاسخ واقعی انجام می‌شود.
OB_FIELD_SETS = [
    ("primary", dict(time="hEven", level="number",
                     b_p="pMeDem", b_q="qTitMeDem", b_o="zOrdMeDem",
                     a_p="pMeOf", a_q="qTitMeOf", a_o="zOrdMeOf")),
    ("alias_short", dict(time="hEven", level="number",
                         b_p="pd", b_q="qd", b_o="zd",
                         a_p="po", a_q="qo", a_o="zo")),
]
REF_KEYS = ("refID", "refId", "RefID", "refid", "RefId")

DAILY_KNOWN = [  # (کلید خام، عنوان فارسی، نام انگلیسی، نوع) — نگاشت بر اساس orbo و tse-market-data؛ مقادیر به ریال
    ("priceFirst", "قیمت اولین معامله", "first_price", "int"), ("pDrCotVal", "آخرین قیمت", "last_price", "int"),
    ("pClosing", "قیمت پایانی", "close_price", "int"), ("priceYesterday", "قیمت دیروز", "prev_close", "int"),
    ("priceMin", "کمترین قیمت", "low_price", "int"), ("priceMax", "بیشترین قیمت", "high_price", "int"),
    ("zTotTran", "تعداد معاملات", "trade_count", "int"), ("qTotTran5J", "حجم معاملات", "volume", "int"),
    ("qTotCap", "ارزش معاملات (ریال)", "value_rial", "int"),
]
DAILY_COMPUTED = {  # (عنوان فارسی، نام انگلیسی)
    "close": ("تغییر قیمت پایانی (محاسبه‌شده)", "close_change"), "close_pct": ("درصد تغییر پایانی (محاسبه‌شده)", "close_change_pct"),
    "last": ("تغییر آخرین قیمت (محاسبه‌شده)", "last_change"), "last_pct": ("درصد تغییر آخرین قیمت (محاسبه‌شده)", "last_change_pct")}

KIND_FA = {
    "network": "خطای شبکه", "http": "خطای HTTP", "blocked": "مسدود یا ممنوع (۴۰۳/۴۲۹)",
    "html": "پاسخ HTML به‌جای JSON", "invalid_json": "JSON نامعتبر یا ناقص",
    "schema": "ساختار پاسخ ناشناخته", "cancelled": "لغو شد",
}


class AppError(Exception):
    """خطای قابل نمایش به کاربر."""


class SchemaError(AppError):
    pass


# ───────────────────────────── ابزارهای متن/عدد/تاریخ ──────────────────────────
_DIGITS = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")


def norm_digits(s):
    return str(s).translate(_DIGITS)


def fa_chars(s):
    """حروف عربی (ي ك ى) → فارسی؛ TSETMC متن‌ها را با حروف عربی برمی‌گرداند."""
    return str(s).replace("ي", "ی").replace("ى", "ی").replace("ك", "ک")


def ar_chars(s):
    return str(s).replace("ی", "ي").replace("ک", "ك")


def fa_key(s):
    return fa_chars(norm_digits(s)).replace("\u200c", "").replace(" ", "").strip().lower()


def to_int_strict(v):
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v) if (math.isfinite(v) and v == int(v)) else None
    if isinstance(v, str):
        t = norm_digits(v).strip()
        if re.fullmatch(r"-?\d+", t):
            return int(t)
    return None


def num_or_none(v):
    """عدد را بدون گردکردن برمی‌گرداند؛ None/خالی → None؛ مقدار نامعتبر → ValueError."""
    if v is None:
        return None
    if isinstance(v, bool):
        raise ValueError("bool")
    if isinstance(v, (int, float)):
        if isinstance(v, float) and not math.isfinite(v):
            raise ValueError("nan")
        return v
    if isinstance(v, str):
        t = norm_digits(v).strip().replace(",", "")
        if t == "":
            return None
        try:
            return int(t)
        except ValueError:
            return float(t)          # ValueError در صورت نامعتبر بودن
    raise ValueError("type")


def parse_hms_int(v):
    """hEven (مثلاً 84501 یا '084501' یا '8:45:01') → عدد HHMMSS؛ نامعتبر → None."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, str) and ":" in v:
        parts = norm_digits(v).strip().split(":")
        if len(parts) != 3 or not all(p.strip().isdigit() for p in parts):
            return None
        h, m, s = (int(p) for p in parts)
        n = h * 10000 + m * 100 + s
    else:
        n = to_int_strict(v)
        if n is None or n < 0:
            return None
        h, m, s = n // 10000, (n // 100) % 100, n % 100
    if h > 23 or m > 59 or s > 59:
        return None
    return n


def hms_str(n):
    return "%02d:%02d:%02d" % (n // 10000, (n // 100) % 100, n % 100)


def parse_clock(s):
    n = parse_hms_int(str(s).strip())
    if n is None:
        raise AppError("ساعت نامعتبر است (قالب HH:MM:SS): %s" % s)
    return n


def parse_jalali(s):
    t = norm_digits(s).strip()
    m = re.fullmatch(r"(\d{4})[/\-.\s](\d{1,2})[/\-.\s](\d{1,2})", t) or re.fullmatch(r"(\d{4})(\d{2})(\d{2})", t)
    if not m:
        raise AppError("قالب تاریخ شمسی نامعتبر است؛ نمونه درست: 1403/05/01")
    try:
        return jdatetime.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        raise AppError("تاریخ شمسی وجود ندارد: %s" % t)


def jalali_to_gdate(s):
    return parse_jalali(s).togregorian()


def gdate_to_jalali_str(d):
    j = jdatetime.date.fromgregorian(date=d)
    return "%04d/%02d/%02d" % (j.year, j.month, j.day)


def deven_to_date(n):
    return _dt.date(n // 10000, (n // 100) % 100, n % 100)


def date_to_deven(d):
    return d.year * 10000 + d.month * 100 + d.day


def tehran_today():
    return _dt.datetime.now(TEHRAN_TZ).date()


def jalali_shift(jd, years=0, months=0):
    y, m = jd.year - years, jd.month - months
    while m < 1:
        m += 12
        y -= 1
    while m > 12:
        m -= 12
        y += 1
    d = jd.day
    while d > 0:
        try:
            return jdatetime.date(y, m, d)
        except ValueError:
            d -= 1
    raise AppError("shift failed")


def validate_range(start_s, end_s):
    js, je = parse_jalali(start_s), parse_jalali(end_s)
    gs, ge = js.togregorian(), je.togregorian()
    if gs > ge:
        raise AppError("تاریخ شروع نمی‌تواند بعد از تاریخ پایان باشد.")
    if ge > tehran_today():
        raise AppError("تاریخ پایان در آینده است.")
    if (ge - gs).days + 1 > MAX_RANGE_DAYS:
        raise AppError("بازه انتخابی بیش از پنج سال است (حداکثر ۵ سال).")
    return gs, ge, js, je


def candidate_days(start, end):
    """تقویم فرضی: شنبه تا چهارشنبه (پنجشنبه و جمعه تعطیل). تعطیلات رسمی حذف نمی‌شوند."""
    d = start
    while d <= end:
        if d.weekday() in (5, 6, 0, 1, 2):
            yield d
        d += _dt.timedelta(days=1)


def safe_filename(s):
    s = re.sub(r'[\\/:*?"<>|\s]+', "_", str(s)).strip("._")
    return s or "symbol"


# ──────────────────────────── فشرده‌سازی رکوردهای خام ─────────────────────────
def pack_records(records):
    cols = []
    seen = set()
    for r in records:
        if isinstance(r, dict):
            for k in r:
                if k not in seen:
                    seen.add(k)
                    cols.append(k)
    rows = [[r.get(c) for c in cols] if isinstance(r, dict) else None for r in records]
    raw = json.dumps({"cols": cols, "rows": rows}, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return zlib.compress(raw, 6)


def unpack_records(blob):
    obj = json.loads(zlib.decompress(blob).decode("utf-8"))
    cols = obj["cols"]
    return [dict(zip(cols, row)) if row is not None else None for row in obj["rows"]]


# ───────────────────────────── تحلیل پاسخ‌های TSETMC ──────────────────────────
def parse_search(payload):
    items = payload.get("instrumentSearch") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        raise SchemaError("کلید instrumentSearch در پاسخ جست‌وجو یافت نشد.")
    out = []
    for it in items:
        if not isinstance(it, dict):
            continue
        ins = str(it.get("insCode") or "").strip()
        if not ins.isdigit():
            continue
        out.append({"ins_code": ins,
                    "symbol": fa_chars(str(it.get("lVal18AFC") or "").strip()),
                    "name": fa_chars(str(it.get("lVal30") or "").strip()),
                    "market": fa_chars(str(it.get("flowTitle") or "").strip()),
                    "isin": str(it.get("cIsin") or "").strip()})
    return out


def parse_identity(payload):
    ident = payload.get("instrumentIdentity") if isinstance(payload, dict) else None
    if not isinstance(ident, dict):
        raise SchemaError("کلید instrumentIdentity در پاسخ یافت نشد.")
    sector = ident.get("sector") if isinstance(ident.get("sector"), dict) else {}
    company = ident.get("lSoc30") or ident.get("lVal30") or ""
    return {"symbol": fa_chars(str(ident.get("lVal18AFC") or "").strip()),
            "company": fa_chars(str(company).strip()),
            "industry": fa_chars(str(sector.get("lSecVal") or "").strip())}


def classify_list_payload(payload, key):
    """('ok', list) | ('empty', []) | ('schema', پیام) — پاسخ خالیِ معتبر با ساختار نامعتبر یکی نیست."""
    if not isinstance(payload, dict):
        return "schema", "پاسخ از نوع شیء JSON نیست (نوع: %s)" % type(payload).__name__
    if key not in payload:
        return "schema", "کلید %s در پاسخ نیست؛ کلیدهای موجود: %s" % (key, ", ".join(list(payload)[:8]) or "—")
    val = payload[key]
    if val is None or (isinstance(val, list) and len(val) == 0):
        return "empty", []
    if not isinstance(val, list):
        return "schema", "مقدار %s فهرست نیست." % key
    return "ok", val


def detect_ob_schema(records):
    keys = set()
    for r in records[:500]:
        if isinstance(r, dict):
            keys.update(r.keys())
    if not keys:
        return None, "رکوردها شیء JSON نیستند."
    for name, fm in OB_FIELD_SETS:
        if fm["time"] in keys and fm["level"] in keys \
                and all(fm[k] in keys for k in ("b_p", "b_q", "b_o", "a_p", "a_q", "a_o")):
            ref = next((k for k in REF_KEYS if k in keys), None)
            return name, dict(fm, ref=ref)
    return None, ("فیلدهای لازم بازسازی (hEven، number و فیلدهای خرید/فروش هر دو سمت) یافت نشد؛ "
                  "کلیدهای دیده‌شده: " + ", ".join(sorted(keys)[:14]))


Event = collections.namedtuple("Event", "t ref idx level bid ask")
Snapshot = collections.namedtuple("Snapshot", "t rows")


def _side(r, kp, kq, ko):
    p, q, o = num_or_none(r.get(kp)), num_or_none(r.get(kq)), num_or_none(r.get(ko))
    if p is None and q is None and o is None:
        return None            # این سمت در این رکورد به‌روز نشده است
    return (p, q, o)


def normalize_ob_events(records, fm):
    events, invalid = [], 0
    for idx, r in enumerate(records):
        if not isinstance(r, dict):
            invalid += 1
            continue
        t = parse_hms_int(r.get(fm["time"]))
        lv = to_int_strict(r.get(fm["level"]))
        if t is None or lv is None or not 1 <= lv <= 5:
            invalid += 1
            continue
        try:
            bid = _side(r, fm["b_p"], fm["b_q"], fm["b_o"])
            ask = _side(r, fm["a_p"], fm["a_q"], fm["a_o"])
        except ValueError:
            invalid += 1
            continue
        ref = to_int_strict(r.get(fm["ref"])) if fm.get("ref") else None
        events.append(Event(t, ref, idx, lv, bid, ask))
    return events, invalid


def reconstruct_snapshots(events, win_start, win_end, stats=None):
    """
    بازسازی وضعیت کامل ۵ سطح از رویدادهای تغییرمحور (یک روز).
    ۱) مرتب‌سازی بر اساس زمان و سپس refID (در صورت وجود برای همه رکوردها) یا ترتیب منبع.
    ۲) وضعیت هر سطح فقط با داده‌های گذشته پر می‌شود؛ سطحِ بدون داده «ناموجود» می‌ماند.
    ۳) در هر زمان، پس از اعمال همه تغییرات همان زمان، یک اسنپ‌شات ثبت می‌شود.
    ۴) بازسازی با کل داده روز انجام می‌شود، ولی فقط اسنپ‌شات‌های داخل بازه خروجی گرفته می‌شوند.
    """
    st = stats if stats is not None else {}
    st.update(snapshots_total=0, snapshots_window=0, incomplete_snapshots=0, missing_level_cells=0,
              ambiguous_groups=0, events=len(events), ref_all=bool(events) and all(e.ref is not None for e in events),
              ref_any=any(e.ref is not None for e in events))
    have_ref = st["ref_all"]
    evs = sorted(events, key=(lambda e: (e.t, e.ref, e.idx)) if have_ref else (lambda e: (e.t, e.idx)))
    state = {lv: {"bid": None, "ask": None, "ref": None} for lv in range(1, 6)}
    i, n = 0, len(evs)
    while i < n:
        t, j = evs[i].t, i
        touched = {}
        group_refs = collections.defaultdict(set)
        while j < n and evs[j].t == t:
            e = evs[j]
            s = state[e.level]
            if e.ref is not None:
                group_refs[e.level].add(e.ref)
            for side, val in (("bid", e.bid), ("ask", e.ask)):
                if val is not None:
                    key = (e.level, side)
                    if key not in touched:
                        touched[key] = val
                    elif not have_ref and touched[key] != val:
                        touched[key] = "AMBIG"  # ترتیب تغییرها در همان ثانیه قابل اثبات نیست
                    elif touched[key] != "AMBIG":
                        touched[key] = val
                    s[side] = val
            s["ref"] = e.ref if e.ref is not None else s["ref"]
            j += 1
        # مقدار مبهم را به آخرین ردیف ورودی تقلیل نمی‌دهیم؛ آن سمت تا رویداد معتبر بعدی ناموجود می‌ماند.
        ambiguous_sides = [key for key, value in touched.items() if value == "AMBIG"]
        for level, side in ambiguous_sides:
            state[level][side] = None
        ambiguous_refs = [level for level, refs in group_refs.items() if len(refs) > 1 and not have_ref]
        for level in ambiguous_refs:
            state[level]["ref"] = None
        if ambiguous_sides or ambiguous_refs:
            st["ambiguous_groups"] += 1
        st["snapshots_total"] += 1
        if win_start <= t <= win_end:
            rows, missing = [], 0
            for lv in range(1, 6):
                s = state[lv]
                a = s["ask"] if s["ask"] is not None else (None, None, None)
                b = s["bid"] if s["bid"] is not None else (None, None, None)
                missing += (s["ask"] is None) + (s["bid"] is None)
                rows.append((lv, s["ref"], a[0], a[1], a[2], b[2], b[1], b[0]))
            st["snapshots_window"] += 1
            st["missing_level_cells"] += missing
            if missing:
                st["incomplete_snapshots"] += 1
            yield Snapshot(t, tuple(rows))
        i = j


def summarize_events(events, win_start, win_end):
    st = {}
    for _ in reconstruct_snapshots(events, win_start, win_end, st):
        pass
    return st


# ───────────────────────────────── کلاینت HTTP ─────────────────────────────────
class FetchResult:
    __slots__ = ("status", "data", "detail", "http_code", "elapsed", "nbytes", "url", "ctype")

    def __init__(self, status, data=None, detail="", http_code=None, elapsed=0.0, nbytes=0, url="", ctype=""):
        self.status, self.data, self.detail, self.http_code = status, data, detail, http_code
        self.elapsed, self.nbytes, self.url, self.ctype = elapsed, nbytes, url, ctype

    def brief(self):
        return {"status": self.status, "detail": self.detail, "http_code": self.http_code,
                "ms": int(self.elapsed * 1000), "bytes": self.nbytes}


_BLOCK_WORDS = ("access denied", "forbidden", "blocked", "captcha", "request rejected", "cloudflare",
                "not allowed", "مسدود", "دسترسی")


class ApiClient:
    """requests.Session با timeout، retry محدود، backoff افزایشی و تفکیک نوع خطا. retry نامحدود وجود ندارد."""

    def __init__(self, base=DEFAULT_API_BASE, proxy="", max_attempts=3, backoff_base=1.5,
                 timeout=(8, 45)):
        if requests is None:
            raise AppError("کتابخانه requests نصب نیست.")
        self.base = (base or DEFAULT_API_BASE).rstrip("/")
        self.max_attempts, self.backoff_base, self.timeout = max_attempts, backoff_base, timeout
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": BROWSER_UA, "Accept": "application/json, text/plain, */*",
                               "Accept-Language": "fa,en;q=0.8"})
        if proxy:
            self.s.proxies = {"http": proxy, "https": proxy}

    def url(self, path):
        return self.base + "/" + path.lstrip("/")

    def _wait(self, secs, cancel):
        if cancel is not None:
            return cancel.wait(secs)
        time.sleep(secs)
        return False

    def get(self, path, cancel=None):
        url = self.url(path)
        attempt = 0
        while True:
            attempt += 1
            t0 = time.time()
            if cancel is not None and cancel.is_set():
                return FetchResult("cancelled", url=url)
            backoff = min(self.backoff_base * (2 ** (attempt - 1)), 20.0)
            try:
                resp = self.s.get(url, timeout=self.timeout, stream=True, allow_redirects=False)
                try:
                    body_buf, total = bytearray(), 0
                    content_length = resp.headers.get("Content-Length", "")
                    if content_length.isdigit() and int(content_length) > MAX_RESPONSE_BYTES:
                        return FetchResult("http", detail="حجم پاسخ از سقف مجاز بیشتر است", url=url,
                                           http_code=resp.status_code, elapsed=time.time() - t0)
                    for ch in resp.iter_content(65536):
                        total += len(ch)
                        if total > MAX_RESPONSE_BYTES:
                            return FetchResult("http", detail="حجم پاسخ از سقف مجاز بیشتر است", url=url,
                                               http_code=resp.status_code, elapsed=time.time() - t0)
                        body_buf.extend(ch)
                    body = body_buf
                finally:
                    resp.close()
            except (requests.Timeout, requests.ConnectionError,
                    requests.exceptions.ChunkedEncodingError, requests.exceptions.ContentDecodingError) as e:
                if attempt < self.max_attempts and not self._wait(backoff, cancel):
                    continue
                return FetchResult("network", detail=_short_exc(e), url=url, elapsed=time.time() - t0)
            except requests.RequestException as e:
                return FetchResult("network", detail=_short_exc(e), url=url, elapsed=time.time() - t0)

            code, el = resp.status_code, time.time() - t0
            ctype = resp.headers.get("Content-Type", "")
            if code in (408, 500, 502, 503, 504):
                if attempt < self.max_attempts and not self._wait(backoff, cancel):
                    continue
                return FetchResult("http", detail="HTTP %d" % code, http_code=code, elapsed=el, nbytes=len(body),
                                   url=url, ctype=ctype)
            if code == 429:
                ra = resp.headers.get("Retry-After", "")
                wait = min(float(ra), 30.0) if re.fullmatch(r"\d+(\.\d+)?", ra or "") else backoff
                if attempt < self.max_attempts and not self._wait(wait, cancel):
                    continue
                return FetchResult("blocked", detail="HTTP 429 (محدودیت نرخ درخواست)", http_code=code, elapsed=el,
                                   nbytes=len(body), url=url, ctype=ctype)
            if code in (401, 403):
                return FetchResult("blocked", detail="HTTP %d — دسترسی از این IP/شبکه ممنوع است یا User-Agent رد شده" % code,
                                   http_code=code, elapsed=el, nbytes=len(body), url=url, ctype=ctype)
            if code != 200:
                return FetchResult("http", detail="HTTP %d" % code, http_code=code, elapsed=el, nbytes=len(body),
                                   url=url, ctype=ctype)
            text = body.decode("utf-8-sig", errors="replace")
            head = text.lstrip()[:400].lower()
            if "html" in ctype.lower() or head.startswith("<"):
                kind = "blocked" if any(w in head for w in _BLOCK_WORDS) else "html"
                return FetchResult(kind, detail="به‌جای JSON، صفحه HTML دریافت شد", http_code=code, elapsed=el,
                                   nbytes=len(body), url=url, ctype=ctype)
            try:
                data = json.loads(text)
            except ValueError as e:
                cl = resp.headers.get("Content-Length")
                extra = " (Content-Length=%s، دریافت‌شده=%d)" % (cl, len(body)) if cl and cl.isdigit() and int(cl) != len(body) else ""
                return FetchResult("invalid_json", detail="JSON ناقص/نامعتبر: %s%s" % (str(e)[:80], extra),
                                   http_code=code, elapsed=el, nbytes=len(body), url=url, ctype=ctype)
            return FetchResult("ok", data=data, http_code=code, elapsed=el, nbytes=len(body), url=url, ctype=ctype)


def _short_exc(e):
    return ("%s: %s" % (type(e).__name__, str(e)))[:240]


# ─────────────────────────────── ذخیره‌سازی SQLite ────────────────────────────
class Store:
    def __init__(self, data_dir):
        self.dir = Path(data_dir)
        self.exports = self.dir / "exports"
        self.exports.mkdir(parents=True, exist_ok=True)
        self.path = self.dir / "tse_cache.sqlite3"
        with self.connect() as c:
            c.executescript("""
            CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);
            CREATE TABLE IF NOT EXISTS instruments(ins_code TEXT PRIMARY KEY, symbol TEXT, company TEXT,
                industry TEXT, market TEXT, isin TEXT, identity_ok INTEGER, updated_at TEXT);
            CREATE TABLE IF NOT EXISTS daily_raw(ins_code TEXT PRIMARY KEY, status TEXT, payload BLOB,
                n_records INTEGER, first_deven INTEGER, last_deven INTEGER, detail TEXT, fetched_at TEXT, schema_ver INTEGER);
            CREATE TABLE IF NOT EXISTS ob_day(ins_code TEXT, deven INTEGER, status TEXT, kind TEXT, detail TEXT,
                payload BLOB, n_events INTEGER, n_invalid INTEGER, n_snap_total INTEGER, n_snap_window INTEGER,
                n_incomplete INTEGER, n_missing_cells INTEGER, n_ambiguous INTEGER, has_ref INTEGER,
                schema_name TEXT, window_used TEXT, nbytes INTEGER, fetched_at TEXT, schema_ver INTEGER,
                PRIMARY KEY(ins_code, deven));
            CREATE TABLE IF NOT EXISTS jobs(id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT, spec TEXT, state TEXT,
                summary TEXT, started_at TEXT, finished_at TEXT);
            CREATE TABLE IF NOT EXISTS outputs(ins_code TEXT PRIMARY KEY, fp TEXT, report TEXT, created_at TEXT);
            CREATE INDEX IF NOT EXISTS idx_instr_symbol ON instruments(symbol);
            """)

    def connect(self):
        c = sqlite3.connect(str(self.path), timeout=30)
        c.row_factory = sqlite3.Row
        try:
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA synchronous=NORMAL")
        except sqlite3.DatabaseError:
            pass
        return _Conn(c)

    # تنظیمات
    def get_setting(self, key, default=None):
        with self.connect() as c:
            r = c.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return r["value"] if r else default

    def set_setting(self, key, value):
        with self.connect() as c:
            c.execute("INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)", (key, value))

    # نمادها
    def upsert_instrument(self, ins, **kw):
        cur = self.get_instrument(ins) or {}
        cur.update({k: v for k, v in kw.items() if v is not None})
        with self.connect() as c:
            c.execute("INSERT OR REPLACE INTO instruments VALUES(?,?,?,?,?,?,?,?)",
                      (ins, cur.get("symbol", ""), cur.get("company", ""), cur.get("industry", ""),
                       cur.get("market", ""), cur.get("isin", ""), int(bool(cur.get("identity_ok"))), _now()))

    def find_instrument_by_symbol(self, symbol):
        key = fa_key(symbol)
        with self.connect() as c:
            rows = c.execute("SELECT * FROM instruments WHERE symbol=?", (fa_chars(str(symbol)).strip(),)).fetchall()
            if not rows:
                rows = [r for r in c.execute("SELECT * FROM instruments").fetchall() if fa_key(r["symbol"]) == key]
        return dict(rows[0]) if rows else None

    def get_instrument(self, ins):
        with self.connect() as c:
            r = c.execute("SELECT * FROM instruments WHERE ins_code=?", (ins,)).fetchone()
        return dict(r) if r else None

    # روزانه
    def put_daily(self, ins, status, records=None, detail=""):
        blob, n, fd, ld = None, 0, None, None
        if records:
            blob, n = pack_records(records), len(records)
            dv = [d for d in (to_int_strict(r.get("dEven")) for r in records if isinstance(r, dict)) if d]
            fd, ld = (min(dv), max(dv)) if dv else (None, None)
        with self.connect() as c:
            c.execute("INSERT OR REPLACE INTO daily_raw VALUES(?,?,?,?,?,?,?,?,?)",
                      (ins, status, blob, n, fd, ld, detail, _now(), SCHEMA_VER))

    def get_daily(self, ins, with_records=True):
        with self.connect() as c:
            r = c.execute("SELECT * FROM daily_raw WHERE ins_code=?", (ins,)).fetchone()
        if not r:
            return None
        d = dict(r)
        blob = d.pop("payload")
        d["records"] = unpack_records(blob) if (with_records and blob) else []
        return d

    # دفتر سفارشات روزانه
    def put_ob_day(self, ins, deven, status, kind="", detail="", records=None, stats=None, schema_name="",
                   window_used="", nbytes=0):
        stats = stats or {}
        blob = pack_records(records) if (records and status == "ok") else None
        has_ref = 1 if stats.get("ref_all") else (2 if stats.get("ref_any") else 0)
        with self.connect() as c:
            c.execute("INSERT OR REPLACE INTO ob_day VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                      (ins, deven, status, kind, detail, blob, stats.get("events", 0), stats.get("invalid", 0),
                       stats.get("snapshots_total", 0), stats.get("snapshots_window", 0),
                       stats.get("incomplete_snapshots", 0), stats.get("missing_level_cells", 0),
                       stats.get("ambiguous_groups", 0), has_ref, schema_name, window_used, nbytes, _now(), SCHEMA_VER))

    def get_ob_meta(self, ins, deven):
        with self.connect() as c:
            r = c.execute("SELECT ins_code,deven,status,kind,detail,n_events,n_invalid,n_snap_total,n_snap_window,"
                          "n_incomplete,n_missing_cells,n_ambiguous,has_ref,schema_name,window_used,nbytes,fetched_at,"
                          "schema_ver,(payload IS NOT NULL) AS has_payload FROM ob_day WHERE ins_code=? AND deven=?",
                          (ins, deven)).fetchone()
        return dict(r) if r else None

    def purge_ob_payloads(self, ins, d1, d2):
        """داده‌ی خام روزها را پس از ساخت فایل پاک می‌کند (وضعیت و آمار می‌ماند)؛ فضای آزادشده توسط SQLite دوباره مصرف می‌شود."""
        with self.connect() as c:
            c.execute("UPDATE ob_day SET payload=NULL WHERE ins_code=? AND deven BETWEEN ? AND ?", (ins, d1, d2))

    def get_outputs(self, ins):
        with self.connect() as c:
            r = c.execute("SELECT fp, report FROM outputs WHERE ins_code=?", (ins,)).fetchone()
        return (r["fp"], json.loads(r["report"])) if r else (None, None)

    def put_outputs(self, ins, fp, report):
        with self.connect() as c:
            c.execute("INSERT OR REPLACE INTO outputs VALUES(?,?,?,?)", (ins, fp, json.dumps(report, ensure_ascii=False), _now()))

    def del_outputs(self, ins):
        with self.connect() as c:
            c.execute("DELETE FROM outputs WHERE ins_code=?", (ins,))

    def get_ob_records(self, ins, deven):
        with self.connect() as c:
            r = c.execute("SELECT payload, schema_name FROM ob_day WHERE ins_code=? AND deven=?", (ins, deven)).fetchone()
        if not r or not r["payload"]:
            return None, ""
        return unpack_records(r["payload"]), r["schema_name"]

    def list_ob_meta(self, ins, d1, d2):
        with self.connect() as c:
            rows = c.execute("SELECT ins_code,deven,status,kind,detail,n_events,n_invalid,n_snap_total,n_snap_window,"
                             "n_incomplete,n_missing_cells,n_ambiguous,has_ref,schema_name,window_used,nbytes,fetched_at,"
                             "schema_ver,(payload IS NOT NULL) AS has_payload FROM ob_day "
                             "WHERE ins_code=? AND deven BETWEEN ? AND ? ORDER BY deven",
                             (ins, d1, d2)).fetchall()
        return [dict(r) for r in rows]

    def save_job(self, job):
        with self.connect() as c:
            c.execute("INSERT INTO jobs(kind,spec,state,summary,started_at,finished_at) VALUES(?,?,?,?,?,?)",
                      (job.kind, json.dumps(job.spec, ensure_ascii=False), job.state,
                       json.dumps(job.public(), ensure_ascii=False, default=str)[:200000], job.started_iso, _now()))


class _Conn:
    """context manager که commit می‌کند و اتصال را می‌بندد."""

    def __init__(self, c):
        self.c = c

    def __enter__(self):
        return self.c

    def __exit__(self, et, ev, tb):
        try:
            if et is None:
                self.c.commit()
            else:
                self.c.rollback()
        finally:
            self.c.close()
        return False


def _now():
    return _dt.datetime.now(TEHRAN_TZ).strftime("%Y-%m-%d %H:%M:%S")


# ───────────────────────────── سنجش منابع سیستم ─────────────────────────────
def system_resources():
    """حد مجاز هم‌زمانی را با توجه به پردازنده و حافظه فیزیکی محاسبه می‌کند."""
    cpus = max(1, os.cpu_count() or 1)
    memory_bytes = None
    try:
        if os.name == "nt":
            import ctypes
            class _MemoryStatus(ctypes.Structure):
                _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                            ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                            ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                            ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                            ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]
            mem = _MemoryStatus()
            mem.dwLength = ctypes.sizeof(_MemoryStatus)
            if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(mem)):
                memory_bytes = int(mem.ullTotalPhys)
        elif hasattr(os, "sysconf"):
            memory_bytes = int(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"))
    except (OSError, AttributeError, ValueError):
        memory_bytes = None
    memory_gb = round(memory_bytes / (1024 ** 3), 1) if memory_bytes else None
    # هر worker عمدتاً I/O محور است؛ سقف محافظه‌کارانه مانع فشار بی‌رویه به RAM و API می‌شود.
    cpu_limit = max(4, cpus * 2)
    ram_limit = max(4, int(memory_gb * 2)) if memory_gb else 16
    max_workers = max(1, min(16, cpu_limit, ram_limit))
    recommended = max(1, min(max_workers, max(2, cpus)))
    return {"logical_cpus": cpus, "memory_gb": memory_gb,
            "max_workers": max_workers, "recommended_workers": recommended}


# ─────────────────────────────────── زمینه برنامه ──────────────────────────────
class Context:
    def __init__(self, data_dir, api_base=None, delay=None, backoff_base=1.5, max_attempts=3, workers=None):
        self.store = Store(data_dir)
        self.workers_override = workers
        self.api_base_override = api_base
        self.delay_override = delay
        self.backoff_base, self.max_attempts = backoff_base, max_attempts
        self.jobs = JobManager(self)

    def settings(self):
        base = self.api_base_override or self.store.get_setting("api_base", DEFAULT_API_BASE)
        proxy = self.store.get_setting("proxy", "")
        try:
            delay = float(self.delay_override if self.delay_override is not None else self.store.get_setting("delay", DEFAULT_DELAY))
        except (TypeError, ValueError):
            delay = DEFAULT_DELAY
        resources = system_resources()
        try:
            worker_default = resources["recommended_workers"]
            workers = int(self.workers_override if self.workers_override is not None
                          else self.store.get_setting("workers", worker_default))
        except (TypeError, ValueError):
            workers = resources["recommended_workers"]
        return {"api_base": base, "proxy": proxy, "delay": delay,
                "workers": max(1, min(resources["max_workers"], workers)), "resources": resources}

    def make_client(self):
        s = self.settings()
        return ApiClient(s["api_base"], s["proxy"], max_attempts=self.max_attempts, backoff_base=self.backoff_base)


# ───────────────────────────────── ابزار روزهای معاملاتی ───────────────────────
def get_trading_days(store, ins, start, end):
    """روزهای معاملاتی از فهرست روزانه‌ی خودِ منبع؛ در نبود آن، تقویم فرضی شنبه–چهارشنبه (با برچسب)."""
    row = store.get_daily(ins)
    d1, d2 = date_to_deven(start), date_to_deven(end)
    if row and row["status"] == "ok" and row["schema_ver"] == SCHEMA_VER:
        days = sorted({d for d in (to_int_strict(r.get("dEven")) for r in row["records"] if isinstance(r, dict))
                       if d and d1 <= d <= d2})
        return days, "daily_list"
    return [date_to_deven(d) for d in candidate_days(start, end)], "calendar_assumed"


def ob_coverage(store, ins, start, end):
    days, source = get_trading_days(store, ins, start, end)
    d1, d2 = date_to_deven(start), date_to_deven(end)
    meta = {m["deven"]: m for m in store.list_ob_meta(ins, d1, d2)}
    ok = empty = err = pending = 0
    kinds = collections.Counter()
    ev = sw = inc = miss = amb = inv = 0
    ref_days = collections.Counter()
    first_ok = last_ok = None
    strip = []
    for d in days:
        m = meta.get(d)
        if not m or m["schema_ver"] != SCHEMA_VER:
            pending += 1
            strip.append([d, "pending"])
            continue
        strip.append([d, m["status"]])
        if m["status"] == "ok":
            ok += 1
            ev += m["n_events"]; sw += m["n_snap_window"]; inc += m["n_incomplete"]
            miss += m["n_missing_cells"]; amb += m["n_ambiguous"]; inv += m["n_invalid"]
            ref_days[m["has_ref"]] += 1
            first_ok = d if first_ok is None else min(first_ok, d)
            last_ok = d if last_ok is None else max(last_ok, d)
        elif m["status"] == "empty":
            empty += 1
        else:
            err += 1
            kinds[m["kind"] or "unknown"] += 1
    ref_state = ("all_days" if ref_days[1] == ok and ok else "none" if ref_days[0] == ok and ok
                 else "mixed" if ok else "n/a")
    daily = store.get_daily(ins, with_records=False)
    return {
        "days_source": source, "expected_days": len(days), "fetched_ok": ok, "empty_responses": empty,
        "errors": err, "error_kinds": dict(kinds), "not_fetched_yet": pending,
        "raw_events": ev, "invalid_events": inv, "snapshots_rebuilt_in_window": sw,
        "output_rows_estimate": sw * 5, "incomplete_snapshots": inc, "missing_level_cells": miss,
        "ambiguous_same_second_groups": amb, "refid_in_source": ref_state,
        "first_usable_date": (gdate_to_jalali_str(deven_to_date(first_ok)) + " / " + str(deven_to_date(first_ok))) if first_ok else None,
        "last_usable_date": (gdate_to_jalali_str(deven_to_date(last_ok)) + " / " + str(deven_to_date(last_ok))) if last_ok else None,
        "daily_list_first_deven": daily["first_deven"] if daily else None,
        "daily_list_last_deven": daily["last_deven"] if daily else None,
        "strip": strip,
    }


# ─────────────────────────────────── مدیریت عملیات ─────────────────────────────
class Busy(AppError):
    pass


class Job:
    def __init__(self, kind, spec, jid):
        self.kind, self.spec, self.id = kind, spec, jid
        self.state = "running"
        self.message = ""
        self.progress = {}
        self.errors = collections.deque(maxlen=25)
        self.plan = None
        self.result = {}
        self.reports = []                    # گزارش هر نماد؛ از مسیر جدا صفحه‌به‌صفحه خوانده می‌شود
        self.cancel_event = threading.Event()
        self._lock = threading.Lock()
        self.started = time.time()
        self.started_iso = _now()
        self.finished = None

    def set(self, **kw):
        with self._lock:
            self.progress.update(kw)

    def inc(self, key, n=1):
        with self._lock:
            self.progress[key] = self.progress.get(key, 0) + n

    def err(self, msg):
        with self._lock:
            self.errors.append(msg)

    def cancelled(self):
        return self.cancel_event.is_set()

    def sleep(self, secs):
        if secs > 0:
            self.cancel_event.wait(secs)

    def add_report(self, rep):
        with self._lock:
            self.reports.append(rep)

    def reports_page(self, offset, limit):
        with self._lock:
            return len(self.reports), list(self.reports[offset:offset + limit])

    def public(self):
        with self._lock:
            spec = {k: v for k, v in self.spec.items() if k != "symbols"}
            spec["n_symbols"] = len(self.spec.get("symbols") or [])
            return {"id": self.id, "kind": self.kind, "state": self.state, "message": self.message,
                    "progress": dict(self.progress), "errors": list(self.errors), "plan": self.plan,
                    "result": self.result, "spec": spec, "n_reports": len(self.reports),
                    "elapsed": round((self.finished or time.time()) - self.started, 1)}


class JobManager:
    def __init__(self, ctx):
        self.ctx = ctx
        self.current = None
        self._lock = threading.Lock()
        self._next = 1
        self.last_spec = {}

    def start(self, kind, spec, runner):
        with self._lock:
            if self.current:
                old_thread = getattr(self.current, "thread", None)
                if self.current.state == "running" or (old_thread is not None and old_thread.is_alive()):
                    raise Busy("یک عملیات قبلی هنوز در حال خاتمه است؛ تا پایان کامل آن صبر کنید.")
            job = Job(kind, spec, self._next)
            self._next += 1
            self.current = job
        self.last_spec[kind] = spec
        th = threading.Thread(target=self._run, args=(job, runner), daemon=True)
        job.thread = th
        th.start()
        return job

    def _run(self, job, runner):
        try:
            runner(job, self.ctx)
            if job.state == "running":
                job.state = "cancelled" if job.cancelled() else "done"
        except AppError as e:
            job.state, job.message = "failed", str(e)
        except Exception as e:  # noqa
            job.state, job.message = "failed", "خطای داخلی: %s" % e
            job.err(traceback.format_exc()[-1500:])
        finally:
            job.finished = time.time()
            try:
                self.ctx.store.save_job(job)
            except Exception:
                pass

    def cancel(self):
        j = self.current
        if j and j.state == "running":
            j.cancel_event.set()
            j.message = "در حال لغو…"
            return True
        return False

    def wait(self, timeout=60):
        j = self.current
        if j and getattr(j, "thread", None):
            j.thread.join(timeout)
        return j


# ───────────────────────────────── منطق عملیات‌ها ──────────────────────────────
def spec_range(spec):
    gs, ge, js, je = validate_range(spec["start"], spec["end"])
    return gs, ge, "%04d%02d%02d" % (js.year, js.month, js.day), "%04d%02d%02d" % (je.year, je.month, je.day)


def spec_window(spec):
    ws = parse_clock(spec.get("window_start") or DEFAULT_WINDOW[0])
    we = parse_clock(spec.get("window_end") or DEFAULT_WINDOW[1])
    if ws > we:
        raise AppError("ساعت شروع بازه خروجی بعد از ساعت پایان است.")
    return ws, we


def need_instrument(spec):
    ins = str(spec.get("ins_code") or "").strip()
    if not ins.isdigit():
        raise AppError("ابتدا یک نماد را از فهرست پیشنهادها انتخاب کنید.")
    return ins


def fetch_daily_list(ctx, ins, job=None, force=False):
    """یک درخواست برای تمام تاریخچه روزانه؛ نتیجه در کش ذخیره می‌شود."""
    cur = ctx.store.get_daily(ins, with_records=False)
    if cur and cur["status"] in ("ok", "empty") and cur["schema_ver"] == SCHEMA_VER and not force:
        return cur["status"], "cached"
    cl = ctx.make_client()
    res = cl.get("ClosingPrice/GetClosingPriceDailyList/%s/0" % ins, cancel=job.cancel_event if job else None)
    if res.status == "cancelled":
        return "cancelled", ""
    if res.status != "ok":
        ctx.store.put_daily(ins, "error", None, "%s: %s" % (res.status, res.detail))
        return "error", "%s — %s" % (KIND_FA.get(res.status, res.status), res.detail)
    cls, val = classify_list_payload(res.data, "closingPriceDaily")
    if cls == "ok":
        recs = [r for r in val if isinstance(r, dict) and to_int_strict(r.get("dEven"))]
        if not recs:
            ctx.store.put_daily(ins, "error", None, "هیچ رکوردی دارای dEven معتبر نبود")
            return "error", "ساختار رکوردهای روزانه نامعتبر است (dEven یافت نشد)."
        ctx.store.put_daily(ins, "ok", recs, "")
        return "ok", "%d رکورد" % len(recs)
    if cls == "empty":
        ctx.store.put_daily(ins, "empty", None, "پاسخ معتبر اما خالی")
        return "empty", "پاسخ معتبر اما خالی (ممکن است فقط همین endpoint برای نماد خالی باشد)"
    ctx.store.put_daily(ins, "error", None, val)
    return "error", val


def run_fetch_daily(job, ctx):
    spec = job.spec
    ins = need_instrument(spec)
    gs, ge, js, je = spec_range(spec)
    job.set(phase="daily", percent=0, symbol=spec.get("symbol", ""), range="%s تا %s" % (spec["start"], spec["end"]))
    status, msg = fetch_daily_list(ctx, ins, job, force=bool(spec.get("force")))
    if status == "cancelled":
        return
    row = ctx.store.get_daily(ins)
    inrange = 0
    if row and row["status"] == "ok":
        d1, d2 = date_to_deven(gs), date_to_deven(ge)
        inrange = sum(1 for r in row["records"] if d1 <= (to_int_strict(r.get("dEven")) or 0) <= d2)
    job.set(percent=100, daily_status=status, daily_records_total=row["n_records"] if row else 0,
            daily_records_in_range=inrange)
    job.result = {"daily": {"status": status, "message": msg, "records_total": row["n_records"] if row else 0,
                            "records_in_range": inrange,
                            "list_first_deven": row["first_deven"] if row else None,
                            "list_last_deven": row["last_deven"] if row else None}}
    if status == "error":
        job.state, job.message = "failed", msg
        job.err(msg)
    else:
        job.message = msg


def os_replace(a, b):
    os.replace(str(a), str(b))


# ───────────────────────────── بسته‌های اختیاری (نصب در لحظه‌ی نیاز) ─────────────────────────────
def ensure_optional_package(module, pip_name=None):
    """در نبود بسته اختیاری، قابلیت را بدون نصب خودکار غیرفعال می‌کند."""
    if importlib.util.find_spec(module) is not None:
        return
    pip_name = pip_name or module
    raise AppError(
        "قابلیت موردنظر به بسته «%s» نیاز دارد. برای نصب، در CMD داخل پوشه برنامه اجرا کنید: "
        " .venv\\Scripts\\python.exe -m pip install %s" % (pip_name, pip_name)
    )



def _pyarrow():
    ensure_optional_package("pyarrow")
    import pyarrow as pa
    import pyarrow.parquet as pq
    return pa, pq


# ───────────────────────────── نویسنده‌های فایل خروجی ─────────────────────────────
class _OutBase:
    def __init__(self, final, cols, meta):
        self.final, self.cols, self.meta = Path(final), cols, meta
        self.tmp = self.final.with_name(self.final.name + ".tmp")
        self.rows = 0
        self.note = ""
        self.skipped = False

    def commit(self):
        if not self.skipped:
            os_replace(self.tmp, self.final)

    def abort(self):
        try:
            if self.tmp.exists():
                self.tmp.unlink()
        except OSError:
            pass


class CsvOut(_OutBase):
    """CSV با کدگذاری UTF-8 همراه BOM (برای نمایش درست فارسی در Excel) و عنوان‌های فارسی."""

    def __init__(self, final, cols, meta):
        super().__init__(final, cols, meta)
        self.f = open(str(self.tmp), "w", encoding="utf-8-sig", newline="")
        self.w = csv.writer(self.f)
        self.w.writerow([c[1] for c in cols])

    @staticmethod
    def _safe_text_cell(value):
        if value is None:
            return value
        text = str(value)
        probe = text.lstrip(" \t\r\n")
        # از تفسیر متن‌های فرمول‌مانند به‌عنوان فرمول اجرایی در صفحه‌گسترده جلوگیری می‌شود.
        if probe.startswith(("=", "+", "-", "@")):
            return "'" + text
        return text

    def write_rows(self, rows):
        count = 0

        def sanitized_rows():
            nonlocal count
            for row in rows:
                cells = list(row)
                for idx, col in enumerate(self.cols):
                    if idx < len(cells) and col[2] == "str":
                        cells[idx] = self._safe_text_cell(cells[idx])
                count += 1
                yield cells

        self.w.writerows(sanitized_rows())
        self.rows += count

    def flush(self):
        self.f.close()

    def abort(self):
        try:
            self.f.close()
        except Exception:  # noqa
            pass
        super().abort()


class ParquetOut(_OutBase):
    BATCH = 200_000

    def __init__(self, final, cols, meta):
        super().__init__(final, cols, meta)
        self.pa, self.pq = _pyarrow()
        pa = self.pa
        types = {"str": pa.string(), "int": pa.int64(), "float": pa.float64(), "date": pa.date32()}
        self.schema = pa.schema([pa.field(c[0], types[c[2]], metadata={"fa": c[1]}) for c in cols],
                                metadata={"TSE_LOB": json.dumps(meta, ensure_ascii=False)})
        self.pw = self.pq.ParquetWriter(str(self.tmp), self.schema, compression="zstd")
        self.buf = []
        self._dates = {}

    def _col(self, kind, name, vals):
        if kind == "int":
            out = []
            for v in vals:
                if v is None or type(v) is int:
                    out.append(v)
                elif type(v) is float and v == v and v - v == 0 and v == int(v):
                    out.append(int(v))
                else:
                    raise AppError("مقدار غیرصحیح (%r) در ستون عددی «%s»؛ ذخیره بدون گردکردن ممکن نیست." % (v, name))
            return self.pa.array(out, type=self.pa.int64())
        if kind == "date":
            dc = self._dates
            out = []
            for v in vals:
                d = dc.get(v)
                if d is None and v is not None:
                    d = dc[v] = _dt.date.fromisoformat(v)
                out.append(d)
            return self.pa.array(out, type=self.pa.date32())
        if kind == "float":
            return self.pa.array(list(vals), type=self.pa.float64())
        return self.pa.array(list(vals), type=self.pa.string())

    def _flush(self):
        if not self.buf:
            return
        cols = list(zip(*self.buf))
        arrays = [self._col(c[2], c[0], cols[i]) for i, c in enumerate(self.cols)]
        self.pw.write_table(self.pa.Table.from_arrays(arrays, schema=self.schema))
        self.buf = []

    def write_rows(self, rows):
        self.buf.extend(rows)
        self.rows += len(rows)
        if len(self.buf) >= self.BATCH:
            self._flush()

    def flush(self):
        self._flush()
        self.pw.close()

    def abort(self):
        try:
            self.pw.close()
        except Exception:  # noqa
            pass
        super().abort()


_OUT_CLASSES = {"csv": CsvOut, "parquet": ParquetOut}


class OutputSet:
    """چند فرمت هم‌زمان با یک بار خواندن/بازسازی داده؛ ردیف‌ها به همه‌ی نویسنده‌ها داده می‌شود."""

    def __init__(self, formats, outdir, stem, cols, meta):
        self.outs = []
        try:
            for fmt in formats:
                self.outs.append(_OUT_CLASSES[fmt](Path(outdir) / ("%s.%s" % (stem, FORMAT_EXT[fmt])), cols, meta))
        except Exception:
            self.abort()
            raise

    def write_rows(self, rows):
        for o in self.outs:
            o.write_rows(rows)

    def finish(self):
        """فاز ۱: همه‌ی فرمت‌ها در فایل موقت کامل می‌شوند؛ فاز ۲: فقط اگر همه موفق بودند، با هم نام نهایی می‌گیرند."""
        files, notes = [], []
        try:
            for o in self.outs:
                o.flush()
            for o in self.outs:
                o.commit()
                if o.note:
                    notes.append(o.note)
                if not o.skipped:
                    files.append(o.final)
        except BaseException:
            self.abort()
            raise
        return files, notes

    def abort(self):
        for o in self.outs:
            try:
                o.abort()
            except Exception:  # noqa
                pass


def file_stem(symbol, js, je, kind):
    """نام فایل: نماد_تاریخ‌شروع_تاریخ‌پایان_نوع‌گزارش (پسوند = فرمت داده)."""
    return "%s_%s_%s_%s" % (safe_filename(symbol), js, je, kind)


def format_list(spec):
    raw = spec.get("formats")
    if raw is None:
        raw = ["csv"]
    fmts = [f for f in FORMAT_ORDER if f in set(str(x).lower() for x in raw)]
    if not fmts:
        raise AppError("حداقل یک فرمت خروجی (Parquet یا CSV) را انتخاب کنید.")
    return fmts


def fetch_one_ob_day(ctx, cl, cancel, ins, d, ws, we):
    """یک روز دفتر سفارش را می‌گیرد، طبقه‌بندی می‌کند و در کش می‌نویسد. خروجی: (outcome, FetchResult, info)."""
    wused = "%s-%s" % (hms_str(ws), hms_str(we))
    res = cl.get("BestLimits/%s/%d" % (ins, d), cancel=cancel)
    info = {"detail": res.detail, "blob": 0, "stats": None}
    if res.status == "cancelled":
        return "cancelled", res, info
    st = ctx.store
    if res.status != "ok":
        st.put_ob_day(ins, d, "error", res.status, res.detail, nbytes=res.nbytes)
        return res.status, res, info
    cls, val = classify_list_payload(res.data, "bestLimitsHistory")
    if cls == "empty":
        st.put_ob_day(ins, d, "empty", detail="پاسخ معتبر اما خالی", nbytes=res.nbytes)
        return "empty", res, info
    if cls == "schema":
        st.put_ob_day(ins, d, "error", "schema", val, nbytes=res.nbytes)
        info["detail"] = val
        return "schema", res, info
    sname, fm = detect_ob_schema(val)
    if sname is None:
        st.put_ob_day(ins, d, "error", "schema", fm, nbytes=res.nbytes)
        info["detail"] = fm
        return "schema", res, info
    events, invalid = normalize_ob_events(val, fm)
    stats = summarize_events(events, ws, we)
    stats["invalid"] = invalid
    st.put_ob_day(ins, d, "ok", records=val, stats=stats, schema_name=sname, window_used=wused, nbytes=res.nbytes)
    info["stats"], info["blob"] = stats, len(pack_records(val))
    return "ok", res, info


def _jset(job, track, **kw):
    if track:
        job.set(**kw)


# ───────────────────────────── دریافت هم‌زمان دفتر سفارش یک نماد ─────────────────────────────
def fetch_orderbook_range(job, ctx, ins, symbol, gs, ge, ws, we, force, range_txt, on_progress=None):
    """دریافت (یا ادامه‌ی) تاریخچه دفتر سفارش یک نماد با چند درخواست هم‌زمان؛ توقف/لغو روی job ثبت می‌شود."""
    st = ctx.store
    prog = on_progress or (lambda f: job.set(percent=round(100 * f, 1)))
    tag = (symbol + " ") if symbol else ""
    job.set(phase="daily", symbol=symbol, range=range_txt, trading_days=0, processed=0, ok=0, empty=0, failed=0,
            cached=0, raw_events=0, snapshots=0, current_day="", consecutive_fail=0)
    prog(0)
    status, msg = fetch_daily_list(ctx, ins, job, force=force)     # برای استخراج روزهای معاملاتی از خود منبع
    if status == "cancelled":
        return
    if status == "error":
        job.err("%sفهرست روزانه دریافت نشد؛ از تقویم فرضی استفاده می‌شود: %s" % (tag, msg))
    days, source = get_trading_days(st, ins, gs, ge)
    job.set(phase="orderbook", trading_days=len(days), days_source=source)
    if source == "calendar_assumed":
        job.err("%sروزهای معاملاتی از منبع استخراج نشد؛ تقویم فرضی شنبه–چهارشنبه به کار رفت (تعطیلات رسمی حذف نشده‌اند و "
                "به‌صورت «پاسخ خالی» ثبت می‌شوند)." % tag)
    metas = {m["deven"]: m for m in st.list_ob_meta(ins, date_to_deven(gs), date_to_deven(ge))}
    pending = []
    for d in days:
        m = metas.get(d)
        if (not force and m and m["schema_ver"] == SCHEMA_VER
                and (m["status"] == "empty" or (m["status"] == "ok" and m["has_payload"]))):
            job.inc("cached"); job.inc("processed")
            if m["status"] == "ok":
                job.inc("raw_events", m["n_events"]); job.inc("snapshots", m["n_snap_window"])
        else:
            pending.append(d)
    total = max(1, len(days))
    prog((len(days) - len(pending)) / total)
    if not pending:
        return
    cfg = ctx.settings()
    delay = cfg["delay"]
    q, lock = collections.deque(pending), threading.Lock()
    state = {"done": len(days) - len(pending), "consec": 0, "blocked": 0}

    def worker():
        cl = ctx.make_client()
        while not job.cancelled() and job.state == "running":
            with lock:
                if not q:
                    return
                d = q.popleft()
            jd = gdate_to_jalali_str(deven_to_date(d))
            job.set(current_day=jd)
            outcome, res, info = fetch_one_ob_day(ctx, cl, job.cancel_event, ins, d, ws, we)
            if outcome == "cancelled":
                return
            job.inc("processed")
            if outcome == "ok":
                job.inc("ok"); job.inc("raw_events", info["stats"]["events"]); job.inc("snapshots", info["stats"]["snapshots_window"])
            elif outcome == "empty":
                job.inc("empty")
            else:
                job.inc("failed")
                job.err("%s%s: %s — %s" % (tag, jd, KIND_FA.get(outcome, outcome), info["detail"]))
            trip = False
            with lock:
                state["done"] += 1
                prog(state["done"] / total)
                if outcome in ("ok", "empty"):
                    state["consec"] = state["blocked"] = 0
                else:
                    state["consec"] += 1
                    state["blocked"] = state["blocked"] + 1 if outcome == "blocked" else 0
                job.set(consecutive_fail=state["consec"])
                if (state["blocked"] >= MAX_CONSEC_BLOCKED or state["consec"] >= MAX_CONSEC_FAIL) and job.state == "running":
                    why = ("دسترسی مسدود شد (۴۰۳/۴۲۹)؛ از شبکه/IP دیگری امتحان کنید یا در «تنظیمات» پراکسی مجاز خودتان را بدهید."
                           if state["blocked"] >= MAX_CONSEC_BLOCKED else
                           "%d روز متوالی ناموفق بود؛ دریافت برای جلوگیری از درخواست‌های بی‌فایده متوقف شد." % state["consec"])
                    job.state, job.message = "stopped", tag + why + " پیشرفت ذخیره شد و قابل ادامه است."
                    trip = True
            if trip:
                return
            job.sleep(delay)

    threads = [threading.Thread(target=worker, daemon=True, name="fetch-%d" % i)
               for i in range(max(1, min(cfg["workers"], len(pending))))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def run_fetch_orderbook(job, ctx):
    spec = job.spec
    ins = need_instrument(spec)
    gs, ge, js, je = spec_range(spec)
    ws, we = spec_window(spec)
    fetch_orderbook_range(job, ctx, ins, spec.get("symbol", ""), gs, ge, ws, we, bool(spec.get("force")),
                          "%s تا %s" % (spec["start"], spec["end"]))
    cov = ob_coverage(ctx.store, ins, gs, ge)
    job.result = {"coverage": cov, "coverage_rows": cov_rows(cov), "window": "%s-%s" % (hms_str(ws), hms_str(we))}
    if job.cancelled() and job.state == "running":
        job.message = "لغو شد؛ پیشرفت ذخیره شد و قابل ادامه است."
    elif job.state == "running":
        job.message = "پایان یافت."


# ───────────────────────────── ساخت فایل‌ها (یک عبور، چند فرمت) ─────────────────────────────
def _day_rows(symbol, company, industry, jd, gd, snaps):
    rows = []
    for sn in snaps:
        ts = hms_str(sn.t)
        for r in sn.rows:
            rows.append([symbol, company, industry, jd, gd, ts, r[1], r[0], r[2], r[3], r[4], r[5], r[6], r[7]])
    return rows


def build_orderbook_files(job, ctx, ins, spec, outdir, formats, name=None, track=True):
    """دفتر سفارشات یک نماد را در همه‌ی فرمت‌های انتخابی می‌سازد. لغو → None؛ نبود داده → AppError."""
    gs, ge, js, je = spec_range(spec)
    ws, we = spec_window(spec)
    symbol, company, industry, inst = instrument_labels(ctx, ins, spec)
    days, source = get_trading_days(ctx.store, ins, gs, ge)
    metas = {m["deven"]: m for m in ctx.store.list_ob_meta(ins, date_to_deven(gs), date_to_deven(ge))}
    ok_days = [d for d in days if metas.get(d) and metas[d]["status"] == "ok" and metas[d]["schema_ver"] == SCHEMA_VER
               and metas[d]["has_payload"]]
    if not ok_days:
        raise AppError("هیچ روزِ دارای داده‌ای در بازه انتخابی برای دفتر سفارشات ذخیره نشده است.")
    hr = collections.Counter(metas[d]["has_ref"] for d in ok_days)
    ref_state = ("در همه روزها" if hr[1] == len(ok_days) else "در منبع وجود نداشت؛ ستون خالی است" if hr[0] == len(ok_days)
                 else "فقط در بخشی از روزها")
    window_txt = "%s تا %s (وقت محلی تهران)" % (hms_str(ws), hms_str(we))
    meta = {"app": "TSETMC Order Book App %s" % APP_VERSION, "report": "OrderBook", "symbol": symbol, "ins_code": ins,
            "range": "%s تا %s" % (spec["start"], spec["end"]), "window": window_txt, "price_unit": "rial",
            "refid": ref_state, "identity": "current (not historical)",
            "method": "reconstructed 5-level snapshots from BestLimits incremental events; missing levels stay null"}
    stem = file_stem(name or symbol, js, je, "OrderBook")
    outs = OutputSet(formats, outdir, stem, OB_COLS, meta)
    agg = collections.Counter()
    _jset(job, track, phase="write", days_done=0, days_total=len(ok_days), rows_written=0)
    try:
        for k, d in enumerate(ok_days):
            if job.cancelled():
                outs.abort()
                return None
            recs, _ = ctx.store.get_ob_records(ins, d)
            sname, fm = detect_ob_schema(recs or [])
            if sname is None:
                raise AppError("ساختار داده‌ی ذخیره‌شده‌ی روز %d معتبر نیست؛ با «دریافت مجدد» دوباره دریافت کنید." % d)
            events, invalid = normalize_ob_events(recs, fm)
            stt = {}
            snaps = list(reconstruct_snapshots(events, ws, we, stt))
            rows = _day_rows(symbol, company, industry, gdate_to_jalali_str(deven_to_date(d)),
                             deven_to_date(d).isoformat(), snaps)
            if rows:
                outs.write_rows(rows)
            agg["rows"] += len(rows)
            agg["snapshots"] += stt["snapshots_window"]
            agg["events"] += stt["events"]
            agg["invalid"] += invalid
            agg["incomplete"] += stt["incomplete_snapshots"]
            agg["missing"] += stt["missing_level_cells"]
            agg["ambiguous"] += stt["ambiguous_groups"]
            _jset(job, track, days_done=k + 1, rows_written=agg["rows"])
        if agg["rows"] == 0:
            raise AppError("در بازه ساعتی انتخاب‌شده هیچ اسنپ‌شاتی وجود ندارد.")
        files, notes = outs.finish()
    except BaseException:
        outs.abort()
        raise
    rep = {"output_rows": agg["rows"], "snapshots": agg["snapshots"], "events": agg["events"],
           "invalid_events": agg["invalid"], "incomplete_snapshots": agg["incomplete"],
           "missing_level_cells": agg["missing"], "ambiguous_groups": agg["ambiguous"], "refid": ref_state,
           "days_exported": len(ok_days)}
    return {"files": files, "rep": rep, "notes": notes, "stem": stem}


def build_daily_files(job, ctx, ins, spec, outdir, formats, name=None, track=True):
    gs, ge, js, je = spec_range(spec)
    symbol, company, industry, inst = instrument_labels(ctx, ins, spec)
    row = ctx.store.get_daily(ins)
    if not row or row["status"] != "ok":
        raise AppError("اطلاعات روزانه این نماد دریافت نشده یا منبع پاسخ خالی/ناموفق داده است.")
    d1, d2 = date_to_deven(gs), date_to_deven(ge)
    recs = sorted((r for r in row["records"] if isinstance(r, dict) and d1 <= (to_int_strict(r.get("dEven")) or 0) <= d2),
                  key=lambda r: to_int_strict(r["dEven"]))
    if not recs:
        raise AppError("در بازه انتخابی هیچ رکورد روزانه‌ای در داده منبع نیست.")
    keys = []
    for r in recs:
        for k in r:
            if k not in keys:
                keys.append(k)
    known = [(k, fa, en, t) for k, fa, en, t in DAILY_KNOWN if k in keys]
    comp = []
    if "pClosing" in keys and "priceYesterday" in keys:
        comp += ["close", "close_pct"]
    if "pDrCotVal" in keys and "priceYesterday" in keys:
        comp += ["last", "last_pct"]
    cols = ([("symbol", "نماد", "str"), ("company", "نام شرکت", "str"), ("industry", "نوع صنعت", "str"),
             ("date_jalali", "تاریخ شمسی", "str"), ("date_gregorian", "تاریخ میلادی", "date"), ("event_time", "ساعت ثبت", "str")]
            + [(en, fa, t) for _, fa, en, t in known] + [(DAILY_COMPUTED[c][1], DAILY_COMPUTED[c][0], "float") for c in comp])

    def val(r, k):
        try:
            return num_or_none(r.get(k))
        except ValueError:
            return r.get(k)

    def build(r):
        d = to_int_strict(r["dEven"])
        h = parse_hms_int(r.get("hEven"))
        out = [symbol, company, industry, gdate_to_jalali_str(deven_to_date(d)), deven_to_date(d).isoformat(),
               hms_str(h) if h is not None else None]
        out += [val(r, k) for k, _, _, _ in known]
        for kind in comp:
            y = val(r, "priceYesterday")
            base = val(r, "pClosing" if kind.startswith("close") else "pDrCotVal")
            if not isinstance(y, (int, float)) or not isinstance(base, (int, float)):
                out.append(None)
            elif kind.endswith("_pct"):
                out.append((base - y) / y * 100 if y else None)
            else:
                out.append(float(base - y))
        return out

    meta = {"app": "TSETMC Order Book App %s" % APP_VERSION, "report": "DailyBoard", "symbol": symbol, "ins_code": ins,
            "range": "%s تا %s" % (spec["start"], spec["end"]), "price_unit": "rial", "identity": "current (not historical)",
            "note": "computed columns derived from same-record prices without rounding"}
    stem = file_stem(name or symbol, js, je, "DailyBoard")
    outs = OutputSet(formats, outdir, stem, cols, meta)
    _jset(job, track, phase="write", days_total=len(recs), days_done=0)
    try:
        outs.write_rows([build(r) for r in recs])
        files, notes = outs.finish()
    except BaseException:
        outs.abort()
        raise
    _jset(job, track, days_done=len(recs))
    return {"files": files, "rep": {"records": len(recs), "columns": len(cols)}, "notes": notes, "stem": stem}


# ───────────────────────────── اجرای دسته‌ای صدها نماد ─────────────────────────────
def _flag(spec, key, default=True):
    v = spec.get(key, default)
    return v not in (False, 0, "0", "false", "False", None)


def bundle_flags(spec):
    inc_ob, inc_daily = _flag(spec, "include_orderbook"), _flag(spec, "include_daily")
    if not (inc_ob or inc_daily):
        raise AppError("حداقل یکی از «تاریخچه دفتر سفارشات» یا «اطلاعات روزانه تابلو» را انتخاب کنید.")
    return inc_ob, inc_daily


def unique_stem(label, ins, used):
    base = safe_filename(label)
    name = base if base not in used else "%s_%s" % (base, ins)
    used.add(name)
    return name


def output_fingerprint(ins, spec, formats, inc_ob, inc_daily):
    gs, ge, js, je = spec_range(spec)
    ws, we = spec_window(spec)
    key = [APP_VERSION, OUTPUT_ALGO_VERSION, SCHEMA_VER, ins, js, je, hms_str(ws), hms_str(we), bool(inc_ob), bool(inc_daily), list(formats)]
    return hashlib.sha1(json.dumps(key).encode("utf-8")).hexdigest()


def normalize_symbol_items(raw):
    items, seen = [], set()
    for s in raw or []:
        s = s or {}
        ins = str(s.get("ins_code") or "").strip()
        name = str(s.get("name") or s.get("symbol") or "").strip()
        if ins.isdigit():
            key = "i:" + ins
        elif name:
            key = "n:" + fa_key(name)
        else:
            continue
        if key in seen:
            continue
        seen.add(key)
        items.append({"ins_code": ins if ins.isdigit() else "", "name": name})
    return items


def resolve_symbol_item(ctx, item):
    """(نماد، یادداشت) — شناسه از کش یا از جست‌وجوی دقیقِ TSETMC؛ نبود نتیجه ⇒ (None، دلیل)."""
    ins = item.get("ins_code") or ""
    if ins:
        inst = ctx.store.get_instrument(ins)
        if inst and inst.get("identity_ok"):
            return inst, ""
        return select_instrument(ctx, {"ins_code": ins, "symbol": item.get("name", "")}), ""
    name = item.get("name", "")
    inst = ctx.store.find_instrument_by_symbol(name)
    if inst and inst.get("identity_ok"):
        return inst, ""
    found = search_instruments(ctx, name)
    exact = [i for i in found if fa_key(i["symbol"]) == fa_key(name)]
    if not exact:
        return None, "نماد دقیقی با این نام پیدا نشد"
    inst = select_instrument(ctx, exact[0])
    note = ("چند ابزار با این نماد بود؛ اولین مورد (InsCode %s) انتخاب شد" % inst["ins_code"]) if len(exact) > 1 else ""
    return inst, note


def run_bundle(job, ctx):
    """
    برای هر نماد (به‌ترتیب): شناسایی ← دریافت ← ساخت فایل (فرمت‌های انتخابی) ← پاک‌سازی کش خام.
    قابل ادامه: نمادهایی که فایل‌هایشان با همین تنظیمات ساخته شده، رد می‌شوند و روزهای دریافت‌شده دوباره گرفته نمی‌شوند.
    """
    spec = job.spec
    inc_ob, inc_daily = bundle_flags(spec)
    formats = format_list(spec)
    items = normalize_symbol_items(spec.get("symbols"))
    if not items:
        raise AppError("هیچ نمادی انتخاب نشده است.")
    if len(items) > BUNDLE_MAX_SYMBOLS:
        raise AppError("حداکثر %d نماد در هر دریافت مجاز است." % BUNDLE_MAX_SYMBOLS)
    gs, ge, js, je = spec_range(spec)
    ws, we = spec_window(spec)
    force = bool(spec.get("force"))
    purge = _flag(spec, "purge_cache", True)
    n = len(items)
    range_txt = "%s تا %s" % (spec["start"], spec["end"])
    if "parquet" in formats:
        _pyarrow()                                      # نصب خودکار pyarrow در صورت نبود
    job.set(symbols_total=n, symbols_done=0, symbols_failed=0, files_built=0, percent=0, current_symbol="",
            range=range_txt, include_orderbook=inc_ob, include_daily=inc_daily, formats=formats)
    used, done_ins, streak, failed = set(), set(), 0, 0
    outdir = ctx.store.exports
    d1, d2 = date_to_deven(gs), date_to_deven(ge)

    def fail(name, ins, note, status="error"):
        job.add_report({"symbol": name, "ins_code": ins, "status": status, "files": [], "notes": [note]})
        job.err("%s — %s" % (name, note))

    for k, item in enumerate(items):
        if job.cancelled() or job.state != "running":
            break
        base = k / n
        job.set(symbols_done=k, current_symbol=item["name"] or item["ins_code"], phase="resolve")
        try:
            inst, rnote = resolve_symbol_item(ctx, item)
            streak = 0
        except AppError as e:
            failed += 1
            fail(item["name"] or item["ins_code"], item["ins_code"], str(e))
            streak += 1
            if streak >= MAX_CONSEC_FAIL:
                job.state = "stopped"
                job.message = "%d نماد متوالی شناسایی نشد (شبکه/دسترسی)؛ پیشرفت ذخیره شد و قابل ادامه است." % streak
            job.set(symbols_failed=failed)
            continue
        if inst is None:
            failed += 1
            fail(item["name"], "", rnote, "not_found")
            job.set(symbols_failed=failed)
            continue
        ins, label = inst["ins_code"], (inst.get("symbol") or item["name"] or inst["ins_code"])
        if ins in done_ins:                              # همان ابزار یک بار با نام و یک بار با شناسه آمده است
            job.set(percent=round(100 * (k + 1) / n, 2))
            continue
        done_ins.add(ins)
        job.set(current_symbol=label)
        fname = unique_stem(label, ins, used)
        fp = output_fingerprint(ins, spec, formats, inc_ob, inc_daily)
        if not force:
            fp0, rep0 = ctx.store.get_outputs(ins)
            if fp0 == fp and rep0 and rep0.get("files") and all((outdir / f).exists() for f in rep0["files"]):
                rep0["status"] = "skipped"
                job.add_report(rep0)
                job.inc("files_built", 0)
                job.set(percent=round(100 * (k + 1) / n, 1))
                continue
        # دریافت
        if inc_ob:
            fetch_orderbook_range(job, ctx, ins, label, gs, ge, ws, we, force, range_txt,
                                  on_progress=lambda f, base=base: job.set(percent=round(100 * (base + 0.85 * f / n), 2)))
            if job.state != "running" or job.cancelled():
                break
        else:
            job.set(phase="daily", symbol=label)
            status, msg = fetch_daily_list(ctx, ins, job, force=force)
            if status == "cancelled":
                break
            if status == "error":
                failed += 1
                fail(label, ins, "اطلاعات روزانه: " + msg)
                streak += 1
                if (msg.startswith(KIND_FA["blocked"]) and streak >= MAX_CONSEC_BLOCKED) or streak >= MAX_CONSEC_FAIL:
                    job.state = "stopped"
                    job.message = "دسترسی/شبکه مشکل دارد؛ پیشرفت ذخیره شد و قابل ادامه است."
                job.set(symbols_failed=failed)
                continue
            streak = 0
            job.sleep(ctx.settings()["delay"])
        # ساخت فایل‌ها
        job.set(phase="export", percent=round(100 * (base + 0.9 / n), 2))
        sp = dict(spec, ins_code=ins, symbol=label)
        rep = {"symbol": label, "ins_code": ins, "status": "done", "files": [], "notes": [rnote] if rnote else [],
               "output_rows": None, "daily_records": None, "expected_days": None, "fetched_ok": None,
               "empty_responses": None, "errors": None}
        ob_built = False
        if inc_ob:
            rep["output_rows"] = 0
            try:
                res = build_orderbook_files(job, ctx, ins, sp, outdir, formats, name=fname, track=False)
                if res is None:
                    break
                rep["files"] += [f.name for f in res["files"]]
                rep["notes"] += res["notes"]
                rep["output_rows"] = res["rep"]["output_rows"]
                ob_built = True
            except AppError as e:
                rep["notes"].append("دفتر سفارشات: %s" % e)
            cov = ob_coverage(ctx.store, ins, gs, ge)
            rep.update({x: cov[x] for x in ("expected_days", "fetched_ok", "empty_responses", "errors")})
        if inc_daily:
            try:
                res = build_daily_files(job, ctx, ins, sp, outdir, formats, name=fname, track=False)
                rep["files"] += [f.name for f in res["files"]]
                rep["notes"] += res["notes"]
                rep["daily_records"] = res["rep"]["records"]
            except AppError as e:
                rep["notes"].append("اطلاعات روزانه: %s" % e)
        if not rep["files"]:
            rep["status"] = "error"
            failed += 1
        else:
            ctx.store.put_outputs(ins, fp, rep)
            job.inc("files_built", len(rep["files"]))
            if purge and ob_built:
                ctx.store.purge_ob_payloads(ins, d1, d2)
        for note in rep["notes"]:
            job.err("%s — %s" % (label, note))
        job.add_report(rep)
        job.set(symbols_failed=failed, percent=round(100 * (k + 1) / n, 2))
    all_reps = job.reports_page(0, 10 ** 9)[1]
    total_files = sum(len(r["files"]) for r in all_reps)
    with_files = sum(1 for r in all_reps if r["files"])
    job.set(phase="done")
    job.result = {"files_total": total_files, "symbols_total": n, "symbols_failed": failed}
    if job.state == "running" and job.cancelled():
        job.message = "لغو شد؛ پیشرفت ذخیره شد و قابل ادامه است."
    elif job.state == "running":
        job.set(symbols_done=n, percent=100)
        job.message = "پایان یافت: %d فایل برای %d نماد." % (total_files, with_files)


# ───────────────────────────── ZIP جریانی بدون پوشه ─────────────────────────────
class _ZipSink:
    def __init__(self):
        self.chunks, self.pos = [], 0

    def write(self, b):
        self.chunks.append(bytes(b))
        self.pos += len(b)
        return len(b)

    def flush(self):
        pass

    def tell(self):
        return self.pos

    def drain(self):
        out, self.chunks = b"".join(self.chunks), []
        return out


def zip_stream(paths):
    """ZIP مسطح (فقط نام فایل، بدون پوشه) که مستقیم به مرورگر جریان می‌یابد؛ فایل موقت ساخته نمی‌شود."""
    sink = _ZipSink()
    zf = zipfile.ZipFile(sink, "w", zipfile.ZIP_STORED, allowZip64=True)
    for p in paths:
        zi = zipfile.ZipInfo(Path(p).name, date_time=time.localtime(Path(p).stat().st_mtime)[:6])
        zi.compress_type = zipfile.ZIP_STORED
        with zf.open(zi, "w", force_zip64=True) as dest, open(str(p), "rb") as src:
            while True:
                chunk = src.read(1 << 20)
                if not chunk:
                    break
                dest.write(chunk)
                data = sink.drain()
                if data:
                    yield data
        data = sink.drain()
        if data:
            yield data
    zf.close()
    data = sink.drain()
    if data:
        yield data


def cov_rows(c):
    src = {"daily_list": "فهرست روزانه خودِ منبع", "calendar_assumed": "تقویم فرضی شنبه–چهارشنبه (تأییدنشده)"}
    ref = {"all_days": "در همه روزهای دریافت‌شده وجود دارد", "none": "در منبع وجود ندارد (ستون خالی می‌ماند)",
           "mixed": "فقط در بخشی از روزها وجود دارد", "n/a": "—"}
    kinds = "، ".join("%s: %d" % (KIND_FA.get(k, k), v) for k, v in c["error_kinds"].items()) or "—"
    return [
        ["منبع تعیین روزهای معاملاتی", src.get(c["days_source"], c["days_source"])],
        ["تعداد روزهای مورد انتظار", c["expected_days"]],
        ["روزهای دریافت‌شده (دارای داده)", c["fetched_ok"]],
        ["پاسخ‌های خالی (معتبر اما بدون رکورد)", c["empty_responses"]],
        ["روزهای ناموفق (خطا)", c["errors"]],
        ["نوع خطاها", kinds],
        ["هنوز دریافت نشده", c["not_fetched_yet"]],
        ["رویدادهای خام", c["raw_events"]],
        ["رویدادهای نامعتبر ردشده", c["invalid_events"]],
        ["اسنپ‌شات‌های بازسازی‌شده (بازه خروجی)", c["snapshots_rebuilt_in_window"]],
        ["ردیف‌های خروجی (اسنپ‌شات × ۵)", c["output_rows_estimate"]],
        ["اسنپ‌شات‌های دارای سطح ناقص/ناموجود", c["incomplete_snapshots"]],
        ["خانه‌های سطح ناموجود (سمت خرید/فروش)", c["missing_level_cells"]],
        ["گروه‌های مبهم هم‌ثانیه (بدون refID)", c["ambiguous_same_second_groups"]],
        ["refID در داده منبع", ref.get(c["refid_in_source"], c["refid_in_source"])],
        ["اولین روز قابل استفاده", c["first_usable_date"] or "—"],
        ["آخرین روز قابل استفاده", c["last_usable_date"] or "—"],
    ]


def instrument_labels(ctx, ins, spec):
    inst = ctx.store.get_instrument(ins) or {}
    return (inst.get("symbol") or spec.get("symbol") or ins, inst.get("company") or "", inst.get("industry") or "", inst)


def search_instruments(ctx, q):
    q = norm_digits(q).strip()
    if not q:
        return []
    cl = ctx.make_client()
    cl.max_attempts = 2
    variants = [q] + ([ar_chars(q)] if ar_chars(q) != q else [])
    items, last = [], None
    for v in variants:
        res = cl.get("Instrument/GetInstrumentSearch/" + urllib.parse.quote(v, safe=""))
        if res.status != "ok":
            last = res
            continue
        items = parse_search(res.data)
        if items:
            break
    if not items and last is not None:
        raise AppError("%s — %s" % (KIND_FA.get(last.status, last.status), last.detail))
    qk = fa_key(q)
    items.sort(key=lambda it: (0 if fa_key(it["symbol"]) == qk else 1 if fa_key(it["symbol"]).startswith(qk) else 2,
                               it["symbol"]))
    return items[:30]


def select_instrument(ctx, item):
    ins = str(item.get("ins_code") or "").strip()
    if not ins.isdigit():
        raise AppError("شناسه ابزار (InsCode) نامعتبر است.")
    ctx.store.upsert_instrument(ins, symbol=fa_chars(item.get("symbol") or ""), company=fa_chars(item.get("name") or ""),
                                market=fa_chars(item.get("market") or ""), isin=item.get("isin") or "")
    note = ""
    cl = ctx.make_client()
    cl.max_attempts = 2
    res = cl.get("Instrument/GetInstrumentIdentity/%s" % ins)
    if res.status == "ok":
        try:
            ident = parse_identity(res.data)
            ctx.store.upsert_instrument(ins, company=ident["company"] or None, industry=ident["industry"] or None,
                                        symbol=ident["symbol"] or None, identity_ok=1)
        except SchemaError as e:
            note = "هویت ابزار خوانده نشد: %s (نام شرکت از نتیجه جست‌وجو و صنعت خالی می‌ماند)" % e
    else:
        note = "هویت ابزار دریافت نشد (%s)؛ نام شرکت از جست‌وجو و صنعت خالی می‌ماند." % KIND_FA.get(res.status, res.status)
    inst = ctx.store.get_instrument(ins)
    inst["note"] = note
    return inst


def run_check(ctx, ins, start_s=None, end_s=None):
    cl = ctx.make_client()
    cl.max_attempts, cl.timeout = 1, (6, 25)
    probes = []

    def probe(name, path, key=None, is_obj=False):
        res = cl.get(path)
        row = {"name": name, "path": path, "status": res.status, "http_code": res.http_code,
               "ms": int(res.elapsed * 1000), "detail": res.detail, "records": None}
        if res.status == "ok":
            if is_obj:
                row["status"] = "ok" if isinstance(res.data, dict) and key in res.data else "schema"
                row["detail"] = "" if row["status"] == "ok" else "کلید %s یافت نشد" % key
            else:
                cls, val = classify_list_payload(res.data, key)
                row["status"] = cls
                row["records"] = len(val) if cls == "ok" else 0
                row["detail"] = val if cls == "schema" else ("پاسخ معتبر اما خالی" if cls == "empty" else "")
                if cls == "ok" and key == "bestLimitsHistory":
                    sname, fm = detect_ob_schema(val)
                    row["detail"] = ("ساختار تشخیص‌داده‌شده: " + sname + (" | refID: " + fm["ref"] if fm.get("ref") else " | refID: ندارد")) if sname else fm
                    if not sname:
                        row["status"] = "schema"
        probes.append(row)
        return row

    probe("جست‌وجوی نماد", "Instrument/GetInstrumentSearch/" + urllib.parse.quote("فولاد", safe=""), "instrumentSearch")
    if ins:
        probe("هویت ابزار", "Instrument/GetInstrumentIdentity/%s" % ins, "instrumentIdentity", is_obj=True)
        probe("فهرست روزانه (۵ رکورد آخر)", "ClosingPrice/GetClosingPriceDailyList/%s/5" % ins, "closingPriceDaily")
        daily = ctx.store.get_daily(ins)
        recent = old = None
        try:
            gs, ge, _, _ = validate_range(start_s, end_s)
        except Exception:
            gs, ge = tehran_today() - _dt.timedelta(days=30), tehran_today()
        if daily and daily["status"] == "ok":
            ds = sorted(d for d in (to_int_strict(r.get("dEven")) for r in daily["records"]) if d)
            inr = [d for d in ds if date_to_deven(gs) <= d <= date_to_deven(ge)]
            if inr:
                recent, old = inr[-1], inr[0]
        if recent is None:
            cds = [date_to_deven(d) for d in candidate_days(gs, ge)]
            if cds:
                recent, old = cds[-1], cds[0]
        if recent:
            probe("دفتر سفارش — روز جدید (%d)" % recent, "BestLimits/%s/%d" % (ins, recent), "bestLimitsHistory")
        if old and old != recent:
            probe("دفتر سفارش — قدیمی‌ترین روز بازه (%d)" % old, "BestLimits/%s/%d" % (ins, old), "bestLimitsHistory")
    bad = [p for p in probes if p["status"] in ("network", "http", "blocked", "html", "invalid_json", "schema")]
    if not bad:
        verdict = "همه بررسی‌ها پاسخ معتبر داشتند (پاسخ خالی ممکن است طبیعی باشد؛ جزئیات هر مورد را ببینید)."
    elif any(p["status"] == "blocked" for p in bad):
        verdict = "دسترسی مسدود/ممنوع است (۴۰۳/۴۲۹ یا صفحه مسدودسازی). IP یا شبکه دیگر، یا پراکسی مجاز خودتان را در «تنظیمات» امتحان کنید."
    elif any(p["status"] == "network" for p in bad):
        verdict = "اتصال شبکه به سرور برقرار نشد (DNS/timeout/اتصال)."
    else:
        verdict = "برخی پاسخ‌ها نامعتبر یا با ساختار ناشناخته بودند؛ جزئیات را ببینید."
    return {"base": cl.base, "probes": probes, "verdict": verdict, "checked_at": _now()}


_BENCH = {}


def bench_format(fmt, n=20000):
    """سرعت واقعی نوشتن یک فرمت روی همین رایانه (ردیف/ثانیه) و حجم هر ردیف."""
    if fmt in _BENCH:
        return _BENCH[fmt]
    d = Path(tempfile.mkdtemp(prefix="tse_bench_"))
    try:
        rows = [["فولاد", "فولاد مبارکه اصفهان", "فلزات اساسی", "1403/05/01", "2024-07-22",
                 "09:%02d:%02d" % (i // 60 % 60, i % 60), None if i % 50 == 0 else i + 1, i % 5 + 1, 4240 + i % 7,
                 4358512 + i, 17 + i % 9, 20, 1382803 + i, 4230] for i in range(n)]
        t0 = time.time()
        o = OutputSet([fmt], d, "bench", OB_COLS, {})
        o.write_rows(rows)
        files, _ = o.finish()
        el = max(time.time() - t0, 1e-3)
        _BENCH[fmt] = (n / el, files[0].stat().st_size / n)
    finally:
        shutil.rmtree(str(d), ignore_errors=True)
    return _BENCH[fmt]


def run_estimate(job, ctx):
    spec = job.spec
    ins = need_instrument(spec)
    gs, ge, js, je = spec_range(spec)
    ws, we = spec_window(spec)
    inc_ob, inc_daily = bundle_flags(spec)
    fmts = format_list(spec)
    nsym = max(1, len(normalize_symbol_items(spec.get("symbols"))))
    if not inc_ob:
        msg = "فقط «اطلاعات روزانه تابلو» انتخاب شده است؛ برای هر نماد فقط یک درخواست لازم و حجم خروجی کوچک است."
        job.result = {"estimate": {"message": msg, "sample_days": [], "symbols": nsym}}
        job.message = msg
        return
    job.set(phase="estimate", percent=0, symbol=spec.get("symbol", ""))
    status, msg = fetch_daily_list(ctx, ins, job)
    if status == "cancelled":
        return
    days, source = get_trading_days(ctx.store, ins, gs, ge)
    if not days:
        raise AppError("در بازه انتخابی روز معاملاتی یافت نشد.")
    idx = sorted({0, len(days) // 2, len(days) - 1})
    sample = [days[i] for i in idx]
    cl = ctx.make_client()
    cfg = ctx.settings()
    delay = cfg["delay"]
    rows, lat, blobs, snaps, events = [], [], [], [], []
    for k, d in enumerate(sample):
        if job.cancelled():
            return
        meta = ctx.store.get_ob_meta(ins, d)
        if meta and meta["status"] == "ok" and meta["schema_ver"] == SCHEMA_VER and meta["has_payload"]:
            recs, _ = ctx.store.get_ob_records(ins, d)
            blobs.append(len(pack_records(recs)))
            snaps.append(meta["n_snap_window"]); events.append(meta["n_events"])
            rows.append({"day": gdate_to_jalali_str(deven_to_date(d)), "outcome": "ok (کش)", "events": meta["n_events"]})
            continue
        outcome, res, info = fetch_one_ob_day(ctx, cl, job.cancel_event, ins, d, ws, we)
        if outcome == "cancelled":
            return
        lat.append(res.elapsed)
        rows.append({"day": gdate_to_jalali_str(deven_to_date(d)), "outcome": outcome,
                     "events": (info["stats"] or {}).get("events", 0),
                     "detail": info["detail"] if outcome not in ("ok", "empty") else ""})
        if outcome == "ok":
            blobs.append(info["blob"]); snaps.append(info["stats"]["snapshots_window"]); events.append(info["stats"]["events"])
        job.set(percent=round(100 * (k + 1) / len(sample)))
        job.sleep(delay)
    est = {"sample_days": rows, "trading_days": len(days), "days_source": source, "symbols": nsym}
    if not snaps:
        est["message"] = "هیچ‌یک از روزهای نمونه داده نداد؛ برآورد ممکن نیست (جزئیات هر روز را ببینید)."
    else:
        ok_frac = len(snaps) / len(sample)
        n_ok = len(days) * ok_frac
        avg_snap, avg_ev, avg_blob = sum(snaps) / len(snaps), sum(events) / len(events), sum(blobs) / len(blobs)
        rows_est = avg_snap * n_ok * 5
        lat_avg = (sum(lat) / len(lat)) if lat else 1.0
        sized = [f for f in fmts if f in ("csv", "parquet")]
        files_mb = write_min = 0.0
        for f in sized:
            rps, bpr = bench_format(f)
            files_mb += rows_est * bpr / 1048576
            write_min += rows_est / rps / 60
        cache_one = n_ok * avg_blob / 1048576
        note = ""
        est.update({
            "avg_events_per_day": round(avg_ev), "avg_snapshots_per_day": round(avg_snap),
            "est_output_rows": round(rows_est * nsym),
            "est_fetch_minutes": round(len(days) * (lat_avg + delay) / max(1, cfg["workers"]) / 60 * nsym, 1),
            "est_cache_mb": round(cache_one if _flag(spec, "purge_cache", True) else cache_one * nsym, 1),
            "est_files_mb": round(files_mb * nsym, 1), "est_write_minutes": round(write_min * nsym, 1),
            "message": ("برآورد تقریبی با برون‌یابی از %d روز نمونه است؛ اعداد واقعی ممکن است تفاوت زیادی داشته باشند." % len(sample))
                       + (" برآورد با نماد «%s» انجام و برای %d نماد ضرب شده است." % (spec.get("symbol", ""), nsym) if nsym > 1 else "")
                       + note})
    job.result = {"estimate": est}
    job.message = est["message"]


# ─────────────────────────────────── Backend (Flask) ───────────────────────────
def make_presets():
    end = jdatetime.date.fromgregorian(date=tehran_today())
    one = _dt.timedelta(days=1)

    def fmt(d):
        return "%04d/%02d/%02d" % (d.year, d.month, d.day)

    def back(**kw):
        s = jalali_shift(end, **kw)
        return fmt(jdatetime.date.fromgregorian(date=s.togregorian() + one))

    return {"end": fmt(end), "5y": back(years=5), "1y": back(years=1), "3m": back(months=3), "1m": back(months=1),
            "1w": fmt(jdatetime.date.fromgregorian(date=end.togregorian() - 6 * one))}


def create_app(ctx):
    if flask is None:
        raise AppError("کتابخانه flask نصب نیست.")
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 1024 * 1024

    @app.after_request
    def _security_headers(resp):
        # محدودسازی قابلیت‌های مرورگر برای رابط محلی.
        resp.headers["X-Content-Type-Options"] = "nosniff"
        resp.headers["X-Frame-Options"] = "DENY"
        resp.headers["Referrer-Policy"] = "no-referrer"
        resp.headers["Content-Security-Policy"] = ("default-src 'self'; script-src 'self' 'unsafe-inline'; "
            "style-src 'self' 'unsafe-inline'; font-src 'self' data:; connect-src 'self'; "
            "img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
        return resp

    @app.before_request
    def _guard():
        # سرویس فقط به رابط بازگشت محلی IPv4 متصل می‌شود.
        host = urllib.parse.urlsplit("//" + (request.host or "")).hostname
        if host != "127.0.0.1":
            return jsonify(error="Host not allowed"), 403
        if request.method == "POST":
            if request.headers.get("X-App") != "tse":
                return jsonify(error="درخواست نامعتبر (سربرگ X-App)"), 403
            origin = request.headers.get("Origin")
            if origin:
                origin_parts = urllib.parse.urlsplit(origin)
                if origin_parts.scheme not in ("http", "https") or origin_parts.netloc.lower() != (request.host or "").lower():
                    return jsonify(error="Origin not allowed"), 403

    def api(fn):
        def wrapper(*a, **kw):
            try:
                return fn(*a, **kw)
            except Busy as e:
                return jsonify(error=str(e)), 409
            except AppError as e:
                return jsonify(error=str(e)), 400
            except Exception:  # جزئیات خطا نباید به مرورگر/کلاینت ارسال شود.
                logging.getLogger("tse_lob").exception("Unhandled API exception")
                return jsonify(error="خطای داخلی رخ داد؛ جزئیات فقط در گزارش محلی ثبت شده است."), 500
        wrapper.__name__ = fn.__name__
        return wrapper

    @app.route("/")
    def index():
        return Response(INDEX_HTML.replace("__VERSION__", APP_VERSION), mimetype="text/html; charset=utf-8")

    @app.route("/api/config")
    @api
    def config():
        s = ctx.settings()
        return jsonify(version=APP_VERSION, window=DEFAULT_WINDOW, settings=s, resources=s["resources"], data_dir=str(ctx.store.dir),
                       presets=make_presets(), max_days=MAX_RANGE_DAYS,
                       last_spec=ctx.jobs.last_spec)

    @app.route("/api/settings", methods=["GET", "POST"])
    @api
    def settings():
        if request.method == "POST":
            d = request.get_json(force=True, silent=True) or {}
            base = str(d.get("api_base") or DEFAULT_API_BASE).strip()
            proxy = str(d.get("proxy") or "").strip()
            base_parts = urllib.parse.urlsplit(base)
            if (len(base) > 2048 or base_parts.scheme.lower() not in ("http", "https")
                    or not base_parts.hostname or base_parts.username or base_parts.password
                    or base_parts.query or base_parts.fragment):
                raise AppError("آدرس API نامعتبر است؛ فقط URL معتبر http/https بدون اطلاعات ورود، Query یا Fragment پذیرفته می‌شود.")
            if proxy:
                proxy_parts = urllib.parse.urlsplit(proxy)
                if (len(proxy) > 2048 or proxy_parts.scheme.lower() not in ("http", "https")
                        or not proxy_parts.hostname or proxy_parts.query or proxy_parts.fragment):
                    raise AppError("آدرس پراکسی نامعتبر است؛ URL معتبر http/https وارد کنید.")
            try:
                delay = float(d.get("delay", DEFAULT_DELAY))
            except (TypeError, ValueError):
                raise AppError("تأخیر بین درخواست‌ها عدد نیست.")
            if not 0.1 <= delay <= 30:
                raise AppError("تأخیر باید بین ۰٫۱ و ۳۰ ثانیه باشد.")
            try:
                workers = int(d.get("workers", 2))
            except (TypeError, ValueError):
                raise AppError("تعداد درخواست هم‌زمان عدد صحیح نیست.")
            max_workers = system_resources()["max_workers"]
            if not 1 <= workers <= max_workers:
                raise AppError("تعداد درخواست هم‌زمان باید بین ۱ و %d باشد (حد متناسب با منابع سیستم)." % max_workers)
            ctx.store.set_setting("api_base", base)
            ctx.store.set_setting("proxy", proxy)
            ctx.store.set_setting("delay", str(delay))
            ctx.store.set_setting("workers", str(workers))
        return jsonify(ctx.settings())

    @app.route("/api/search")
    @api
    def search():
        return jsonify(items=search_instruments(ctx, request.args.get("q", "")))

    @app.route("/api/select", methods=["POST"])
    @api
    def select():
        return jsonify(select_instrument(ctx, request.get_json(force=True, silent=True) or {}))

    @app.route("/api/convert")
    @api
    def convert():
        g = jalali_to_gdate(request.args.get("d", ""))
        names = ["دوشنبه", "سه‌شنبه", "چهارشنبه", "پنجشنبه", "جمعه", "شنبه", "یکشنبه"]
        return jsonify(gregorian=g.isoformat(), jalali=gdate_to_jalali_str(g), weekday=names[g.weekday()])

    @app.route("/api/check", methods=["POST"])
    @api
    def check():
        d = request.get_json(force=True, silent=True) or {}
        ins = str(d.get("ins_code") or "").strip()
        return jsonify(run_check(ctx, ins if ins.isdigit() else None, d.get("start"), d.get("end")))

    RUNNERS = {"daily": run_fetch_daily, "orderbook": run_fetch_orderbook, "estimate": run_estimate, "bundle": run_bundle}

    @app.route("/api/resolve", methods=["POST"])
    @api
    def resolve():
        """افزودن گروهی نمادها: هر نام با تطبیق دقیق نماد پیدا و هویتش (شرکت/صنعت) خوانده می‌شود."""
        d = request.get_json(force=True, silent=True) or {}
        names, seen = [], set()
        for nm in re.split(r"[,;،؛\n\r\t]+", str(d.get("names") or "")):
            nm = nm.strip()
            if nm and fa_key(nm) not in seen:
                seen.add(fa_key(nm))
                names.append(nm)
        if not names:
            raise AppError("نامی وارد نشده است.")
        if len(names) > 60:
            raise AppError("در هر بار حداکثر ۶۰ نام قابل افزودن است.")
        out, missing, fails = [], [], 0
        delay = min(ctx.settings()["delay"], 0.4)
        for nm in names:
            try:
                items = search_instruments(ctx, nm)
            except AppError as e:
                missing.append({"name": nm, "reason": str(e)})
                fails += 1
                if fails >= 2:                       # شبکه/دسترسی مشکل دارد؛ ادامه بی‌فایده است
                    break
                continue
            exact = [i for i in items if fa_key(i["symbol"]) == fa_key(nm)]
            if not exact:
                missing.append({"name": nm, "reason": "نماد دقیقی با این نام پیدا نشد."})
                continue
            inst = select_instrument(ctx, exact[0])
            inst["ambiguous"] = len(exact)
            out.append(inst)
            time.sleep(delay)
        return jsonify(resolved=out, missing=missing)

    @app.route("/api/job/start", methods=["POST"])
    @api
    def job_start():
        spec = request.get_json(force=True, silent=True) or {}
        kind = spec.get("kind")
        if kind not in RUNNERS:
            raise AppError("نوع عملیات نامعتبر است.")
        if kind in ("bundle", "estimate"):
            bundle_flags(spec)
            format_list(spec)
        if kind == "bundle":
            if not normalize_symbol_items(spec.get("symbols")):
                raise AppError("حداقل یک نماد را انتخاب کنید.")
            if len(normalize_symbol_items(spec.get("symbols"))) > BUNDLE_MAX_SYMBOLS:
                raise AppError("حداکثر %d نماد در هر دریافت مجاز است." % BUNDLE_MAX_SYMBOLS)
        else:
            need_instrument(spec)
        spec_range(spec)       # اعتبارسنجی تاریخ‌ها پیش از شروع
        spec_window(spec)
        job = ctx.jobs.start(kind, spec, RUNNERS[kind])
        return jsonify(job.public())

    @app.route("/api/job/cancel", methods=["POST"])
    @api
    def job_cancel():
        return jsonify(cancelled=ctx.jobs.cancel())

    @app.route("/api/job/status")
    @api
    def job_status():
        j = ctx.jobs.current
        return jsonify(job=j.public() if j else None)

    @app.route("/api/coverage")
    @api
    def coverage():
        ins = request.args.get("ins_code", "")
        if not ins.isdigit():
            raise AppError("نماد انتخاب نشده است.")
        gs, ge, _, _ = validate_range(request.args.get("start", ""), request.args.get("end", ""))
        cov = ob_coverage(ctx.store, ins, gs, ge)
        return jsonify(coverage=cov, rows=cov_rows(cov))

    @app.route("/api/job/results")
    @api
    def job_results():
        j = ctx.jobs.current
        if not j:
            return jsonify(total=0, offset=0, items=[])
        try:
            off = max(0, int(request.args.get("offset", 0)))
            lim = max(1, min(200, int(request.args.get("limit", 50))))
        except ValueError:
            raise AppError("پارامتر صفحه‌بندی نامعتبر است.")
        total, items = j.reports_page(off, lim)
        return jsonify(total=total, offset=off, items=items)

    @app.route("/api/files")
    @api
    def list_files():
        """فهرست فایل‌های معتبر خروجی را مستقیماً از دیسک می‌خواند؛ رفرش صفحه اثری بر آن ندارد."""
        try:
            offset = max(0, int(request.args.get("offset", 0)))
            limit = max(1, min(200, int(request.args.get("limit", 50))))
        except ValueError:
            raise AppError("پارامتر صفحه‌بندی فایل‌ها نامعتبر است.")
        rows = []
        for path in ctx.store.exports.iterdir():
            if path.suffix.lower() not in (".csv", ".parquet") or path.is_symlink() or not path.is_file():
                continue
            try:
                st = path.stat()
                rows.append({"name": path.name, "format": path.suffix[1:].upper(), "size": st.st_size,
                             "modified": _dt.datetime.fromtimestamp(st.st_mtime, TEHRAN_TZ).strftime("%Y-%m-%d %H:%M:%S")})
            except OSError:
                continue
        rows.sort(key=lambda x: (x["modified"], x["name"]), reverse=True)
        return jsonify(total=len(rows), offset=offset, limit=limit, items=rows[offset:offset + limit])

    @app.route("/api/files/zip")
    @api
    def all_files_zip():
        """تمام خروجی‌های موجود را مستقل از Job جاری در یک ZIP جریانی تحویل می‌دهد."""
        paths = []
        for path in ctx.store.exports.iterdir():
            if path.suffix.lower() in (".csv", ".parquet") and not path.is_symlink() and path.is_file():
                paths.append(path)
        paths.sort(key=lambda p: p.name)
        if not paths:
            return jsonify(error="فایلی برای دانلود وجود ندارد."), 404
        return Response(stream_with_context(zip_stream(paths)), mimetype="application/zip",
                        headers={"Content-Disposition": 'attachment; filename="TSE_LOB_exports.zip"'})

    @app.route("/api/job/zip")
    def job_zip():
        j = ctx.jobs.current
        names = []
        if j:
            for rep in j.reports_page(0, 10 ** 9)[1]:
                names += [n for n in rep.get("files", []) if Path(n).name == n]
        paths = [ctx.store.exports / n for n in dict.fromkeys(names) if (ctx.store.exports / n).is_file()]
        if not paths:
            return jsonify(error="فایلی برای دانلود وجود ندارد."), 404
        try:
            gs, ge, js, je = spec_range(j.spec)
        except AppError:
            js = je = "x"
        kinds = "-".join(k for k, f in (("OrderBook", _flag(j.spec, "include_orderbook")), ("DailyBoard", _flag(j.spec, "include_daily"))) if f)
        try:
            fmts = "-".join(format_list(j.spec))
        except AppError:
            fmts = "data"
        n_sym = sum(1 for r in j.reports_page(0, 10 ** 9)[1] if r.get("files"))
        zname = "TSE_LOB_%dsymbols_%s_%s_%s_%s.zip" % (n_sym, js, je, kinds, fmts)
        return Response(stream_with_context(zip_stream(paths)), mimetype="application/zip",
                        headers={"Content-Disposition": 'attachment; filename="%s"' % zname})

    @app.route("/api/download/<path:name>")
    def download(name):
        if Path(name).suffix.lower() not in (".csv", ".parquet") or Path(name).name != name or len(name) > 240:
            return jsonify(error="فایل نامعتبر است."), 404
        target = ctx.store.exports / name
        if target.is_symlink() or not target.is_file() or target.resolve().parent != ctx.store.exports.resolve():
            return jsonify(error="فایل در فهرست خروجی موجود نیست."), 404
        return send_from_directory(str(ctx.store.exports), name, as_attachment=True)

    return app


# ───────────────────────────── رابط کاربری تعبیه‌شده ───────────────────────────
# فونت وزیرمتن (Vazirmatn) — © 2015 The Vazirmatn Project Authors، مجوز SIL Open Font License 1.1
# (https://github.com/rastikerdar/vazirmatn). زیرمجموعه‌ی فونت متغیر (وزن ۴۰۰ تا ۸۰۰) برای جاسازی در صفحه
# تا نمایش فارسی به اینترنت وابسته نباشد.
_FONT_WOFF2_B64 = """
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
"""

INDEX_HTML_TEMPLATE = r"""<!doctype html>
<html lang="fa" dir="rtl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="theme-color" content="#4c1d95">
<title>TSE_LOB — دریافت دفتر سفارشات و اطلاعات روزانه TSETMC</title>
<style>
@font-face{font-family:"Vazirmatn";src:url(data:font/woff2;base64,__FONT_B64__) format("woff2");font-weight:400 800;font-style:normal;font-display:swap}
:root{--bg:#f4f1fb;--bg2:#e9e3f7;--panel:#fff;--ink:#1e1535;--mute:#6b6385;--line:#e2dcf0;--accent:#6d28d9;--accent-d:#5b21b6;--accent-ink:#fff;
--soft:#f2eefc;--zebra:rgba(109,40,217,.045);--ring:rgba(109,40,217,.28);--ok:#15803d;--warn:#b45309;--bad:#b91c1c;--pend:#ddd6ee;--empty:#e0a82e;
--shadow:0 1px 2px rgba(50,25,100,.06),0 12px 30px -16px rgba(70,35,140,.32);color-scheme:light dark}
@media (prefers-color-scheme:dark){:root{--bg:#120c22;--bg2:#1b1233;--panel:#1b1330;--ink:#efeafc;--mute:#a99fc8;--line:#32264f;--accent:#a78bfa;--accent-d:#8b5cf6;
--accent-ink:#150d29;--soft:#261b45;--zebra:rgba(167,139,250,.07);--ring:rgba(167,139,250,.38);--ok:#4ade80;--warn:#fbbf24;--bad:#fca5a5;--pend:#3a2f58;--empty:#c99730;
--shadow:0 1px 2px rgba(0,0,0,.4),0 12px 30px -16px rgba(0,0,0,.7)}}
*{box-sizing:border-box}
html{-webkit-text-size-adjust:100%}
body{margin:0;background:linear-gradient(180deg,var(--bg2),var(--bg) 340px) no-repeat,var(--bg);color:var(--ink);
font:400 15.5px/1.95 "Vazirmatn","Segoe UI",Tahoma,"Noto Sans Arabic",sans-serif;font-variant-numeric:tabular-nums;text-rendering:optimizeLegibility;-webkit-font-smoothing:antialiased}
b,strong,th,h1,h2{font-weight:700}
header{background:linear-gradient(118deg,#2a1158 0%,#4c1d95 48%,#7c3aed 100%);color:#fff;padding:30px clamp(16px,4vw,48px) 26px;position:relative;overflow:hidden;border-bottom:3px solid #a78bfa}
header::before{content:"";position:absolute;inset:auto -80px -120px auto;width:340px;height:340px;border-radius:50%;background:radial-gradient(circle,rgba(255,255,255,.16),transparent 68%)}
.hd{max-width:1280px;margin:0 auto;display:flex;align-items:center;gap:16px;position:relative}
.logo{flex:none;width:58px;height:58px;filter:drop-shadow(0 6px 14px rgba(0,0,0,.28))}
h1.brand{margin:0;font-size:clamp(30px,5vw,42px);line-height:1.25;font-weight:800;letter-spacing:0}
.app{font-size:16px;font-weight:500;margin-top:2px;opacity:.96}
.sub{font-size:12.5px;margin-top:6px;opacity:.78;line-height:1.8}
main{max-width:1280px;margin:0 auto;display:grid;gap:18px;padding:22px clamp(16px,4vw,48px) 56px;grid-template-columns:minmax(0,5fr) minmax(0,7fr);align-items:start}
@media (max-width:980px){main{grid-template-columns:1fr}}
section{background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:18px 20px;margin-bottom:18px;box-shadow:var(--shadow)}
section h2{display:flex;align-items:center;gap:10px;font-size:17px;margin:0 0 12px;color:var(--ink)}
section h2 .n{flex:none;width:28px;height:28px;border-radius:50%;background:var(--accent);color:var(--accent-ink);display:inline-flex;align-items:center;justify-content:center;font-size:14px;font-weight:700;line-height:1}
label{display:block;font-size:13.5px;color:var(--mute);margin:10px 0 4px;font-weight:500}
input[type=text],input[type=number],textarea{width:100%;padding:9px 13px;border:1.5px solid var(--line);border-radius:11px;background:var(--bg);color:var(--ink);font:inherit;transition:border-color .15s,box-shadow .15s}
textarea{min-height:70px;resize:vertical}
input[type=text]:focus,input[type=number]:focus,textarea:focus{outline:none;border-color:var(--accent);box-shadow:0 0 0 4px var(--ring)}
input[type=checkbox]{accent-color:var(--accent);width:16px;height:16px;vertical-align:-3px;margin-left:6px}
button:focus-visible,summary:focus-visible,.opt:focus-within{outline:3px solid var(--ring);outline-offset:2px}
.row{display:flex;gap:12px;flex-wrap:wrap}.row>*{flex:1 1 150px}
button{font:inherit;font-weight:500;padding:8px 15px;border-radius:11px;border:1.5px solid var(--line);background:var(--panel);color:var(--ink);cursor:pointer;transition:background .15s,border-color .15s,transform .05s}
button:hover:not(:disabled){border-color:var(--accent);background:var(--soft)}
button:active:not(:disabled){transform:translateY(1px)}
button.p{background:linear-gradient(180deg,var(--accent),var(--accent-d));color:var(--accent-ink);border-color:var(--accent-d);font-weight:700;box-shadow:0 8px 18px -10px var(--accent)}
button.p:hover:not(:disabled){filter:brightness(1.08);background:linear-gradient(180deg,var(--accent),var(--accent-d))}
button:disabled{opacity:.48;cursor:not-allowed}
.btns{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:10px;margin-top:12px}
.big{padding:13px 16px;font-size:16.5px;border-radius:13px}
.chips button{padding:2px 13px;font-size:13.5px;margin:0 0 6px 6px;border-radius:999px;background:var(--soft);border-color:transparent;color:var(--accent);font-weight:600}
.chips button:hover{border-color:var(--accent)}
#sugg{border:1.5px solid var(--line);border-radius:12px;margin-top:6px;max-height:270px;overflow:auto;background:var(--panel);box-shadow:var(--shadow)}
#sugg div{padding:8px 13px;cursor:pointer;border-bottom:1px solid var(--line);font-size:14.5px}
#sugg div:last-child{border-bottom:0}
#sugg div:hover,#sugg div:focus{background:var(--soft);outline:none}
#sugg small{color:var(--mute)}
#chips{display:flex;flex-wrap:wrap;gap:8px;margin-top:10px}
.chip{display:inline-flex;align-items:center;gap:2px;border:1.5px solid var(--accent);border-radius:999px;padding:0 5px 0 13px;cursor:pointer;background:var(--panel);font-size:14.5px;transition:background .15s}
.chip:hover{background:var(--soft)}
.chip.on{background:var(--accent);color:var(--accent-ink)}
.chip button{border:0;background:transparent;color:inherit;padding:0 8px;font-size:18px;line-height:1.6;border-radius:999px}
.chip button:hover:not(:disabled){background:rgba(0,0,0,.14)}
.sel{margin-top:12px;padding:10px 14px;border:1.5px dashed var(--accent);border-radius:12px;font-size:14.5px;background:var(--soft)}
.err{color:var(--bad)}.okc{color:var(--ok)}.warnc{color:var(--warn)}.mute{color:var(--mute)}
.opts{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:10px;margin:2px 0 14px}
.opt{position:relative;display:flex;gap:11px;align-items:flex-start;padding:12px 14px;border:1.5px solid var(--line);border-radius:14px;cursor:pointer;background:var(--bg);margin:0;color:var(--ink);transition:border-color .15s,background .15s}
.opt:hover{border-color:var(--accent)}
.opt.on{border-color:var(--accent);background:var(--soft)}
.opt input{position:absolute;opacity:0;width:1px;height:1px}
.opt .box{flex:none;width:22px;height:22px;margin-top:3px;border:2px solid var(--accent);border-radius:7px;background:var(--panel);position:relative;transition:background .15s}
.opt.on .box{background:var(--accent)}
.opt.on .box::after{content:"";position:absolute;left:6px;top:2px;width:6px;height:11px;border:solid var(--accent-ink);border-width:0 2.5px 2.5px 0;transform:rotate(45deg)}
.opt b{display:block;font-size:15.5px;line-height:1.7}
.opt small{display:block;font-size:12.5px;color:var(--mute);line-height:1.7;font-weight:400}
table{width:100%;border-collapse:collapse;font-size:14px}
td,th{padding:7px 10px;border-bottom:1px solid var(--line);text-align:right;vertical-align:top}
th{color:var(--mute);font-weight:600;font-size:13px}
tr:nth-child(even) td{background:var(--zebra)}
.bar{height:12px;background:var(--pend);border-radius:999px;overflow:hidden;margin:10px 0}
.bar i{display:block;height:100%;background:linear-gradient(270deg,var(--accent-d),var(--accent));width:0;transition:width .35s;border-radius:999px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(125px,1fr));gap:10px;margin:10px 0}
.grid div{border:1px solid var(--line);border-radius:12px;padding:8px 12px;background:var(--soft)}
.grid b{display:block;font-size:19px;line-height:1.6}.grid span{font-size:12.5px;color:var(--mute);line-height:1.6;display:block}
canvas{width:100%;height:32px;display:block;border:1px solid var(--line);border-radius:8px}
.legend{margin-top:8px}.legend span{display:inline-block;margin-left:14px;font-size:12.5px;color:var(--mute)}
.legend i{display:inline-block;width:11px;height:11px;margin-left:5px;border-radius:3px;vertical-align:-1px}
details summary{cursor:pointer;color:var(--accent);font-weight:600;list-style-position:inside}
a{color:var(--accent);font-weight:600}
.log{max-height:160px;overflow:auto;font-size:13px;background:var(--bg);border:1px solid var(--line);border-radius:10px;padding:8px 12px;line-height:1.8}
.ltr,.fn{direction:ltr;unicode-bidi:isolate;display:inline-block}.fn{text-align:left;overflow-wrap:anywhere}
main>div,section{min-width:0}#res,#files,#cov{overflow-x:auto}td .fn,td .ltr,td a.fn{white-space:nowrap;overflow-wrap:normal}#files td a.fn{white-space:normal;overflow-wrap:anywhere}
.f3{grid-template-columns:repeat(3,minmax(0,1fr))}#chips{max-height:240px;overflow:auto;padding:2px}.chip.pend{border-style:dashed}
.btn-link{display:inline-block}.rep-files a{display:block}.rep-note{font-size:12.5px;color:var(--warn);line-height:1.8}
.hint{font-size:12.5px;color:var(--mute);margin-top:6px;line-height:1.8}
@media (prefers-reduced-motion:reduce){*{transition:none!important}}
</style>
</head>
<body>
<header>
<div class="hd">
<svg class="logo" viewBox="0 0 64 64" aria-hidden="true"><rect width="64" height="64" rx="16" fill="#fff" fill-opacity=".16"/><rect x="1" y="1" width="62" height="62" rx="15" fill="none" stroke="#fff" stroke-opacity=".35" stroke-width="2"/><path d="M22 47V17h22M22 31h16" fill="none" stroke="#fff" stroke-width="5.5" stroke-linecap="round" stroke-linejoin="round"/></svg>
<div>
<h1 class="brand">TSE_LOB</h1>
<div class="app">دریافت تاریخچه دفتر سفارشات و اطلاعات روزانه تابلو — TSETMC</div>
</div>
</div>
</header>
<main>
<div>
<section>
<h2><span class="n">۱</span>نمادها</h2>
<label for="q">جست‌وجوی نماد</label>
<input type="text" id="q" placeholder="مثلاً فولاد" autocomplete="off">
<div id="sugg" hidden></div>
<div id="qmsg" class="err" style="font-size:13px"></div>
<details style="margin-top:10px"><summary>افزودن چند نماد</summary>
<label for="bulk">نمادها</label>
<textarea id="bulk" placeholder="فولاد، شپنا، خودرو"></textarea>
<div class="row" style="margin-top:8px"><button id="bBulk">افزودن</button><button id="bFile">بارگذاری از فایل</button><button id="bClear">حذف همه</button></div>
<input type="file" id="fileIn" accept=".txt,.csv" hidden>
<div id="bulkMsg" style="font-size:13.5px;margin-top:6px"></div>
</details>
<div id="selCount" class="mute" style="margin-top:12px;font-size:13.5px"></div>
<div id="chips"></div>
<div id="selbox" class="sel mute" hidden></div>
</section>
<section>
<h2><span class="n">۲</span>بازه زمانی</h2>
<div class="row">
<div><label for="d1">از تاریخ (شمسی)</label><input type="text" id="d1" dir="ltr" placeholder="1403/05/01"><div class="mute" id="g1" style="font-size:12.5px"></div></div>
<div><label for="d2">تا تاریخ (شمسی)</label><input type="text" id="d2" dir="ltr" placeholder="1403/05/31"><div class="mute" id="g2" style="font-size:12.5px"></div></div>
</div>
<div class="chips" style="margin-top:10px">
<button data-p="5y">پنج سال اخیر</button><button data-p="1y">یک سال</button><button data-p="3m">سه ماه</button><button data-p="1m">یک ماه</button><button data-p="1w">یک هفته</button>
</div>
<details style="margin-top:10px"><summary>تنظیمات پیشرفته</summary>
<div class="row">
<div><label for="ws">شروع بازه ساعتی</label><input type="text" id="ws" dir="ltr" value="08:45:00"></div>
<div><label for="we">پایان بازه ساعتی</label><input type="text" id="we" dir="ltr" value="12:30:59"></div>
</div>
<label><input type="checkbox" id="pPurge" checked>حذف داده‌ی خام پس از ساخت فایل</label>
<label><input type="checkbox" id="force">دریافت مجدد</label>
</details>
</section>
<section>
<h2><span class="n">۳</span>خروجی</h2>
<label>داده</label>
<div class="opts" role="group" aria-label="داده">
<label class="opt on" id="lOb"><input type="checkbox" id="oOb" checked><span class="box"></span><span><b>تاریخچه دفتر سفارشات</b></span></label>
<label class="opt on" id="lDaily"><input type="checkbox" id="oDaily" checked><span class="box"></span><span><b>اطلاعات روزانه تابلو</b></span></label>
</div>
<label>فرمت</label>
<div class="opts" role="group" aria-label="فرمت">
<label class="opt" id="lParquet"><input type="checkbox" id="fParquet"><span class="box"></span><span><b>Parquet</b></span></label>
<label class="opt on" id="lCsv"><input type="checkbox" id="fCsv" checked><span class="box"></span><span><b>CSV</b></span></label>
</div>
<button id="bBundle" class="p big" style="width:100%">دریافت داده‌ها</button>
<div class="hint err" id="optMsg"></div>
<div class="btns">
<button id="bCheck">بررسی دسترسی</button>
<button id="bEst">برآورد زمان و حجم</button>
<button id="bResume" hidden>ادامه از آخرین نقطه</button>
<button id="bCancel" disabled>لغو</button>
</div>
</section>
<section>
<details><summary>تنظیمات شبکه</summary>
<label for="sBase">آدرس پایه API</label><input type="text" id="sBase" dir="ltr">
<label for="sProxy">پراکسی</label><input type="text" id="sProxy" dir="ltr" placeholder="http://127.0.0.1:8080">
<div class="row"><div><label for="sDelay">تأخیر بین درخواست‌ها (ثانیه)</label><input type="number" id="sDelay" step="0.1" min="0.1" max="30"></div>
<div><label for="sWorkers">درخواست هم‌زمان</label><input type="number" id="sWorkers" step="1" min="1" max="16"></div></div>
<div class="hint" id="resourceInfo">حد هم‌زمانی بر اساس CPU و حافظه فیزیکی این سیستم محاسبه می‌شود.</div>
<div class="row" style="margin-top:10px;align-items:center"><button id="bSet" style="flex:0 0 auto">ذخیره</button><span id="setMsg" class="mute"></span></div>
</details>
</section>
</div>
<div>
<section id="jobSec">
<h2><span class="n">۴</span>وضعیت عملیات</h2>
<div id="jobTxt" class="mute">عملیاتی در حال اجرا نیست.</div>
<div class="bar" hidden id="barW"><i id="bar"></i></div>
<div class="grid" id="jobGrid"></div>
<div id="jobPlan" class="warnc" style="font-size:13.5px"></div>
<div id="jobErr" class="log" hidden></div>
</section>
<section>
<h2><span class="n">۵</span>پوشش روزهای معاملاتی <span id="covSym" class="mute" style="font-size:13.5px;font-weight:500"></span></h2>
<canvas id="strip" height="32"></canvas>
<div class="legend"><span><i style="background:var(--ok)"></i>دریافت‌شده</span><span><i style="background:var(--empty)"></i>پاسخ خالی</span><span><i style="background:var(--bad)"></i>ناموفق</span><span><i style="background:var(--pend)"></i>دریافت نشده</span><span id="stripTip"></span></div>
<div id="cov" style="margin-top:10px"></div>
</section>
<section id="infoSec" hidden><h2>گزارش</h2><div id="info"></div></section>
<section id="repSec" hidden>
<h2><span class="n">۶</span>نتیجه</h2>
<div class="row" style="align-items:center;margin-bottom:8px"><button id="bZip" class="p" style="flex:0 0 auto">دانلود دسته‌جمعی (ZIP)</button><span id="repCount" class="mute"></span></div>
<div id="rep"></div>
<div class="row" style="margin-top:8px;align-items:center"><button id="bPrev" style="flex:0 0 auto">قبلی</button><button id="bNext" style="flex:0 0 auto">بعدی</button></div>
</section>
<section id="filesSec">
<h2><span class="n">۷</span>فایل‌های آماده دانلود</h2>
<div class="row" style="align-items:center;margin-bottom:8px">
<button id="bFilesRefresh" style="flex:0 0 auto">به‌روزرسانی فهرست</button>
<button id="bFilesZip" class="p" style="flex:0 0 auto">دانلود همه (ZIP)</button>
<span id="filesCount" class="mute"></span>
</div>
<div id="files"><div class="mute">در حال خواندن فایل‌های ذخیره‌شده…</div></div>
<div class="row" style="margin-top:8px"><button id="bFilesMore" hidden>نمایش فایل‌های بیشتر</button></div>
</section>
</div>
</main>
<script>
const $=(s,r=document)=>r.querySelector(s);
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const nf=n=>typeof n==='number'?n.toLocaleString('fa-IR'):esc(n);
const nz=v=>v==null?'—':nf(v);
const fnh=n=>{const m=/^(.*?)(_\d{8}_\d{8}_(?:OrderBook|DailyBoard)\.\w+)$/.exec(n);return m?'<bdi>'+esc(m[1])+'</bdi>'+esc(m[2]):esc(n);};
let cfg=null,syms=[],active=0,poll=null,tick=0,covT=null,srchT=null,repOff=0,follow=true,running=false;
const cur=()=>syms[active]||null;
async function api(path,body){
  const o={headers:{'X-App':'tse'}};
  if(body!==undefined){o.method='POST';o.headers['Content-Type']='application/json';o.body=JSON.stringify(body);}
  let r,j;try{r=await fetch(path,o);j=await r.json();}catch(e){return {error:'ارتباط با سرور محلی برقرار نشد.'};}
  if(!r.ok&&!j.error)j.error='خطا ('+r.status+')';return j;
}
const KIND={daily:'دریافت اطلاعات روزانه',orderbook:'دریافت تاریخچه دفتر سفارشات',estimate:'برآورد حجم و زمان',bundle:'دریافت داده‌ها'};
const STATE={running:'در حال اجرا',done:'پایان یافت',cancelled:'لغو شد',stopped:'متوقف شد',failed:'ناموفق'};
const PHASE={resolve:'شناسایی نماد',daily:'دریافت',orderbook:'دریافت',export:'ساخت فایل',done:'پایان',write:'نوشتن'};
const RSTAT={done:['ساخته شد','okc'],skipped:['از قبل موجود','mute'],error:['ناموفق','err'],not_found:['پیدا نشد','err']};
const OPTS=[['oOb','lOb'],['oDaily','lDaily'],['fParquet','lParquet'],['fCsv','lCsv']];
function flags(){const f=[];if($('#fParquet').checked)f.push('parquet');if($('#fCsv').checked)f.push('csv');
  return {include_orderbook:$('#oOb').checked,include_daily:$('#oDaily').checked,formats:f,purge_cache:$('#pPurge').checked};}
function syncOpts(){
  OPTS.forEach(([i,l])=>$('#'+l).classList.toggle('on',$('#'+i).checked));
  const f=flags(),bad=(!f.include_orderbook&&!f.include_daily)||!f.formats.length;
  $('#bBundle').disabled=bad||running;$('#optMsg').textContent=bad?'حداقل یک داده و یک فرمت را انتخاب کنید.':'';
}
OPTS.forEach(([i])=>$('#'+i).onchange=syncOpts);
function spec(kind,c){return Object.assign({kind,ins_code:c&&c.ins_code,symbol:c&&c.symbol,
  symbols:syms.map(s=>s.pending?{name:s.symbol}:{ins_code:s.ins_code,symbol:s.symbol}),start:$('#d1').value,end:$('#d2').value,
  window_start:$('#ws').value,window_end:$('#we').value,force:$('#force').checked},flags());}
async function init(){
  cfg=await api('/api/config');
  if(cfg.error){$('#qmsg').textContent=cfg.error;return;}
  $('#d2').value=cfg.presets.end;$('#d1').value=cfg.presets['1m'];
  $('#ws').value=cfg.window[0];$('#we').value=cfg.window[1];
  $('#sBase').value=cfg.settings.api_base;$('#sProxy').value=cfg.settings.proxy;$('#sDelay').value=cfg.settings.delay;$('#sWorkers').value=cfg.settings.workers;
  $('#sWorkers').max=cfg.resources.max_workers;
  $('#resourceInfo').textContent='پردازنده منطقی: '+cfg.resources.logical_cpus+' · حافظه فیزیکی: '+(cfg.resources.memory_gb==null?'نامشخص':cfg.resources.memory_gb+' GB')+' · پیشنهاد: '+cfg.resources.recommended_workers+' درخواست · سقف: '+cfg.resources.max_workers;
  conv();status();syncOpts();renderSyms();loadStoredFiles(true);
}
function conv(){for(const k of ['1','2']){const v=$('#d'+k).value;const t=$('#g'+k);
  if(!v){t.textContent='';continue;}
  api('/api/convert?d='+encodeURIComponent(v)).then(j=>{t.textContent=j.error?'خطا: '+j.error:'میلادی: '+j.gregorian+' ('+j.weekday+')';t.className=j.error?'err':'mute';});}
  clearTimeout(covT);covT=setTimeout(cover,500);}
$('#d1').oninput=$('#d2').oninput=conv;
document.querySelectorAll('[data-p]').forEach(b=>b.onclick=()=>{$('#d2').value=cfg.presets.end;$('#d1').value=cfg.presets[b.dataset.p];conv();});
function renderSyms(){
  $('#chips').innerHTML=syms.map((s,i)=>'<span class="chip'+(i===active?' on':'')+(s.pending?' pend':'')+'" data-i="'+i+'" title="'+esc(s.company||'')+'"><b>'+esc(s.symbol)+'</b><button data-rm="'+i+'" aria-label="حذف '+esc(s.symbol)+'">×</button></span>').join('');
  $('#selCount').textContent=syms.length?nf(syms.length)+' نماد':'';
  const c=cur(),b=$('#selbox');b.hidden=!c||c.pending;
  if(c&&!c.pending)b.innerHTML='<b>'+esc(c.symbol)+'</b> · '+esc(c.company||'—')+' · صنعت: '+esc(c.industry||'—')+' · بازار: '+esc(c.market||'—')+'<div class="mute" style="font-size:12.5px">InsCode: '+esc(c.ins_code)+'</div>'+(c.note?'<div class="warnc" style="font-size:12.5px">'+esc(c.note)+'</div>':'');
  $('#covSym').textContent=c?'— '+c.symbol:'';
}
$('#chips').onclick=async e=>{const rm=e.target.closest('[data-rm]');
  if(rm){const i=+rm.dataset.rm;syms.splice(i,1);if(active>=syms.length)active=Math.max(0,syms.length-1);renderSyms();cover();return;}
  const ch=e.target.closest('.chip');if(ch){active=+ch.dataset.i;renderSyms();cover();}};
function addSym(j){if(syms.some(s=>j.ins_code?s.ins_code===j.ins_code:s.symbol===j.symbol))return false;syms.push(j);active=syms.length-1;return true;}
async function activeResolved(){const c=cur();if(!c)return null;if(!c.pending)return c;
  const j=await api('/api/resolve',{names:c.symbol});
  if(j.error||!j.resolved||!j.resolved.length){alert('نماد «'+c.symbol+'» پیدا نشد.');return null;}
  const r=j.resolved[0];const dup=syms.findIndex((s,i)=>i!==active&&s.ins_code===r.ins_code);
  syms[active]=r;if(dup>=0){syms.splice(dup,1);active=syms.indexOf(r);}renderSyms();return r;}
$('#q').oninput=()=>{clearTimeout(srchT);const v=$('#q').value.trim();if(!v){$('#sugg').hidden=true;return;}
  srchT=setTimeout(async()=>{$('#qmsg').textContent='';const j=await api('/api/search?q='+encodeURIComponent(v));
    const s=$('#sugg');if(j.error){s.hidden=true;$('#qmsg').textContent=j.error;return;}
    if(!j.items.length){s.hidden=false;s.innerHTML='<div class="mute">نمادی پیدا نشد.</div>';return;}
    s.hidden=false;s.innerHTML=j.items.map((it,i)=>'<div tabindex="0" data-i="'+i+'"><b>'+esc(it.symbol)+'</b> — '+esc(it.name)+' <small>'+esc(it.market)+' · '+esc(it.ins_code)+'</small></div>').join('');
    s.querySelectorAll('div[data-i]').forEach(d=>{const go=()=>pick(j.items[+d.dataset.i]);d.onclick=go;d.onkeydown=e=>{if(e.key==='Enter')go();};});},350);};
async function pick(it){
  $('#sugg').hidden=true;$('#qmsg').textContent='در حال دریافت هویت نماد…';
  const j=await api('/api/select',it);$('#qmsg').textContent='';
  if(j.error){$('#qmsg').textContent=j.error;return;}
  if(!addSym(j))active=syms.findIndex(s=>s.ins_code===j.ins_code);
  renderSyms();$('#q').value='';cover();
}
$('#bBulk').onclick=()=>{const m=$('#bulkMsg');const names=$('#bulk').value.split(/[,;،؛\n\r\t]+/).map(x=>x.trim()).filter(Boolean);
  if(!names.length){m.className='err';m.textContent='نامی وارد نشده است.';return;}
  let added=0;names.forEach(n=>{if(addSym({symbol:n,pending:true}))added++;});
  renderSyms();m.className='okc';m.textContent=nf(added)+' نماد افزوده شد.';$('#bulk').value='';};
$('#bFile').onclick=()=>$('#fileIn').click();
$('#fileIn').onchange=e=>{const f=e.target.files[0];if(!f)return;const r=new FileReader();r.onload=()=>{$('#bulk').value=String(r.result||'');$('#bBulk').click();};r.readAsText(f,'utf-8');e.target.value='';};
$('#bClear').onclick=()=>{syms=[];active=0;renderSyms();cover();$('#bulkMsg').textContent='';};
async function cover(){
  const c=cur();if(!c||c.pending){draw([]);$('#cov').innerHTML='';return;}
  const j=await api('/api/coverage?ins_code='+c.ins_code+'&start='+encodeURIComponent($('#d1').value)+'&end='+encodeURIComponent($('#d2').value));
  if(j.error){$('#cov').innerHTML='<span class="err">'+esc(j.error)+'</span>';draw([]);return;}
  draw(j.coverage.strip);tbl('#cov',j.rows);
}
function tbl(sel_,rows){$(sel_).innerHTML='<table>'+rows.map(r=>'<tr><th>'+esc(r[0])+'</th><td>'+nf(r[1])+'</td></tr>').join('')+'</table>';}
let stripData=[];
function draw(st){stripData=st;const c=$('#strip');const w=c.clientWidth||600;c.width=w*2;c.height=64;const x=c.getContext('2d');
  const cs=getComputedStyle(document.body);const col={ok:'--ok',empty:'--empty',error:'--bad',pending:'--pend'};
  x.clearRect(0,0,c.width,c.height);if(!st.length)return;const bw=c.width/st.length;
  st.forEach((d,i)=>{x.fillStyle=cs.getPropertyValue(col[d[1]]||'--pend');x.fillRect(Math.floor(i*bw),0,Math.max(1,Math.ceil(bw)-(bw>3?1:0)),64);});}
$('#strip').onmousemove=e=>{if(!stripData.length)return;const r=e.target.getBoundingClientRect();const i=Math.min(stripData.length-1,Math.max(0,Math.floor(((r.right-e.clientX)/r.width)*stripData.length)));
  const d=stripData[i];$('#stripTip').innerHTML=d?('<span class="ltr">'+String(d[0]).replace(/(\d{4})(\d\d)(\d\d)/,'$1-$2-$3')+'</span> : '+({ok:'دریافت‌شده',empty:'پاسخ خالی',error:'ناموفق',pending:'دریافت‌نشده'}[d[1]]||d[1])):'';};
function need(){if(!syms.length){alert('ابتدا حداقل یک نماد را اضافه کنید.');return false;}return true;}
async function run(kind,c){$('#info').innerHTML='';$('#infoSec').hidden=true;repOff=0;follow=true;
  const j=await api('/api/job/start',spec(kind,c));if(j.error){alert(j.error);return;}startPoll();}
$('#bBundle').onclick=()=>{if(need())run('bundle',null);};
$('#bEst').onclick=async()=>{if(!need())return;const c=await activeResolved();if(c)run('estimate',c);};
$('#bResume').onclick=()=>{$('#force').checked=false;run('bundle',null);};
$('#bCancel').onclick=async()=>{await api('/api/job/cancel',{});};
$('#bCheck').onclick=async()=>{const b=$('#bCheck');b.disabled=true;b.textContent='در حال بررسی…';
  const c=cur()&&!cur().pending?cur():await activeResolved();
  const j=await api('/api/check',{ins_code:c&&c.ins_code,start:$('#d1').value,end:$('#d2').value});b.disabled=false;b.textContent='بررسی دسترسی';
  $('#infoSec').hidden=false;if(j.error){$('#info').innerHTML='<div class="err">'+esc(j.error)+'</div>';return;}
  const cls={ok:'okc',empty:'warnc'};
  $('#info').innerHTML='<div>'+esc(j.verdict)+'</div><div class="mute" style="font-size:12.5px"><span class="ltr">'+esc(j.base)+'</span> · '+esc(j.checked_at)+'</div><table><tr><th>بررسی</th><th>وضعیت</th><th>زمان</th><th>توضیح</th></tr>'+
  j.probes.map(p=>'<tr><td>'+esc(p.name)+'</td><td class="'+(cls[p.status]||'err')+'">'+esc(p.status)+(p.http_code?' ('+p.http_code+')':'')+(p.records!=null?' · '+p.records+' رکورد':'')+'</td><td>'+p.ms+'ms</td><td>'+esc(p.detail)+'</td></tr>').join('')+'</table>';};
$('#bSet').onclick=async()=>{const j=await api('/api/settings',{api_base:$('#sBase').value,proxy:$('#sProxy').value,delay:$('#sDelay').value,workers:$('#sWorkers').value});
  $('#setMsg').textContent=j.error?j.error:'ذخیره شد.';$('#setMsg').className=j.error?'err':'okc';};
function startPoll(){clearInterval(poll);tick=0;poll=setInterval(status,1000);status();}
async function status(){const r=await api('/api/job/status');const j=r.job;running=!!(j&&j.state==='running');
  $('#bCancel').disabled=!running;['bCheck','bEst'].forEach(i=>$('#'+i).disabled=running);syncOpts();
  if(!j)return;render(j);
  if(running){if(++tick%3===1)loadReports();}
  else{clearInterval(poll);poll=null;loadReports();cover();loadStoredFiles(true);
    $('#bResume').hidden=!(j.kind==='bundle'&&['stopped','cancelled','failed'].includes(j.state));showResult(j);}}
async function loadReports(){
  const st=await api('/api/job/status');const n=st.job?st.job.n_reports:0;
  if(!n){$('#repSec').hidden=true;return;}
  if(follow&&running)repOff=Math.max(0,n-50);
  const j=await api('/api/job/results?offset='+repOff+'&limit=50');if(j.error)return;
  $('#repSec').hidden=false;$('#repCount').textContent=nf(j.offset+1)+' تا '+nf(j.offset+j.items.length)+' از '+nf(j.total);
  $('#bPrev').disabled=j.offset<=0;$('#bNext').disabled=j.offset+j.items.length>=j.total;
  $('#rep').innerHTML='<table><tr><th>نماد</th><th>وضعیت</th><th>ردیف دفتر سفارش</th><th>فایل‌ها</th></tr>'+j.items.map(r=>{const s=RSTAT[r.status]||[r.status,''];
    return '<tr><td><b>'+esc(r.symbol)+'</b></td><td class="'+s[1]+'">'+s[0]+'</td><td>'+nz(r.output_rows)+'</td><td class="rep-files">'+(r.files||[]).map(f=>'<a class="fn" href="/api/download/'+encodeURIComponent(f)+'">'+fnh(f)+'</a>').join('')+((r.notes||[]).length?'<div class="rep-note">'+r.notes.map(esc).join('<br>')+'</div>':'')+'</td></tr>';}).join('')+'</table>';}
$('#bPrev').onclick=()=>{follow=false;repOff=Math.max(0,repOff-50);loadReports();};
$('#bNext').onclick=()=>{repOff+=50;loadReports();};
$('#bZip').onclick=()=>{window.location.href='/api/job/zip';};
let storedFiles=[],filesTotal=0,filesOffset=0;
function renderStoredFiles(){
  if(!storedFiles.length){$('#files').innerHTML='<div class="mute">فایلی آماده دانلود نیست.</div>';return;}
  $('#files').innerHTML='<table><tr><th>نام فایل</th><th>فرمت</th><th>حجم</th><th>زمان ذخیره</th><th>دریافت</th></tr>'+storedFiles.map(f=>'<tr><td class="fn">'+esc(f.name)+'</td><td>'+esc(f.format)+'</td><td>'+nf(f.size/1048576).replace(/٫?0+$/,'')+' MB</td><td><span class="ltr">'+esc(f.modified)+'</span></td><td><a href="/api/download/'+encodeURIComponent(f.name)+'">دانلود</a></td></tr>').join('')+'</table>';
}
async function loadStoredFiles(reset){
  if(reset){storedFiles=[];filesOffset=0;}
  const j=await api('/api/files?offset='+filesOffset+'&limit=50');
  if(j.error){$('#files').innerHTML='<div class="err">'+esc(j.error)+'</div>';return;}
  filesTotal=j.total;storedFiles=storedFiles.concat(j.items||[]);filesOffset+=((j.items||[]).length);
  $('#filesCount').textContent=nf(filesTotal)+' فایل ذخیره‌شده';
  $('#bFilesMore').hidden=filesOffset>=filesTotal;$('#bFilesZip').disabled=filesTotal===0;
  renderStoredFiles();
}
$('#bFilesRefresh').onclick=()=>loadStoredFiles(true);
$('#bFilesMore').onclick=()=>loadStoredFiles(false);
$('#bFilesZip').onclick=()=>{window.location.href='/api/files/zip';};
const G=[['trading_days','روز معاملاتی'],['processed','پردازش‌شده'],['ok','موفق'],['empty','پاسخ خالی'],['failed','ناموفق'],['cached','از کش']];
function render(j){const p=j.progress||{};
  $('#jobTxt').innerHTML='<b>'+esc(KIND[j.kind]||j.kind)+'</b> · '+esc(p.symbol||p.current_symbol||'')+' — <span class="'+(j.state==='done'?'okc':j.state==='running'?'':'err')+'">'+esc(STATE[j.state]||j.state)+'</span>'+(p.current_day&&p.phase==='orderbook'?' · '+esc(p.current_day):'')+'<div class="mute">'+esc(j.message||'')+' ('+nf(j.elapsed)+' ثانیه)</div>';
  const pc=p.percent;$('#barW').hidden=pc==null;$('#bar').style.width=(pc||0)+'%';
  let g='';
  if(j.kind==='bundle'){g='<div><b>'+nf(p.symbols_done||0)+' / '+nf(p.symbols_total||0)+'</b><span>نماد</span></div><div><b>'+esc(PHASE[p.phase]||'—')+'</b><span>مرحله</span></div><div><b>'+nf(p.files_built||0)+'</b><span>فایل ساخته‌شده</span></div><div><b>'+nf(p.symbols_failed||0)+'</b><span>نماد ناموفق</span></div>';
    if(p.include_orderbook!==false&&(p.phase==='orderbook'||p.phase==='daily')){g+=G.map(x=>'<div><b>'+nf(p[x[0]]||0)+'</b><span>'+x[1]+'</span></div>').join('');}
    g+='<div><b>'+nf(Math.round(pc||0))+'٪</b><span>پیشرفت کل</span></div>';}
  else if(j.kind==='orderbook'){g=G.map(x=>'<div><b>'+nf(p[x[0]]||0)+'</b><span>'+x[1]+'</span></div>').join('')+'<div><b>'+nf(Math.round(pc||0))+'٪</b><span>پیشرفت</span></div>';}
  $('#jobGrid').innerHTML=g;$('#jobPlan').textContent=j.plan?j.plan.text:'';
  const e=$('#jobErr');if(j.errors&&j.errors.length){e.hidden=false;e.innerHTML=j.errors.slice().reverse().map(esc).join('<br>');}else e.hidden=true;}
function showResult(j){const r=j.result||{};let h='';
  if(r.estimate){const e=r.estimate;h+='<div>'+esc(e.message)+'</div>';
    if(e.est_output_rows!=null)h+='<table>'+[['تعداد نماد',e.symbols],['میانگین رویداد روزانه (نماد نمونه)',e.avg_events_per_day],['میانگین اسنپ‌شات روزانه (نماد نمونه)',e.avg_snapshots_per_day],['ردیف خروجی (برآورد کل)',e.est_output_rows],['زمان دریافت (دقیقه)',e.est_fetch_minutes],['حجم کش خام (MB)',e.est_cache_mb],['حجم فایل‌ها (MB)',e.est_files_mb],['زمان نوشتن فایل‌ها (دقیقه)',e.est_write_minutes]].map(x=>'<tr><th>'+x[0]+'</th><td>'+nf(x[1])+'</td></tr>').join('')+'</table>';
    if(e.sample_days&&e.sample_days.length)h+='<table><tr><th>روز نمونه</th><th>نتیجه</th><th>رویداد</th><th></th></tr>'+e.sample_days.map(s=>'<tr><td>'+esc(s.day)+'</td><td>'+esc(s.outcome)+'</td><td>'+nf(s.events)+'</td><td>'+esc(s.detail||'')+'</td></tr>').join('')+'</table>';}
  if(j.state==='failed'||j.state==='stopped'){h+='<div class="err">'+esc(j.message)+'</div>';}
  if(h){$('#infoSec').hidden=false;$('#info').innerHTML=h;}}
$('#sugg').hidden=true;init();window.addEventListener('resize',()=>draw(stripData));
</script>
</body>
</html>
"""
INDEX_HTML = INDEX_HTML_TEMPLATE.replace("__FONT_B64__", "".join(_FONT_WOFF2_B64.split()))


# ═══════════════════════════ آزمون‌های داخلی (--self-test) ═══════════════════════════
# همه آزمون‌ها با «داده آزمایشی داخلی» و یک سرور HTTP محلیِ ساختگی اجرا می‌شوند؛ هیچ ارتباطی با TSETMC واقعی ندارند.
import http.server


class FakeTsetmc:
    """سرور محلی ساختگی که فقط برای آزمون است. داده‌های آن هیچ‌گاه وارد خروجی واقعی نمی‌شود."""

    def __init__(self):
        self.hits = collections.Counter()
        self.handler = lambda path: (404, "application/json", b"{}")
        self.after = None
        outer = self

        class H(http.server.BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_GET(self):
                path = urllib.parse.unquote(self.path)
                outer.hits[path] += 1
                code, ctype, body = outer.handler(path)
                self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                if outer.after:
                    outer.after(path)

        self.srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.base = "http://127.0.0.1:%d/api" % self.srv.server_address[1]
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def stop(self):
        self.srv.shutdown()
        self.srv.server_close()

    def count(self, needle):
        return sum(v for k, v in self.hits.items() if needle in k)


def _j(obj, code=200):
    return code, "application/json; charset=utf-8", json.dumps(obj, ensure_ascii=False).encode("utf-8")


def mk_rec(t, level, bid=None, ask=None, ref=None, with_ref=True):
    r = {"hEven": t, "number": level}
    if with_ref:
        r["refID"] = ref
    b = bid or (None, None, None)
    a = ask or (None, None, None)
    r.update({"pMeDem": b[0], "qTitMeDem": b[1], "zOrdMeDem": b[2], "pMeOf": a[0], "qTitMeOf": a[1], "zOrdMeOf": a[2]})
    return r


def synth_day(n=10, with_ref=False):
    """رویدادهای آزمایشی: n رویداد در ثانیه‌های متوالی، سطح‌ها به‌صورت چرخشی."""
    out = []
    for i in range(n):
        lv = i % 5 + 1
        out.append(mk_rec(90000 + i, lv, bid=(4000 - lv, 100 + i, 1 + lv), ask=(4100 + lv, 200 + i, 2 + lv),
                          ref=1000 + i if with_ref else None, with_ref=with_ref))
    return out


def _ev(records):
    sname, fm = detect_ob_schema(records)
    assert sname, fm
    return normalize_ob_events(records, fm)[0]


def snaps(records, ws=0, we=235959, st=None):
    return list(reconstruct_snapshots(_ev(records), ws, we, st))


TESTS = []


def test(title, internal=True):
    def deco(fn):
        TESTS.append((len(TESTS) + 1, title, fn))
        return fn
    return deco


def _mk(fake=None, **kw):
    tmp = tempfile.mkdtemp(prefix="tse_selftest_")
    ctx = Context(tmp, api_base=fake.base if fake else "http://127.0.0.1:9/api", delay=0.0, backoff_base=0.01,
                  max_attempts=kw.get("max_attempts", 3), workers=kw.get("workers", 1))
    return ctx, tmp


SPEC_RANGE = {"start": "1403/05/01", "end": "1403/05/31"}
DAYS = [date_to_deven(jalali_to_gdate("1403/05/%02d" % k)) for k in (1, 2, 3, 4, 5, 6)]


def daily_payload(days):
    return {"closingPriceDaily": [{"dEven": d, "hEven": 123000, "priceFirst": 4000 + i, "pDrCotVal": 4010 + i,
                                   "pClosing": 4020 + i, "priceYesterday": 4000, "priceMin": 3990, "priceMax": 4050,
                                   "zTotTran": 100 + i, "qTotTran5J": 1000000 + i, "qTotCap": 4000000000 + i,
                                   "priceChange": 20 + i, "iClose": False} for i, d in enumerate(days)]}


def standard_handler(fake, ob=None, days=DAYS, daily=True):
    ob = ob or {}

    def h(path):
        if "/Instrument/GetInstrumentSearch/" in path:
            return _j({"instrumentSearch": [{"insCode": "111", "lVal18AFC": "فولاد", "lVal30": "فولاد مباركه اصفهان",
                                             "flowTitle": "بازار بورس", "cIsin": None},
                                            {"insCode": "222", "lVal18AFC": "فولاد", "lVal30": "فولاد دیگر", "flowTitle": "فرابورس"}]})
        if "/Instrument/GetInstrumentIdentity/" in path:
            return _j({"instrumentIdentity": {"lVal18AFC": "فولاد", "lSoc30": "فولاد مباركه اصفهان",
                                              "sector": {"cSecVal": "27 ", "lSecVal": "فلزات اساسي "}}})
        if "/ClosingPrice/GetClosingPriceDailyList/" in path:
            return _j(daily_payload(days)) if daily else _j({"closingPriceDaily": []})
        if "/BestLimits/" in path:
            d = int(path.rsplit("/", 1)[1])
            f = ob.get(d)
            return f(path) if callable(f) else _j({"bestLimitsHistory": synth_day(10)})
        return 404, "application/json", b"{}"
    fake.handler = h


def run_job(ctx, kind, spec, runner):
    ctx.jobs.start(kind, spec, runner)
    return ctx.jobs.wait(60)


def base_spec(**kw):
    s = dict(SPEC_RANGE, ins_code="111", symbol="فولاد", window_start="08:45:00", window_end="12:30:59")
    s.update(kw)
    return s


def seed_instrument(ctx):
    ctx.store.upsert_instrument("111", symbol="فولاد", company="فولاد مبارکه اصفهان", industry="فلزات اساسی",
                                market="بازار بورس", identity_ok=1)


@test("جست‌وجوی نماد و نگاشت شناسه ابزار (InsCode)؛ جایگزینی حروف عربی/فارسی")
def t_search(_):
    fake = FakeTsetmc()
    ctx, tmp = _mk(fake)
    try:
        def h(path):
            q = path.rsplit("/", 1)[1]
            if "ی" in q:
                return _j({"instrumentSearch": []})
            if "ي" in q or "فولاد" in q:
                return _j({"instrumentSearch": [
                    {"insCode": "333", "lVal18AFC": "فولادي", "lVal30": "x", "flowTitle": "بازار بورس"},
                    {"insCode": "111", "lVal18AFC": "فولاد", "lVal30": "فولاد مباركه اصفهان", "flowTitle": "بازار بورس"},
                    {"insCode": "222", "lVal18AFC": "فولاد", "lVal30": "فولاد دیگر", "flowTitle": "فرابورس"}]})
            return _j({"instrumentSearch": []})
        fake.handler = h
        items = search_instruments(ctx, "فولاد")
        assert [i["ins_code"] for i in items[:2]] == ["111", "222"], items       # تطبیق دقیق نماد جلوتر است
        assert items[0]["symbol"] == "فولاد"
        assert items[0]["name"] == "فولاد مبارکه اصفهان", items[0]["name"]       # حروف عربی به فارسی
        items2 = search_instruments(ctx, "فولادی")                                  # نسخه فارسی خالی → نسخه عربی
        assert items2 and fake.count(urllib.parse.unquote("%D9%81%D9%88%D9%84%D8%A7%D8%AF%D9%8A")) >= 1
    finally:
        fake.stop(); shutil.rmtree(tmp, ignore_errors=True)


@test("تبدیل تاریخ شمسی/میلادی، از جمله مرزها و سال کبیسه")
def t_dates(_):
    cases = {"1403/12/30": "2025-03-20", "1399/12/30": "2021-03-20", "1404/01/01": "2025-03-21",
             "1402/12/29": "2024-03-19", "1401/01/01": "2022-03-21", "۱۴۰۳/۰۵/۰۱": "2024-07-22"}
    for j, g in cases.items():
        assert jalali_to_gdate(j).isoformat() == g, (j, jalali_to_gdate(j))
        assert gdate_to_jalali_str(jalali_to_gdate(j)) == "%04d/%02d/%02d" % tuple(
            int(x) for x in norm_digits(j).split("/"))
    for bad in ("1404/12/30", "1403/13/01", "1403-5", "abc"):
        try:
            parse_jalali(bad)
        except AppError:
            continue
        raise AssertionError("باید نامعتبر باشد: " + bad)
    assert validate_range("1403/05/01", "1403/05/31")[0] < validate_range("1403/05/01", "1403/05/31")[1]
    for a, b in (("1403/06/01", "1403/05/01"), ("1403/01/01", "1409/01/01"), ("1390/01/01", "1403/01/01")):
        try:
            validate_range(a, b)
        except AppError:
            continue
        raise AssertionError("بازه نامعتبر پذیرفته شد: %s %s" % (a, b))


@test("تبدیل ساعت‌های عددی فاقد صفر ابتدایی")
def t_time(_):
    assert hms_str(parse_hms_int(84501)) == "08:45:01"
    assert hms_str(parse_hms_int("84501")) == "08:45:01"
    assert hms_str(parse_hms_int(5)) == "00:00:05"
    assert hms_str(parse_hms_int(123059)) == "12:30:59"
    assert hms_str(parse_hms_int("8:45:01")) == "08:45:01"
    assert parse_hms_int(246100) is None and parse_hms_int(126100) is None and parse_hms_int(-1) is None
    assert parse_hms_int(None) is None and parse_hms_int(True) is None and parse_hms_int("x") is None


@test("بازسازی پنج سطح از داده‌های تغییرمحور")
def t_recon(_):
    recs = [mk_rec(90000 + i, i + 1, bid=(1000 - i, 10 + i, 1 + i), ask=(1100 + i, 20 + i, 2 + i), with_ref=False)
            for i in range(5)]
    ss = snaps(recs)
    assert len(ss) == 5
    last = ss[-1].rows
    for i, row in enumerate(last):
        # (سطح، refID، قیمت فروش، حجم فروش، تعداد فروش، تعداد خرید، حجم خرید، قیمت خرید)
        assert row == (i + 1, None, 1100 + i, 20 + i, 2 + i, 1 + i, 10 + i, 1000 - i), row


@test("حفظ مقدار قبلی یک سطح هنگام تغییر سطح دیگر")
def t_carry(_):
    recs = [mk_rec(90000, 1, bid=(1000, 10, 1), ask=(1100, 20, 2), with_ref=False),
            mk_rec(90001, 2, bid=(990, 11, 3), ask=(1110, 21, 4), with_ref=False),
            mk_rec(90002, 1, bid=(1001, 12, 5), ask=None, with_ref=False)]     # فقط سمت خرید سطح ۱ تغییر کرد
    ss = snaps(recs)
    assert ss[2].rows[1] == (2, None, 1110, 21, 4, 3, 11, 990)                   # سطح ۲ دست‌نخورده
    assert ss[2].rows[0] == (1, None, 1100, 20, 2, 5, 12, 1001)                  # سمت فروش سطح ۱ حفظ شد


@test("چند تغییر هم‌زمان؛ ترتیب با refID قطعی و بدون آن مبهم گزارش می‌شود")
def t_same_time(_):
    recs = [mk_rec(90100, 1, bid=(10, 1, 1), ask=(20, 1, 1), with_ref=False),
            mk_rec(90100, 2, bid=(9, 1, 1), ask=(21, 1, 1), with_ref=False)]
    st = {}
    ss = snaps(recs, st=st)
    assert len(ss) == 1 and ss[0].rows[0][7] == 10 and ss[0].rows[1][7] == 9 and st["ambiguous_groups"] == 0
    amb = [mk_rec(90100, 1, bid=(10, 1, 1), ask=(20, 1, 1), with_ref=False),
           mk_rec(90100, 1, bid=(11, 1, 1), ask=(21, 1, 1), with_ref=False)]
    st = {}
    amb_snaps = snaps(amb, st=st)
    assert st["ambiguous_groups"] == 1, st                                        # ابهام ثبت می‌شود
    assert amb_snaps[0].rows[0][2:] == (None,) * 6, amb_snaps[0].rows[0]            # مقدار مبهم به خروجی مالی راه پیدا نمی‌کند
    later = amb + [mk_rec(90101, 1, bid=(12, 3, 1), ask=None, with_ref=False)]
    later_snaps = snaps(later)
    assert later_snaps[-1].rows[0][7] == 12 and later_snaps[-1].rows[0][2:5] == (None, None, None)
    withref = [mk_rec(90100, 1, bid=(11, 1, 1), ask=(21, 1, 1), ref=2), mk_rec(90100, 1, bid=(10, 1, 1), ask=(20, 1, 1), ref=1)]
    st = {}
    ss = snaps(withref, st=st)
    assert ss[0].rows[0][7] == 11 and ss[0].rows[0][1] == 2 and st["ambiguous_groups"] == 0   # refID بزرگ‌تر آخر اعمال می‌شود


@test("عدم تولید داده مصنوعی برای سطوح ناموجود و پرنکردن از داده آینده")
def t_missing(_):
    recs = [mk_rec(90000, 1, bid=(10, 1, 1), ask=(20, 1, 1), with_ref=False),
            mk_rec(90010, 3, bid=(8, 1, 1), ask=(22, 1, 1), with_ref=False)]
    st = {}
    ss = snaps(recs, st=st)
    assert ss[0].rows[2] == (3, None, None, None, None, None, None, None)         # سطح ۳ هنوز داده ندارد (نه صفر)
    assert ss[0].rows[3][2:] == (None,) * 6 and ss[1].rows[4][2:] == (None,) * 6
    assert ss[1].rows[2][2] == 22
    assert st["incomplete_snapshots"] == 2 and st["missing_level_cells"] == 8 + 6      # ۴ سطح×۲ سمت + ۳ سطح×۲ سمت


@test("حذف اسنپ‌شات‌های خارج از بازه ساعتی، با حفظ وضعیت ابتدای بازه")
def t_window(_):
    recs = [mk_rec(83000, 1, bid=(10, 1, 1), ask=(20, 1, 1), with_ref=False),
            mk_rec(84500, 2, bid=(9, 1, 1), ask=(21, 1, 1), with_ref=False),
            mk_rec(100000, 1, bid=(11, 1, 1), ask=(19, 1, 1), with_ref=False),
            mk_rec(123059, 1, bid=(12, 1, 1), ask=(18, 1, 1), with_ref=False),
            mk_rec(123100, 1, bid=(13, 1, 1), ask=(17, 1, 1), with_ref=False)]
    ss = snaps(recs, parse_clock("08:45:00"), parse_clock("12:30:59"))
    assert [s.t for s in ss] == [84500, 100000, 123059]
    assert ss[0].rows[0][7] == 10                                                  # از رویداد پیش از بازه حفظ شده است


@test("برخورد صحیح با پاسخ خالی API (خالی ≠ خطا)")
def t_empty(_):
    fake = FakeTsetmc()
    ctx, tmp = _mk(fake)
    try:
        standard_handler(fake, ob={DAYS[0]: lambda p: _j({"bestLimitsHistory": []})})
        out, res, info = fetch_one_ob_day(ctx, ctx.make_client(), None, "111", DAYS[0], 84500, 123059)
        assert out == "empty" and ctx.store.get_ob_meta("111", DAYS[0])["status"] == "empty"
        assert classify_list_payload({"bestLimitsHistory": None}, "bestLimitsHistory")[0] == "empty"
        assert classify_list_payload({"other": []}, "bestLimitsHistory")[0] == "schema"      # ساختار نامعتبر ≠ خالی
    finally:
        fake.stop(); shutil.rmtree(tmp, ignore_errors=True)


@test("تشخیص پاسخ HTML، JSON ناقص، مسدودی ۴۰۳، خطای ۵xx با retry محدود و خطای شبکه")
def t_errors(_):
    fake = FakeTsetmc()
    ctx, tmp = _mk(fake)
    try:
        def h(path):
            if path.endswith("/html"):
                return 200, "text/html", b"<html><body>Error page</body></html>"
            if path.endswith("/blockhtml"):
                return 200, "text/html", b"<html><body>Access Denied</body></html>"
            if path.endswith("/trunc"):
                return 200, "application/json", b'{"bestLimitsHistory":[{"hEven":1'
            if path.endswith("/403"):
                return 403, "text/html", b"Forbidden"
            if path.endswith("/500"):
                return 500, "text/plain", b"boom"
            if path.endswith("/ok"):
                return _j({"a": 1})
            return 404, "text/plain", b"nf"
        fake.handler = h
        cl = ctx.make_client()
        assert cl.get("x/html").status == "html"
        assert cl.get("x/blockhtml").status == "blocked"
        assert cl.get("x/trunc").status == "invalid_json"
        assert cl.get("x/403").status == "blocked" and fake.count("/403") == 1              # بدون retry
        r = cl.get("x/500")
        assert r.status == "http" and fake.count("/500") == 3                                # دقیقاً سه تلاش، نه نامحدود
        assert cl.get("x/ok").status == "ok" and cl.get("x/zzz").status == "http"
        dead = ApiClient("http://127.0.0.1:9/api", max_attempts=2, backoff_base=0.01)
        assert dead.get("a").status == "network"
        ev = threading.Event(); ev.set()
        assert cl.get("x/ok", cancel=ev).status == "cancelled"
    finally:
        fake.stop(); shutil.rmtree(tmp, ignore_errors=True)



def _fetch_ob(ctx):
    j = run_job(ctx, "orderbook", base_spec(), run_fetch_orderbook)
    return j


@test("ادامه عملیات پس از توقف و تلاش مجدد روزهای ناموفق بدون دریافت دوباره روزهای موفق")
def t_resume(_):
    fake = FakeTsetmc()
    ctx, tmp = _mk(fake)
    try:
        fail = {DAYS[1]}
        standard_handler(fake, ob={DAYS[1]: lambda p: _j({}, 500) if DAYS[1] in fail else _j({"bestLimitsHistory": synth_day(10)})})
        fake.after = lambda path: ctx.jobs.current.cancel_event.set() if path.endswith("/%d" % DAYS[2]) and "BestLimits" in path else None
        j1 = _fetch_ob(ctx)
        assert j1.state == "cancelled", j1.state
        m = {d: (ctx.store.get_ob_meta("111", d) or {}).get("status") for d in DAYS}
        assert m[DAYS[0]] == "ok" and m[DAYS[1]] == "error" and m[DAYS[2]] == "ok" and m[DAYS[3]] is None, m
        fail.clear(); fake.after = None
        j2 = _fetch_ob(ctx)
        assert j2.state == "done", (j2.state, j2.message)
        cov = j2.result["coverage"]
        assert cov["fetched_ok"] == 6 and cov["errors"] == 0 and cov["not_fetched_yet"] == 0, cov
        h = lambda d: fake.count("/BestLimits/111/%d" % d)           # noqa: E731
        assert h(DAYS[0]) == 1 and h(DAYS[2]) == 1, "روز موفق دوباره دریافت شد"
        assert h(DAYS[1]) == 3 + 1 and all(h(d) == 1 for d in DAYS[3:])
    finally:
        fake.stop(); shutil.rmtree(tmp, ignore_errors=True)


@test("جلوگیری از دریافت مکرر داده‌های دارای کش معتبر؛ دریافت مجدد فقط با تیک صریح")
def t_cache(_):
    fake = FakeTsetmc()
    ctx, tmp = _mk(fake)
    try:
        standard_handler(fake)
        assert _fetch_ob(ctx).state == "done"
        before = fake.count("/BestLimits/"), fake.count("GetClosingPriceDailyList")
        j = _fetch_ob(ctx)
        assert j.progress["cached"] == 6 and (fake.count("/BestLimits/"), fake.count("GetClosingPriceDailyList")) == before
        j = run_job(ctx, "orderbook", base_spec(force=True), run_fetch_orderbook)
        assert fake.count("/BestLimits/") == before[0] + 6
    finally:
        fake.stop(); shutil.rmtree(tmp, ignore_errors=True)


@test("گزارش جداگانه روزهای ناموفق و روزهای دارای پاسخ خالی")
def t_failed_vs_empty(_):
    fake = FakeTsetmc()
    ctx, tmp = _mk(fake)
    try:
        standard_handler(fake, days=DAYS[:4],
                         ob={DAYS[1]: lambda p: _j({"bestLimitsHistory": []}), DAYS[2]: lambda p: _j({}, 500)})
        j = _fetch_ob(ctx)
        cov = j.result["coverage"]
        assert (cov["fetched_ok"], cov["empty_responses"], cov["errors"]) == (2, 1, 1), cov
        assert cov["error_kinds"] == {"http": 1} and cov["expected_days"] == 4
        strip = dict((d, s) for d, s in cov["strip"])
        assert strip[DAYS[1]] == "empty" and strip[DAYS[2]] == "error"
    finally:
        fake.stop(); shutil.rmtree(tmp, ignore_errors=True)


@test("توقف ایمن هنگام مسدودشدن (۴۰۳) بدون retry نامحدود")
def t_breaker(_):
    fake = FakeTsetmc()
    ctx, tmp = _mk(fake)
    try:
        standard_handler(fake, ob={d: (lambda p: (403, "text/html", b"Forbidden")) for d in DAYS})
        j = _fetch_ob(ctx)
        assert j.state == "stopped" and fake.count("/BestLimits/") == MAX_CONSEC_BLOCKED, (j.state, fake.count("/BestLimits/"))
        assert "قابل ادامه" in j.message
    finally:
        fake.stop(); shutil.rmtree(tmp, ignore_errors=True)



@test("ساختار ناشناخته رد می‌شود (نه داده ظاهراً کامل)؛ نگاشت جایگزین شناسایی می‌شود")
def t_schema(_):
    bad = [{"x": 1, "y": 2}]
    assert detect_ob_schema(bad)[0] is None
    nolevel = [{"hEven": 90000, "pMeDem": 1, "qTitMeDem": 1, "zOrdMeDem": 1, "pMeOf": 1, "qTitMeOf": 1, "zOrdMeOf": 1}]
    assert detect_ob_schema(nolevel)[0] is None
    short = [{"hEven": 90000, "number": 1, "pd": 10, "qd": 5, "zd": 2, "po": 11, "qo": 6, "zo": 3}]
    name, fm = detect_ob_schema(short)
    assert name == "alias_short" and snaps(short)[0].rows[0] == (1, None, 11, 6, 3, 2, 5, 10)
    ev, inv = normalize_ob_events([{"hEven": 999999, "number": 1, "pMeDem": 1, "qTitMeDem": 1, "zOrdMeDem": 1,
                                    "pMeOf": 1, "qTitMeOf": 1, "zOrdMeOf": 1},
                                   {"hEven": 90000, "number": 9, "pMeDem": 1, "qTitMeDem": 1, "zOrdMeDem": 1,
                                    "pMeOf": 1, "qTitMeOf": 1, "zOrdMeOf": 1}, "junk"], OB_FIELD_SETS[0][1])
    assert ev == [] and inv == 3                                            # ساعت/سطح/رکورد نامعتبر ردشده و شمرده شد
    fake = FakeTsetmc()
    ctx, tmp = _mk(fake)
    try:
        standard_handler(fake, ob={DAYS[0]: lambda p: _j({"bestLimitsHistory": bad})})
        out, _, info = fetch_one_ob_day(ctx, ctx.make_client(), None, "111", DAYS[0], 84500, 123059)
        assert out == "schema" and ctx.store.get_ob_meta("111", DAYS[0])["status"] == "error"
    finally:
        fake.stop(); shutil.rmtree(tmp, ignore_errors=True)


@test("تعیین روزهای معاملاتی از منبع؛ تقویم فرضی فقط با برچسب و در نبود منبع")
def t_days(_):
    ctx, tmp = _mk()
    try:
        gs, ge = _dt.date(2024, 7, 20), _dt.date(2024, 7, 28)
        days, src = get_trading_days(ctx.store, "111", gs, ge)
        assert src == "calendar_assumed" and all(deven_to_date(d).weekday() in (5, 6, 0, 1, 2) for d in days)
        assert 20240725 not in days and 20240726 not in days            # پنجشنبه/جمعه
        ctx.store.put_daily("111", "ok", [{"dEven": 20240721}, {"dEven": 20240723}, {"dEven": 20230101}])
        days, src = get_trading_days(ctx.store, "111", gs, ge)
        assert src == "daily_list" and days == [20240721, 20240723]
    finally:
        shutil.rmtree(tmp, ignore_errors=True)



@test("بررسی وابستگی‌ها: تشخیص بسته‌های لازم بدون نصب خودکار")
def t_bootstrap(_):
    assert _missing_packages({"nonexistent_mod_xyz": "nonexistent-pkg", "json": "json"}) == ["nonexistent-pkg"]
    assert set(REQUIRED_PACKAGES) == {"flask", "requests", "jdatetime"} and set(TEST_PACKAGES) == {"pyarrow"}
    if not _missing_packages():
        _bootstrap_deps()          # وقتی همه نصب‌اند فقط پیش‌نیازها را بررسی می‌کند




# ═══════════════════ آزمون‌های خروجی CSV و Parquet، نام‌گذاری، ZIP و مقیاس ═══════════════════
OB_EN = [c[0] for c in OB_COLS]


def _seed_days(ctx, with_ref=False, n_events=10, days=DAYS[:2], window_used="08:45:00-12:30:59"):
    seed_instrument(ctx)
    for d in days:
        recs = synth_day(n_events, with_ref=with_ref)
        sname, fm = detect_ob_schema(recs)
        ev, inv = normalize_ob_events(recs, fm)
        st = summarize_events(ev, 84500, 123059)
        st["invalid"] = inv
        ctx.store.put_ob_day("111", d, "ok", records=recs, stats=st, schema_name=sname, window_used=window_used)
    ctx.store.put_daily("111", "ok", daily_payload(days)["closingPriceDaily"])


def _build_ob(ctx, formats, name=None, **spec_extra):
    spec = base_spec(**spec_extra)
    res = build_orderbook_files(Job("t", spec, 1), ctx, "111", spec, ctx.store.exports, formats, name=name, track=False)
    assert res is not None
    return res


def _read_csv(path):
    with open(str(path), encoding="utf-8-sig", newline="") as f:
        return list(csv.reader(f))


def _read_pq(path):
    pa, pq = _pyarrow()
    return pq.read_table(str(path))


def _parquet_rows_as_csv_strings(t):
    out = []
    for r in zip(*[t.column(i).to_pylist() for i in range(t.num_columns)]):
        out.append(["" if v is None else str(v) for v in r])
    return out


@test("نام‌گذاری خروجی: نماد_تاریخ‌شروع_تاریخ‌پایان_نوع‌گزارش با پسوند فرمت داده")
def t_naming(_):
    assert file_stem("فولاد", "14030501", "14030531", "OrderBook") == "فولاد_14030501_14030531_OrderBook"
    assert file_stem('خ/ودرو ?:"', "1", "2", "DailyBoard") == "خ_ودرو_1_2_DailyBoard"
    ctx, tmp = _mk()
    try:
        _seed_days(ctx)
        res = _build_ob(ctx, ["csv", "parquet"])
        assert sorted(f.name for f in res["files"]) == ["فولاد_14030501_14030531_OrderBook.csv",
                                                       "فولاد_14030501_14030531_OrderBook.parquet"]
        assert not [p for p in ctx.store.exports.iterdir() if p.suffix == ".tmp"]
        assert format_list({"formats": ["CSV", "parquet", "x"]}) == ["parquet", "csv"]
        for bad in ({"formats": []}, {"formats": ["xlsx"]}):
            try:
                format_list(bad)
            except AppError:
                continue
            raise AssertionError("فرمت نامعتبر پذیرفته شد")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


@test("عدم ساخت مقدار جعلی برای refID وقتی منبع آن را ندارد")
def t_refid(_):
    for with_ref in (False, True):
        ctx, tmp = _mk()
        try:
            _seed_days(ctx, with_ref=with_ref)
            res = _build_ob(ctx, ["csv"])
            rows = _read_csv(res["files"][0])
            col = [r[6] for r in rows[1:]]
            if with_ref:
                assert col[0] == "1000" and any(c for c in col)
            else:
                assert all(c == "" for c in col), "refID باید خالی بماند"
                assert res["rep"]["refid"].startswith("در منبع وجود نداشت")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


@test("ستون‌ها: CSV با عنوان فارسی و BOM؛ Parquet با نام انگلیسی، نوع‌های درست و برچسب فارسی")
def t_columns(_):
    ctx, tmp = _mk()
    try:
        _seed_days(ctx)
        res = _build_ob(ctx, ["csv", "parquet"])
        fc = [f for f in res["files"] if f.suffix == ".csv"][0]
        fp = [f for f in res["files"] if f.suffix == ".parquet"][0]
        assert open(str(fc), "rb").read(3) == b"\xef\xbb\xbf"
        rows = _read_csv(fc)
        assert rows[0] == OB_HEADERS and all(len(r) == 14 for r in rows)
        assert rows[1][:6] == ["فولاد", "فولاد مبارکه اصفهان", "فلزات اساسی", "1403/05/01", "2024-07-22", "09:00:00"]
        t = _read_pq(fp)
        assert t.schema.names == OB_EN and t.num_columns == 14
        assert [str(t.schema.field(i).type) for i in range(14)] == \
               ["string", "string", "string", "string", "date32[day]", "string"] + ["int64"] * 8
        assert [t.schema.field(i).metadata[b"fa"].decode("utf-8") for i in range(14)] == OB_HEADERS
        meta = json.loads(t.schema.metadata[b"TSE_LOB"].decode("utf-8"))
        assert meta["symbol"] == "فولاد" and meta["report"] == "OrderBook" and meta["price_unit"] == "rial"
        import datetime as dtm
        assert t.column("date_gregorian").to_pylist()[0] == dtm.date(2024, 7, 22)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


@test("دو قالب خروجی داده‌ی یکسان دارند؛ عددها عددی و سطح ناموجود خالی (نه صفر)")
def t_formats_equal(_):
    ctx, tmp = _mk()
    try:
        _seed_days(ctx, days=DAYS[:3])
        res = _build_ob(ctx, ["csv", "parquet"])
        csv_rows = _read_csv([f for f in res["files"] if f.suffix == ".csv"][0])[1:]
        t = _read_pq([f for f in res["files"] if f.suffix == ".parquet"][0])
        assert len(csv_rows) == t.num_rows == res["rep"]["output_rows"] == 3 * 10 * 5
        assert _parquet_rows_as_csv_strings(t) == csv_rows
        nulls = sum(t.column(i).null_count for i in range(8, 14))
        assert nulls == sum(1 for r in csv_rows for v in r[8:] if v == "") and nulls > 0
        assert all(v == "" or v.lstrip("-").isdigit() for r in csv_rows for v in r[6:])
        first_levels = {r[7]: r for r in csv_rows[:5]}
        assert first_levels["1"][8] != "" and first_levels["5"][8] == ""      # سطح ۵ هنوز داده ندارد
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


@test("نویسنده‌ها: نقل‌قول CSV، خالی‌ها، مقدار غیرصحیح در ستون عددی خطا، پاک‌سازی فایل موقت")
def t_writers(_):
    d = Path(tempfile.mkdtemp(prefix="tse_w_"))
    try:
        cols = [("a", "متن", "str"), ("b", "عدد", "int"), ("c", "اعشار", "float"), ("d", "تاریخ", "date")]
        rows = [['x,"y"\nz', 5, 0.5, "2024-07-22"], [None, None, None, None], ["فارسی ي", 7, 1e16, "2024-07-23"]]
        o = OutputSet(["csv", "parquet"], d, "w", cols, {"k": 1})
        o.write_rows(rows)
        files, notes = o.finish()
        assert sorted(f.name for f in files) == ["w.csv", "w.parquet"] and notes == []
        got = _read_csv(d / "w.csv")
        assert got[0] == ["متن", "عدد", "اعشار", "تاریخ"] and got[1] == ['x,"y"\nz', "5", "0.5", "2024-07-22"]
        assert got[2] == ["", "", "", ""] and got[3][0] == "فارسی ي"
        t = _read_pq(d / "w.parquet")
        assert t.column("a").to_pylist() == ['x,"y"\nz', None, "فارسی ي"] and t.column("b").to_pylist() == [5, None, 7]
        assert t.column("c").to_pylist()[2] == 1e16 and t.column("d").null_count == 1
        o2 = OutputSet(["csv", "parquet"], d, "bad", cols, {})
        o2.write_rows([["a", 1.5, 0.1, "2024-07-22"]])
        try:
            o2.finish()
        except AppError as e:
            assert "غیرصحیح" in str(e)
        else:
            raise AssertionError("مقدار 1.5 در ستون صحیح باید خطا بدهد")
        assert not list(d.glob("bad*")) and not list(d.glob("*.tmp"))
        o3 = OutputSet(["csv", "parquet"], d, "ab", cols, {})
        o3.write_rows(rows)
        o3.abort()
        assert not list(d.glob("ab*"))
        assert float(5 + 0.5) == 5.5 and _read_pq(d / "w.parquet").num_rows == 3
        safe_csv = CsvOut(d / "formula.csv", [("x", "متن", "str")], {})
        safe_csv.write_rows([[" =HYPERLINK(\"https://example.invalid\")"]])
        safe_csv.flush(); safe_csv.commit()
        assert _read_csv(d / "formula.csv")[1][0].startswith("'")
    finally:
        shutil.rmtree(d, ignore_errors=True)


@test("اطلاعات روزانه: بدون ستون «شناسه ابزار» و «خام»؛ نام انگلیسی در Parquet و عنوان فارسی در CSV")
def t_daily(_):
    fake = FakeTsetmc()
    ctx, tmp = _mk(fake)
    try:
        standard_handler(fake)
        seed_instrument(ctx)
        j = run_job(ctx, "daily", base_spec(), run_fetch_daily)
        assert j.state == "done" and j.result["daily"]["records_in_range"] == 6
        spec = base_spec()
        res = build_daily_files(Job("t", spec, 1), ctx, "111", spec, ctx.store.exports, ["csv", "parquet"], track=False)
        names = sorted(f.name for f in res["files"])
        assert names == ["فولاد_14030501_14030531_DailyBoard.csv", "فولاد_14030501_14030531_DailyBoard.parquet"]
        rows = _read_csv([f for f in res["files"] if f.suffix == ".csv"][0])
        h = rows[0]
        assert h[:6] == ["نماد", "نام شرکت", "نوع صنعت", "تاریخ شمسی", "تاریخ میلادی", "ساعت ثبت"]
        assert "قیمت پایانی" in h and "تغییر قیمت پایانی (محاسبه‌شده)" in h
        assert not any("شناسه ابزار" in x or "InsCode" in x or x.startswith("خام") for x in h)
        r0 = dict(zip(h, rows[1]))
        assert r0["قیمت پایانی"] == "4020" and r0["تغییر قیمت پایانی (محاسبه‌شده)"] == "20.0" and r0["ساعت ثبت"] == "12:30:00"
        t = _read_pq([f for f in res["files"] if f.suffix == ".parquet"][0])
        assert t.schema.names[:6] == ["symbol", "company", "industry", "date_jalali", "date_gregorian", "event_time"]
        assert "close_price" in t.schema.names and "close_change_pct" in t.schema.names
        assert str(t.schema.field("close_price").type) == "int64" and str(t.schema.field("close_change").type) == "double"
        assert t.num_rows == 6 and len(rows) == 7
    finally:
        fake.stop(); shutil.rmtree(tmp, ignore_errors=True)


def _bundle_spec(symbols, **kw):
    s = dict(SPEC_RANGE, symbols=symbols, window_start="08:45:00", window_end="12:30:59", formats=["csv"])
    s.update(kw)
    return s


def _two_instruments(ctx):
    ctx.store.upsert_instrument("111", symbol="فولاد", company="فولاد مبارکه اصفهان", industry="فلزات اساسی", identity_ok=1)
    ctx.store.upsert_instrument("222", symbol="شپنا", company="پالایش نفت اصفهان", industry="فرآورده‌های نفتی", identity_ok=1)


SYMS2 = [{"ins_code": "111", "symbol": "فولاد"}, {"ins_code": "222", "symbol": "شپنا"}]


def _zip_entries(ctx, files=None):
    names = files or sorted(p.name for p in ctx.store.exports.iterdir() if p.suffix in (".csv", ".parquet"))
    data = b"".join(zip_stream([ctx.store.exports / n for n in names]))
    z = zipfile.ZipFile(io.BytesIO(data))
    assert z.testzip() is None
    return z, names


@test("اجرای دسته‌ای: فایل جدا برای هر نماد/گزارش/فرمت با نام‌گذاری استاندارد؛ ZIP مسطح بدون پوشه")
def t_bundle(_):
    fake = FakeTsetmc()
    ctx, tmp = _mk(fake)
    try:
        standard_handler(fake)
        _two_instruments(ctx)
        j = run_job(ctx, "bundle", _bundle_spec(SYMS2, formats=["csv", "parquet"]), run_bundle)
        assert j.state == "done", (j.state, j.message, list(j.errors))
        want = sorted(["%s_14030501_14030531_%s.%s" % (s, k, e) for s in ("فولاد", "شپنا")
                       for k in ("OrderBook", "DailyBoard") for e in ("csv", "parquet")])
        got = sorted(p.name for p in ctx.store.exports.iterdir())
        assert got == want, got                                   # فقط همین فایل‌ها؛ بدون json و بدون xlsx و بدون .tmp
        total, reps = j.reports_page(0, 10)
        assert total == 2 and [r["symbol"] for r in reps] == ["فولاد", "شپنا"] and all(r["status"] == "done" for r in reps)
        assert reps[0]["output_rows"] == 300 and reps[0]["daily_records"] == 6 and reps[0]["fetched_ok"] == 6
        for r in reps:
            rows = _read_csv(ctx.store.exports / ("%s_14030501_14030531_OrderBook.csv" % r["symbol"]))
            assert {x[0] for x in rows[1:]} == {r["symbol"]} and len(rows) == 301
        z, names = _zip_entries(ctx)
        assert sorted(z.namelist()) == want and all("/" not in n and not n.endswith("/") for n in z.namelist())
        assert all(zi.is_dir() is False for zi in z.infolist())
        assert z.read(names[0]) == (ctx.store.exports / names[0]).read_bytes()
        assert fake.count("/BestLimits/111/") == 6 and fake.count("/BestLimits/222/") == 6
    finally:
        fake.stop(); shutil.rmtree(tmp, ignore_errors=True)


@test("انتخاب خروجی: فقط روزانه، فقط دفتر سفارشات، یا هر دو؛ بدون داده یا فرمت ⇒ خطا")
def t_flags(_):
    fake = FakeTsetmc()
    ctx, tmp = _mk(fake)
    try:
        standard_handler(fake)
        ctx.store.upsert_instrument("111", symbol="فولاد", company="فولاد مبارکه اصفهان", industry="فلزات اساسی", identity_ok=1)
        sym = [{"ins_code": "111", "symbol": "فولاد"}]

        def names():
            return sorted(p.name for p in ctx.store.exports.iterdir())
        j = run_job(ctx, "bundle", _bundle_spec(sym, include_orderbook=False, include_daily=True), run_bundle)
        assert j.state == "done" and names() == ["فولاد_14030501_14030531_DailyBoard.csv"], names()
        assert fake.count("/BestLimits/") == 0, "با انتخاب فقط روزانه نباید درخواست دفتر سفارش برود"
        j = run_job(ctx, "bundle", _bundle_spec(sym, include_orderbook=True, include_daily=False), run_bundle)
        assert j.state == "done" and "فولاد_14030501_14030531_OrderBook.csv" in names()
        j = run_job(ctx, "bundle", _bundle_spec(sym, include_orderbook=False, include_daily=False), run_bundle)
        assert j.state == "failed" and "حداقل یکی" in j.message
        j = run_job(ctx, "bundle", _bundle_spec(sym, formats=[]), run_bundle)
        assert j.state == "failed" and "فرمت" in j.message
    finally:
        fake.stop(); shutil.rmtree(tmp, ignore_errors=True)


@test("ادامه‌ی دسته‌ای: نمادهای ساخته‌شده رد می‌شوند (بدون هیچ درخواست)؛ تغییر فرمت یا «دریافت مجدد» دوباره می‌سازد")
def t_resume_symbols(_):
    fake = FakeTsetmc()
    ctx, tmp = _mk(fake)
    try:
        standard_handler(fake)
        _two_instruments(ctx)
        spec = _bundle_spec(SYMS2)
        j = run_job(ctx, "bundle", spec, run_bundle)
        assert j.state == "done"
        before = sum(fake.hits.values())
        j2 = run_job(ctx, "bundle", spec, run_bundle)
        assert j2.state == "done" and [r["status"] for r in j2.reports_page(0, 9)[1]] == ["skipped", "skipped"]
        assert sum(fake.hits.values()) == before, "نماد ساخته‌شده نباید درخواست جدید بفرستد"
        j3 = run_job(ctx, "bundle", _bundle_spec(SYMS2, formats=["csv", "parquet"]), run_bundle)
        assert [r["status"] for r in j3.reports_page(0, 9)[1]] == ["done", "done"]
        assert (ctx.store.exports / "فولاد_14030501_14030531_OrderBook.parquet").exists()
        before = fake.count("/BestLimits/")
        j4 = run_job(ctx, "bundle", dict(spec, force=True), run_bundle)
        assert fake.count("/BestLimits/") == before + 12 and j4.reports_page(0, 9)[1][0]["status"] == "done"
    finally:
        fake.stop(); shutil.rmtree(tmp, ignore_errors=True)


@test("پاک‌سازی کش خام پس از ساخت فایل: فضای کش آزاد می‌شود و روز دوباره‌دریافت می‌شود")
def t_purge(_):
    fake = FakeTsetmc()
    ctx, tmp = _mk(fake)
    try:
        standard_handler(fake)
        _two_instruments(ctx)
        sym = [SYMS2[0]]
        run_job(ctx, "bundle", _bundle_spec(sym, purge_cache=False), run_bundle)
        assert all(m["has_payload"] for m in ctx.store.list_ob_meta("111", 0, 99999999) if m["status"] == "ok")
        run_job(ctx, "bundle", _bundle_spec(sym, purge_cache=True, formats=["csv", "parquet"]), run_bundle)
        metas = ctx.store.list_ob_meta("111", 0, 99999999)
        assert metas and all(m["status"] == "ok" and not m["has_payload"] for m in metas)
        assert ctx.store.get_ob_records("111", metas[0]["deven"])[0] is None
        n = fake.count("/BestLimits/111/")
        j = run_job(ctx, "bundle", _bundle_spec(sym, purge_cache=True, formats=["csv"]), run_bundle)   # فرمت دیگر ⇒ ساخت دوباره
        assert j.state == "done" and fake.count("/BestLimits/111/") == n + 6        # کش پاک بود ⇒ دوباره دریافت
        assert not any(m["has_payload"] for m in ctx.store.list_ob_meta("111", 0, 99999999))
    finally:
        fake.stop(); shutil.rmtree(tmp, ignore_errors=True)


@test("مدیریت چرخه عمر Job: عملیات متوقف‌شده تا پایان واقعی نخ، عملیات تازه نمی‌پذیرد")
def t_job_lifecycle(_):
    tmp = Path(tempfile.mkdtemp(prefix="tse_job_lifecycle_"))
    try:
        ctx = Context(tmp)
        entered, release = threading.Event(), threading.Event()

        def slow_runner(job, _ctx):
            entered.set()
            release.wait(5)
            job.state = "stopped"

        first = ctx.jobs.start("test", {}, slow_runner)
        assert entered.wait(2), "نخ عملیات آغاز نشد"
        first.state = "stopped"  # مشابه وضعیتی که runner پیش از خاتمه‌ی workerها ثبت می‌کند
        try:
            ctx.jobs.start("test", {}, lambda job, _ctx: None)
        except Busy:
            pass
        else:
            raise AssertionError("تا زمانی که نخ قبلی زنده است نباید Job جدید شروع شود")
        release.set()
        first.thread.join(3)
        assert not first.thread.is_alive()
        second = ctx.jobs.start("test", {}, lambda job, _ctx: None)
        second.thread.join(3)
        assert second.state == "done"
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


@test("دریافت هم‌زمان: هر روز دقیقاً یک بار، شمارنده‌ها درست، با ۴ درخواست هم‌زمان")
def t_concurrency(_):
    fake = FakeTsetmc()
    ctx, tmp = _mk(fake, workers=4)
    try:
        days12 = [date_to_deven(jalali_to_gdate("1403/05/%02d" % k)) for k in range(1, 13)]
        standard_handler(fake, days=days12)
        seed_instrument(ctx)
        j = run_job(ctx, "orderbook", base_spec(), run_fetch_orderbook)
        assert j.state == "done" and j.progress["ok"] == 12 and j.progress["processed"] == 12
        assert all(fake.count("/BestLimits/111/%d" % d) == 1 for d in days12)
        assert j.result["coverage"]["fetched_ok"] == 12 and j.progress["raw_events"] == 120
        names = {th.name for th in threading.enumerate()}
        assert not [n for n in names if n.startswith("fetch-")]               # نخ‌ها پایان یافته‌اند
    finally:
        fake.stop(); shutil.rmtree(tmp, ignore_errors=True)


@test("نمادهای نام‌محور: شناسایی داخل کار با جست‌وجوی دقیق؛ ناموجود گزارش می‌شود و بار دوم از کش خوانده می‌شود")
def t_names(_):
    fake = FakeTsetmc()
    ctx, tmp = _mk(fake)
    try:
        standard_handler(fake)
        inner = fake.handler

        def h(path):
            if "/Instrument/GetInstrumentSearch/" in path and urllib.parse.unquote(path).rsplit("/", 1)[1] != "فولاد":
                return _j({"instrumentSearch": []})
            return inner(path)
        fake.handler = h
        syms = [{"name": "فولاد"}, {"name": "فولاد"}, {"name": "ناموجود"}, {"name": " "}]
        j = run_job(ctx, "bundle", _bundle_spec(syms), run_bundle)
        assert j.state == "done", (j.state, j.message)
        total, reps = j.reports_page(0, 10)
        assert total == 2 and reps[0]["status"] == "done" and reps[1]["status"] == "not_found" and reps[1]["symbol"] == "ناموجود"
        assert any("اولین مورد" in n for n in reps[0]["notes"])               # دو ابزار با نماد «فولاد»
        assert (ctx.store.exports / "فولاد_14030501_14030531_OrderBook.csv").exists()
        s1 = fake.count("GetInstrumentSearch")
        run_job(ctx, "bundle", _bundle_spec([{"name": "فولاد"}], force=True), run_bundle)
        assert fake.count("GetInstrumentSearch") == s1, "نماد شناخته‌شده نباید دوباره جست‌وجو شود"
    finally:
        fake.stop(); shutil.rmtree(tmp, ignore_errors=True)


@test("مسدودی ۴۰۳ در اجرای دسته‌ای: توقف ایمن بدون فایل ناقص؛ ادامه پس از رفع مشکل")
def t_bundle_block(_):
    fake = FakeTsetmc()
    ctx, tmp = _mk(fake)
    try:
        blocked = {"on": True}
        h = lambda p: (403, "text/html", b"Forbidden") if blocked["on"] else _j({"bestLimitsHistory": synth_day(10)})   # noqa: E731
        standard_handler(fake, ob={d: h for d in DAYS})
        _two_instruments(ctx)
        spec = _bundle_spec(SYMS2)
        j = run_job(ctx, "bundle", spec, run_bundle)
        assert j.state == "stopped" and not list(ctx.store.exports.iterdir()), (j.state, j.message)
        assert fake.count("/BestLimits/") == MAX_CONSEC_BLOCKED and "قابل ادامه" in j.message
        blocked["on"] = False
        j2 = run_job(ctx, "bundle", spec, run_bundle)
        assert j2.state == "done" and len(list(ctx.store.exports.glob("*.csv"))) == 4, (j2.state, j2.message)
        assert fake.count("/BestLimits/222/") == 6
    finally:
        fake.stop(); shutil.rmtree(tmp, ignore_errors=True)


@test("ZIP جریانی: مسطح، بدون پوشه، محتوای فایل‌ها یکسان، فایل بزرگ تکه‌تکه")
def t_zip_stream(_):
    d = Path(tempfile.mkdtemp(prefix="tse_z_"))
    try:
        (d / "فولاد_1_2_OrderBook.csv").write_bytes(b"a,b\n1,2\n")
        (d / "x.parquet").write_bytes(os.urandom(5 * 1024 * 1024 + 7))
        (d / "empty.csv").write_bytes(b"")
        paths = [d / "فولاد_1_2_OrderBook.csv", d / "x.parquet", d / "empty.csv"]
        chunks = list(zip_stream(paths))
        assert len(chunks) > 3
        z = zipfile.ZipFile(io.BytesIO(b"".join(chunks)))
        assert z.testzip() is None and z.namelist() == ["فولاد_1_2_OrderBook.csv", "x.parquet", "empty.csv"]
        assert all("/" not in n for n in z.namelist()) and all(not zi.is_dir() for zi in z.infolist())
        assert all(z.read(p.name) == p.read_bytes() for p in paths)
    finally:
        shutil.rmtree(d, ignore_errors=True)


@test("برآورد حجم/زمان از روزهای نمونه برای فرمت‌های انتخابی")
def t_estimate(_):
    fake = FakeTsetmc()
    ctx, tmp = _mk(fake)
    try:
        standard_handler(fake)
        j = run_job(ctx, "estimate", _bundle_spec(SYMS2, formats=["csv", "parquet"], ins_code="111", symbol="فولاد"), run_estimate)
        e = j.result["estimate"]
        assert j.state == "done" and e["est_output_rows"] > 0 and len(e["sample_days"]) == 3 and e["symbols"] == 2, e
        assert e["est_files_mb"] > 0 and e["est_cache_mb"] >= 0
        j = run_job(ctx, "estimate", _bundle_spec(SYMS2, include_orderbook=False, ins_code="111", symbol="فولاد"), run_estimate)
        assert j.state == "done" and "est_output_rows" not in j.result["estimate"]
    finally:
        fake.stop(); shutil.rmtree(tmp, ignore_errors=True)


@test("رابط Flask: عنوان و گزینه‌ها، حذف بخش‌ها، نتایج صفحه‌به‌صفحه، ZIP جریانی و دانلود امن")
def t_flask(_):
    if flask is None:
        raise AssertionError("SKIP: flask نصب نیست")
    fake = FakeTsetmc()
    ctx, tmp = _mk(fake)
    try:
        standard_handler(fake)
        c = create_app(ctx).test_client()
        H = {"X-App": "tse"}
        html = c.get("/").get_data(as_text=True)
        assert 'dir="rtl"' in html and "TSE_LOB" in html and "#6d28d9" in html
        assert "فایل‌های آماده دانلود" in html
        for i in ("oOb", "oDaily", "fParquet", "fCsv", "bZip", "sWorkers", "pPurge", "files", "bFilesRefresh", "bFilesZip"):
            assert 'id="%s"' % i in html, i
        assert html.index("TSE_LOB") < html.index("نمادها</h2>") and "font/woff2;base64,d09GMg" in html
        assert c.get("/api/files").status_code == 200 and c.get("/api/files").get_json()["total"] == 0
        assert c.post("/api/select", json={}).status_code == 403
        assert c.get("/api/config", headers={"Host": "evil.example"}).status_code == 403
        assert c.get("/api/convert?d=1403/12/30").get_json()["gregorian"] == "2025-03-20"
        assert c.post("/api/settings", json={"api_base": "ftp://x", "delay": 1}, headers=H).status_code == 400
        assert c.post("/api/settings", json={"api_base": "http://x", "delay": 1, "workers": 99}, headers=H).status_code == 400
        st = c.post("/api/settings", json={"api_base": fake.base, "delay": 0.1, "workers": 3}, headers=H)
        assert st.status_code == 200 and ctx.store.get_setting("workers") == "3"
        assert st.get_json()["resources"]["max_workers"] >= 3
        items = c.get("/api/search?q=" + urllib.parse.quote("فولاد")).get_json()["items"]
        sel = c.post("/api/select", json=items[0], headers=H).get_json()
        assert sel["industry"] == "فلزات اساسی"
        rs = c.post("/api/resolve", json={"names": "فولاد، شپنا"}, headers=H).get_json()
        assert [x["symbol"] for x in rs["resolved"]] == ["فولاد"] and rs["missing"][0]["name"] == "شپنا"
        _two_instruments(ctx)
        sy = [{"ins_code": "111", "symbol": "فولاد"}]
        assert c.post("/api/job/start", json=dict(_bundle_spec(sy), kind="bundle", formats=[]), headers=H).status_code == 400
        assert c.post("/api/job/start", json=dict(_bundle_spec([]), kind="bundle"), headers=H).status_code == 400
        assert c.get("/api/job/zip").status_code == 404 and c.get("/api/job/results").get_json()["total"] == 0
        r = c.post("/api/job/start", json=dict(_bundle_spec(SYMS2 + [{"name": "فولاد"}], formats=["csv", "parquet"]), kind="bundle"), headers=H)
        assert r.status_code == 200 and "symbols" not in r.get_json()["spec"]       # لیست بلند نمادها در وضعیت تکرار نمی‌شود
        ctx.jobs.wait(60)
        jb = c.get("/api/job/status").get_json()["job"]
        assert jb["state"] == "done" and jb["n_reports"] == 2 and jb["progress"]["files_built"] == 8, jb["message"]
        pg = c.get("/api/job/results?offset=1&limit=1").get_json()
        assert pg["total"] == 2 and pg["offset"] == 1 and [x["symbol"] for x in pg["items"]] == ["شپنا"]
        zr = c.get("/api/job/zip")
        assert zr.status_code == 200 and zr.mimetype == "application/zip"
        z = zipfile.ZipFile(io.BytesIO(zr.data))
        assert len(z.namelist()) == 8 and all("/" not in n for n in z.namelist()) and z.testzip() is None
        assert "TSE_LOB_2symbols_14030501_14030531_OrderBook-DailyBoard_parquet-csv.zip" in zr.headers["Content-Disposition"]
        d = c.get("/api/download/" + urllib.parse.quote("فولاد_14030501_14030531_OrderBook.csv"))
        assert d.status_code == 200 and d.data[:3] == b"\xef\xbb\xbf"
        fl = c.get("/api/files?offset=0&limit=100").get_json()
        assert fl["total"] == 8 and len(fl["items"]) == 8
        assert all(Path(x["name"]).suffix in (".csv", ".parquet") for x in fl["items"])
        # فهرست پس از ساخت Context جدید نیز از فایل‌های پایدار دیسک بازیابی می‌شود.
        c2 = create_app(Context(ctx.store.dir)).test_client()
        assert c2.get("/api/files").get_json()["total"] == 8
        zall = c.get("/api/files/zip")
        assert zall.status_code == 200 and zipfile.ZipFile(io.BytesIO(zall.data)).testzip() is None
        assert c.get("/api/download/..%2f..%2ftse_cache.sqlite3").status_code == 404
        assert c.get("/api/download/tse_cache.sqlite3").status_code == 404
        chk = c.post("/api/check", json={"ins_code": "111", "start": "1403/05/01", "end": "1403/05/31"}, headers=H).get_json()
        assert any(p["name"].startswith("دفتر سفارش") and p["status"] == "ok" for p in chk["probes"])
    finally:
        fake.stop(); shutil.rmtree(tmp, ignore_errors=True)


@test("مقیاس: ۸۵۰ نماد (فقط نام) در یک اجرا؛ شناسایی، دریافت، ساخت ۱۷۰۰ فایل، وضعیت سبک، ZIP کامل")
def t_scale_850(_):
    fake = FakeTsetmc()
    ctx, tmp = _mk(fake, workers=8)
    try:
        standard_handler(fake, days=DAYS[:1])
        inner = fake.handler

        def h(path):
            if "/Instrument/GetInstrumentSearch/" in path:
                q = urllib.parse.unquote(path).rsplit("/", 1)[1]
                m = re.fullmatch(r"S(\d{3})", q)
                if m:
                    return _j({"instrumentSearch": [{"insCode": str(50000 + int(m.group(1))), "lVal18AFC": q,
                                                     "lVal30": "شرکت " + q, "flowTitle": "بازار بورس"}]})
                return _j({"instrumentSearch": []})
            if "/Instrument/GetInstrumentIdentity/" in path:
                i = int(path.rsplit("/", 1)[1]) - 50000
                return _j({"instrumentIdentity": {"lVal18AFC": "S%03d" % i, "lSoc30": "شرکت S%03d" % i,
                                                  "sector": {"lSecVal": "صنعت"}}})
            return inner(path)
        fake.handler = h
        syms = [{"name": "S%03d" % i} for i in range(1, 851)]
        spec = _bundle_spec(syms, end="1403/05/05", start="1403/05/01")
        t0 = time.time()
        ctx.jobs.start("bundle", spec, run_bundle)
        j = ctx.jobs.current
        j.thread.join(600)
        el = time.time() - t0
        assert j.state == "done", (j.state, j.message, list(j.errors)[-3:])
        total, reps = j.reports_page(0, 10 ** 9)
        assert total == 850 and all(r["status"] == "done" and len(r["files"]) == 2 for r in reps), \
            [r for r in reps if r["status"] != "done"][:2]
        assert j.progress["files_built"] == 1700 and j.progress["symbols_failed"] == 0
        files = [p for p in ctx.store.exports.iterdir()]
        assert len(files) == 1700 and not [p for p in files if p.suffix == ".tmp"]
        size = len(json.dumps(j.public(), ensure_ascii=False, default=str))
        assert size < 20000, "وضعیت هر ثانیه نباید با تعداد نمادها بزرگ شود (%d بایت)" % size
        total2, page = j.reports_page(800, 50)
        assert total2 == 850 and len(page) == 50 and page[-1]["symbol"] == "S850"
        z = zipfile.ZipFile(io.BytesIO(b"".join(zip_stream(sorted(files)))))
        assert len(z.namelist()) == 1700 and all("/" not in n for n in z.namelist())
        print("        [۸۵۰ نماد: %.0f ثانیه، %d درخواست محلی]" % (el, sum(fake.hits.values())))
    finally:
        fake.stop(); shutil.rmtree(tmp, ignore_errors=True)


def run_self_tests():
    print("═" * 78)
    print("آزمون‌های داخلی — همگی با «داده آزمایشی داخلی» و سرور محلی ساختگی؛ ارتباطی با TSETMC واقعی ندارند.")
    print("═" * 78)
    ok = fail = skip = 0
    for n, title, fn in TESTS:
        t0 = time.time()
        try:
            fn(None)
            ok += 1
            print("  ✔ %02d  %s  (%.2fs)" % (n, title, time.time() - t0))
        except AssertionError as e:
            if str(e).startswith("SKIP"):
                skip += 1
                print("  ⏭ %02d  %s  — %s" % (n, title, e))
            else:
                fail += 1
                print("  ✘ %02d  %s\n        %s" % (n, title, "".join(traceback.format_exception_only(type(e), e)).strip()))
                traceback.print_exc(limit=4)
        except Exception as e:  # noqa
            fail += 1
            print("  ✘ %02d  %s\n        %s" % (n, title, e))
            traceback.print_exc(limit=6)
    print("─" * 78)
    print("نتیجه: %d موفق، %d ناموفق، %d ردشده از %d آزمون." % (ok, fail, skip, len(TESTS)))
    print("توجه: وضعیت API زنده TSETMC با این آزمون‌ها «تأییدنشده» است؛ برای آزمون واقعی:  --live-check <نماد>")
    return 0 if fail == 0 else 1


# ═══════════════════════════ آزمون واقعی (--live-check) ═════════════════════════════
def live_check(ctx, symbol):
    """آزمون پوشش تاریخی روی TSETMC واقعی (نیازمند اینترنت/IP مجاز). هیچ داده‌ای جعل نمی‌شود."""
    P = print
    P("آزمون واقعی TSETMC برای نماد «%s» — منبع: %s" % (symbol, ctx.settings()["api_base"]))
    try:
        items = search_instruments(ctx, symbol)
    except AppError as e:
        P("✘ جست‌وجوی نماد ناموفق: %s\n→ وضعیت API زنده: تأییدنشده (شبکه/IP را بررسی کنید)." % e)
        return 1
    if not items:
        P("✘ نمادی پیدا نشد.")
        return 1
    qk = fa_key(symbol)
    it = next((i for i in items if fa_key(i["symbol"]) == qk), items[0])
    P("✔ نماد: %s | %s | %s | InsCode=%s" % (it["symbol"], it["name"], it["market"], it["ins_code"]))
    inst = select_instrument(ctx, it)
    P("  شرکت: %s | صنعت: %s %s" % (inst["company"], inst["industry"] or "—", ("| " + inst["note"]) if inst["note"] else ""))
    ins = it["ins_code"]
    st, msg = fetch_daily_list(ctx, ins, None, force=True)
    P("  فهرست روزانه: %s — %s" % (st, msg))
    today = tehran_today()
    start = today - _dt.timedelta(days=MAX_RANGE_DAYS - 1)
    days, source = get_trading_days(ctx.store, ins, start, today)
    P("  روزهای معاملاتی پنج‌ساله (%s): %d" % ("از منبع" if source == "daily_list" else "تقویم فرضی", len(days)))
    if not days:
        return 1
    sample = sorted({days[-1], days[len(days) // 2], days[0]})
    ws, we = parse_clock(DEFAULT_WINDOW[0]), parse_clock(DEFAULT_WINDOW[1])
    cl = ctx.make_client()
    bad = 0
    for d in sample:
        out, res, info = fetch_one_ob_day(ctx, cl, None, ins, d, ws, we)
        P("\n── %s (%s) → %s  [%d ms, %d بایت] %s" % (gdate_to_jalali_str(deven_to_date(d)), deven_to_date(d), out,
                                                   res.elapsed * 1000, res.nbytes, info["detail"] if out not in ("ok",) else ""))
        if out != "ok":
            bad += out != "empty"
            continue
        recs, sname = ctx.store.get_ob_records(ins, d)
        P("   ساختار تشخیص‌داده‌شده: %s | کلیدها: %s" % (sname, ", ".join(recs[0].keys())))
        for r in recs[:3]:
            P("   نمونه رکورد خام:", r)
        s = info["stats"]
        P("   رویداد=%d نامعتبر=%d اسنپ‌شات(کل=%d، بازه=%d) ناقص=%d مبهم=%d refID(همه=%s، حداقل‌یکی=%s)" % (
            s["events"], s["invalid"], s["snapshots_total"], s["snapshots_window"], s["incomplete_snapshots"],
            s["ambiguous_groups"], s["ref_all"], s["ref_any"]))
        fmx = detect_ob_schema(recs)[1]
        evs, _ = normalize_ob_events(recs, fmx)
        for sn in list(reconstruct_snapshots(evs, ws, we))[:2]:
            P("   اسنپ‌شات %s:" % hms_str(sn.t))
            for row in sn.rows:
                P("     ", row)
    P("\nپایان. اگر خروجی بالا ساختار/refID/سطوح را نشان می‌دهد، همین مقادیر مبنای واقعی نگاشت فیلدهاست.")
    return 1 if bad else 0


# ════════════════════════════════════ اجرا ═══════════════════════════════════════
def _pause():
    if os.name == "nt" and sys.stdin and sys.stdin.isatty():
        try:
            input("\nبرای بستن پنجره Enter بزنید…")
        except EOFError:
            pass


def find_port(host, preferred, tries=30):
    for p in range(preferred, preferred + tries):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind((host, p))
                return p
            except OSError:
                continue
    raise AppError("هیچ درگاه آزادی پیدا نشد (از %d تا %d)." % (preferred, preferred + tries - 1))


def main(argv=None):
    ap = argparse.ArgumentParser(description="TSE_LOB — دریافت و گزارش‌گیری TSETMC")
    ap.add_argument("--self-test", action="store_true", help="اجرای آزمون‌های داخلی بدون اینترنت")
    ap.add_argument("--live-check", metavar="SYMBOL", help="آزمون پوشش تاریخی روی TSETMC واقعی")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--no-browser", action="store_true")
    ap.add_argument("--data-dir", default=str(Path(__file__).resolve().parent / "TSE_LOB_DATA"))
    ap.add_argument("--api-base", default=None)
    ap.add_argument("--version", action="store_true")
    a = ap.parse_args(argv)
    if os.name == "nt" and os.environ.get("TSE_LOB_LAUNCHED") != "1":
        print("برای اجرای استاندارد، فایل RUN_TSE_LOB.bat را اجرا کنید.")
        _pause()
        return 3
    if a.version:
        print(APP_VERSION)
        return 0
    msg = _missing_deps_message(need_flask=not (a.self_test or a.live_check))
    if msg:
        print(msg)
        _pause()
        return 2
    if a.self_test:
        return run_self_tests()
    ctx = Context(a.data_dir, api_base=a.api_base)
    if a.live_check:
        return live_check(ctx, a.live_check)
    if a.host != "127.0.0.1":
        print("نسخه امن فقط روی 127.0.0.1 اجرا می‌شود؛ دسترسی از شبکه محلی پذیرفته نیست.")
        return 2
    port = find_port(a.host, a.port)
    app = create_app(ctx)
    url = "http://%s:%d/" % ("127.0.0.1" if a.host == "0.0.0.0" else a.host, port)
    print("برنامه اجرا شد: %s\nپوشه داده/کش/خروجی: %s\nبرای توقف: Ctrl+C" % (url, ctx.store.dir))
    if not a.no_browser:
        threading.Timer(1.2, lambda: webbrowser.open(url)).start()
    import logging
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    app.run(host=a.host, port=port, threaded=True, use_reloader=False, debug=False)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nخارج شد.")
    except Exception:
        traceback.print_exc()
        _pause()
        sys.exit(1)
