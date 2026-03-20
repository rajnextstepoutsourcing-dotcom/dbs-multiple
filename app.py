"""
app.py — DBS Check Service (NextStep SaaS)
Full implementation: auth, Redis queue, identity chain,
isolated storage, ownership checks, token tracking.
"""
import sys, asyncio, anyio, logging, json, os, io, csv, re, secrets
import datetime, shutil, uuid, zipfile, time
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List
from zoneinfo import ZoneInfo

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
log = logging.getLogger(__name__)

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, FileResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.background import BackgroundTask

import fitz
import pdfplumber

try:
    from google import genai
    from google.genai import types
except Exception:
    genai = None; types = None

APP_DIR      = os.path.dirname(os.path.abspath(__file__))
REDIS_URL    = os.environ.get("REDIS_URL", "redis://localhost:6379")
DBS_QUEUE    = "nextstep:dbs:jobs"
STORAGE_ROOT = Path("/tmp/nextstep")
STORAGE_ROOT.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="DBS Check — NextStep")
app.mount("/static", StaticFiles(directory=os.path.join(APP_DIR, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(APP_DIR, "templates"))

# ── Gemini ────────────────────────────────────────────────────────────────────
def _env(name, default=None):
    v = os.getenv(name)
    return default if v is None or str(v).strip() == "" else v

GEMINI_API_KEY    = _env("GEMINI_API_KEY")
GEMINI_MODEL_FAST = _env("GEMINI_MODEL_FAST", "gemini-2.0-flash-001")
GEMINI_MODEL_STRONG = _env("GEMINI_MODEL_STRONG", "gemini-2.5-pro")

def get_gemini_client():
    if not GEMINI_API_KEY or genai is None: return None
    try: return genai.Client(api_key=GEMINI_API_KEY)
    except Exception: return None

GEMINI_CLIENT = get_gemini_client()

# ── Redis ─────────────────────────────────────────────────────────────────────
_redis = None
def get_redis():
    global _redis
    if _redis is None:
        try:
            import redis
            _redis = redis.Redis.from_url(REDIS_URL, decode_responses=False)
            _redis.ping()
        except Exception as e:
            log.error("[Redis] %s", e); _redis = None
    return _redis

def _jget(job_id):
    r = get_redis()
    if not r: return None
    try:
        raw = r.get(f"nextstep:dbs:job:{job_id}")
        return json.loads(raw) if raw else None
    except: return None

def _jset(job_id, state):
    r = get_redis()
    if not r: return
    try: r.setex(f"nextstep:dbs:job:{job_id}", 3600, json.dumps(state))
    except: pass

def _owner_set(job_id, tenant_id):
    r = get_redis()
    if not r: return
    try: r.setex(f"nextstep:dbs:owner:{job_id}", 3600, str(tenant_id))
    except: pass

def _owner_get(job_id):
    r = get_redis()
    if not r: return None
    try:
        v = r.get(f"nextstep:dbs:owner:{job_id}")
        return int(v) if v else None
    except: return None

# ── Auth ──────────────────────────────────────────────────────────────────────
def _get_ctx(request: Request):
    token = (request.headers.get("X-NextStep-Token")
             or request.cookies.get("ns_token")
             or request.query_params.get("ns_token") or "")
    if not token: return None
    try:
        import db; return db.validate_user_token(token)
    except Exception as e:
        log.warning("[Auth] %s", e); return None

def _auth(request: Request):
    ctx = _get_ctx(request)
    if not ctx: raise HTTPException(401, "Not authenticated. Please log in at nextstep.co.uk")
    return ctx

# ── Storage ───────────────────────────────────────────────────────────────────
def _storage(tenant_id, user_id, job_id):
    p = STORAGE_ROOT / str(tenant_id) / str(user_id) / job_id
    p.mkdir(parents=True, exist_ok=True)
    return p

# ── Helpers (kept from original) ─────────────────────────────────────────────
def normalize_ws(s):
    return re.sub(r"\s+", " ", (s or "").strip())

MONTHS = {
    "JANUARY":"01","FEBRUARY":"02","MARCH":"03","APRIL":"04","MAY":"05","JUNE":"06",
    "JULY":"07","AUGUST":"08","SEPTEMBER":"09","OCTOBER":"10","NOVEMBER":"11","DECEMBER":"12",
    "JAN":"01","FEB":"02","MAR":"03","APR":"04","JUN":"06","JUL":"07","AUG":"08",
    "SEP":"09","SEPT":"09","OCT":"10","NOV":"11","DEC":"12",
}

def parse_uk_date_words(date_str):
    if not date_str: return None
    s = normalize_ws(date_str).upper()
    m = re.search(r"\b(\d{1,2})\s+([A-Z]{3,9})\s+(\d{4})\b", s)
    if not m: return None
    dd = m.group(1).zfill(2); mon = MONTHS.get(m.group(2)); yyyy = m.group(3)
    return (dd, mon, yyyy) if mon else None

def parse_ddmmyyyy(date_str):
    if not date_str: return None
    m = re.search(r"\b(\d{1,2})[\/\.-](\d{1,2})[\/\.-](\d{2,4})\b", normalize_ws(date_str))
    if not m: return None
    dd = m.group(1).zfill(2); mm = m.group(2).zfill(2); yy = m.group(3)
    if len(yy) == 2: yy = ("19" if int(yy) > 30 else "20") + yy
    return dd, mm, yy

def validate_cert_number(s):
    if not s: return None
    digits = re.sub(r"\D", "", s)
    return digits if 10 <= len(digits) <= 14 else None

def score_cert_number(val):
    digits = re.sub(r"\D", "", val or "")
    if not digits: return 0
    return min(100, 80 + (20 if validate_cert_number(digits) else 0)) if 10 <= len(digits) <= 14 else 30

def score_surname(val, *, source=""):
    s = (val or "").strip()
    if not s: return 0
    cleaned = re.sub(r"[^A-Za-z\-\s]", "", s).strip()
    if not cleaned: return 10
    length = len(cleaned.replace(" ", ""))
    score = 35 if length < 2 else (70 if length < 4 else 85)
    if cleaned != s: score -= 10
    if (source or "").lower().startswith("pdf"): score = min(100, score + 5)
    return max(0, min(100, score))

def score_dob(dd, mm, yyyy, *, source=""):
    dd, mm, yyyy = str(dd or "").strip(), str(mm or "").strip(), str(yyyy or "").strip()
    if not (dd or mm or yyyy): return 0
    if not (dd and mm and yyyy): return 45
    try:
        datetime.date(int(yyyy), int(mm), int(dd))
        score = 95
        if (source or "").lower().startswith("pdf"): score = min(100, score+5)
        return score
    except: return 20

def _safe_filename(name, default):
    name = normalize_ws(name).strip() or default
    name = re.sub(r'[\\/:"*?<>|]+', "-", name)
    return re.sub(r"\s+", " ", name).strip()

def _uk_checked_date():
    return datetime.datetime.now(tz=ZoneInfo("Europe/London")).strftime("%d.%m.%Y")

def _dmy(dd, mm, yy):
    dd, mm, yy = str(dd or "").strip(), str(mm or "").strip(), str(yy or "").strip()
    return f"{dd.zfill(2)}/{mm.zfill(2)}/{yy}" if (dd and mm and yy) else ""

# ── PDF extraction helpers (kept from original) ───────────────────────────────
def extract_text_from_pdf(pdf_bytes, max_pages=2):
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            return "\n".join([p.extract_text() or "" for p in pdf.pages[:max_pages]]).strip()
    except: return ""

def pdf_to_images_bytes(pdf_bytes, max_pages=1, dpi=240):
    images = []
    if not pdf_bytes or len(pdf_bytes) < 100: raise ValueError("File too small.")
    if not pdf_bytes.lstrip().startswith(b"%PDF"): raise ValueError("Not a PDF.")
    try: doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception as e: raise ValueError(f"Cannot open PDF: {e}")
    try:
        if getattr(doc, "is_encrypted", False):
            try: doc.authenticate("")
            except: pass
        if getattr(doc, "page_count", 0) <= 0: return images
        page = doc.load_page(0)
        zoom = dpi / 72.0; mat = fitz.Matrix(zoom, zoom); rect = page.rect
        for x0,y0,x1,y1 in [(0,0,1,.32),(0,.28,1,.72),(0,.68,1,1)]:
            clip = fitz.Rect(rect.x0+rect.width*x0, rect.y0+rect.height*y0,
                             rect.x0+rect.width*x1, rect.y0+rect.height*y1)
            images.append(page.get_pixmap(matrix=mat, clip=clip, alpha=False).tobytes("png"))
        images.append(page.get_pixmap(matrix=mat, alpha=False).tobytes("png"))
        return images
    finally:
        try: doc.close()
        except: pass

def extract_fields_from_text(text):
    t = normalize_ws(text)
    out = {"certificate_number": None, "surname": None, "dob": None}
    m = re.search(r"Certificate\s*Number[:\s]*([0-9\s]{8,20})", t, re.IGNORECASE)
    if m: out["certificate_number"] = validate_cert_number(m.group(1))
    m = re.search(r"Surname[:\s]*([A-Z'\-\s]{2,40})", t, re.IGNORECASE)
    if m: out["surname"] = normalize_ws(m.group(1)).upper()
    m = re.search(r"Date\s*of\s*Birth[:\s]*([0-9]{1,2}\s+[A-Za-z]{3,9}\s+[0-9]{4}|\d{1,2}[\/\.-]\d{1,2}[\/\.-]\d{2,4})", t, re.IGNORECASE)
    if m:
        parts = parse_uk_date_words(m.group(1)) or parse_ddmmyyyy(m.group(1))
        if parts: out["dob"] = {"dd": parts[0], "mm": parts[1], "yyyy": parts[2]}
    return out

VISION_PROMPT = """Extract from UK DBS Certificate. Return STRICT JSON:
{"certificate_number":{"value":"","confidence":0.0},"surname":{"value":"","confidence":0.0},
"forename":{"value":"","confidence":0.0},"dob":{"day":"","month":"","year":"","confidence":0.0},
"issue_date":{"day":"","month":"","year":"","confidence":0.0}}
Certificate number = digits only. Do NOT use Print Date for issue_date."""

def _parse_json_response(text):
    if not text: return {}
    text = re.sub(r"^```(json)?", "", text.strip(), flags=re.IGNORECASE).strip()
    text = re.sub(r"```$", "", text).strip()
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if not m: return {}
    try: return json.loads(m.group(0))
    except: return {}

def gemini_vision_extract_images(images):
    if GEMINI_CLIENT is None or not images: return {}
    def _call(model):
        parts = [types.Part.from_text(text=VISION_PROMPT)]
        for b, mime in images: parts.append(types.Part.from_bytes(data=b, mime_type=mime))
        resp = GEMINI_CLIENT.models.generate_content(model=model, contents=[types.Content(role="user", parts=parts)])
        data = _parse_json_response(getattr(resp, "text", None) or "")
        if isinstance(data, dict): data["_model"] = model
        return data if isinstance(data, dict) else {}

    def _missing(d):
        if not d: return True
        def _v(x): return str(x.get("value","") if isinstance(x, dict) else x or "")
        cert_ok = bool(validate_cert_number(_v(d.get("certificate_number",""))))
        sn_ok = bool(normalize_ws(_v(d.get("surname",""))).strip())
        dob = d.get("dob") or {}
        dob_ok = bool(str(dob.get("day","")).strip() and str(dob.get("month","")).strip() and str(dob.get("year","")).strip()) if isinstance(dob, dict) else False
        return not (cert_ok and sn_ok and dob_ok)

    data = {}
    try: data = _call(GEMINI_MODEL_FAST)
    except: data = {}
    if _missing(data):
        try: data = _call(GEMINI_MODEL_STRONG)
        except: pass

    def _gv(k): v = data.get(k); return str(v.get("value","") if isinstance(v, dict) else v or "")
    def _gc(k):
        v = data.get(k)
        try: return float(v.get("confidence", 0) if isinstance(v, dict) else 0)
        except: return 0.0
    def _pct(f):
        try: return int(round(max(0.0,min(1.0,float(f or 0)))*100))
        except: return 0

    out = {
        "certificate_number": validate_cert_number(_gv("certificate_number")),
        "surname": normalize_ws(_gv("surname")).upper() or None,
        "forename": normalize_ws(_gv("forename")).upper() or None,
        "dob": None, "issue_date": None,
        "confidence": {"certificate_number": _pct(_gc("certificate_number")),
                       "surname": _pct(_gc("surname")), "dob": _pct(_gc("dob")),
                       "issue_date": _pct(_gc("issue_date"))},
        "_model": str(data.get("_model",""))
    }
    def _parse_date_obj(key):
        obj = data.get(key)
        if isinstance(obj, dict):
            dd = str(obj.get("day","")).zfill(2) if str(obj.get("day","")).strip() else ""
            mm = str(obj.get("month","")).zfill(2) if str(obj.get("month","")).strip() else ""
            yy = str(obj.get("year","")).strip()
            if dd and mm and yy: return {"dd": dd, "mm": mm, "yyyy": yy}
        parts = parse_uk_date_words(_gv(key)) or parse_ddmmyyyy(_gv(key))
        return {"dd": parts[0], "mm": parts[1], "yyyy": parts[2]} if parts else None
    out["dob"] = _parse_date_obj("dob")
    out["issue_date"] = _parse_date_obj("issue_date")
    return out

# ── Spreadsheet parsing (kept from original) ──────────────────────────────────
def _norm_col(name):
    return re.sub(r"[^a-z0-9]+", "", (name or "").strip().lower())

def _parse_dob_value(v):
    if v is None: return ("","","")
    if isinstance(v, (datetime.date, datetime.datetime)):
        d = v.date() if isinstance(v, datetime.datetime) else v
        return (str(d.day).zfill(2), str(d.month).zfill(2), str(d.year))
    s = str(v).strip()
    if not s: return ("","","")
    m = re.match(r"^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$", s)
    if m: return (m.group(3).zfill(2), m.group(2).zfill(2), m.group(1))
    m = re.match(r"^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$", s)
    if m: return (m.group(1).zfill(2), m.group(2).zfill(2), m.group(3))
    return ("","","")

def parse_csv_rows(content):
    text = None
    for enc in ("utf-8-sig","utf-8","latin-1"):
        try: text = content.decode(enc); break
        except: continue
    if text is None: raise ValueError("Cannot decode CSV.")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames: raise ValueError("CSV has no header.")
    cols = {_norm_col(c): c for c in reader.fieldnames}
    return _rows_from_dict_iter(reader, cols)

def parse_xlsx_rows(content):
    from openpyxl import load_workbook
    wb = load_workbook(io.BytesIO(content), data_only=True, read_only=True)
    ws = wb.active; rows = list(ws.iter_rows(values_only=True))
    if not rows: raise ValueError("Excel is empty.")
    headers = [str(h).strip() if h is not None else "" for h in rows[0]]
    cols = {_norm_col(c): c for c in headers}
    dict_rows = [{h: rows[i][j] if j < len(rows[i]) else None for j,h in enumerate(headers) if h} for i in range(1, len(rows))]
    return _rows_from_dict_iter(dict_rows, cols)

def _rows_from_dict_iter(iterable, cols):
    def fp(keys):
        for k in keys:
            if _norm_col(k) in cols: return cols[_norm_col(k)]
        return None
    cert_col = fp(["certificate_number","certno","certnumber","certificate","cert"])
    sn_col   = fp(["surname","lastname","last_name"])
    fn_col   = fp(["forename","firstname","first_name","givenname"])
    dob_col  = fp(["dob","dateofbirth","dateofbirthddmmyyyy"])
    dd_col   = fp(["dobday","day","dd"]); mm_col = fp(["dobmonth","month","mm"]); yy_col = fp(["dobyear","year","yyyy","yy"])
    issue_col = fp(["issuedate","dateofissue","issue_date"])
    if not cert_col or not sn_col or not (dob_col or (dd_col and mm_col and yy_col)):
        raise ValueError("Spreadsheet must have Certificate No, Surname, and DOB columns.")
    out = []
    for d in iterable:
        if not d: continue
        cert = str(d.get(cert_col) or "").strip()
        sn   = str(d.get(sn_col) or "").strip()
        fn   = str(d.get(fn_col) or "").strip() if fn_col else ""
        if dob_col: dd,mm,yy = _parse_dob_value(d.get(dob_col))
        else:
            dd = str(d.get(dd_col) or "").strip().zfill(2) if str(d.get(dd_col) or "").strip() else ""
            mm = str(d.get(mm_col) or "").strip().zfill(2) if str(d.get(mm_col) or "").strip() else ""
            yy = str(d.get(yy_col) or "").strip()
        if not (cert or sn or (dd and mm and yy)): continue
        issue_dd = issue_mm = issue_yy = ""
        if issue_col:
            ip = parse_uk_date_words(str(d.get(issue_col) or "")) or parse_ddmmyyyy(str(d.get(issue_col) or ""))
            if ip: issue_dd, issue_mm, issue_yy = ip
        out.append({"certificate_number":cert,"surname":sn,"forename":fn,
                    "dob_day":dd,"dob_month":mm,"dob_year":yy,
                    "issue_day":issue_dd,"issue_month":issue_mm,"issue_year":issue_yy})
    return out

# ── Export helpers ────────────────────────────────────────────────────────────
def _csv_bytes(rows, columns):
    buf = io.StringIO(); w = csv.DictWriter(buf, fieldnames=columns, extrasaction="ignore")
    w.writeheader(); [w.writerow({c: r.get(c,"") for c in columns}) for r in rows]
    return buf.getvalue().encode("utf-8-sig")

def _xlsx_bytes(rows, columns):
    from openpyxl import Workbook
    wb = Workbook(); ws = wb.active; ws.append(columns)
    [ws.append([r.get(c,"") for c in columns]) for r in rows]
    out = io.BytesIO(); wb.save(out); return out.getvalue()

def _export_rows_results(payload):
    checked_date = (payload.get("checked_date") or "").strip()
    return [{
        "Forename": (r.get("forename") or "").strip(),
        "Surname": (r.get("surname") or "").strip(),
        "Certificate Number": (r.get("certificate_number") or "").strip(),
        "DOB": _dmy(r.get("dob_day"), r.get("dob_month"), r.get("dob_year")),
        "Issue Date": _dmy(r.get("issue_day"), r.get("issue_month"), r.get("issue_year")),
        "Status": (r.get("status") or "").strip(),
        "Checked Date": checked_date,
        "PDF Filename": (r.get("pdf_filename") or "").strip(),
        "Notes": (r.get("error") or r.get("notes") or "").strip(),
    } for r in (payload.get("rows") or [])]

def _export_rows_extract(items):
    return [{
        "Forename": (it.get("forename") or "").strip(),
        "Surname": (it.get("surname") or "").strip(),
        "Certificate Number": (it.get("certificate_number") or "").strip(),
        "DOB": _dmy(it.get("dob_day"), it.get("dob_month"), it.get("dob_year")),
        "Issue Date": _dmy(it.get("issue_day"), it.get("issue_month"), it.get("issue_year")),
        "PDF Filename": (it.get("original_filename") or "").strip(),
        "Notes": "",
    } for it in (items or [])]

# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/health")
def health():
    r = get_redis(); ok = False
    try:
        if r: r.ping(); ok = True
    except: pass
    return {"ok": True, "redis": ok, "gemini": bool(GEMINI_CLIENT), "db": bool(os.getenv("DATABASE_URL"))}

@app.post("/dbs/extract")
async def dbs_extract(request: Request, files: List[UploadFile] = File(...)):
    _auth(request)
    if not files: raise HTTPException(400, "No file uploaded.")
    items: List[Dict] = []; cap = 100
    for file in files[:100]:
        content = await file.read()
        fname = file.filename or ""; filename = fname.lower()
        if len(content) > 25*1024*1024: raise HTTPException(413, f"'{fname}' too large.")
        if filename.endswith(".docx"):
            try:
                from docx import Document
                doc = Document(io.BytesIO(content))
                text = "\n".join([p.text for p in doc.paragraphs if (p.text or "").strip()])
            except Exception as e: raise HTTPException(400, f"DOCX error: {e}")
            fields = extract_fields_from_text(text)
            dob = fields.get("dob") if isinstance(fields.get("dob"), dict) else {}
            issue = fields.get("issue_date") if isinstance(fields.get("issue_date"), dict) else {}
            items.append({"original_filename": fname, "forename": fields.get("forename",""),
                "certificate_number": fields.get("certificate_number",""), "surname": fields.get("surname",""),
                "dob_day": dob.get("dd",""), "dob_month": dob.get("mm",""), "dob_year": dob.get("yyyy",""),
                "issue_day": issue.get("dd",""), "issue_month": issue.get("mm",""), "issue_year": issue.get("yyyy",""),
                "confidence": {"certificate_number": score_cert_number(fields.get("certificate_number","")),
                               "surname": score_surname(fields.get("surname",""),source="docx"),
                               "dob": score_dob(dob.get("dd",""),dob.get("mm",""),dob.get("yyyy",""),source="docx"), "issue_date": 0},
                "source": {"certificate_number":"DOCX","surname":"DOCX","dob":"DOCX","issue_date":""}})
            if len(items) >= cap: break; continue
        if filename.endswith(".webp"):
            try:
                from PIL import Image
                im = Image.open(io.BytesIO(content)).convert("RGB"); buf = io.BytesIO(); im.save(buf, "PNG")
                content = buf.getvalue(); filename = fname.lower().replace(".webp",".png")
            except Exception as e: raise HTTPException(400, f"WEBP error: {e}")
        if filename.endswith(".csv") or filename.endswith(".xlsx"):
            try: rows = parse_csv_rows(content) if filename.endswith(".csv") else parse_xlsx_rows(content)
            except Exception as e: raise HTTPException(400, f"Spreadsheet error: {e}")
            for ri, r in enumerate(rows, 2):
                if len(items) >= cap: break
                items.append({"original_filename": f"{fname} (Row {ri})", "forename": r.get("forename",""),
                    "certificate_number": r.get("certificate_number",""), "surname": r.get("surname",""),
                    "dob_day": r.get("dob_day",""), "dob_month": r.get("dob_month",""), "dob_year": r.get("dob_year",""),
                    "issue_day": r.get("issue_day",""), "issue_month": r.get("issue_month",""), "issue_year": r.get("issue_year",""),
                    "confidence": {"certificate_number": score_cert_number(r.get("certificate_number","")),
                                   "surname": score_surname(r.get("surname",""),source="Spreadsheet"),
                                   "dob": score_dob(r.get("dob_day",""),r.get("dob_month",""),r.get("dob_year",""),source="Spreadsheet"), "issue_date": 0},
                    "source": {"certificate_number":"Spreadsheet","surname":"Spreadsheet","dob":"Spreadsheet","issue_date":"Spreadsheet"}})
            if len(items) >= cap: break; continue
        # PDF / image
        fields = {"certificate_number": None, "surname": None, "dob": None, "issue_date": None}
        source = {"certificate_number": "", "surname": "", "dob": "", "issue_date": ""}
        vision = {}
        if filename.endswith(".pdf"):
            text = extract_text_from_pdf(content)
            if text and len(text) > 60:
                fields = extract_fields_from_text(text)
                for k in ("certificate_number","surname","dob","issue_date"):
                    if fields.get(k): source[k] = "PDF text"
            if not fields.get("certificate_number") or not fields.get("surname") or not fields.get("dob"):
                try:
                    imgs = pdf_to_images_bytes(content, max_pages=1, dpi=240)
                    vision = gemini_vision_extract_images([(b,"image/png") for b in imgs])
                    for k in ("certificate_number","surname","forename","dob","issue_date"):
                        if not fields.get(k) and vision.get(k): fields[k] = vision.get(k); source[k] = "Image scan"
                except Exception as e: raise HTTPException(400, str(e))
        else:
            is_png = content[:8] == b"\x89PNG\r\n\x1a\n"; mime = "image/png" if is_png else "image/jpeg"
            vision = gemini_vision_extract_images([(content, mime)])
            fields = {k: vision.get(k) for k in ("certificate_number","surname","forename","dob","issue_date")}
            for k in ("certificate_number","surname","dob","issue_date"):
                if fields.get(k): source[k] = "Image scan"
        dob = fields.get("dob") if isinstance(fields.get("dob"), dict) else {}
        issue = fields.get("issue_date") if isinstance(fields.get("issue_date"), dict) else {}
        vconf = (vision.get("confidence") if isinstance(vision, dict) else {}) or {}
        items.append({"original_filename": fname, "forename": fields.get("forename",""),
            "certificate_number": fields.get("certificate_number",""), "surname": fields.get("surname",""),
            "dob_day": dob.get("dd",""), "dob_month": dob.get("mm",""), "dob_year": dob.get("yyyy",""),
            "issue_day": issue.get("dd",""), "issue_month": issue.get("mm",""), "issue_year": issue.get("yyyy",""),
            "confidence": {"certificate_number": int(vconf.get("certificate_number") or score_cert_number(fields.get("certificate_number",""))) ,
                           "surname": int(vconf.get("surname") or score_surname(fields.get("surname",""),source=source.get("surname",""))),
                           "dob": int(vconf.get("dob") or score_dob(dob.get("dd",""),dob.get("mm",""),dob.get("yyyy",""),source=source.get("dob",""))),
                           "issue_date": int(vconf.get("issue_date") or 0)},
            "source": source})
        if len(items) >= cap: break
    resp = {"items": items}
    if len(items) >= cap and len(files) > 0: resp["notice"] = "Row limit reached (100)."
    return JSONResponse(resp)

@app.post("/dbs/export/extract")
async def export_extract(request: Request):
    _auth(request)
    data = await request.json(); fmt = (data.get("format") or "xlsx").lower()
    rows = _export_rows_extract(data.get("items") or [])
    cols = ["Forename","Surname","Certificate Number","DOB","Issue Date","PDF Filename","Notes"]
    if fmt == "csv":
        return Response(_csv_bytes(rows,cols), media_type="text/csv", headers={"Content-Disposition":"attachment; filename=extract.csv"})
    return Response(_xlsx_bytes(rows,cols), media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    headers={"Content-Disposition":"attachment; filename=extract.xlsx"})

@app.post("/dbs/export/results")
async def export_results(request: Request):
    _auth(request)
    data = await request.json(); fmt = (data.get("format") or "xlsx").lower()
    rows = _export_rows_results(data)
    cols = ["Forename","Surname","Certificate Number","DOB","Issue Date","Status","Checked Date","PDF Filename","Notes"]
    if fmt == "csv":
        return Response(_csv_bytes(rows,cols), media_type="text/csv", headers={"Content-Disposition":"attachment; filename=results.csv"})
    return Response(_xlsx_bytes(rows,cols), media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    headers={"Content-Disposition":"attachment; filename=results.xlsx"})

@app.post("/dbs/run")
async def dbs_run(request: Request):
    ctx = _auth(request)
    tenant_id = ctx["tenant_id"]; user_id = ctx["user_id"]

    payload: Dict = {}
    ctype = (request.headers.get("content-type") or "").lower()
    if "application/json" in ctype:
        try: payload = await request.json()
        except: payload = {}
    else:
        form = await request.form(); payload = dict(form)

    org_name    = (payload.get("organisation_name") or payload.get("org_name") or "").strip()
    emp_fn      = (payload.get("employee_forename") or payload.get("forename") or "").strip()
    emp_sn      = (payload.get("employee_surname") or payload.get("surname_user") or payload.get("surname") or "").strip()
    if not (org_name and emp_fn and emp_sn):
        raise HTTPException(400, "Organisation/Forename/Surname is required.")

    items = payload.get("items")
    if not isinstance(items, list) or not items:
        raise HTTPException(400, "No items provided.")
    items = items[:100]

    # Token check
    try:
        import db
        tokens = db.get_tenant_tokens_remaining(tenant_id)
        if tokens == 0: raise HTTPException(402, "No tokens remaining.")
        if 0 < tokens < len(items): items = items[:tokens]
    except HTTPException: raise
    except Exception as e: log.warning("[Run] Token check skipped: %s", e)

    job_id = str(uuid.uuid4())
    storage_path = _storage(tenant_id, user_id, job_id)

    db_job_id = None
    try:
        import db
        db_job_id = db.create_job_record(tenant_id=tenant_id, user_id=user_id, total_items=len(items))
    except Exception as e: log.warning("[Run] DB record failed: %s", e)

    rows = [{"row": i+1, "status": "queued",
             "certificate_number": (it.get("certificate_number","") if isinstance(it,dict) else ""),
             "surname": (it.get("surname","") if isinstance(it,dict) else ""),
             "forename": (it.get("forename","") if isinstance(it,dict) else ""),
             "dob_day": (it.get("dob_day","") if isinstance(it,dict) else ""),
             "dob_month": (it.get("dob_month","") if isinstance(it,dict) else ""),
             "dob_year": (it.get("dob_year","") if isinstance(it,dict) else ""),
             "issue_day": (it.get("issue_day","") if isinstance(it,dict) else ""),
             "issue_month": (it.get("issue_month","") if isinstance(it,dict) else ""),
             "issue_year": (it.get("issue_year","") if isinstance(it,dict) else ""),
             "original_filename": (it.get("original_filename",f"Row {i+1}") if isinstance(it,dict) else f"Row {i+1}"),
             "pdf_filename": "", "pdf_url": "", "error": "", "notes": ""}
            for i, it in enumerate(items)]

    _jset(job_id, {"state": "queued", "rows": rows, "zip_ready": False,
                   "zip_name": "", "zip_url": "", "checked_date": "",
                   "message": "Job queued — processing starts shortly...",
                   "successful": 0, "failed": 0})
    _owner_set(job_id, tenant_id)

    job_data = {"job_id": job_id, "db_job_id": db_job_id,
                "tenant_id": tenant_id, "user_id": user_id,
                "items": items, "storage_path": str(storage_path),
                "organisation_name": org_name,
                "employee_forename": emp_fn,
                "employee_surname": emp_sn}

    enqueued = False
    try:
        from rq import Queue as RQ
        import redis as rl
        q = RQ(DBS_QUEUE, connection=rl.Redis.from_url(REDIS_URL))
        q.enqueue("dbs_tasks.process_dbs_job", job_data, job_timeout=7200, result_ttl=3600)
        enqueued = True
        log.info("[Run] DBS job %s enqueued %d items tenant=%d", job_id, len(items), tenant_id)
    except Exception as e:
        log.error("[Run] Enqueue failed, running directly: %s", e)
        asyncio.get_event_loop().create_task(_direct(job_id, job_data))

    return JSONResponse({"job_id": job_id, "mode": "bulk",
                         "status_url": f"/dbs/status/{job_id}",
                         "rows": rows, "queued": enqueued})

async def _direct(job_id, job_data):
    try:
        import dbs_tasks
        from functools import partial
        await anyio.to_thread.run_sync(partial(dbs_tasks.process_dbs_job, job_data), cancellable=True)
    except Exception as e:
        log.error("[Direct] %s", e)
        _jset(job_id, {"state": "done", "message": f"Job failed: {e}", "zip_ready": False})

@app.get("/dbs/status/{job_id}")
async def dbs_status(job_id: str, request: Request):
    _auth(request)
    state = _jget(job_id)
    if not state: raise HTTPException(404, "Job expired or not found.")
    rows = state.get("rows") or []
    done = sum(1 for r in rows if r.get("status") in ("clear","needs_review","portal_unavailable","failed"))
    return JSONResponse({"job_id": job_id, "mode": "bulk",
                         "state": state.get("state","queued"),
                         "checked_date": state.get("checked_date",""),
                         "running": {"done": done, "total": len(rows) or 1},
                         "rows": rows,
                         "zip_ready": bool(state.get("zip_ready")),
                         "zip_name": state.get("zip_name",""),
                         "zip_url": state.get("zip_url",""),
                         "message": state.get("message",""),
                         "successful": state.get("successful",0),
                         "failed": state.get("failed",0)})

@app.get("/dbs/download/{job_id}/{name}")
async def dbs_download(job_id: str, name: str, request: Request):
    ctx = _auth(request); tenant_id = ctx["tenant_id"]
    job_tenant = _owner_get(job_id)
    if job_tenant is not None and job_tenant != tenant_id:
        log.warning("[DL] Tenant %d tried job owned by %d", tenant_id, job_tenant)
        raise HTTPException(403, "Access denied.")
    tenant_root = STORAGE_ROOT / str(tenant_id); file_path = None
    for p in tenant_root.rglob(name):
        if job_id in str(p): file_path = p; break
    if not file_path or not file_path.exists():
        raise HTTPException(404, "Download expired or not found.")
    bg = None
    if name.lower().endswith(".zip"):
        sp = STORAGE_ROOT / str(tenant_id) / str(ctx["user_id"]) / job_id
        bg = BackgroundTask(_cleanup, sp, job_id)
    return FileResponse(str(file_path), filename=name, media_type="application/octet-stream", background=bg)

def _cleanup(storage_path: Path, job_id: str):
    import time as t; t.sleep(5)
    try:
        if storage_path.exists(): shutil.rmtree(storage_path, ignore_errors=True)
    except Exception as e: log.warning("[Cleanup] %s", e)
    r = get_redis()
    if r:
        try: r.delete(f"nextstep:dbs:job:{job_id}"); r.delete(f"nextstep:dbs:owner:{job_id}")
        except: pass

# ── Rerun (dirty rows only) ────────────────────────────────────────────────────
@app.post("/dbs/rerun")
async def dbs_rerun(request: Request):
    ctx = _auth(request)
    tenant_id = ctx["tenant_id"]; user_id = ctx["user_id"]
    try: payload = await request.json()
    except: raise HTTPException(400, "Invalid JSON.")

    old_job_id = (payload.get("job_id") or "").strip()
    dirty_items = payload.get("items") or []
    org_name  = (payload.get("organisation_name") or "").strip()
    emp_fn    = (payload.get("employee_forename") or "").strip()
    emp_sn    = (payload.get("employee_surname") or "").strip()

    if not old_job_id: raise HTTPException(400, "job_id required.")
    if not dirty_items: raise HTTPException(400, "No items for rerun.")

    job_tenant = _owner_get(old_job_id)
    if job_tenant is not None and job_tenant != tenant_id:
        raise HTTPException(403, "Access denied.")

    old_state = _jget(old_job_id)
    if not old_state: raise HTTPException(404, "Original job expired. Please run fresh.")

    old_rows = old_state.get("rows") or []
    old_storage = STORAGE_ROOT / str(tenant_id) / str(user_id) / old_job_id

    dirty_count = len(dirty_items)
    try:
        import db
        tokens = db.get_tenant_tokens_remaining(tenant_id)
        if tokens == 0: raise HTTPException(402, "No tokens remaining.")
        if 0 < tokens < dirty_count: dirty_items = dirty_items[:tokens]; dirty_count = len(dirty_items)
    except HTTPException: raise
    except Exception as e: log.warning("[Rerun] Token check skipped: %s", e)

    new_job_id = str(uuid.uuid4())
    storage_path = old_storage  # reuse same folder

    db_job_id = None
    try:
        import db
        db_job_id = db.create_job_record(tenant_id=tenant_id, user_id=user_id, total_items=dirty_count)
    except Exception as e: log.warning("[Rerun] DB record: %s", e)

    dirty_row_nums = {it.get("row_number") for it in dirty_items if it.get("row_number")}
    merged_rows = []
    for r in old_rows:
        rnum = r.get("row", 0)
        if rnum in dirty_row_nums:
            merged_rows.append({**r, "status": "queued", "pdf_filename": "", "pdf_url": "", "error": "", "notes": ""})
        else:
            merged_rows.append(r)

    _jset(new_job_id, {"state": "queued", "rows": merged_rows,
                        "zip_ready": old_state.get("zip_ready", False),
                        "zip_name": old_state.get("zip_name", ""),
                        "zip_url": old_state.get("zip_url", ""),
                        "checked_date": old_state.get("checked_date", ""),
                        "message": f"Rerunning {dirty_count} edited row(s)…",
                        "successful": 0, "failed": 0})
    _owner_set(new_job_id, tenant_id)

    job_data = {"job_id": new_job_id, "db_job_id": db_job_id,
                "tenant_id": tenant_id, "user_id": user_id,
                "items": dirty_items, "storage_path": str(storage_path),
                "organisation_name": org_name, "employee_forename": emp_fn, "employee_surname": emp_sn,
                "is_rerun": True, "old_rows": old_rows, "dirty_row_nums": list(dirty_row_nums)}

    enqueued = False
    try:
        from rq import Queue as RQ
        import redis as rl
        q = RQ(DBS_QUEUE, connection=rl.Redis.from_url(REDIS_URL))
        q.enqueue("dbs_tasks.process_dbs_job", job_data, job_timeout=7200, result_ttl=3600)
        enqueued = True
        log.info("[Rerun] DBS job %s queued — %d dirty rows", new_job_id, dirty_count)
    except Exception as e:
        log.error("[Rerun] Enqueue failed: %s", e)
        asyncio.get_event_loop().create_task(_direct(new_job_id, job_data))

    return JSONResponse({"job_id": new_job_id, "status_url": f"/dbs/status/{new_job_id}",
                         "rows": merged_rows, "queued": enqueued})
