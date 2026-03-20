"""
dbs_tasks.py — DBS job task executed by RQ worker
Processes all DBS rows for a single job sequentially (1 at a time).
"""

import logging, os, shutil, time, zipfile, re, json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)

STORAGE_ROOT = Path("/tmp/nextstep")

# Hard failures — skip retry
HARD_FAILURE_PATTERNS = [
    "site unavailable", "service unavailable", "maintenance",
    "503", "502", "connection refused", "network is unreachable",
    "portal_unavailable",
]

def _is_hard_failure(error_str: str) -> bool:
    el = (error_str or "").lower()
    return any(p in el for p in HARD_FAILURE_PATTERNS)

def _safe_filename(name: str, default: str) -> str:
    name = re.sub(r"\s+", " ", (name or "").strip()) or default
    return re.sub(r'[\\/:"*?<>|]+', "-", name).strip()

def _uk_date() -> str:
    try:
        return datetime.now(tz=ZoneInfo("Europe/London")).strftime("%d.%m.%Y")
    except Exception:
        return datetime.utcnow().strftime("%d.%m.%Y")

def _get_redis():
    import redis as rl
    return rl.Redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379"))

def _rstate(job_id: str, updates: Dict) -> None:
    try:
        r = _get_redis()
        key = f"nextstep:dbs:job:{job_id}"
        raw = r.get(key)
        existing = json.loads(raw) if raw else {}
        existing.update(updates)
        r.setex(key, 3600, json.dumps(existing))
    except Exception as e:
        log.warning("[Task] Redis update failed: %s", e)

def _rrows(job_id: str, rows: List[Dict]) -> None:
    _rstate(job_id, {"rows": rows})

def _schedule_cleanup(path: Path, delay: int = 600) -> None:
    import threading
    def _del():
        time.sleep(delay)
        try:
            if path.exists():
                shutil.rmtree(path, ignore_errors=True)
                log.info("[Cleanup] Deleted: %s", path)
        except Exception as e:
            log.warning("[Cleanup] %s: %s", path, e)
    threading.Thread(target=_del, daemon=True).start()


def process_dbs_job(job_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main task executed by RQ worker.
    Handles both single and bulk DBS checks sequentially.
    """
    from dbs_runner import run_dbs_check_and_download_pdf
    import db

    job_id       = job_data["job_id"]
    db_job_id    = job_data.get("db_job_id")
    tenant_id    = job_data.get("tenant_id", 0)
    user_id      = job_data.get("user_id", 0)
    items        = job_data.get("items", [])
    storage_path = Path(job_data["storage_path"])
    org_name     = job_data.get("organisation_name", "")
    emp_forename = job_data.get("employee_forename", "")
    emp_surname  = job_data.get("employee_surname", "")
    storage_path.mkdir(parents=True, exist_ok=True)

    log.info("[Task] DBS job %s started — %d items tenant=%s", job_id, len(items), tenant_id)
    _rstate(job_id, {"state": "running", "message": "Processing checks..."})

    checked_date = _uk_date()
    pdf_names: List[str] = []
    rows = []
    successful = 0
    failed = 0

    # Init rows
    for i, it in enumerate(items[:100]):
        it_d = it if isinstance(it, dict) else {}
        rows.append({
            "row": i + 1,
            "status": "queued",
            "certificate_number": it_d.get("certificate_number", ""),
            "surname": it_d.get("surname", ""),
            "forename": it_d.get("forename", ""),
            "dob_day": it_d.get("dob_day", ""),
            "dob_month": it_d.get("dob_month", ""),
            "dob_year": it_d.get("dob_year", ""),
            "issue_day": it_d.get("issue_day", ""),
            "issue_month": it_d.get("issue_month", ""),
            "issue_year": it_d.get("issue_year", ""),
            "original_filename": it_d.get("original_filename", f"Row {i+1}"),
            "pdf_filename": "", "pdf_url": "",
            "error": "", "notes": "",
        })
    _rrows(job_id, rows)

    # ── Rerun: only process dirty rows ──────────────────────────────────────
    is_rerun = job_data.get("is_rerun", False)
    dirty_row_nums = set(job_data.get("dirty_row_nums", []))
    old_rows = job_data.get("old_rows", [])

    if is_rerun and old_rows:
        rows = []
        for r in old_rows:
            rnum = r.get("row", 0)
            if rnum in dirty_row_nums:
                rows.append({**r, "status": "queued", "pdf_filename": "", "pdf_url": "", "error": "", "notes": ""})
            else:
                rows.append(r)
        items = [it for it in items if it.get("row_number") in dirty_row_nums]
        _rrows(job_id, rows)

    # Process one at a time
    for i, it in enumerate(items[:100]):
        it_d = it if isinstance(it, dict) else {}
        row  = rows[i]

        cert    = str(it_d.get("certificate_number") or "").strip()
        surname = str(it_d.get("surname") or it_d.get("surname_extracted") or "").strip()
        dob_dd  = str(it_d.get("dob_day") or "").strip()
        dob_mm  = str(it_d.get("dob_month") or "").strip()
        dob_yy  = str(it_d.get("dob_year") or "").strip()

        # Validate
        from app import validate_cert_number
        cert_clean = validate_cert_number(cert)
        if not cert_clean:
            row["status"] = "needs_review"; row["error"] = "Invalid certificate number."; failed += 1
            _rrows(job_id, rows); continue
        if not surname:
            row["status"] = "needs_review"; row["error"] = "Surname missing."; failed += 1
            _rrows(job_id, rows); continue
        if not (dob_dd and dob_mm and dob_yy):
            row["status"] = "needs_review"; row["error"] = "DOB incomplete."; failed += 1
            _rrows(job_id, rows); continue

        row["status"] = "running"
        _rrows(job_id, rows)
        log.info("[Task] DBS job %s row %d cert=%s", job_id, i+1, cert_clean)

        item_dir = storage_path / f"item_{i+1:03d}"
        item_dir.mkdir(parents=True, exist_ok=True)

        try:
            result = run_dbs_check_and_download_pdf(
                organisation_name=org_name,
                employee_forename=emp_forename,
                employee_surname=emp_surname,
                certificate_number=cert_clean,
                applicant_surname=surname,
                dob_day=dob_dd.zfill(2),
                dob_month=dob_mm.zfill(2),
                dob_year=dob_yy,
                out_dir=item_dir,
                headless=True,
            )
        except Exception as e:
            result = {"status": "needs_review", "error": str(e), "pdf_path": ""}

        status = (result.get("status") or "needs_review").strip()
        if status not in ("clear", "needs_review", "portal_unavailable"):
            status = "needs_review"

        row["status"] = status

        if status == "portal_unavailable":
            row["error"] = "DBS portal unavailable. Try again later."
            failed += 1
            _rrows(job_id, rows); continue

        pdf_src = result.get("pdf_path") or ""
        if not pdf_src or not Path(pdf_src).exists():
            if result.get("no_pdf") and status == "needs_review":
                row["notes"] = "Needs Review — no PDF from portal."
                failed += 1
                _rrows(job_id, rows); continue
            row["error"] = result.get("error") or "PDF not produced."
            failed += 1
            _rrows(job_id, rows); continue

        status_label = "Clear" if status == "clear" else "Needs-Review"
        out_sn = _safe_filename(surname.upper(), f"SURNAME{i+1}")
        final_name = _safe_filename(
            f"{out_sn} - {cert_clean} - {status_label} - {checked_date}.pdf",
            f"DBS-Result-{i+1}.pdf"
        )
        final_path = storage_path / final_name
        try:
            if final_path.exists(): final_path.unlink()
            shutil.move(pdf_src, str(final_path))
        except Exception:
            final_path = Path(pdf_src); final_name = final_path.name

        pdf_names.append(final_name)
        row.update({
            "status": status,
            "pdf_filename": final_name,
            "pdf_url": f"/dbs/download/{job_id}/{final_name}",
        })
        successful += 1
        log.info("[Task] DBS row %d done: %s", i+1, status)
        _rrows(job_id, rows)

    # ZIP
    if is_rerun:
        zip_name, zip_ready = _build_zip_from_folder(storage_path, checked_date)
        if zip_ready: log.info("[Task] ZIP rebuilt: %s", zip_name)
        else: zip_name = ""; zip_ready = False
    else:
        zip_name = ""; zip_ready = False
        if len(pdf_names) >= 2:
            zip_name = _safe_filename(f"DBS_Checks_{checked_date}.zip", "DBS_Checks.zip")
            zip_path = storage_path / zip_name
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for pn in pdf_names:
                    fp = storage_path / pn
                    if fp.exists(): zf.write(fp, arcname=pn)
            zip_ready = True
            log.info("[Task] ZIP: %s", zip_name)

    msg = "" if successful > 0 else "No PDFs generated. DBS portal may be unavailable."
    if not successful and not failed:
        msg = "All items were skipped due to validation errors."

    # Merge rerun rows back with full row list
    if is_rerun and old_rows:
        final_rows = []
        rerun_map = {r.get("row",0): r for r in rows if r.get("row",0) in dirty_row_nums}
        for r in old_rows:
            rnum = r.get("row", 0)
            if rnum in rerun_map: final_rows.append(rerun_map[rnum])
            else: final_rows.append(r)
        rows = final_rows

    _rstate(job_id, {
        "state": "done", "rows": rows,
        "zip_ready": zip_ready, "zip_name": zip_name,
        "zip_url": f"/dbs/download/{job_id}/{zip_name}" if zip_ready else "",
        "message": msg, "checked_date": checked_date,
        "successful": successful, "failed": failed,
    })
    log.info("[Task] DBS job %s done — %d ok, %d failed", job_id, successful, failed)

    # Update DB
    if db_job_id:
        db.update_job_status(db_job_id=db_job_id,
                             status="completed" if successful > 0 else "failed",
                             successful_items=successful, failed_items=failed)
    if successful > 0 and tenant_id:
        db.record_usage(tenant_id=tenant_id, user_id=user_id,
                        db_job_id=db_job_id, successful_outputs=successful)

    _schedule_cleanup(storage_path, delay=600)
    return {"ok": True, "successful": successful, "failed": failed}


def _build_zip_from_folder(storage_path, checked_date):
    """Rebuild ZIP from ALL PDFs in folder. Used on rerun."""
    all_pdfs = [f.name for f in storage_path.glob("*.pdf") if f.is_file()]
    if len(all_pdfs) < 2: return "", False
    zip_name = _safe_filename(f"DBS_Checks_{checked_date}.zip", "DBS_Checks.zip")
    zip_path = storage_path / zip_name
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for pn in all_pdfs:
            fp = storage_path / pn
            if fp.exists(): zf.write(fp, arcname=pn)
    return zip_name, True
