from __future__ import annotations

import time
from pathlib import Path
from typing import Dict, Any, Optional

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, Response

CRSC_CHECK = "https://secure.crbonline.gov.uk/crsc/check"

STATUS_CLEAR = "clear"
STATUS_NEEDS_REVIEW = "needs_review"
STATUS_PORTAL_UNAVAILABLE = "portal_unavailable"


def _tag() -> str:
    return time.strftime("%Y%m%d-%H%M%S")


def _s(x: Any) -> str:
    return ("" if x is None else str(x)).strip()


def _goto_with_retry(page, url: str, tries: int = 3, timeout: int = 60000) -> Optional[Response]:
    last_resp: Optional[Response] = None
    last_err: Optional[Exception] = None
    for i in range(tries):
        try:
            resp = page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            last_resp = resp
            if resp is None or resp.status < 400:
                return resp
        except Exception as e:
            last_err = e
        page.wait_for_timeout(900 + i * 700)

    if last_resp is not None:
        raise RuntimeError(f"Failed to load {url} (HTTP {last_resp.status})")
    if last_err is not None:
        raise RuntimeError(f"Failed to load {url}: {last_err}")
    raise RuntimeError(f"Failed to load {url}")


def _click_continue(page, timeout: int = 15000) -> None:
    for sel in [
        "input[type='submit'][value='Continue']",
        "input.button[value='Continue']",
        "button:has-text('Continue')",
    ]:
        loc = page.locator(sel).first
        if loc.count():
            loc.wait_for(state="visible", timeout=timeout)
            loc.click()
            return
    raise RuntimeError("Continue button not found")


def _handle_legal_declaration(page) -> None:
    # Legal Declaration modal (may appear after Step 2 continue)
    try:
        page.wait_for_selector("text=Legal Declaration", timeout=8000)

        try:
            page.get_by_label("I agree with the Legal Declaration").check(timeout=5000)
        except Exception:
            page.locator("input[type='checkbox']").first.check(timeout=5000)

        try:
            page.locator("input[type='submit'][value='Continue']").last.click(timeout=5000)
        except Exception:
            page.locator("button:has-text('Continue')").last.click(timeout=5000)

        page.wait_for_load_state("domcontentloaded", timeout=30000)
        page.wait_for_timeout(800)
    except Exception:
        pass


def _fill_dob_step2(page, dd: str, mm: str, yyyy: str) -> None:
    """
    Based on your trace, Step 2 uses:
      - dayOfBirth
      - monthOfBirth
      - yearOfBirth
    We still keep fallbacks just in case DBS changes.
    """
    # Primary (from trace)
    try:
        page.fill("input[name='dayOfBirth']", dd)
        page.fill("input[name='monthOfBirth']", mm)
        page.fill("input[name='yearOfBirth']", yyyy)
        return
    except Exception:
        pass

    # Fallbacks
    for trio in [
        ("day", "month", "year"),
        ("dobDay", "dobMonth", "dobYear"),
        ("dateOfBirthDay", "dateOfBirthMonth", "dateOfBirthYear"),
    ]:
        try:
            page.fill(f"input[name='{trio[0]}']", dd)
            page.fill(f"input[name='{trio[1]}']", mm)
            page.fill(f"input[name='{trio[2]}']", yyyy)
            return
        except Exception:
            continue

    # last resort: try first 3 small inputs under DOB area
    boxes = page.locator("input[type='text'], input[type='tel'], input[type='number'], input:not([type])")
    # Prefer empty + short maxlength
    candidates = []
    for i in range(min(20, boxes.count())):
        inp = boxes.nth(i)
        name = (inp.get_attribute("name") or "").lower()
        if "cert" in name or name == "surname" or "org" in name or "forename" in name:
            continue
        ml = inp.get_attribute("maxlength") or ""
        if ml in ("2", "4") or name in ("dayofbirth", "monthofbirth", "yearofbirth"):
            candidates.append(inp)
    if len(candidates) >= 3:
        candidates[0].fill(dd)
        candidates[1].fill(mm)
        candidates[2].fill(yyyy)
        return

    raise RuntimeError("Could not fill DOB on Step 2 (selectors did not match).")



def _looks_like_portal_unavailable(err: Exception) -> bool:
    msg = str(err) if err else ""
    msg_l = msg.lower()
    needles = [
        "err_empty_response",
        "err_connection_reset",
        "err_timed_out",
        "err_connection_refused",
        "name or service not known",
        "dns",
        "timed out",
        "timeout",
        "http 502",
        "http 503",
        "http 504",
        "server returned",
        "failed to load",
    ]
    return any(n in msg_l for n in needles)


def _has_any(page, selectors) -> bool:
    for sel in selectors:
        try:
            if page.locator(sel).first.count() > 0:
                return True
        except Exception:
            continue
    return False



def _classify_result_page(page) -> Dict[str, Any]:
    """Classify the portal page into a status and whether a PDF output should be saved."""
    try:
        body_txt = (page.inner_text("body") or "")
    except Exception:
        body_txt = ""
    t = body_txt.lower()

    # Validation / form errors (Needs Review but NO PDF)
    validation_needles = [
        "please fix the following errors",
        "certificate number is invalid",
        "details entered do not match our records",
        "the details entered do not match",
        "surname does not match",
        "date of birth does not match",
    ]
    if any(n in t for n in validation_needles) and ("continue" in t or "back" in t or "errors" in t):
        return {"status": STATUS_NEEDS_REVIEW, "pdf_allowed": False, "reason": "validation"}

    # Clear (explicit wording)
    clear_needles = [
        "did not reveal any information",
        "remains current",
    ]
    if all(n in t for n in clear_needles):
        return {"status": STATUS_CLEAR, "pdf_allowed": True, "reason": "clear"}

    # Needs review results (mismatch / no longer current / could not confirm)
    needs_review_needles = [
        "do not match",
        "no longer current",
        "unable to confirm",
        "cannot confirm",
        "not match",
    ]
    if any(n in t for n in needs_review_needles):
        return {"status": STATUS_NEEDS_REVIEW, "pdf_allowed": True, "reason": "result"}

    # Fallback: if we reached a results-like page, treat as Needs Review with PDF
    return {"status": STATUS_NEEDS_REVIEW, "pdf_allowed": True, "reason": "unknown"}

    return STATUS_NEEDS_REVIEW



def run_dbs_check_and_download_pdf(
    *,
    organisation_name: str,
    employee_forename: str,
    employee_surname: str,
    certificate_number: str,
    applicant_surname: str,
    dob_day: str,
    dob_month: str,
    dob_year: str,
    out_dir: Path,
    headless: bool = False,  # keep visible to reduce bot flags
) -> Dict[str, Any]:

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    tag = _tag()
    trace_path = out_dir / f"dbs-{tag}.trace.zip"
    error_png = out_dir / f"dbs-{tag}.error.png"
    pdf_path = out_dir / f"DBS-Portal-{tag}.pdf"

    org = _s(organisation_name)
    ef = _s(employee_forename)
    es = _s(employee_surname)

    cert = _s(certificate_number)
    app_sur = _s(applicant_surname)

    dd = _s(dob_day).zfill(2)
    mm = _s(dob_month).zfill(2)
    yyyy = _s(dob_year)

    if not (org and ef and es):
        return {"ok": False, "error": "Missing Step 1 fields (organisation/forename/surname).", "trace_path": str(trace_path)}
    if not (cert and app_sur and dd and mm and yyyy):
        return {"ok": False, "error": "Missing Step 2 fields (certificate/applicant surname/DOB).", "trace_path": str(trace_path)}

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        context = browser.new_context(
            viewport={"width": 1280, "height": 800},
            locale="en-GB",
            timezone_id="Europe/London",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            extra_http_headers={"Accept-Language": "en-GB,en;q=0.9"},
        )
        context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")

        context.tracing.start(screenshots=True, snapshots=True, sources=True)
        page = context.new_page()

        try:
            # --- Step 1 ---
            _goto_with_retry(page, CRSC_CHECK, tries=3, timeout=60000)
            page.wait_for_timeout(700)

            page.fill("input[name='organisationName']", org)
            page.fill("input[name='forename']", ef)
            page.fill("input[name='surname']", es)

            _click_continue(page)
            page.wait_for_load_state("domcontentloaded", timeout=30000)
            page.wait_for_timeout(800)

            # --- Step 2 ---
            page.fill("input[name='certificateNumber']", cert)

            # Applicant surname on Step 2 is ALSO name="surname" (confirmed in trace)
            page.fill("input[name='surname']", app_sur)

            _fill_dob_step2(page, dd, mm, yyyy)

            _click_continue(page)
            page.wait_for_load_state("domcontentloaded", timeout=30000)
            page.wait_for_timeout(900)

            # Legal Declaration
            _handle_legal_declaration(page)

            # If server replies "Invalid request." then fail with a clear message
            if "Invalid request" in page.content():
                page.screenshot(path=str(error_png), full_page=True)
                raise RuntimeError("DBS returned 'Invalid request' (portal unavailable / rate-limit).")

            # Classify result (structure-based) and apply output rules
            page.wait_for_load_state("domcontentloaded", timeout=30000)
            page.wait_for_timeout(900)

            classified = _classify_result_page(page)
            status = classified.get("status") or STATUS_NEEDS_REVIEW
            pdf_allowed = bool(classified.get("pdf_allowed", True))
            reason = classified.get("reason") or ""

            if status == STATUS_PORTAL_UNAVAILABLE:
                return {"ok": True, "status": STATUS_PORTAL_UNAVAILABLE, "pdf_path": "", "trace_path": str(trace_path)}

            # Save PDF output for Clear / Needs Review (skip validation form pages)
            if not pdf_allowed:
                return {"ok": True, "status": status, "pdf_path": "", "trace_path": str(trace_path), "no_pdf": True, "reason": reason}

            page.wait_for_timeout(500)
            page.pdf(path=str(pdf_path), print_background=True)

            return {"ok": True, "status": status, "pdf_path": str(pdf_path) if status != STATUS_PORTAL_UNAVAILABLE else "", "trace_path": str(trace_path)}

        except (PWTimeoutError, Exception) as e:
            try:
                page.screenshot(path=str(error_png), full_page=True)
            except Exception:
                pass
            status = STATUS_PORTAL_UNAVAILABLE if _looks_like_portal_unavailable(e) else STATUS_NEEDS_REVIEW
            return {"ok": True if status == STATUS_PORTAL_UNAVAILABLE else False, "status": status, "error": str(e), "error_png": str(error_png), "trace_path": str(trace_path), "pdf_path": ""}
        finally:
            try:
                context.tracing.stop(path=str(trace_path))
            except Exception:
                pass
            context.close()
            browser.close()
