# DBS Check (Render + GitHub)

Single-page web app:
1) User enters **Organisation Name**, **Forename**, **Surname** (manual)
2) User uploads DBS certificate (**PDF or image**)
3) Click **Extract** → shows editable extracted fields
4) Click **Run DBS Check** → runs DBS site automation and **downloads PDF**

Output filename:
- `{Surname} - DBS Check - {IssueDate}.pdf` (IssueDate formatted as `DD.MM.YYYY`)

## Local run

```bash
python -m venv venv
# Windows: venv\Scripts\activate
# macOS/Linux: source venv/bin/activate
pip install -r requirements.txt
playwright install chromium
uvicorn app:app --reload
```

Open: http://127.0.0.1:8000

## Render deploy

- Push this repo to GitHub
- Create a new Render Web Service
- Build command:
  - `pip install -r requirements.txt && playwright install --with-deps chromium`
- Start command:
  - `uvicorn app:app --host 0.0.0.0 --port $PORT`

### Environment variables

For best extraction on scanned PDFs/images:
- `GEMINI_API_KEY` = your Gemini API key
- Optional: `GEMINI_MODEL` (default `gemini-2.0-flash`)

## Notes
- Extraction uses PDF text when available, otherwise uses Gemini Vision for scanned/photos.
- DBS website may change flow/selectors or add CAPTCHA; automation may require updates.
