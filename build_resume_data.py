from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
XLSX_PATH = (BASE_DIR.parent / "Walid_cv_data_2pages.xlsx").resolve()
OUTPUT_PATH = BASE_DIR / "data" / "resume.json"


def _clean_value(val: Any) -> str:
    if pd.isna(val):
        return ""
    text = str(val).strip()
    return text


def _coerce_year(val: Any):
    try:
        if isinstance(val, str) and not val.strip():
            return None
        num = float(val)
        return int(num) if num.is_integer() else num
    except Exception:
        return None


def _span(start: Any, end: Any) -> str:
    s = _clean_value(start)
    e = _clean_value(end)
    s_num, e_num = _coerce_year(start), _coerce_year(end)
    if s_num is not None and e_num is not None and s_num > e_num:
        s_num, e_num = e_num, s_num
        s, e = str(s_num), str(e_num)
    if not s and not e:
        return ""
    if e.lower() == "current":
        return f"{s} – current" if s else "current"
    if s and e:
        return f"{s} – {e}"
    return s or e


def parse_text_blocks(df: pd.DataFrame) -> Dict[str, str]:
    blocks: Dict[str, str] = {}
    for _, row in df.iterrows():
        key = _clean_value(row.iloc[0])
        val = _clean_value(row.iloc[1])
        if key and key.lower() != "id used for finding text block":
            blocks[key] = val
    return blocks


def parse_entries(df: pd.DataFrame) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    if df.empty:
        return entries
    header_val = _clean_value(df.iloc[0, 0]).lower()
    desc_cols = [c for c in df.columns if "description" in c.lower()]
    # Also treat any columns after the first 6 as potential description fields (Unnamed:* in the workbook)
    for idx, col in enumerate(df.columns):
        if idx >= 6 and col not in desc_cols:
            desc_cols.append(col)
    for _, row in df.iterrows():
        section = _clean_value(row.iloc[0])
        if not section or section.lower() in {"", header_val, "section"}:
            continue
        title = _clean_value(row.get("Main title of the entry"))
        location = _clean_value(row.get("Location the entry occured"))
        org = _clean_value(row.get("Primary institution affiliation for entry"))
        start = row.get("Start date of entry (year)")
        end = row.get("End year of entry. Set to \"current\" if entry is still ongoing.")
        bullets = []
        for col in desc_cols:
            col_lower = str(col).lower()
            # skip filter/helper columns
            if any(key in col_lower for key in ["filter", "resume", "in_resume"]):
                continue
            val = _clean_value(row.get(col))
            if not val:
                continue
            if val.lower() in {"true", "false"}:
                continue
            bullets.append(val)
        entries.append({
            "section": section,
            "title": title,
            "location": location,
            "org": org,
            "span": _span(start, end),
            "bullets": bullets,
        })
    return entries


def parse_skills(df: pd.DataFrame) -> List[Dict[str, Any]]:
    skills: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        name = _clean_value(row.iloc[0])
        if not name or name.lower() in {"skill", "name of language"}:
            continue
        level_raw = row.iloc[1]
        try:
            level = float(level_raw)
        except Exception:
            continue
        skills.append({"name": name, "level": level})
    return skills


def parse_languages(df: pd.DataFrame) -> List[Dict[str, Any]]:
    langs: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        name = _clean_value(row.iloc[0])
        if not name or name.lower() in {"skill", "name of language"}:
            continue
        level_raw = row.iloc[1]
        try:
            level = float(level_raw)
        except Exception:
            continue
        langs.append({"name": name, "level": level})
    return langs


def parse_contact(df: pd.DataFrame) -> List[Dict[str, str]]:
    contacts: List[Dict[str, str]] = []
    for _, row in df.iterrows():
        id_val = _clean_value(row.iloc[0])
        icon = _clean_value(row.iloc[1])
        value = _clean_value(row.iloc[2])
        if not id_val or id_val.lower() == "id of contact section":
            continue
        if icon.lower() == "icon" and value.lower() == "contact":
            continue
        contacts.append({
            "id": id_val,
            "icon": icon,
            "value": value,
        })
    return contacts


def build_payload() -> Dict[str, Any]:
    if not XLSX_PATH.exists():
        raise FileNotFoundError(f"Workbook not found at {XLSX_PATH}")

    xls = pd.ExcelFile(XLSX_PATH)
    required = [
        "text_blocks",
        "entries",
        "computer_science_skills",
        "languages",
        "contact_info",
    ]
    missing = [name for name in required if name not in xls.sheet_names]
    if missing:
        raise ValueError(f"Missing sheets: {', '.join(missing)}")

    data = {
        "text_blocks": parse_text_blocks(xls.parse("text_blocks")),
        "entries": parse_entries(xls.parse("entries")),
        "skills": parse_skills(xls.parse("computer_science_skills")),
        "languages": parse_languages(xls.parse("languages")),
        "contact_info": parse_contact(xls.parse("contact_info")),
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "workbook": XLSX_PATH.name,
    }
    return data


def main() -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = build_payload()
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=True))
    print(f"Wrote {OUTPUT_PATH} from {XLSX_PATH}")


if __name__ == "__main__":
    main()
