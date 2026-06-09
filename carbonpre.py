
import streamlit as st
import pandas as pd
import os
import re
import difflib
import time
from io import BytesIO
from datetime import datetime, date
from functools import lru_cache

st.set_page_config(page_title="Power Plant Data Merger", layout="wide")
st.title("⚡ Power Plant Data Merger Tool")
st.write("Upload 30 Daily Generation files and 30 Daily Coal files (XLS/XLSX)")

# Ask user which month/year they're uploading (used as fallback)
col1, col2 = st.columns(2)
with col1:
    month_name = st.selectbox("Select month (fallback)", [
        "January","February","March","April","May","June","July","August",
        "September","October","November","December"
    ], index=datetime.now().month-1)
with col2:
    year_val = st.number_input("Select year (fallback)", min_value=2000, max_value=2100,
                               value=datetime.now().year, step=1)

generation_files = st.file_uploader(
    "Upload 30 Daily Generation Files",
    type=["xls", "xlsx"],
    accept_multiple_files=True
)

coal_files = st.file_uploader(
    "Upload 30 Daily Coal Files",
    type=["xls", "xlsx"],
    accept_multiple_files=True
)

# ====== Performance caches (added, safe) ======
# Cache per-file mapping so we don't re-scan the same Excel multiple times.
coal_file_cache = {}  # key -> { "mapping":..., "raw_date":..., "coal_df":..., "cons_pos":... }

# ===== Helpers =====
def read_excel_auto(file, header=None, nrows=None, usecols=None):
    return pd.read_excel(file, header=header, nrows=nrows, usecols=usecols)

def detect_coal_header(file):
    for hdr in range(0, 10):
        try:
            df = pd.read_excel(file, header=hdr, nrows=10)
            cols = [str(c) for c in df.columns]
            if any("Thermal" in c or "Station" in c for c in cols):
                return hdr
        except Exception:
            continue
    return 0

def detect_generation_col(columns):
    for c in columns:
        cname = str(c).upper()
        if "TODAY" in cname and "ACTUAL" in cname and "APRIL" not in cname and "TILL" not in cname:
            return c
    for c in columns:
        cname = str(c).upper()
        if "ACTUAL" in cname or ("DAILY" in cname and "GEN" in cname):
            return c
    return None

def detect_coal_col(columns):
    for c in columns:
        cname = str(c).upper()
        if "CONSUM" in cname or "COAL" in cname:
            return c
    return None

def format_num(val):
    try:
        if pd.isna(val):
            return ""
        return f"{float(val):.4f}"
    except:
        try:
            s = str(val).strip().replace(",", "").replace("(", "").replace(")", "")
            return f"{float(s):.4f}"
        except:
            return ""

# original normalization function (kept)
def normalize_name(s: str) -> str:
    # robust normalization to avoid space/punctuation issues
    if pd.isna(s):
        return ""
    s = str(s)
    s = s.replace("&", " and ")
    s = re.sub(r"\(.*?\)", " ", s)        # remove parentheses and content
    s = s.replace("stps", "tps")
    s = s.replace("station", "stn")
    s = s.replace("power limited", "power")
    s = re.sub(r"[^A-Za-z0-9\s]", " ", s) # remove punctuation
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

# Cached/fast normalizer to speed repeated normalization calls
@lru_cache(maxsize=10000)
def fast_normalize(s: str) -> str:
    return normalize_name(s)

def token_jaccard(a: str, b: str):
    a_tokens = set(a.split())
    b_tokens = set(b.split())
    if not a_tokens and not b_tokens:
        return 0.0
    inter = a_tokens.intersection(b_tokens)
    union = a_tokens.union(b_tokens)
    return len(inter) / max(1, len(union))

def find_best_match_index(series: pd.Series, plant_name: str, min_ratio: float = 0.55, debug=False):
    if series.empty:
        return None
    n_target = fast_normalize(plant_name)
    candidates = ["" if pd.isna(v) else fast_normalize(v) for v in series.tolist()]
    # exact
    for i, cand in enumerate(candidates):
        if cand and cand == n_target:
            return series.index[i]
    # substring
    for i, cand in enumerate(candidates):
        if not cand:
            continue
        if n_target and n_target in cand:
            return series.index[i]
        if cand in n_target and cand != "":
            return series.index[i]
    # fuzzy
    best_ratio = 0.0
    best_idx = None
    for i, cand in enumerate(candidates):
        if not cand:
            continue
        ratio = difflib.SequenceMatcher(None, n_target, cand).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_idx = series.index[i]
    if debug:
        best_candidate_val = series.get(best_idx, "")
        st.write(f"DEBUG matching '{plant_name}' -> norm '{n_target}' | best_ratio={best_ratio:.2f} | best_candidate='{best_candidate_val}'")
    if best_ratio >= min_ratio:
        return best_idx
    return None

def find_best_match_in_list(name_list, target, min_ratio=0.60, debug=False):
    """
    name_list: iterable of strings (candidates)
    target: string to match to candidates
    returns best candidate string or None
    Uses: exact normalized, substring, fuzzy ratio, and token overlap fallback.
    """
    if not name_list:
        return None
    tnorm = fast_normalize(target)
    best = None
    best_ratio = 0.0
    # 1) exact normalized
    for cand in name_list:
        if not cand:
            continue
        if fast_normalize(cand) == tnorm:
            if debug: st.write("find_best_match_in_list: exact normalized match:", cand)
            return cand
    # 2) substring / containment
    for cand in name_list:
        if not cand:
            continue
        cnorm = fast_normalize(cand)
        if tnorm in cnorm or cnorm in tnorm:
            if debug: st.write("find_best_match_in_list: substring match:", cand)
            return cand
    # 3) fuzzy ratio
    for cand in name_list:
        if not cand:
            continue
        cnorm = fast_normalize(cand)
        ratio = difflib.SequenceMatcher(None, tnorm, cnorm).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best = cand
    if best_ratio >= min_ratio:
        if debug: st.write("find_best_match_in_list: fuzzy match:", best, best_ratio)
        return best
    # 4) token overlap fallback (helps where punctuation/roman numerals differ)
    best_tok = None
    best_tok_score = 0.0
    for cand in name_list:
        if not cand:
            continue
        cnorm = fast_normalize(cand)
        tok_score = token_jaccard(tnorm, cnorm)
        if tok_score > best_tok_score:
            best_tok_score = tok_score
            best_tok = cand
    # threshold for token overlap
    if best_tok_score >= 0.45:
        if debug: st.write("find_best_match_in_list: token overlap match:", best_tok, best_tok_score)
        return best_tok
    return None

def col_letter_to_index(col):
    """Convert Excel column letters (A, B, AA) to zero-based index."""
    col = str(col).upper().strip()
    idx = 0
    for ch in col:
        if 'A' <= ch <= 'Z':
            idx = idx * 26 + (ord(ch) - 64)
    return idx - 1

# DATE PARSING HELPERS
month_map = {m.lower(): i for i, m in enumerate([
    "January","February","March","April","May","June","July","August",
    "September","October","November","December"
], start=1)}

def parse_date_string_to_date(s: str, fallback_year=None):
    if not s or pd.isna(s):
        return None
    s = str(s).strip()
    s = s.strip("() ").replace(".", "")
    patterns = [
        r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        r"(\d{4}-\d{2}-\d{2})",
        r"(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})",
        r"([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})"
    ]
    for pat in patterns:
        m = re.search(pat, s)
        if m:
            candidate = m.group(1)
            for fmt in ("%d/%m/%Y","%d-%m-%Y","%d/%m/%y","%d-%m-%y","%Y-%m-%d","%d %b %Y","%d %B %Y","%B %d %Y"):
                try:
                    return datetime.strptime(candidate, fmt).date()
                except:
                    pass
            try:
                return datetime.strptime(candidate, "%d %B %Y").date()
            except:
                pass
    m = re.search(r"DAY\s*(\d+)", s.upper())
    if m:
        return int(m.group(1))
    return None

def get_date_from_generation_file(file, fallback_month, fallback_year, debug=False):
    try:
        top = pd.read_excel(file, header=None, nrows=10)
    except Exception:
        return None
    candidate_cells = []
    if top.shape[0] > 1:
        candidate_cells.append(top.iloc[1, 0])  # A2
    for r in range(min(10, top.shape[0])):
        candidate_cells.append(top.iloc[r, 0])
    combined = " ".join([str(x) for x in candidate_cells if pd.notna(x)])
    parsed = parse_date_string_to_date(combined, fallback_year)
    # if parse returns a year < 2000 (rare), try to replace with fallback_year
    if isinstance(parsed, date) and parsed.year < 2000:
        try:
            parsed = date(fallback_year, parsed.month, parsed.day)
        except Exception:
            pass
    if isinstance(parsed, date):
        return parsed
    for cell in candidate_cells:
        parsed = parse_date_string_to_date(str(cell), fallback_year)
        if isinstance(parsed, date):
            if parsed.year < 2000:
                try:
                    parsed = date(fallback_year, parsed.month, parsed.day)
                except Exception:
                    pass
            return parsed
        if isinstance(parsed, int):
            return parsed
    fname = os.path.splitext(os.path.basename(getattr(file, "name", str(file))))[0]
    parsed = parse_date_string_to_date(fname, fallback_year)
    if isinstance(parsed, date):
        if parsed.year < 2000:
            try:
                parsed = date(fallback_year, parsed.month, parsed.day)
            except Exception:
                pass
        return parsed
    if isinstance(parsed, int):
        return parsed
    m = re.search(r"DAY\s*(\d+)", fname.upper())
    if m:
        return int(m.group(1))
    m2 = re.search(r"\b(\d{1,2})\b", fname)
    if m2:
        day = int(m2.group(1))
        try:
            return date(fallback_year, month_map.get(fallback_month.lower(), 1), day)
        except:
            pass
    return None

# ==== COAL: Improved date extraction (AN3 primary, then A3, AP3, then top-3-row scan) ====
def get_date_from_coal_file(file, fallback_month, fallback_year, debug=False):
    """
    Try AN3; if not parsed, try A3 and AP3; if not, scan top 3 rows across columns for a date string.
    2-digit years use fallback_year.
    No filename fallback (unless all above fail, then None).
    """
    def col_letter_to_index_local(col):
        col = str(col).upper().strip()
        idx = 0
        for ch in col:
            if 'A' <= ch <= 'Z':
                idx = idx * 26 + (ord(ch) - 64)
        return idx - 1

    # helper to parse a single cell
    def try_parse_cell(cell):
        # avoid returning pd.NaT: treat NA-like as None early
        if pd.isna(cell):
            return None
        # handle datetime-like
        if isinstance(cell, (datetime, date, pd.Timestamp)):
            try:
                # ensure we return a python date object (not pd.Timestamp/NaT)
                parsed_dt = pd.to_datetime(cell)
                if pd.isna(parsed_dt):
                    return None
                return parsed_dt.date()
            except:
                pass
        # excel serial
        if isinstance(cell, (int, float)):
            try:
                dt = pd.to_datetime(cell, unit="d", origin="1899-12-30")
                if pd.isna(dt):
                    return None
                return dt.date()
            except:
                pass
        s = str(cell).strip()
        if not s:
            return None
        s_up = s.upper()
        s_up = re.sub(r"AS[\s\-]*ON", " ", s_up)
        s_up = re.sub(r"AS[\s\-]*AT", " ", s_up)
        s_up = re.sub(r"[()]", " ", s_up)
        s_up = s_up.replace("ON:", " ").replace("ON", " ").strip()
        # search for dd-mm-yyyy etc
        m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})", s_up)
        if m:
            day = int(m.group(1))
            mon = int(m.group(2))
            yr_token = m.group(3)
            if len(yr_token) == 2:
                year = int(fallback_year)
            else:
                year = int(yr_token)
            try:
                return date(year, mon, day)
            except:
                return None
        # textual parse attempts
        for fmt in ("%d %b %Y", "%d %B %Y", "%d %m %Y", "%d %b %y", "%d %B %y"):
            try:
                parsed = datetime.strptime(s_up, fmt).date()
                if parsed.year < 2000:
                    parsed = date(int(fallback_year), parsed.month, parsed.day)
                return parsed
            except:
                pass
        # two-part day-month (use fallback year)
        m2 = re.search(r"(\d{1,2})[/-](\d{1,2})", s_up)
        if m2:
            d = int(m2.group(1)); mo = int(m2.group(2))
            try:
                return date(int(fallback_year), mo, d)
            except:
                return None
        return None

    # Attempt 1: AN3
    try:
        an_idx = col_letter_to_index_local("AN")
        try:
            df_an = pd.read_excel(file, header=None, nrows=3, usecols="AN")
        except Exception:
            # fallback to numeric read up to AN
            usecols = list(range(0, an_idx + 1))
            df_tmp = pd.read_excel(file, header=None, nrows=3, usecols=usecols)
            if df_tmp.shape[1] > an_idx:
                df_an = df_tmp.iloc[:, [an_idx]]
            else:
                df_an = None
    except Exception:
        df_an = None

    if df_an is not None and df_an.shape[0] > 2:
        raw = df_an.iloc[2,0]
        parsed = try_parse_cell(raw)
        if parsed:
            if debug: st.write("COAL date from AN3 ->", parsed, raw)
            return parsed

    # Attempt 2: A3
    try:
        df_a3 = pd.read_excel(file, header=None, nrows=3, usecols="A")
        if df_a3.shape[0] > 2:
            raw = df_a3.iloc[2,0]
            parsed = try_parse_cell(raw)
            if parsed:
                if debug: st.write("COAL date from A3 ->", parsed, raw)
                return parsed
    except Exception:
        pass

    # Attempt 3: AP3 (column AP)
    try:
        df_ap3 = pd.read_excel(file, header=None, nrows=3, usecols="AP")
        if df_ap3.shape[0] > 2:
            raw = df_ap3.iloc[2,0]
            parsed = try_parse_cell(raw)
            if parsed:
                if debug: st.write("COAL date from AP3 ->", parsed, raw)
                return parsed
    except Exception:
        pass

    # Attempt 4: scan top 3 rows across all columns for date-like string
    try:
        top3 = pd.read_excel(file, header=None, nrows=3)
        for r in range(min(3, top3.shape[0])):
            for c in range(top3.shape[1]):
                cell = top3.iloc[r,c]
                if pd.isna(cell):
                    continue
                parsed = try_parse_cell(cell)
                if parsed:
                    if debug: st.write("COAL date from top3 scan ->", parsed, cell, "at", r+1, c+1)
                    return parsed
    except Exception:
        pass

    # Not found
    if debug:
        st.write("COAL: date not found in AN3/A3/AP3/top3 for file", getattr(file,"name",str(file)))
    return None

def format_date_label(d, fallback_month, fallback_year):
    # Guard against pandas NaT/NA and pandas Timestamp
    if d is None or pd.isna(d):
        return None, None
    # If pandas Timestamp convert to python date
    if isinstance(d, pd.Timestamp):
        try:
            pydate = d.to_pydatetime().date()
            return pydate.strftime("%d/%m/%Y"), pydate
        except Exception:
            return None, None
    # If datetime.datetime convert to date
    if isinstance(d, datetime):
        try:
            pydate = d.date()
            return pydate.strftime("%d/%m/%Y"), pydate
        except:
            return None, None
    # If it's a python date already
    if isinstance(d, date):
        return d.strftime("%d/%m/%Y"), d
    # If integer day
    if isinstance(d, int):
        try:
            dt = date(fallback_year, month_map.get(fallback_month.lower(), 1), d)
            return dt.strftime("%d/%m/%Y"), dt
        except:
            return f"DAY {d}", None
    return None, None

# ===== Plant info (paste your full mapping here) =====
plant_info = {
    "PANIPAT TPS": {"State":"Haryana","Region":"NORTHERN","Tech":"Subcritical","Year":1979,"Age":47},
    "RAJIV GANDHI TPS": {"State":"Haryana","Region":"NORTHERN","Tech":"Subcritical (600MW)","Year":2010,"Age":16},
    "YAMUNA NAGAR TPS": {"State":"Haryana","Region":"NORTHERN","Tech":"Subcritical (300MW)","Year":2007,"Age":19},
    "MAHATMA GANDHI TPS": {"State":"Haryana","Region":"NORTHERN","Tech":"Supercritical (660MW)","Year":2012,"Age":14},
    "INDIRA GANDHI STPP": {"State":"Haryana","Region":"NORTHERN","Tech":"Subcritical (500MW)","Year":2010,"Age":16},

    # --- PUNJAB (NORTHERN) ---
    "GH TPS (LEH.MOH.)": {"State":"Punjab","Region":"NORTHERN","Tech":"Subcritical (210MW)","Year":1999,"Age":27},
    "GOINDWAL SAHIB TPP": {"State":"Punjab","Region":"NORTHERN","Tech":"Subcritical (270MW)","Year":2016,"Age":10},
    "ROPAR TPS": {"State":"Punjab","Region":"NORTHERN","Tech":"Subcritical (210MW)","Year":1984,"Age":42},
    "RAJPURA TPP": {"State":"Punjab","Region":"NORTHERN","Tech":"Supercritical (700MW)","Year":2014,"Age":12},
    "TALWANDI SABO TPP": {"State":"Punjab","Region":"NORTHERN","Tech":"Supercritical (660MW)","Year":2014,"Age":12},

    # --- RAJASTHAN (NORTHERN) ---
    "CHHABRA-II TPP": {"State":"Rajasthan","Region":"NORTHERN","Tech":"Supercritical (660MW)","Year":2017,"Age":9},
    "CHHABRA-I PH-1 TPP": {"State":"Rajasthan","Region":"NORTHERN","Tech":"Subcritical (250MW)","Year":2010,"Age":16},
    "CHHABRA-I PH-2 TPP": {"State":"Rajasthan","Region":"NORTHERN","Tech":"Subcritical (250MW)","Year":2011,"Age":15},
    "KALISINDH TPS": {"State":"Rajasthan","Region":"NORTHERN","Tech":"Subcritical (600MW)","Year":2014,"Age":12},
    "KOTA TPS": {"State":"Rajasthan","Region":"NORTHERN","Tech":"Subcritical","Year":1983,"Age":43},
    "SURATGARH STPS": {"State":"Rajasthan","Region":"NORTHERN","Tech":"Supercritical (660MW)","Year":1998,"Age":28},
    "SURATGARH TPS": {"State":"Rajasthan","Region":"NORTHERN","Tech":"Subcritical (250MW)","Year":1998,"Age":28},
    "GIRAL TPS": {"State":"Rajasthan","Region":"NORTHERN","Tech":"Subcritical","Year":2007,"Age":19},
    "ADANI POWER LIMITED KAWAI TPP": {"State":"Rajasthan","Region":"NORTHERN","Tech":"Supercritical (660MW)","Year":2013,"Age":13},
    "JALIPA KAPURDI TPP": {"State":"Rajasthan","Region":"NORTHERN","Tech":"Subcritical (Lignite)","Year":2010,"Age":16},
    "SHREE CEMENT LTD TPS": {"State":"Rajasthan","Region":"NORTHERN","Tech":"Captive/Subcritical","Year":2011,"Age":15},

    # --- UTTAR PRADESH (NORTHERN) ---
    "ANPARA TPS": {"State":"Uttar Pradesh","Region":"NORTHERN","Tech":"Subcritical","Year":1986,"Age":40},
    "HARDUAGANJ TPS": {"State":"Uttar Pradesh","Region":"NORTHERN","Tech":"Subcritical","Year":1942,"Age":84},
    "JAWAHARPUR STPP": {"State":"Uttar Pradesh","Region":"NORTHERN","Tech":"Supercritical (660MW)","Year":2023,"Age":3},
    "OBRA TPS": {"State":"Uttar Pradesh","Region":"NORTHERN","Tech":"Subcritical","Year":1980,"Age":46},
    "PARICHHA TPS": {"State":"Uttar Pradesh","Region":"NORTHERN","Tech":"Subcritical","Year":1984,"Age":42},
    "ANPARA C TPS": {"State":"Uttar Pradesh","Region":"NORTHERN","Tech":"Subcritical (600MW)","Year":2011,"Age":15},
    "BARKHERA TPS": {"State":"Uttar Pradesh","Region":"NORTHERN","Tech":"Subcritical","Year":2011,"Age":15},
    "KHAMBARKHERA TPS": {"State":"Uttar Pradesh","Region":"NORTHERN","Tech":"Subcritical","Year":2011,"Age":15},
    "KUNDARKI TPS": {"State":"Uttar Pradesh","Region":"NORTHERN","Tech":"Subcritical","Year":2012,"Age":14},
    "MAQSOODPUR TPS": {"State":"Uttar Pradesh","Region":"NORTHERN","Tech":"Subcritical","Year":2011,"Age":15},
    "PRAYAGRAJ TPP": {"State":"Uttar Pradesh","Region":"NORTHERN","Tech":"Subcritical","Year":2015,"Age":11},
    "ROSA TPP Ph-I": {"State":"Uttar Pradesh","Region":"NORTHERN","Tech":"Subcritical (300MW)","Year":2009,"Age":17},
    "UTRAULA TPS": {"State":"Uttar Pradesh","Region":"NORTHERN","Tech":"Subcritical","Year":2012,"Age":14},
    "DADRI (NCTPP)": {"State":"Uttar Pradesh","Region":"NORTHERN","Tech":"Subcritical","Year":1991,"Age":35},
    "GHATAMPUR TPP": {"State":"Uttar Pradesh","Region":"NORTHERN","Tech":"Supercritical (660MW)","Year":2024,"Age":2},
    "KHURJA TPP": {"State":"Uttar Pradesh","Region":"NORTHERN","Tech":"Subcritical/Supercritical","Year":2025,"Age":1},
    "MEJA STPP": {"State":"Uttar Pradesh","Region":"NORTHERN","Tech":"Supercritical (660MW)","Year":2018,"Age":8},
    "RIHAND STPS": {"State":"Uttar Pradesh","Region":"NORTHERN","Tech":"Subcritical (500MW)","Year":1988,"Age":38},
    "SINGRAULI STPS": {"State":"Uttar Pradesh","Region":"NORTHERN","Tech":"Subcritical (200/500MW)","Year":1982,"Age":44},
    "TANDA TPS": {"State":"Uttar Pradesh","Region":"NORTHERN","Tech":"Subcritical","Year":1988,"Age":38},
    "UNCHAHAR TPS": {"State":"Uttar Pradesh","Region":"NORTHERN","Tech":"Subcritical","Year":1988,"Age":38},

    # --- CHHATTISGARH (WESTERN) ---
    "DSPM TPS": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Subcritical","Year":2007,"Age":19},
    "KORBA-WEST TPS": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Subcritical","Year":1983,"Age":43},
    "MARWA TPS": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Subcritical (500MW)","Year":2016,"Age":10},
    "ADANI POWER LIMITED RAIGARH TPP": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Subcritical (600MW)","Year":2014,"Age":12},
    "ADANI POWER LIMITED RAIPUR TPP": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Supercritical (685MW)","Year":2016,"Age":10},
    "AKALTARA TPS": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Subcritical (600MW)","Year":2013,"Age":13},
    "BALCO TPS": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Subcritical (Captive)","Year":1988,"Age":38},
    "BANDAKHAR TPP": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Subcritical","Year":2015,"Age":11},
    "BARADARHA TPS": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Subcritical","Year":2014,"Age":12},
    "BINJKOTE TPP": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Subcritical (600MW)","Year":2017,"Age":9},
    "CHAKABURA TPP": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Subcritical","Year":2007,"Age":19},
    "KASAIPALLI TPP": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Subcritical","Year":2011,"Age":15},
    "KATGHORA TPP": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Subcritical","Year":2012,"Age":14},
    "NAWAPARA TPP": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Subcritical","Year":2016,"Age":10},
    "OP JINDAL TPS": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Subcritical (250/600MW)","Year":2007,"Age":19},
    "PATHADI TPP": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Subcritical","Year":2009,"Age":17},
    "RATIJA TPS": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Subcritical","Year":2013,"Age":13},
    "SALORA TPP": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Subcritical","Year":2015,"Age":11},
    "SINGHITARAI TPP": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Subcritical","Year":2025,"Age":1},
    "SVPL TPP": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Subcritical","Year":2011,"Age":15},
    "SWASTIK KORBA TPP": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Subcritical","Year":2015,"Age":11},
    "TAMNAR TPP": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Subcritical","Year":2007,"Age":19},
    "UCHPINDA TPP": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Subcritical","Year":2015,"Age":11},
    "BHILAI TPS": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Subcritical","Year":1982,"Age":44},
    "KORBA STPS": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Subcritical (500MW)","Year":1983,"Age":43},
    "LARA TPP": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Supercritical (800MW)","Year":2019,"Age":7},
    "SIPAT STPS": {"State":"Chhatisgarh","Region":"WESTERN","Tech":"Supercritical (660MW)","Year":2008,"Age":18},

    # --- GUJARAT (WESTERN) ---
    "AKRIMOTA LIG TPS": {"State":"Gujarat","Region":"WESTERN","Tech":"Subcritical (CFBC)","Year":2005,"Age":21},
    "BHAVNAGAR CFBC TPP": {"State":"Gujarat","Region":"WESTERN","Tech":"CFBC","Year":2016,"Age":10},
    "GANDHI NAGAR TPS": {"State":"Gujarat","Region":"WESTERN","Tech":"Subcritical","Year":1977,"Age":49},
    "UKAI TPS": {"State":"Gujarat","Region":"WESTERN","Tech":"Subcritical","Year":1976,"Age":50},
    "WANAKBORI TPS": {"State":"Gujarat","Region":"WESTERN","Tech":"Subcritical","Year":1982,"Age":44},
    "SIKKA REP. TPS": {"State":"Gujarat","Region":"WESTERN","Tech":"Subcritical","Year":2015,"Age":11},
    "KUTCH LIG. TPS": {"State":"Gujarat","Region":"WESTERN","Tech":"Lignite/Small","Year":1997,"Age":29},
    "ADANI POWER LIMITED MUNDRA TPP - III": {"State":"Gujarat","Region":"WESTERN","Tech":"Supercritical (660MW)","Year":2010,"Age":16},
    "ADANI POWER LIMITED MUNDRA TPP - I & II": {"State":"Gujarat","Region":"WESTERN","Tech":"Subcritical (330MW)","Year":2010,"Age":16},
    "MUNDRA UMTPP": {"State":"Gujarat","Region":"WESTERN","Tech":"Ultra-Supercritical (800MW)","Year":2012,"Age":14},
    "SABARMATI (D-F STATIONS)": {"State":"Gujarat","Region":"WESTERN","Tech":"District/repowering","Year":1984,"Age":42},
    "SALAYA TPP": {"State":"Gujarat","Region":"WESTERN","Tech":"Subcritical (600MW)","Year":2012,"Age":14},
    "SURAT LIG. TPS": {"State":"Gujarat","Region":"WESTERN","Tech":"Lignite/Small","Year":1999,"Age":27},

    # --- MADHYA PRADESH (WESTERN) ---
    "AMARKANTAK EXT TPS": {"State":"Madhya Pradesh","Region":"WESTERN","Tech":"Subcritical","Year":2008,"Age":18},
    "SANJAY GANDHI TPS": {"State":"Madhya Pradesh","Region":"WESTERN","Tech":"Subcritical","Year":1993,"Age":33},
    "SATPURA TPS": {"State":"Madhya Pradesh","Region":"WESTERN","Tech":"Subcritical","Year":2013,"Age":13},
    "SHREE SINGAJI TPP": {"State":"Madhya Pradesh","Region":"WESTERN","Tech":"Supercritical (660MW)","Year":2014,"Age":12},
    "ANUPPUR TPP": {"State":"Madhya Pradesh","Region":"WESTERN","Tech":"Subcritical (600MW)","Year":2016,"Age":10},
    "BINA TPS": {"State":"Madhya Pradesh","Region":"WESTERN","Tech":"Subcritical","Year":2012,"Age":14},
    "MAHAN TPP": {"State":"Madhya Pradesh","Region":"WESTERN","Tech":"Subcritical (600MW)","Year":2012,"Age":14},
    "NIGRI TPP": {"State":"Madhya Pradesh","Region":"WESTERN","Tech":"Supercritical (660MW)","Year":2014,"Age":12},
    "NIWARI TPP": {"State":"Madhya Pradesh","Region":"WESTERN","Tech":"Subcritical","Year":2012,"Age":14},
    "SASAN UMTPP": {"State":"Madhya Pradesh","Region":"WESTERN","Tech":"Supercritical (660MW)","Year":2013,"Age":13},
    "GADARWARA TPP": {"State":"Madhya Pradesh","Region":"WESTERN","Tech":"Supercritical (800MW)","Year":2019,"Age":7},
    "KHARGONE STPP": {"State":"Madhya Pradesh","Region":"WESTERN","Tech":"Ultra-Supercritical (660MW)","Year":2019,"Age":7},
    "SEIONI TPP": {"State":"Madhya Pradesh","Region":"WESTERN","Tech":"Subcritical","Year":2016,"Age":10},
    "VINDHYACHAL STPS": {"State":"Madhya Pradesh","Region":"WESTERN","Tech":"Subcritical/Supercritical","Year":1987,"Age":39},

    # --- MAHARASHTRA (WESTERN) ---
    "BHUSAWAL TPS": {"State":"Maharashtra","Region":"WESTERN","Tech":"Subcritical","Year":1968,"Age":58},
    "CHANDRAPUR(MAHARASHTRA) STPS": {"State":"Maharashtra","Region":"WESTERN","Tech":"Subcritical/Supercritical","Year":1984,"Age":42},
    "KHAPARKHEDA TPS": {"State":"Maharashtra","Region":"WESTERN","Tech":"Subcritical","Year":1989,"Age":37},
    "KORADI TPS": {"State":"Maharashtra","Region":"WESTERN","Tech":"Supercritical (660MW)","Year":1974,"Age":52},
    "NASIK TPS": {"State":"Maharashtra","Region":"WESTERN","Tech":"Subcritical","Year":1970,"Age":56},
    "PARAS TPS": {"State":"Maharashtra","Region":"WESTERN","Tech":"Subcritical","Year":1961,"Age":65},
    "PARLI TPS": {"State":"Maharashtra","Region":"WESTERN","Tech":"Subcritical","Year":1971,"Age":55},
    "ADANI POWER LIMITED TIRODA TPP": {"State":"Maharashtra","Region":"WESTERN","Tech":"Supercritical (660MW)","Year":2012,"Age":14},
    "AMRAVATI TPS": {"State":"Maharashtra","Region":"WESTERN","Tech":"Subcritical (270MW)","Year":2013,"Age":13},
    "BELA TPS": {"State":"Maharashtra","Region":"WESTERN","Tech":"Subcritical","Year":2013,"Age":13},
    "BUTIBORI TPP": {"State":"Maharashtra","Region":"WESTERN","Tech":"Subcritical","Year":2012,"Age":14},
    "DAHANU TPS": {"State":"Maharashtra","Region":"WESTERN","Tech":"Subcritical (250MW)","Year":1995,"Age":31},
    "DHARIWAL TPP": {"State":"Maharashtra","Region":"WESTERN","Tech":"Subcritical","Year":2014,"Age":12},
    "GEPL TPP Ph-I": {"State":"Maharashtra","Region":"WESTERN","Tech":"Subcritical","Year":2012,"Age":14},
    "GMR WARORA TPS": {"State":"Maharashtra","Region":"WESTERN","Tech":"Subcritical (300MW)","Year":2013,"Age":13},
    "JSW RATNAGIRI TPP": {"State":"Maharashtra","Region":"WESTERN","Tech":"Subcritical (300MW)","Year":2010,"Age":16},
    "LANCO VIDARBHA TPP": {"State":"Maharashtra","Region":"WESTERN","Tech":"Subcritical","Year":2011,"Age":15},
    "MIHAN TPS": {"State":"Maharashtra","Region":"WESTERN","Tech":"Subcritical","Year":2011,"Age":15},
    "NASIK (P) TPS": {"State":"Maharashtra","Region":"WESTERN","Tech":"Subcritical","Year":1970,"Age":56},
    "SHIRPUR TPP": {"State":"Maharashtra","Region":"WESTERN","Tech":"Subcritical","Year":2017,"Age":9},
    "TROMBAY TPS": {"State":"Maharashtra","Region":"WESTERN","Tech":"Subcritical","Year":1956,"Age":70},
    "WARDHA WARORA TPP": {"State":"Maharashtra","Region":"WESTERN","Tech":"Subcritical","Year":2010,"Age":16},
    "MAUDA TPS": {"State":"Maharashtra","Region":"WESTERN","Tech":"Supercritical (660MW)","Year":2013,"Age":13},
    "SOLAPUR STPS": {"State":"Maharashtra","Region":"WESTERN","Tech":"Supercritical (660MW)","Year":2017,"Age":9},

    # --- ANDHRA PRADESH (SOUTHERN) ---
    "SGPL TPP": {"State":"Andhra Pradesh","Region":"SOUTHERN","Tech":"Supercritical (660MW)","Year":2016,"Age":10},
    "PAINAMPURAM TPP": {"State":"Andhra Pradesh","Region":"SOUTHERN","Tech":"Supercritical (660MW)","Year":2016,"Age":10},
    "Dr. N.TATA RAO TPS": {"State":"Andhra Pradesh","Region":"SOUTHERN","Tech":"Subcritical","Year":1979,"Age":47},
    "RAYALASEEMA TPS": {"State":"Andhra Pradesh","Region":"SOUTHERN","Tech":"Subcritical","Year":1994,"Age":32},
    "DAMODARAM SANJEEVAIAH TPS": {"State":"Andhra Pradesh","Region":"SOUTHERN","Tech":"Supercritical (800MW)","Year":2014,"Age":12},
    "SIMHAPURI TPS": {"State":"Andhra Pradesh","Region":"SOUTHERN","Tech":"Subcritical (150MW)","Year":2012,"Age":14},
    "THAMMINAPATNAM TPS": {"State":"Andhra Pradesh","Region":"SOUTHERN","Tech":"Subcritical","Year":2012,"Age":14},
    "VIZAG TPP": {"State":"Andhra Pradesh","Region":"SOUTHERN","Tech":"Subcritical","Year":2015,"Age":11},

    # --- KARNATAKA (SOUTHERN) ---
    "BELLARY TPS": {"State":"Karnataka","Region":"SOUTHERN","Tech":"Subcritical","Year":1986,"Age":40},
    "RAICHUR TPS": {"State":"Karnataka","Region":"SOUTHERN","Tech":"Subcritical","Year":1985,"Age":41},
    "YERMARUS TPP": {"State":"Karnataka","Region":"SOUTHERN","Tech":"Supercritical (800MW)","Year":2015,"Age":11},
    "ADANI POWER LIMITED UDUPI TPP": {"State":"Karnataka","Region":"SOUTHERN","Tech":"Subcritical (600MW)","Year":2012,"Age":14},
    "TORANGALLU TPS(SBU-I)": {"State":"Karnataka","Region":"SOUTHERN","Tech":"Subcritical","Year":2000,"Age":26},
    "TORANGALLU TPS(SBU-II)": {"State":"Karnataka","Region":"SOUTHERN","Tech":"Subcritical","Year":2000,"Age":26},
    "KUDGI STPP": {"State":"Karnataka","Region":"SOUTHERN","Tech":"Supercritical (800MW)","Year":2016,"Age":10},

    # --- TAMIL NADU (SOUTHERN) ---
    "METTUR TPS": {"State":"Tamil Nadu","Region":"SOUTHERN","Tech":"Subcritical","Year":1987,"Age":39},
    "METTUR TPS - II": {"State":"Tamil Nadu","Region":"SOUTHERN","Tech":"Subcritical","Year":1996,"Age":30},
    "NORTH CHENNAI TPS": {"State":"Tamil Nadu","Region":"SOUTHERN","Tech":"Subcritical","Year":1994,"Age":32},
    "TUTICORIN TPS": {"State":"Tamil Nadu","Region":"SOUTHERN","Tech":"Subcritical","Year":1979,"Age":47},
    "NTPL TUTICORIN TPP": {"State":"Tamil Nadu","Region":"SOUTHERN","Tech":"Subcritical (500MW)","Year":2012,"Age":14},
    "UDANGUDI TPP": {"State":"Tamil Nadu","Region":"SOUTHERN","Tech":"Supercritical (660MW)","Year":2025,"Age":1},
    "ITPCL TPP": {"State":"Tamil Nadu","Region":"SOUTHERN","Tech":"Subcritical","Year":2015,"Age":11},
    "MUTHIARA TPP": {"State":"Tamil Nadu","Region":"SOUTHERN","Tech":"Subcritical","Year":2014,"Age":12},
    "NEYVELI TPS(Z)": {"State":"Tamil Nadu","Region":"SOUTHERN","Tech":"Lignite/Subcritical","Year":2002,"Age":24},
    "TUTICORIN (P) TPP": {"State":"Tamil Nadu","Region":"SOUTHERN","Tech":"Subcritical","Year":1998,"Age":28},
    "TUTICORIN TPP ST-IV": {"State":"Tamil Nadu","Region":"SOUTHERN","Tech":"Subcritical","Year":2005,"Age":21},
    "NEYVELI (EXT) TPS": {"State":"Tamil Nadu","Region":"SOUTHERN","Tech":"Lignite","Year":2019,"Age":7},
    "NEYVELI NEW TPP": {"State":"Tamil Nadu","Region":"SOUTHERN","Tech":"Lignite (Supercritical)","Year":2021,"Age":5},
    "NEYVELI TPS-II": {"State":"Tamil Nadu","Region":"SOUTHERN","Tech":"Lignite","Year":1986,"Age":40},
    "NEYVELI TPS-II EXP": {"State":"Tamil Nadu","Region":"SOUTHERN","Tech":"Lignite","Year":2015,"Age":11},
    "VALLUR TPP": {"State":"Tamil Nadu","Region":"SOUTHERN","Tech":"Subcritical (500MW)","Year":2012,"Age":14},

    # --- TELANGANA (SOUTHERN) ---
    "SINGARENI TPP": {"State":"Telangana","Region":"SOUTHERN","Tech":"Subcritical (600MW)","Year":2016,"Age":10},
    "BHADRADRI TPP": {"State":"Telangana","Region":"SOUTHERN","Tech":"Subcritical (270MW)","Year":2020,"Age":6},
    "KAKATIYA TPS": {"State":"Telangana","Region":"SOUTHERN","Tech":"Subcritical/Supercritical","Year":2010,"Age":16},
    "KOTHAGUDEM TPS (NEW)": {"State":"Telangana","Region":"SOUTHERN","Tech":"Subcritical","Year":2005,"Age":21},
    "KOTHAGUDEM TPS (STAGE-7)": {"State":"Telangana","Region":"SOUTHERN","Tech":"Subcritical","Year":2018,"Age":8},
    "RAMAGUNDEM-B TPS": {"State":"Telangana","Region":"SOUTHERN","Tech":"Subcritical","Year":2014,"Age":12},
    "YADADRI TPS": {"State":"Telangana","Region":"SOUTHERN","Tech":"Supercritical","Year":2023,"Age":3},
    "RAMAGUNDEM STPS": {"State":"Telangana","Region":"SOUTHERN","Tech":"Subcritical (500MW)","Year":1971,"Age":55},
    "TELANGANA STPP PH-1": {"State":"Telangana","Region":"SOUTHERN","Tech":"Supercritical (660MW)","Year":2023,"Age":3},

    # --- BIHAR (EASTERN) ---
    "BARAUNI TPS": {"State":"Bihar","Region":"EASTERN","Tech":"Subcritical","Year":1966,"Age":60},
    "BARH STPS": {"State":"Bihar","Region":"EASTERN","Tech":"Supercritical (660MW)","Year":2021,"Age":5},
    "BUXAR TPP": {"State":"Bihar","Region":"EASTERN","Tech":"Subcritical","Year":2015,"Age":11},
    "KAHALGAON TPS": {"State":"Bihar","Region":"EASTERN","Tech":"Subcritical (500MW)","Year":1992,"Age":34},
    "MUZAFFARPUR TPS": {"State":"Bihar","Region":"EASTERN","Tech":"Subcritical","Year":1985,"Age":41},
    "NABINAGAR STPP": {"State":"Bihar","Region":"EASTERN","Tech":"Supercritical (660MW)","Year":2019,"Age":7},
    "NABINAGAR TPP": {"State":"Bihar","Region":"EASTERN","Tech":"Supercritical (660MW)","Year":2019,"Age":7},

    # --- JHARKHAND (EASTERN) ---
    "TENUGHAT TPS": {"State":"Jharkhand","Region":"EASTERN","Tech":"Subcritical","Year":1996,"Age":30},
    "JOJOBERA TPS": {"State":"Jharkhand","Region":"EASTERN","Tech":"Subcritical","Year":1996,"Age":30},
    "MAHADEV PRASAD STPP": {"State":"Jharkhand","Region":"EASTERN","Tech":"Subcritical","Year":2016,"Age":10},
    "MAITHON RB TPP": {"State":"Jharkhand","Region":"EASTERN","Tech":"Subcritical (525MW)","Year":2011,"Age":15},
    "MAITRISHI USHA TPS": {"State":"Jharkhand","Region":"EASTERN","Tech":"Subcritical","Year":2012,"Age":14},
    "BOKARO TPS A EXP": {"State":"Jharkhand","Region":"EASTERN","Tech":"Subcritical (500MW)","Year":2016,"Age":10},
    "CHANDRAPURA(DVC) TPS": {"State":"Jharkhand","Region":"EASTERN","Tech":"Subcritical","Year":1964,"Age":62},
    "KODARMA TPP": {"State":"Jharkhand","Region":"EASTERN","Tech":"Subcritical (500MW)","Year":2013,"Age":13},
    "NORTH KARANPURA TPP": {"State":"Jharkhand","Region":"EASTERN","Tech":"Supercritical (660MW)","Year":2023,"Age":3},
    "PATRATU STPP": {"State":"Jharkhand","Region":"EASTERN","Tech":"Subcritical","Year":2018,"Age":8},

    # --- ODISHA (EASTERN) ---
    "IB VALLEY TPS": {"State":"Odisha","Region":"EASTERN","Tech":"Subcritical","Year":1994,"Age":32},
    "DERANG TPP": {"State":"Odisha","Region":"EASTERN","Tech":"Subcritical","Year":2014,"Age":12},
    "KAMALANGA TPS": {"State":"Odisha","Region":"EASTERN","Tech":"Subcritical (350MW)","Year":2013,"Age":13},
    "LANCO BABANDH TPP": {"State":"Odisha","Region":"EASTERN","Tech":"Subcritical","Year":2013,"Age":13},
    "UTKAL TPP (IND BARATH)": {"State":"Odisha","Region":"EASTERN","Tech":"Supercritical/Subcritical","Year":2016,"Age":10},
    "VEDANTA TPP": {"State":"Odisha","Region":"EASTERN","Tech":"Subcritical (600MW)","Year":2010,"Age":16},
    "DARLIPALI STPS": {"State":"Odisha","Region":"EASTERN","Tech":"Supercritical (800MW)","Year":2019,"Age":7},
    "TALCHER STPS": {"State":"Odisha","Region":"EASTERN","Tech":"Subcritical (500MW)","Year":1995,"Age":31},

    # --- WEST BENGAL (EASTERN) ---
    "D.P.L. TPS": {"State":"West Bengal","Region":"EASTERN","Tech":"Subcritical","Year":1960,"Age":66},
    "BAKRESWAR TPS": {"State":"West Bengal","Region":"EASTERN","Tech":"Subcritical","Year":1999,"Age":27},
    "BANDEL TPS": {"State":"West Bengal","Region":"EASTERN","Tech":"Subcritical (Old)","Year":1965,"Age":61},
    "KOLAGHAT TPS": {"State":"West Bengal","Region":"EASTERN","Tech":"Subcritical","Year":1984,"Age":42},
    "SAGARDIGHI TPS": {"State":"West Bengal","Region":"EASTERN","Tech":"Subcritical/Supercritical","Year":2008,"Age":18},
    "SANTALDIH TPS": {"State":"West Bengal","Region":"EASTERN","Tech":"Subcritical","Year":1974,"Age":52},
    "SAGARDIGHI TPP ST-III": {"State":"West Bengal","Region":"EASTERN","Tech":"Supercritical/Subcritical","Year":2016,"Age":10},
    "BUDGE BUDGE TPS": {"State":"West Bengal","Region":"EASTERN","Tech":"Subcritical","Year":1997,"Age":29},
    "DISHERGARH TPP": {"State":"West Bengal","Region":"EASTERN","Tech":"Subcritical","Year":2012,"Age":14},
    "HIRANMAYE TPP": {"State":"West Bengal","Region":"EASTERN","Tech":"Subcritical","Year":2017,"Age":9},
    "SOUTHERN REPL. TPS": {"State":"West Bengal","Region":"EASTERN","Tech":"Subcritical (replacement)","Year":1990,"Age":36},
    "TITAGARH TPS": {"State":"West Bengal","Region":"EASTERN","Tech":"Subcritical","Year":1983,"Age":43},
    "DURGAPUR STEEL TPS": {"State":"West Bengal","Region":"EASTERN","Tech":"Subcritical (captive)","Year":2012,"Age":14},
    "FARAKKA STPS": {"State":"West Bengal","Region":"EASTERN","Tech":"Subcritical (500MW)","Year":1986,"Age":40},
    "MEJIA TPS": {"State":"West Bengal","Region":"EASTERN","Tech":"Subcritical (500MW)","Year":1996,"Age":30},
    "RAGHUNATHPUR TPP": {"State":"West Bengal","Region":"EASTERN","Tech":"Subcritical (600MW)","Year":2014,"Age":12},

    # --- ASSAM (NORTH EASTERN) ---
    "BONGAIGAON TPP": {"State":"Assam","Region":"NORTH EASTERN","Tech":"Subcritical","Year":2015,"Age":11},
}

# precompute list of plant_info keys for faster iterations
plant_keys_list = list(plant_info.keys())

# Optional debug checkbox for matching
show_debug = st.checkbox("Show matching debug output", value=False)

# ===== Build / reuse combined plant list (Option A: always show plant_info keys) =====
def _make_upload_key(g_gen_files, g_coal_files):
    def key_for_list(files):
        out = []
        for f in files:
            fname = getattr(f, "name", str(f))
            fsize = getattr(f, "size", None)
            out.append((fname, fsize))
        return tuple(sorted(out))
    return (key_for_list(g_gen_files or []), key_for_list(g_coal_files or []))

initial_menu = sorted(list(plant_info.keys()))

current_upload_key = _make_upload_key(generation_files, coal_files)
if "upload_key" not in st.session_state or st.session_state.get("upload_key") != current_upload_key:
    plants_gen = set()
    plants_coal = set()

    if generation_files:
        for f in generation_files:
            try:
                try:
                    tmp = read_excel_auto(f, header=3, nrows=1000, usecols=[0])
                except Exception:
                    tmp = read_excel_auto(f, header=None, nrows=1000, usecols=[0])
                if tmp.shape[1] >= 1:
                    values = tmp.iloc[:, 0].dropna().astype(str).str.strip().tolist()
                    for v in values:
                        if v and len(v) > 1:
                            plants_gen.add(v)
            except Exception:
                continue

    if coal_files:
        for f in coal_files:
            try:
                header_row = detect_coal_header(f)
                tmp = read_excel_auto(f, header=header_row, nrows=500)
                tmp.columns = [str(c).strip() for c in tmp.columns]
                plant_cols = [c for c in tmp.columns if "Thermal" in c or "Station" in c]
                if plant_cols:
                    vals = tmp[plant_cols[0]].dropna().astype(str).str.strip().tolist()
                    for v in vals:
                        if v and len(v) > 1:
                            plants_coal.add(v)
                else:
                    if tmp.shape[1] >= 1:
                        vals = tmp.iloc[:, 0].dropna().astype(str).str.strip().tolist()
                        for v in vals:
                            if v and len(v) > 1:
                                plants_coal.add(v)
            except Exception:
                continue

    plant_info_keys = set(plant_info.keys())
    combined = sorted(list(plant_info_keys))

    st.session_state["combined_plants"] = combined
    st.session_state["upload_key"] = current_upload_key

combined_plants = st.session_state.get("combined_plants", initial_menu)

# ===== Multiselects (persist selections in session_state) =====
if "selected_gen_plants" not in st.session_state:
    st.session_state["selected_gen_plants"] = ["All"]
if "selected_coal_plants" not in st.session_state:
    st.session_state["selected_coal_plants"] = ["All"]

menu = ["All"] + combined_plants
st.session_state["menu_options"] = menu

def _on_change_gen():
    sel = st.session_state.get("selected_gen_plants", [])
    menu_local = st.session_state.get("menu_options", menu)
    if "All" in sel and set(sel) != set(menu_local):
        st.session_state["selected_gen_plants"] = menu_local.copy()

def _on_change_coal():
    sel = st.session_state.get("selected_coal_plants", [])
    menu_local = st.session_state.get("menu_options", menu)
    if "All" in sel and set(sel) != set(menu_local):
        st.session_state["selected_coal_plants"] = menu_local.copy()

selected_gen = st.multiselect(
    "Select Plants from Generation Files",
    menu,
    default=st.session_state["selected_gen_plants"],
    key="selected_gen_plants",
    on_change=_on_change_gen
)
selected_coal = st.multiselect(
    "Select Plants from Coal Files",
    menu,
    default=st.session_state["selected_coal_plants"],
    key="selected_coal_plants",
    on_change=_on_change_coal
)

# interpret "All"
if "All" in st.session_state.get("selected_gen_plants", []):
    selected_gen_plants = combined_plants.copy()
else:
    selected_gen_plants = st.session_state.get("selected_gen_plants", []).copy()

if "All" in st.session_state.get("selected_coal_plants", []):
    selected_coal_plants = combined_plants.copy()
else:
    selected_coal_plants = st.session_state.get("selected_coal_plants", []).copy()

# ===== Processing with progress bar =====
if st.button("Generate Final Dataset"):
    start_time = time.time()
    generation_files_sorted = sorted(generation_files, key=lambda x: x.name) if generation_files else []
    coal_files_sorted = sorted(coal_files, key=lambda x: x.name) if coal_files else []
    total_steps = max(1, len(selected_gen_plants) * max(1, len(generation_files_sorted)) + len(selected_coal_plants) * max(1, len(coal_files_sorted)))
    progress_bar = st.progress(0)
    status_text = st.empty()
    step_count = 0

    # GENERATION TABLE
    gen_data = []
    for plant in selected_gen_plants:
        for gen_file in generation_files_sorted:
            step_count += 1
            percent = int(step_count / total_steps * 100)
            elapsed = time.time() - start_time
            progress_bar.progress(min(percent, 100))
            status_text.text(f"Processing generation files... {step_count}/{total_steps} ({percent}%) — Elapsed {elapsed:.1f}s")

            raw_date = get_date_from_generation_file(gen_file, month_name, year_val, debug=show_debug)
            date_label, date_obj = format_date_label(raw_date, month_name, year_val)
            if date_label is None:
                date_label = os.path.splitext(os.path.basename(gen_file.name))[0]

            try:
                gen_df = read_excel_auto(gen_file, header=3)
            except Exception:
                gen_df = read_excel_auto(gen_file, header=0)
            gen_df.columns = [str(c).strip() for c in gen_df.columns]

            gen_plant_series = gen_df.iloc[:, 0].astype(str)
            match_idx = find_best_match_index(gen_plant_series, plant, min_ratio=0.55, debug=show_debug)

            todays_actual_val = ""
            if match_idx is not None:
                todays_actual_col = detect_generation_col(gen_df.columns)
                if todays_actual_col:
                    try:
                        todays_actual_val = format_num(gen_df.loc[match_idx, todays_actual_col])
                    except Exception:
                        todays_actual_val = ""
            else:
                if show_debug:
                    st.write(f"[GEN] No match for '{plant}' in file {gen_file.name}")

            state, region = plant_info.get(plant, ("Unknown", "Unknown"))
            gen_data.append([date_label, date_obj, state, plant, region, todays_actual_val])

    # COAL TABLE
    coal_data = []

    # precompute indexes for columns F, U, Y, AB (0-based)
    f_idx = col_letter_to_index('F')
    u_idx = col_letter_to_index('U')   # Indigenous
    y_idx = col_letter_to_index('Y')   # Import
    ab_idx = col_letter_to_index('AB') # Total

    # secondary addresses
    e_idx = col_letter_to_index('E')
    t_idx = col_letter_to_index('T')
    x_idx = col_letter_to_index('X')
    aa_idx = col_letter_to_index('AA')

    # helper to build mapping from raw rows (kept logic unchanged)
    def build_mapping_from_raw(raw_df, name_col_idx, ind_idx, imp_idx, tot_idx, cons_pos_local=None, debug_name="primary", coal_file_obj=None):
        mapping_local = {}
        if raw_df is None:
            return mapping_local
        max_rows = min(500, raw_df.shape[0])
        for r in range(max_rows):
            # if name column index exceeds available columns, stop
            if name_col_idx >= raw_df.shape[1]:
                break
            try:
                raw_cell = raw_df.iloc[r, name_col_idx]
            except Exception:
                continue
            if pd.isna(raw_cell):
                continue
            raw_cell_str = str(raw_cell).strip()
            if not raw_cell_str:
                continue
            # find best match between this cell and plant_info keys
            matched_name = find_best_match_in_list(plant_keys_list, raw_cell_str, min_ratio=0.60, debug=show_debug)
            # also allow exact normalized equality if fuzzy misses (redundant but safe)
            if not matched_name:
                for p in plant_info.keys():
                    if fast_normalize(raw_cell_str) == fast_normalize(p):
                        matched_name = p
                        break
            if not matched_name:
                continue
            nkey = fast_normalize(matched_name)
            if nkey in mapping_local:
                # keep first occurrence
                continue

            # extract U/Y/AB on this same row if present
            ind_val = ""
            imp_val = ""
            tot_val = ""
            coal_raw_val = ""
            try:
                if ind_idx < raw_df.shape[1]:
                    ind_val = format_num(raw_df.iloc[r, ind_idx])
            except:
                ind_val = ""
            try:
                if imp_idx < raw_df.shape[1]:
                    imp_val = format_num(raw_df.iloc[r, imp_idx])
            except:
                imp_val = ""
            try:
                if tot_idx < raw_df.shape[1]:
                    tot_val = format_num(raw_df.iloc[r, tot_idx])
            except:
                tot_val = ""
            # use cons_pos_local (index from header detect) to extract consumption from same raw row if available
            try:
                if cons_pos_local is not None and cons_pos_local < raw_df.shape[1]:
                    coal_raw_val = format_num(raw_df.iloc[r, cons_pos_local])
            except:
                coal_raw_val = ""
            mapping_local[nkey] = {
                "Indigenous U5": ind_val,
                "Import X5": imp_val,
                "Total AA5": tot_val,
                "Daily Coal Raw": coal_raw_val,
                "row_index": r
            }
        if show_debug and coal_file_obj is not None:
            st.write(f"[DEBUG] built mapping from F/E-scan ({debug_name}) for file {getattr(coal_file_obj,'name',str(coal_file_obj))}: {len(mapping_local)} entries")
        return mapping_local

    for coal_file in coal_files_sorted:
        # create a cache key using name and size to avoid reprocessing same file
        file_key = (getattr(coal_file, "name", str(coal_file)), getattr(coal_file, "size", None))

        # If we have cached mapping for this file, reuse it (fast)
        if file_key in coal_file_cache:
            cached = coal_file_cache[file_key]
            mapping = cached.get("mapping", {})
            coal_df = cached.get("coal_df", None)
            cons_pos = cached.get("cons_pos", None)
            raw_date = cached.get("raw_date", None)
            if show_debug:
                st.write(f"[CACHE] Using cached mapping for file {file_key[0]}")
        else:
            # Not cached: build mapping once and then cache it
            # header read for detecting consumption or header-based fallback
            coal_header = detect_coal_header(coal_file)
            try:
                coal_df = read_excel_auto(coal_file, header=coal_header)
            except Exception:
                coal_df = read_excel_auto(coal_file, header=0)
            coal_df.columns = [str(c).strip() for c in coal_df.columns]

            cons_col = detect_coal_col(coal_df.columns)
            cons_pos = None
            if cons_col is not None:
                try:
                    cons_pos = list(coal_df.columns).index(cons_col)
                except:
                    cons_pos = None

            # read raw sheet rows 1..500 (no header)
            try:
                raw_df = pd.read_excel(coal_file, header=None, nrows=500)
            except Exception:
                raw_df = None

            # build mapping using addresses priority (primary F/U/Y/AB then secondary E/T/X/AA),
            # exactly same logic as before but done once and cached
            mapping_primary = {}
            mapping_secondary = {}

            if raw_df is not None:
                mapping_primary = build_mapping_from_raw(raw_df, f_idx, u_idx, y_idx, ab_idx, cons_pos_local=cons_pos, debug_name="primary", coal_file_obj=coal_file)
                if mapping_primary:
                    mapping = mapping_primary
                    if show_debug:
                        st.write(f"[INFO] Using PRIMARY address mapping (F/U/Y/AB) for file {getattr(coal_file,'name',str(coal_file))}")
                else:
                    mapping_secondary = build_mapping_from_raw(raw_df, e_idx, t_idx, x_idx, aa_idx, cons_pos_local=cons_pos, debug_name="secondary", coal_file_obj=coal_file)
                    if mapping_secondary:
                        mapping = mapping_secondary
                        if show_debug:
                            st.write(f"[INFO] Using SECONDARY address mapping (E/T/X/AA) for file {getattr(coal_file,'name',str(coal_file))}")
                    else:
                        mapping = {}
                        if show_debug:
                            st.write(f"[INFO] No primary/secondary mapping found in raw rows for file {getattr(coal_file,'name',str(coal_file))}; will fallback to header-based matching")

            # parse date once and cache
            raw_date = get_date_from_coal_file(coal_file, month_name, year_val, debug=show_debug)

            # cache everything needed
            coal_file_cache[file_key] = {
                "mapping": mapping,
                "coal_df": coal_df,
                "cons_pos": cons_pos,
                "raw_date": raw_date
            }

        # For each selected plant, prefer mapping values; if absent, fallback to header-based extraction, else leave blank
        for plant in selected_coal_plants:
            step_count += 1
            percent = int(step_count / total_steps * 100)
            elapsed = time.time() - start_time
            progress_bar.progress(min(percent, 100))
            status_text.text(f"Processing coal files... {step_count}/{total_steps} ({percent}%) — Elapsed {elapsed:.1f}s")

            date_label, date_obj = format_date_label(raw_date, month_name, year_val)
            if date_label is None:
                date_label = ""
                date_obj = None

            normalized_plant = fast_normalize(plant)
            indigenous_val = ""
            import_val = ""
            total_val = ""
            coal_val = ""

            # Preferred: mapping from F/E-scan (U/Y/AB or T/X/AA)
            if normalized_plant in mapping:
                m = mapping[normalized_plant]
                indigenous_val = m.get("Indigenous U5", "") or ""
                import_val = m.get("Import X5", "") or ""
                total_val = m.get("Total AA5", "") or ""
                coal_val = m.get("Daily Coal Raw", "") or ""
            else:
                # fallback: header-based matching as original logic
                plant_cols = [c for c in coal_df.columns if "Thermal" in c or "Station" in c]
                if plant_cols:
                    plant_col_name = plant_cols[0]
                    coal_plant_series = coal_df[plant_col_name].astype(str)
                    match_idx = find_best_match_index(coal_plant_series, plant, min_ratio=0.55, debug=show_debug)
                    if match_idx is not None:
                        # consumption/coal value (header-based)
                        cons_header_col = detect_coal_col(coal_df.columns)
                        if cons_header_col:
                            try:
                                coal_val = format_num(coal_df.loc[match_idx, cons_header_col])
                            except:
                                coal_val = ""
                        # detect Ind/Import/Total columns by header or row5, then pick values
                        ind_col = None; imp_col = None; tot_col = None
                        # header search
                        for col in coal_df.columns:
                            s = str(col).upper()
                            if "INDIG" in s and ind_col is None:
                                ind_col = col
                            if "IMPORT" in s and imp_col is None:
                                imp_col = col
                            if "TOTAL" in s and tot_col is None:
                                tot_col = col
                        # row5 fallback
                        if any(c is None for c in (ind_col, imp_col, tot_col)) and coal_df.shape[0] > 4:
                            row5 = coal_df.iloc[4].astype(str).tolist()
                            for i, cell in enumerate(row5):
                                cs = str(cell).upper()
                                if "INDIG" in cs and ind_col is None:
                                    ind_col = coal_df.columns[i]
                                if "IMPORT" in cs and imp_col is None:
                                    imp_col = coal_df.columns[i]
                                if "TOTAL" in cs and tot_col is None:
                                    tot_col = coal_df.columns[i]
                        try:
                            if ind_col is not None:
                                indigenous_val = format_num(coal_df.loc[match_idx, ind_col])
                        except:
                            indigenous_val = ""
                        try:
                            if imp_col is not None:
                                import_val = format_num(coal_df.loc[match_idx, imp_col])
                        except:
                            import_val = ""
                        try:
                            if tot_col is not None:
                                total_val = format_num(coal_df.loc[match_idx, tot_col])
                        except:
                            total_val = ""
                    else:
                        if show_debug:
                            st.write(f"[COAL] header-match not found for '{plant}' in file {getattr(coal_file,'name',str(coal_file))}")
                else:
                    if show_debug:
                        st.write(f"[COAL] no plant column detected in headers for file {getattr(coal_file,'name',str(coal_file))}")

            state, region = plant_info.get(plant, ("Unknown", "Unknown"))
            coal_data.append([
                date_label, date_obj, state, plant, region, coal_val,
                indigenous_val, import_val, total_val
            ])

    # finish progress
    progress_bar.progress(100)
    total_elapsed = time.time() - start_time
    status_text.text(f"Processing complete — Elapsed {total_elapsed:.1f}s")

    # Build DataFrames
    gen_result_df = pd.DataFrame(gen_data, columns=[
        "Date_Label", "Date_dt", "State Name", "Thermal Plant", "Region",
        "Daily Electricity Generation (MU)"
    ])
    if not gen_result_df.empty:
        gen_result_df = gen_result_df.sort_values(by=["Date_dt", "Thermal Plant"], na_position="last").reset_index(drop=True)
        gen_result_df["Date"] = gen_result_df.apply(
            lambda r: r["Date_dt"].strftime("%d/%m/%Y") if pd.notna(r["Date_dt"]) else str(r["Date_Label"]),
            axis=1
        )
        gen_result_df = gen_result_df[["Date", "State Name", "Thermal Plant", "Region", "Daily Electricity Generation (MU)"]]
    else:
        gen_result_df = pd.DataFrame(columns=["Date", "State Name", "Thermal Plant", "Region", "Daily Electricity Generation (MU)"])

    coal_result_df = pd.DataFrame(coal_data, columns=[
        "Date_Label", "Date_dt", "State Name", "Thermal Plant", "Region",
        "Daily Coal ('000 T)", "Indigenous U5", "Import X5", "Total AA5"
    ])
    if not coal_result_df.empty:
        coal_result_df = coal_result_df.sort_values(by=["Date_dt", "Thermal Plant"], na_position="last").reset_index(drop=True)
        coal_result_df["Date"] = coal_result_df.apply(
            lambda r: r["Date_dt"].strftime("%d/%m/%Y") if pd.notna(r["Date_dt"]) else str(r["Date_Label"]),
            axis=1
        )
        coal_result_df = coal_result_df[["Date", "State Name", "Thermal Plant", "Region", "Daily Coal ('000 T)", "Indigenous U5", "Import X5", "Total AA5"]]
    else:
        coal_result_df = pd.DataFrame(columns=["Date", "State Name", "Thermal Plant", "Region", "Daily Coal ('000 T)", "Indigenous U5", "Import X5", "Total AA5"])

    # -------------------------
    # FILTER: keep only plants present in plant_info (so final data ONLY contains plant_info plants)
    # -------------------------
    allowed_plants = set(plant_info.keys())
    if not gen_result_df.empty:
        gen_result_df = gen_result_df[gen_result_df["Thermal Plant"].isin(allowed_plants)].reset_index(drop=True)
    if not coal_result_df.empty:
        coal_result_df = coal_result_df[coal_result_df["Thermal Plant"].isin(allowed_plants)].reset_index(drop=True)

    # Show tables (filtered)
    st.subheader("📊 Table 1: Daily Generation Data (filtered to plant_info)")
    st.dataframe(gen_result_df, use_container_width=True)

    st.subheader("📊 Table 2: Daily Coal Data (filtered to plant_info)")
    st.dataframe(coal_result_df, use_container_width=True)

    # Prepare merged download: generation table + coal columns appended (both filtered)
    if not gen_result_df.empty:
        merged = pd.merge(
            gen_result_df,
            coal_result_df[[
                "Date", "Thermal Plant", "Daily Coal ('000 T)", "Indigenous U5", "Import X5", "Total AA5"
            ]],
            on=["Date", "Thermal Plant"],
            how="left"
        )
        cols_order = [
            "Date", "State Name", "Thermal Plant", "Region",
            "Daily Electricity Generation (MU)", "Daily Coal ('000 T)", "Indigenous U5", "Import X5", "Total AA5"
        ]
        # Ensure coal column exists
        if "Daily Coal ('000 T)" not in merged.columns:
            merged["Daily Coal ('000 T)"] = ""
        for c in ["Indigenous U5", "Import X5", "Total AA5"]:
            if c not in merged.columns:
                merged[c] = ""
        merged = merged[cols_order]
    else:
        merged = pd.DataFrame(columns=[
            "Date", "State Name", "Thermal Plant", "Region",
            "Daily Electricity Generation (MU)", "Daily Coal ('000 T)", "Indigenous U5", "Import X5", "Total AA5"
        ])

    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        merged.to_excel(writer, sheet_name="Generation_with_Coal", index=False)
    buffer.seek(0)

    st.download_button(
        "📥 Download Generation (with Coal column) as Excel",
        data=buffer.getvalue(),
        file_name="Generation_with_Coal.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
