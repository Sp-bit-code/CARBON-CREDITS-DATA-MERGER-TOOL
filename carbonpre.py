import streamlit as st
import pandas as pd
import os
import re
import difflib
import time
from io import BytesIO
from datetime import datetime, date

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
        return f"{float(val):.4f}"
    except:
        return ""

def normalize_name(s: str) -> str:
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

def find_best_match_index(series: pd.Series, plant_name: str, min_ratio: float = 0.55, debug=False):
    if series.empty:
        return None
    n_target = normalize_name(plant_name)
    candidates = ["" if pd.isna(v) else normalize_name(v) for v in series.tolist()]
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

# DATE PARSING HELPERS
month_map = {m.lower(): i for i, m in enumerate([
    "January","February","March","April","May","June","July","August",
    "September","October","November","December"
], start=1)}

def parse_date_string_to_date(s: str):
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
    parsed = parse_date_string_to_date(combined)
    if isinstance(parsed, date):
        return parsed
    for cell in candidate_cells:
        parsed = parse_date_string_to_date(str(cell))
        if isinstance(parsed, date):
            return parsed
        if isinstance(parsed, int):
            return parsed
    fname = os.path.splitext(os.path.basename(getattr(file, "name", str(file))))[0]
    parsed = parse_date_string_to_date(fname)
    if isinstance(parsed, date):
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

def get_date_from_coal_file(file, fallback_month, fallback_year, debug=False):
    try:
        top = pd.read_excel(file, header=None, nrows=10)
    except Exception:
        return None
    candidate_cells = []
    if top.shape[0] > 2:
        candidate_cells.append(top.iloc[2, 0])  # A3
    for r in range(min(10, top.shape[0])):
        candidate_cells.append(top.iloc[r, 0])
    combined = " ".join([str(x) for x in candidate_cells if pd.notna(x)])
    parsed = parse_date_string_to_date(combined)
    if isinstance(parsed, date):
        return parsed
    for cell in candidate_cells:
        parsed = parse_date_string_to_date(str(cell))
        if isinstance(parsed, date):
            return parsed
        if isinstance(parsed, int):
            return parsed
    fname = os.path.splitext(os.path.basename(getattr(file, "name", str(file))))[0]
    parsed = parse_date_string_to_date(fname)
    if isinstance(parsed, date):
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

def format_date_label(d, fallback_month, fallback_year):
    if isinstance(d, date):
        return d.strftime("%d/%m/%Y"), d
    if isinstance(d, int):
        try:
            dt = date(fallback_year, month_map.get(fallback_month.lower(), 1), d)
            return dt.strftime("%d/%m/%Y"), dt
        except:
            return f"DAY {d}", None
    return None, None

# ===== Plant info (paste your full mapping here) =====
plant_info = {
    # Haryana Region-NORTHERN
    "PANIPAT TPS": ("Haryana", "NORTHERN"),
    "RAJIV GANDHI TPS": ("Haryana", "NORTHERN"),
    "YAMUNA NAGAR TPS": ("Haryana", "NORTHERN"),
    "MAHATMA GANDHI TPS": ("Haryana", "NORTHERN"),
    "INDIRA GANDHI STPP": ("Haryana", "NORTHERN"),
    
    # Punjab Region-NORTHERN  
    "GH TPS (LEH.MOH.)": ("Punjab", "NORTHERN"),
    "GOINDWAL SAHIB TPP": ("Punjab", "NORTHERN"),
    "ROPAR TPS": ("Punjab", "NORTHERN"),
    "RAJPURA TPP": ("Punjab", "NORTHERN"),
    "TALWANDI SABO TPP": ("Punjab", "NORTHERN"),
    
    # Rajasthan Region-NORTHERN  
    "CHHABRA-II TPP": ("Rajasthan", "NORTHERN"),
    "CHHABRA-I PH-1 TPP": ("Rajasthan", "NORTHERN"),
    "CHHABRA-I PH-2 TPP": ("Rajasthan", "NORTHERN"),
    "KALISINDH TPS": ("Rajasthan", "NORTHERN"),
    "KOTA TPS": ("Rajasthan", "NORTHERN"),
    "SURATGARH STPS": ("Rajasthan", "NORTHERN"),
    "SURATGARH TPS": ("Rajasthan", "NORTHERN"),
    "GIRAL TPS": ("Rajasthan", "NORTHERN"),
    "ADANI POWER LIMITED KAWAI TPP": ("Rajasthan", "NORTHERN"),
    "JALIPA KAPURDI TPP": ("Rajasthan", "NORTHERN"),
    "SHREE CEMENT LTD TPS": ("Rajasthan", "NORTHERN"),
    
    # Uttar Pradesh Region-NORTHERN
    "ANPARA TPS": ("Uttar Pradesh", "NORTHERN"),
    "HARDUAGANJ TPS": ("Uttar Pradesh", "NORTHERN"),
    "JAWAHARPUR STPP": ("Uttar Pradesh", "NORTHERN"),
    "OBRA TPS": ("Uttar Pradesh", "NORTHERN"),
    "PARICHHA TPS": ("Uttar Pradesh", "NORTHERN"),
    "ANPARA C TPS": ("Uttar Pradesh", "NORTHERN"),
    "BARKHERA TPS": ("Uttar Pradesh", "NORTHERN"),
    "KHAMBARKHERA TPS": ("Uttar Pradesh", "NORTHERN"),
    "KUNDARKI TPS": ("Uttar Pradesh", "NORTHERN"),
    "MAQSOODPUR TPS": ("Uttar Pradesh", "NORTHERN"),
    "PRAYAGRAJ TPP": ("Uttar Pradesh", "NORTHERN"),
    "ROSA TPP Ph-I": ("Uttar Pradesh", "NORTHERN"),
    "UTRAULA TPS": ("Uttar Pradesh", "NORTHERN"),
    "DADRI (NCTPP)": ("Uttar Pradesh", "NORTHERN"),
    "GHATAMPUR TPP": ("Uttar Pradesh", "NORTHERN"),
    "KHURJA TPP": ("Uttar Pradesh", "NORTHERN"),
    "MEJA STPP": ("Uttar Pradesh", "NORTHERN"),
    "RIHAND STPS": ("Uttar Pradesh", "NORTHERN"),
    "SINGRAULI STPS": ("Uttar Pradesh", "NORTHERN"),
    "TANDA TPS": ("Uttar Pradesh", "NORTHERN"),
    "UNCHAHAR TPS": ("Uttar Pradesh", "NORTHERN"),
    
    # Chhatisgarh Region-WESTERN  
    "DSPM TPS": ("Chhatisgarh", "WESTERN"),
    "KORBA-WEST TPS": ("Chhatisgarh", "WESTERN"),
    "MARWA TPS": ("Chhatisgarh", "WESTERN"),
    "ADANI POWER LIMITED RAIGARH TPP": ("Chhatisgarh", "WESTERN"),
    "ADANI POWER LIMITED RAIPUR TPP": ("Chhatisgarh", "WESTERN"),
    "AKALTARA TPS": ("Chhatisgarh", "WESTERN"),
    "BALCO TPS": ("Chhatisgarh", "WESTERN"),
    "BANDAKHAR TPP": ("Chhatisgarh", "WESTERN"),
    "BARADARHA TPS": ("Chhatisgarh", "WESTERN"),
    "BINJKOTE TPP": ("Chhatisgarh", "WESTERN"),
    "CHAKABURA TPP": ("Chhatisgarh", "WESTERN"),
    "KASAIPALLI TPP": ("Chhatisgarh", "WESTERN"),
    "KATGHORA TPP": ("Chhatisgarh", "WESTERN"),
    "NAWAPARA TPP": ("Chhatisgarh", "WESTERN"),
    "OP JINDAL TPS": ("Chhatisgarh", "WESTERN"),
    "PATHADI TPP": ("Chhatisgarh", "WESTERN"),
    "RATIJA TPS": ("Chhatisgarh", "WESTERN"),
    "SALORA TPP": ("Chhatisgarh", "WESTERN"),
    "SINGHITARAI TPP": ("Chhatisgarh", "WESTERN"),
    "SVPL TPP": ("Chhatisgarh", "WESTERN"),
    "SWASTIK KORBA TPP": ("Chhatisgarh", "WESTERN"),
    "TAMNAR TPP": ("Chhatisgarh", "WESTERN"),
    "UCHPINDA TPP": ("Chhatisgarh", "WESTERN"),
    "BHILAI TPS": ("Chhatisgarh", "WESTERN"),
    "KORBA STPS": ("Chhatisgarh", "WESTERN"),
    "LARA TPP": ("Chhatisgarh", "WESTERN"),
    "SIPAT STPS": ("Chhatisgarh", "WESTERN"),
    
    # Gujarat Region-WESTERN 
    "AKRIMOTA LIG TPS": ("Gujarat", "WESTERN"),
    "BHAVNAGAR CFBC TPP": ("Gujarat", "WESTERN"),
    "GANDHI NAGAR TPS": ("Gujarat", "WESTERN"),
    "UKAI TPS": ("Gujarat", "WESTERN"),
    "WANAKBORI TPS": ("Gujarat", "WESTERN"),
    "SIKKA REP. TPS": ("Gujarat", "WESTERN"),
    "KUTCH LIG. TPS": ("Gujarat", "WESTERN"),
    "ADANI POWER LIMITED MUNDRA TPP - III": ("Gujarat", "WESTERN"),
    "ADANI POWER LIMITED MUNDRA TPP - I & II": ("Gujarat", "WESTERN"),
    "MUNDRA UMTPP": ("Gujarat", "WESTERN"),
    "SABARMATI (D-F STATIONS)": ("Gujarat", "WESTERN"),
    "SALAYA TPP": ("Gujarat", "WESTERN"),
    "SURAT LIG. TPS": ("Gujarat", "WESTERN"),
    
    # Madhya Pradesh Region-WESTERN  
    "AMARKANTAK EXT TPS": ("Madhya Pradesh", "WESTERN"),
    "SANJAY GANDHI TPS": ("Madhya Pradesh", "WESTERN"),
    "SATPURA TPS": ("Madhya Pradesh", "WESTERN"),
    "SHREE SINGAJI TPP": ("Madhya Pradesh", "WESTERN"),
    "ANUPPUR TPP": ("Madhya Pradesh", "WESTERN"),
    "BINA TPS": ("Madhya Pradesh", "WESTERN"),
    "MAHAN TPP": ("Madhya Pradesh", "WESTERN"),
    "NIGRI TPP": ("Madhya Pradesh", "WESTERN"),
    "NIWARI TPP": ("Madhya Pradesh", "WESTERN"),
    "SASAN UMTPP": ("Madhya Pradesh", "WESTERN"),
    "GADARWARA TPP": ("Madhya Pradesh", "WESTERN"),
    "KHARGONE STPP": ("Madhya Pradesh", "WESTERN"),
    "SEIONI TPP": ("Madhya Pradesh", "WESTERN"),
    "VINDHYACHAL STPS": ("Madhya Pradesh", "WESTERN"),
    
    # Maharashtra Region-WESTERN  
    "BHUSAWAL TPS": ("Maharashtra", "WESTERN"),
    "CHANDRAPUR(MAHARASHTRA) STPS": ("Maharashtra", "WESTERN"),
    "KHAPARKHEDA TPS": ("Maharashtra", "WESTERN"),
    "KORADI TPS": ("Maharashtra", "WESTERN"),
    "NASIK TPS": ("Maharashtra", "WESTERN"),
    "PARAS TPS": ("Maharashtra", "WESTERN"),
    "PARLI TPS": ("Maharashtra", "WESTERN"),
    "ADANI POWER LIMITED TIRODA TPP": ("Maharashtra", "WESTERN"),
    "AMRAVATI TPS": ("Maharashtra", "WESTERN"),
    "BELA TPS": ("Maharashtra", "WESTERN"),
    "BUTIBORI TPP": ("Maharashtra", "WESTERN"),
    "DAHANU TPS": ("Maharashtra", "WESTERN"),
    "DHARIWAL TPP": ("Maharashtra", "WESTERN"),
    "GEPL TPP Ph-I": ("Maharashtra", "WESTERN"),
    "GMR WARORA TPS": ("Maharashtra", "WESTERN"),
    "JSW RATNAGIRI TPP": ("Maharashtra", "WESTERN"),
    "LANCO VIDARBHA TPP": ("Maharashtra", "WESTERN"),
    "MIHAN TPS": ("Maharashtra", "WESTERN"),
    "NASIK (P) TPS": ("Maharashtra", "WESTERN"),
    "SHIRPUR TPP": ("Maharashtra", "WESTERN"),
    "TROMBAY TPS": ("Maharashtra", "WESTERN"),
    "WARDHA WARORA TPP": ("Maharashtra", "WESTERN"),
    "MAUDA TPS": ("Maharashtra", "WESTERN"),
    "SOLAPUR STPS": ("Maharashtra", "WESTERN"),
    
    # Andhra Pradesh Region-SOUTHERN
    "SGPL TPP": ("Andhra Pradesh", "SOUTHERN"),
    "PAINAMPURAM TPP": ("Andhra Pradesh", "SOUTHERN"),
    "Dr. N.TATA RAO TPS": ("Andhra Pradesh", "SOUTHERN"),
    "RAYALASEEMA TPS": ("Andhra Pradesh", "SOUTHERN"),
    "DAMODARAM SANJEEVAIAH TPS": ("Andhra Pradesh", "SOUTHERN"),
    "SIMHAPURI TPS": ("Andhra Pradesh", "SOUTHERN"),
    "THAMMINAPATNAM TPS": ("Andhra Pradesh", "SOUTHERN"),
    "VIZAG TPP": ("Andhra Pradesh", "SOUTHERN"),
    
    # Karnataka Region-SOUTHERN
    "BELLARY TPS": ("Karnataka", "SOUTHERN"),
    "RAICHUR TPS": ("Karnataka", "SOUTHERN"),
    "YERMARUS TPP": ("Karnataka", "SOUTHERN"),
    "ADANI POWER LIMITED UDUPI TPP": ("Karnataka", "SOUTHERN"),
    "TORANGALLU TPS(SBU-I)": ("Karnataka", "SOUTHERN"),
    "TORANGALLU TPS(SBU-II)": ("Karnataka", "SOUTHERN"),
    "KUDGI STPP": ("Karnataka", "SOUTHERN"),
    
    # Tamil Nadu Region-SOUTHERN
    "METTUR TPS": ("Tamil Nadu", "SOUTHERN"),
    "METTUR TPS - II": ("Tamil Nadu", "SOUTHERN"),
    "NORTH CHENNAI TPS": ("Tamil Nadu", "SOUTHERN"),
    "TUTICORIN TPS": ("Tamil Nadu", "SOUTHERN"),
    "NTPL TUTICORIN TPP": ("Tamil Nadu", "SOUTHERN"),
    "UDANGUDI TPP": ("Tamil Nadu", "SOUTHERN"),
    "ITPCL TPP": ("Tamil Nadu", "SOUTHERN"),
    "MUTHIARA TPP": ("Tamil Nadu", "SOUTHERN"),
    "NEYVELI TPS(Z)": ("Tamil Nadu", "SOUTHERN"),
    "TUTICORIN (P) TPP": ("Tamil Nadu", "SOUTHERN"),
    "TUTICORIN TPP ST-IV": ("Tamil Nadu", "SOUTHERN"),
    "NEYVELI (EXT) TPS": ("Tamil Nadu", "SOUTHERN"),
    "NEYVELI NEW TPP": ("Tamil Nadu", "SOUTHERN"),
    "NEYVELI TPS-II": ("Tamil Nadu", "SOUTHERN"),
    "NEYVELI TPS-II EXP": ("Tamil Nadu", "SOUTHERN"),
    "VALLUR TPP": ("Tamil Nadu", "SOUTHERN"),
    
    # Telangana Region-SOUTHERN
    "SINGARENI TPP": ("Telangana", "SOUTHERN"),
    "BHADRADRI TPP": ("Telangana", "SOUTHERN"),
    "KAKATIYA TPS": ("Telangana", "SOUTHERN"),
    "KOTHAGUDEM TPS (NEW)": ("Telangana", "SOUTHERN"),
    "KOTHAGUDEM TPS (STAGE-7)": ("Telangana", "SOUTHERN"),
    "RAMAGUNDEM-B TPS": ("Telangana", "SOUTHERN"),
    "YADADRI TPS": ("Telangana", "SOUTHERN"),
    "RAMAGUNDEM STPS": ("Telangana", "SOUTHERN"),
    "TELANGANA STPP PH-1": ("Telangana", "SOUTHERN"),
    
    # Bihar Region-EASTERN
    "BARAUNI TPS": ("Bihar", "EASTERN"),
    "BARH STPS": ("Bihar", "EASTERN"),
    "BUXAR TPP": ("Bihar", "EASTERN"),
    "KAHALGAON TPS": ("Bihar", "EASTERN"),
    "MUZAFFARPUR TPS": ("Bihar", "EASTERN"),
    "NABINAGAR STPP": ("Bihar", "EASTERN"),
    "NABINAGAR TPP": ("Bihar", "EASTERN"),
    
    # Jharkhand Region-EASTERN 
    "TENUGHAT TPS": ("Jharkhand", "EASTERN"),
    "JOJOBERA TPS": ("Jharkhand", "EASTERN"),
    "MAHADEV PRASAD STPP": ("Jharkhand", "EASTERN"),
    "MAITHON RB TPP": ("Jharkhand", "EASTERN"),
    "MAITRISHI USHA TPS": ("Jharkhand", "EASTERN"),
    "BOKARO TPS A EXP": ("Jharkhand", "EASTERN"),
    "CHANDRAPURA(DVC) TPS": ("Jharkhand", "EASTERN"),
    "KODARMA TPP": ("Jharkhand", "EASTERN"),
    "NORTH KARANPURA TPP": ("Jharkhand", "EASTERN"),
    "PATRATU STPP": ("Jharkhand", "EASTERN"),
    
    # Odisha Region-EASTERN 
    "IB VALLEY TPS": ("Odisha", "EASTERN"),
    "DERANG TPP": ("Odisha", "EASTERN"),
    "KAMALANGA TPS": ("Odisha", "EASTERN"),
    "LANCO BABANDH TPP": ("Odisha", "EASTERN"),
    "UTKAL TPP (IND BARATH)": ("Odisha", "EASTERN"),
    "VEDANTA TPP": ("Odisha", "EASTERN"),
    "DARLIPALI STPS": ("Odisha", "EASTERN"),
    "TALCHER STPS": ("Odisha", "EASTERN"),
    
    # West Bengal Region-EASTERN 
    "D.P.L. TPS": ("West Bengal", "EASTERN"),
    "BAKRESWAR TPS": ("West Bengal", "EASTERN"),
    "BANDEL TPS": ("West Bengal", "EASTERN"),
    "KOLAGHAT TPS": ("West Bengal", "EASTERN"),
    "SAGARDIGHI TPS": ("West Bengal", "EASTERN"),
    "SANTALDIH TPS": ("West Bengal", "EASTERN"),
    "SAGARDIGHI TPP ST-III": ("West Bengal", "EASTERN"),
    "BUDGE BUDGE TPS": ("West Bengal", "EASTERN"),
    "DISHERGARH TPP": ("West Bengal", "EASTERN"),
    "HIRANMAYE TPP": ("West Bengal", "EASTERN"),
    "SOUTHERN REPL. TPS": ("West Bengal", "EASTERN"),
    "TITAGARH TPS": ("West Bengal", "EASTERN"),
    "DURGAPUR STEEL TPS": ("West Bengal", "EASTERN"),
    "FARAKKA STPS": ("West Bengal", "EASTERN"),
    "MEJIA TPS": ("West Bengal", "EASTERN"),
    "RAGHUNATHPUR TPP": ("West Bengal", "EASTERN"),
    
    # Assam Region-NORTH EASTERN
    "BONGAIGAON TPP": ("Assam", "NORTH EASTERN"),
    
    # Additional regions and states can be added as needed
}
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

# If uploads changed we still compute plant lists for internal use, but SELECT menu will always show plant_info keys (Option A)
current_upload_key = _make_upload_key(generation_files, coal_files)
if "upload_key" not in st.session_state or st.session_state.get("upload_key") != current_upload_key:
    # scan generation files (for internal matching only)
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

    # Option A: Always show all plant_info keys in select boxes
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
    for plant in selected_coal_plants:
        for coal_file in coal_files_sorted:
            step_count += 1
            percent = int(step_count / total_steps * 100)
            elapsed = time.time() - start_time
            progress_bar.progress(min(percent, 100))
            status_text.text(f"Processing coal files... {step_count}/{total_steps} ({percent}%) — Elapsed {elapsed:.1f}s")

            raw_date = get_date_from_coal_file(coal_file, month_name, year_val, debug=show_debug)
            date_label, date_obj = format_date_label(raw_date, month_name, year_val)
            if date_label is None:
                date_label = os.path.splitext(os.path.basename(coal_file.name))[0]

            coal_header = detect_coal_header(coal_file)
            try:
                coal_df = read_excel_auto(coal_file, header=coal_header)
            except Exception:
                coal_df = read_excel_auto(coal_file, header=0)
            coal_df.columns = [str(c).strip() for c in coal_df.columns]

            plant_cols = [c for c in coal_df.columns if "Thermal" in c or "Station" in c]
            coal_val = ""
            if plant_cols:
                plant_col_name = plant_cols[0]
                coal_plant_series = coal_df[plant_col_name].astype(str)
                match_idx = find_best_match_index(coal_plant_series, plant, min_ratio=0.55, debug=show_debug)
                if match_idx is not None:
                    cons_col = detect_coal_col(coal_df.columns)
                    if cons_col:
                        try:
                            coal_val = format_num(coal_df.loc[match_idx, cons_col])
                        except Exception:
                            coal_val = ""
                else:
                    if show_debug:
                        st.write(f"[COAL] No match for '{plant}' in file {coal_file.name}")
            else:
                # fallback: try first col
                if coal_df.shape[1] >= 1:
                    coal_plant_series = coal_df.iloc[:, 0].astype(str)
                    match_idx = find_best_match_index(coal_plant_series, plant, min_ratio=0.55, debug=show_debug)
                    if match_idx is not None:
                        cons_col = detect_coal_col(coal_df.columns)
                        if cons_col:
                            try:
                                coal_val = format_num(coal_df.loc[match_idx, cons_col])
                            except Exception:
                                coal_val = ""
                    else:
                        if show_debug:
                            st.write(f"[COAL] (fallback) No match for '{plant}' in file {coal_file.name}")

            state, region = plant_info.get(plant, ("Unknown", "Unknown"))
            coal_data.append([date_label, date_obj, state, plant, region, coal_val])

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
        "Daily Coal ('000 T)"
    ])
    if not coal_result_df.empty:
        coal_result_df = coal_result_df.sort_values(by=["Date_dt", "Thermal Plant"], na_position="last").reset_index(drop=True)
        coal_result_df["Date"] = coal_result_df.apply(
            lambda r: r["Date_dt"].strftime("%d/%m/%Y") if pd.notna(r["Date_dt"]) else str(r["Date_Label"]),
            axis=1
        )
        coal_result_df = coal_result_df[["Date", "State Name", "Thermal Plant", "Region", "Daily Coal ('000 T)"]]
    else:
        coal_result_df = pd.DataFrame(columns=["Date", "State Name", "Thermal Plant", "Region", "Daily Coal ('000 T)"])

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

    # Prepare merged download: generation table + coal column appended (both filtered)
    if not gen_result_df.empty:
        merged = pd.merge(
            gen_result_df,
            coal_result_df[["Date", "Thermal Plant", "Daily Coal ('000 T)"]],
            on=["Date", "Thermal Plant"],
            how="left"
        )
        cols_order = [
            "Date", "State Name", "Thermal Plant", "Region",
            "Daily Electricity Generation (MU)", "Daily Coal ('000 T)"
        ]
        # Ensure coal column exists
        if "Daily Coal ('000 T)" not in merged.columns:
            merged["Daily Coal ('000 T)"] = ""
        merged = merged[cols_order]
    else:
        merged = pd.DataFrame(columns=[
            "Date", "State Name", "Thermal Plant", "Region",
            "Daily Electricity Generation (MU)", "Daily Coal ('000 T)"
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
