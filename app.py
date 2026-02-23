
# ko_tao_dashboard_streamlit_2569_v10_plus_final.py
# Ko Tao EMS Executive + Dispatch + Fuel Dashboard (2569) – v10+
# Fixes vs v9:
# - EMS DG energy is always computed from Dispatch Profiles_15min (DG_Total_MW, DG_NamSaeng_MW, DG_PEA_MW, DG_Mobile_MW)
# - Base(2568) DG energy + fuel cost are taken from KoTao_StandardValue_Charts.xlsx (sheet: Inputs)
# - Monthly merge uses Month number only => no duplicate month rows / no sawtooth charts
# - Dispatch tab: optional day filter + Group A/B background + includes Headroom and DG split lines
# - Cost tab: clean Base vs EMS monthly table + monthly charts + Group A/B hours
#
# Run:
#   cd "E:\#NR_TRAINING BY CHINA\Python EMS\Import_Grid_P_2024\kohtao"
#   streamlit run .\ko_tao_dashboard_streamlit_2569_v10_plus_final.py

from __future__ import annotations

from pathlib import Path
import os
import re
import datetime as _dt
import io
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import streamlit as st
import altair as alt


APP_TITLE = "Koh Tao EMS 2569 – Dispatch A/B + Fuel Dashboard "


# Fixed investment assumption (user-specified)
CAPEX_FIXED_THB = 28_547_600.0  # THB

# -----------------------------
# Defaults (edit to your local paths)
# -----------------------------
DEFAULT_OUT_DIR = str((Path(__file__).resolve().parent / "data").resolve())

DEFAULT_EXEC_XLSX_NAME = "KoTao_Executive_Charts_2567_2568_2569_EMS_v4.xlsx"
DEFAULT_DISPATCH_XLSX_NAME = "KoTao_Dispatch_GroupAB_2569_v3.xlsx"
DEFAULT_STANDARD_XLSX_NAME = "KoTao_StandardValue_Charts.xlsx"

MONTH_LABELS_TH = {
    1: "ม.ค.", 2: "ก.พ.", 3: "มี.ค.", 4: "เม.ย.", 5: "พ.ค.", 6: "มิ.ย.",
    7: "ก.ค.", 8: "ส.ค.", 9: "ก.ย.", 10: "ต.ค.", 11: "พ.ย.", 12: "ธ.ค."
}
MONTH_LABELS_EN = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
}
MONTH_ORDER = list(range(1, 13))

TH_FULL_TO_MONTH = {
    "มกราคม": 1,
    "กุมภาพันธ์": 2,
    "มีนาคม": 3,
    "เมษายน": 4,
    "พฤษภาคม": 5,
    "มิถุนายน": 6,
    "กรกฎาคม": 7,
    "สิงหาคม": 8,
    "กันยายน": 9,
    "ตุลาคม": 10,
    "พฤศจิกายน": 11,
    "ธันวาคม": 12,
    "รวม": None,
}

# -----------------------------
# Utilities
# -----------------------------
def _strip_quotes(s: str) -> str:
    return str(s).strip().strip('"').strip("'")


def _pick_default_excel(out_dir: str, candidates: list[str]) -> str:
    out = Path(out_dir)
    for name in candidates:
        cand = out / name
        if cand.exists():
            return str(cand)
    return str(out / candidates[0])  # fallback (even if missing)


# -----------------------------
# Formatting helpers (finance-friendly)
# -----------------------------
def _safe_div(num: float, den: float, default: float = 0.0) -> float:
    try:
        num = float(num)
        den = float(den)
        return num / den if den != 0 else float(default)
    except Exception:
        return float(default)


def _fmt_num(x: float, decimals: int = 0) -> str:
    try:
        return f"{float(x):,.{int(decimals)}f}"
    except Exception:
        return "0"


def _fmt_thb(x: float, decimals: int = 0, suffix: str = "") -> str:
    s = _fmt_num(x, decimals)
    return f"{s}{suffix}" if suffix else s


def _fmt_mthb(x_thb: float, decimals: int = 2) -> str:
    return _fmt_num(_safe_div(x_thb, 1_000_000.0), decimals)


def _to_bytes_csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def _to_bytes_xlsx(sheets: dict[str, pd.DataFrame]) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        for name, sdf in sheets.items():
            sdf.to_excel(writer, index=False, sheet_name=str(name)[:31])
    bio.seek(0)
    return bio.read()



# -----------------------------
# Executive KPI Thai explanations
# -----------------------------
def _add_exec_kpi_details_th(summary: pd.DataFrame) -> pd.DataFrame:
    """Add Thai meaning/source/formula columns to the Exec_Summary KPI table (if present)."""
    if summary is None or summary.empty:
        return summary

    df = summary.copy()
    if "KPI" not in df.columns:
        return df

    # Normalize KPI names for mapping
    kpi_series = df["KPI"].astype(str).str.strip()

    meaning_map = {
        "Dispatch file (EMS)": "ไฟล์ผลลัพธ์การ Dispatch (EMS) ที่เลือกใช้ใน Dashboard",
        "Template file": "ไฟล์ฐานข้อมูล/ค่ามาตรฐานปีฐาน (Base)2568 ที่ใช้เทียบ (KoTao_StandardValue_Charts.xlsx)",
        "Baseline year": "ปีฐานสำหรับการเปรียบเทียบ (Base year)",
        "Baseline fuel cost (THB)": "ค่าใช้จ่ายเชื้อเพลิงรวมของปีฐาน (บาท/ปี)",
        "Baseline DG energy (kWh)": "พลังงานไฟฟ้ารวมจาก DG ของปีฐาน (kWh/ปี)",
        "Baseline fuel cost proxy (THB/kWh)": "ต้นทุนเฉลี่ยเชื้อเพลิงต่อหน่วยพลังงานของปีฐาน 2568 (บาท/kWh) = FuelCost / DG_kWh",
        "EMS year": "ปีของกรณี EMS (เปรียบเทียบหลังทำ EMS Dispatch)",
        "EMS all-in / fuel-cost proxy (THB/kWh)": "ค่า All-in (บาท/kWh) ที่ใช้คูณกับ DG_kWh เพื่อประมาณต้นุทนน้ำมัน (Fuel cost)  ",
        "EMS DG energy (kWh)": "พลังงานไฟฟ้ารวมจาก DG ของกรณี EMS (kWh/ปี) จาก Profiles_15min ในไฟล์ Dispatch",
        "EMS fuel cost (THB)": "ค่าใช้จ่ายเชื้อเพลิงรวมของกรณี EMS (บาท/ปี) (มาจาก FuelCost_THB หรือคำนวณแบบ proxy)",
    }

    source_map = {
        "Dispatch file (EMS)": "มาจากไฟล์ที่คุณเลือกใน Sidebar (Dispatch output)",
        "Template file": "มาจากไฟล์ที่คุณเลือกใน Sidebar (Standard value/template)",
        "Baseline year": "กำหนดเป็น 2568 ในแท็บ Base vs EMS",
        "Baseline fuel cost (THB)": "อ่านจาก KoTao_StandardValue_Charts.xlsx → sheet 'Inputs' → รวม FuelCost_THB_2568 รายเดือน",
        "Baseline DG energy (kWh)": "อ่านจาก KoTao_StandardValue_Charts.xlsx → sheet 'Inputs' → รวม Energy_kWh_2568 รายเดือน",
        "Baseline fuel cost proxy (THB/kWh)": "คำนวณจากค่าปีฐาน: FuelCost_THB_2568_total / Energy_kWh_2568_total",
        "EMS year": "กำหนดเป็น 2569 ในแท็บ Base vs EMS",
        "EMS all-in / fuel-cost proxy (THB/kWh)": "มาจากค่า All-in ที่ตั้งใน Sidebar (default) และ/หรือ All-in รายเดือน (override)",
        "EMS DG energy (kWh)": "คำนวณจาก Dispatch file → sheet 'Profiles_15min': Σ(DG_Total_MW×1000×dt_h)",
        "EMS fuel cost (THB)": "ถ้ามี FuelCost_THB ใน Profiles_15min และเลือก from_dispatch → Σ(FuelCost_THB),ถ้าไม่มี → proxy: Σ(DG_kWh_step×All-in)",
    }

    formula_map = {
        "Baseline fuel cost (THB)": "FuelCost_THB_base = Σ_month FuelCost_THB_2568",
        "Baseline DG energy (kWh)": "DG_kWh_base = Σ_month Energy_kWh_2568",
        "Baseline fuel cost proxy (THB/kWh)": "All-in_base_effective = FuelCost_THB_base / DG_kWh_base",
        "EMS DG energy (kWh)": "DG_kWh_ems = Σ_t (DG_Total_MW(t) × 1000 × dt_h)",
        "EMS fuel cost (THB)": "from_dispatch: FuelCost_THB_ems = Σ_t FuelCost_THB(t),\nproxy: FuelCost_THB_ems = Σ_t (DG_Total_MW(t)×1000×dt_h×All-in(t))",
    }

    df["ความหมาย (ไทย)"] = kpi_series.map(meaning_map).fillna("")
    df["ที่มา"] = kpi_series.map(source_map).fillna("")
    df["วิธีคำนวณ/สูตร"] = kpi_series.map(formula_map).fillna("")

    # Put the explanation columns right after Value if possible
    cols = df.columns.tolist()
    if "Value" in cols and "ความหมาย (ไทย)" in cols:
        base = []
        for c in cols:
            base.append(c)
            if c == "Value":
                # insert three columns if not already there
                for add in ["ความหมาย (ไทย)", "ที่มา", "วิธีคำนวณ/สูตร"]:
                    if add not in base:
                        base.append(add)
        # remove duplicates while preserving order
        seen = set()
        ordered = []
        for c in base:
            if c not in seen and c in df.columns:
                ordered.append(c); seen.add(c)
        df = df[ordered]

    return df


def _default_exec_path() -> str:
    env = os.environ.get("KOTAO_EXEC_XLSX")
    if env:
        return _strip_quotes(env)
    return _pick_default_excel(DEFAULT_OUT_DIR, [
        DEFAULT_EXEC_XLSX_NAME,
        DEFAULT_EXEC_XLSX_NAME.replace(".xlsx", ".xlsm"),
        "KoTao_Executive_Charts_2567_2568_2569_EMS_v3.xlsx",
    ])


def _default_dispatch_path() -> str:
    env = os.environ.get("KOTAO_DISPATCH_XLSX")
    if env:
        return _strip_quotes(env)
    out = Path(DEFAULT_OUT_DIR)
    candidates = [
        DEFAULT_DISPATCH_XLSX_NAME,
        DEFAULT_DISPATCH_XLSX_NAME.replace(".xlsx", ".xlsm"),
        "KoTao_Dispatch_Test3_withbaseline.xlsx",
        "KoTao_Dispatch_Test2.xlsx",
        "KoTao_Dispatch_GroupAB_REAL.xlsx",
    ]
    p = _pick_default_excel(DEFAULT_OUT_DIR, candidates)
    if Path(p).exists():
        return p
    if out.exists():
        matches = sorted(out.glob("KoTao_Dispatch*.xls*"))
        if matches:
            return str(matches[0])
    return p


def _default_standard_path() -> str:
    env = os.environ.get("KOTAO_STD_XLSX")
    if env:
        return _strip_quotes(env)
    return _pick_default_excel(DEFAULT_OUT_DIR, [
        DEFAULT_STANDARD_XLSX_NAME,
        "KoTao_StandardValue_Charts_rev1.xlsx",
        "KoTao_StandardValue.xlsx",
    ])


def _list_xlsx_in_outdir(out_dir: str) -> list[str]:
    p = Path(out_dir)
    if not p.exists():
        return []
    files = sorted([x.name for x in p.glob("*.xls*")])
    return files[:250]


def _ensure_dt(df: pd.DataFrame, col: str = "Datetime") -> pd.DataFrame:
    out = df.copy()
    if col in out.columns:
        out[col] = pd.to_datetime(out[col], errors="coerce")
    return out


def _infer_dt_h(dt_series: pd.Series, default: str = "15min") -> float:
    try:
        freq = pd.infer_freq(pd.to_datetime(dt_series))
        if not freq:
            freq = default
        return pd.Timedelta(str(freq)).total_seconds() / 3600.0
    except Exception:
        return pd.Timedelta(default).total_seconds() / 3600.0


# -----------------------------
# Loaders
# -----------------------------
@st.cache_data(show_spinner=False)
def load_exec_pack(path: str) -> dict[str, pd.DataFrame]:
    xlsx = pd.ExcelFile(path)
    out: dict[str, pd.DataFrame] = {}
    for sh in ["Exec_Summary", "Monthly", "TypicalDay_PeakMonth", "Inputs"]:
        if sh in xlsx.sheet_names:
            out[sh] = pd.read_excel(xlsx, sheet_name=sh)
    return out


@st.cache_data(show_spinner=False)
def load_dispatch_pack(path: str) -> dict[str, pd.DataFrame]:
    xlsx = pd.ExcelFile(path)
    out: dict[str, pd.DataFrame] = {}
    for sh in [
        "Profiles_15min",
        "Baseline_15min",
        "Monthly_Summary_EMS",
        "Monthly_Summary_Base",
        "TypicalDay_PeakMonth",
    ]:
        if sh in xlsx.sheet_names:
            out[sh] = pd.read_excel(xlsx, sheet_name=sh)
    return out


@st.cache_data(show_spinner=False)
def load_standard_value_2568(path: str) -> pd.DataFrame:
    """
    Parse KoTao_StandardValue_Charts.xlsx sheet 'Inputs' which has a header row inside the sheet.
    Output columns:
      Month (1..12), Month_TH, Month_EN,
      Fuel_L_2568, FuelCost_THB_2568, Energy_kWh_2568, AllIn_THB_per_kWh_2568
    """
    xlsx = pd.ExcelFile(path)
    if "Inputs" not in xlsx.sheet_names:
        raise ValueError("StandardValue file must contain sheet 'Inputs'")

    raw = pd.read_excel(path, sheet_name="Inputs", header=None)

    header_row = None
    for i in range(raw.shape[0]):
        if raw.iloc[i].astype(str).str.contains(r"\bMonth\b", regex=True).any():
            header_row = i
            break
    if header_row is None:
        raise ValueError("Cannot find header row in StandardValue Inputs sheet")

    header = raw.iloc[header_row].tolist()
    data = raw.iloc[header_row + 1:].copy()
    data.columns = [str(x).strip() for x in header]

    # keep only month rows
    data = data.rename(columns={"Month (TH)": "Month_TH_full", "Month": "Month_EN"})
    data["Month_TH_full"] = data["Month_TH_full"].astype(str).str.strip()
    data = data[~data["Month_TH_full"].isin(["nan", "NaN", "None"])].copy()

    data["Month"] = data["Month_TH_full"].map(TH_FULL_TO_MONTH)
    data = data[data["Month"].notna()].copy()
    data["Month"] = data["Month"].astype(int)

    # select columns (tolerant)
    def _col(name: str) -> str:
        if name in data.columns:
            return name
        raise KeyError(f"Missing column '{name}' in Inputs")

    cols = {
        "Fuel_L_2568": _col("Fuel (L) 2568"),
        "FuelCost_THB_2568": _col("Fuel Cost (THB) 2568"),
        "Energy_kWh_2568": _col("Energy (kWh) 2568"),
        "AllIn_THB_per_kWh_2568": _col("All-in (THB/kWh) 2568"),
    }

    out = pd.DataFrame({
        "Month": data["Month"].to_numpy(int),
        "Month_TH": [MONTH_LABELS_TH[int(m)] for m in data["Month"].to_numpy(int)],
        "Month_EN": [MONTH_LABELS_EN[int(m)] for m in data["Month"].to_numpy(int)],
        "Fuel_L_2568": pd.to_numeric(data[cols["Fuel_L_2568"]], errors="coerce").fillna(0.0),
        "FuelCost_THB_2568": pd.to_numeric(data[cols["FuelCost_THB_2568"]], errors="coerce").fillna(0.0),
        "Energy_kWh_2568": pd.to_numeric(data[cols["Energy_kWh_2568"]], errors="coerce").fillna(0.0),
        "AllIn_THB_per_kWh_2568": pd.to_numeric(data[cols["AllIn_THB_per_kWh_2568"]], errors="coerce").fillna(0.0),
    }).sort_values("Month")

    # guard: ensure 12 rows
    if len(out) != 12:
        # still allow, but warn in UI
        pass

    return out


# -----------------------------
# Fuel-price helpers (All-in proxy)
# -----------------------------
def _parse_month_prices(text: str) -> dict[int, float]:
    """Parse:
      1=10.8
      2:10.7
      or "1=10.8,2=10.7"
    """
    if not text or not str(text).strip():
        return {}
    t = str(text).strip()
    t = t.replace(",", "\n").replace(";", "\n")
    out: dict[int, float] = {}
    for raw in t.splitlines():
        s = raw.strip()
        if not s:
            continue
        m = re.match(r"^\s*(\d{1,2})\s*[:=]\s*([0-9.]+)\s*$", s)
        if not m:
            continue
        mm = int(m.group(1))
        if 1 <= mm <= 12:
            out[mm] = float(m.group(2))
    return out


def _month_series_from_dt(dt: pd.Series, month_values: dict[int, float], fallback: float) -> np.ndarray:
    months = pd.to_datetime(dt).dt.month.to_numpy()
    out = np.full(len(months), float(fallback), dtype=float)
    if month_values:
        for m, v in month_values.items():
            out[months == int(m)] = float(v)
    return out


# -----------------------------
# Dispatch normalizer (DG split + Mobile + caps)
# -----------------------------
def _ensure_dg_split(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    for c in ["DG_Total_MW", "DG_NamSaeng_MW", "DG_PEA_MW"]:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0.0)
        else:
            d[c] = 0.0

    # caps (per your rules)
    d["DG_NamSaeng_MW"] = d["DG_NamSaeng_MW"].clip(lower=0.0, upper=5.0)
    d["DG_PEA_MW"] = d["DG_PEA_MW"].clip(lower=0.0, upper=8.0)

    if "DG_Mobile_MW" in d.columns:
        d["DG_Mobile_MW"] = pd.to_numeric(d["DG_Mobile_MW"], errors="coerce").fillna(0.0)
    else:
        d["DG_Mobile_MW"] = (d["DG_Total_MW"] - d["DG_NamSaeng_MW"] - d["DG_PEA_MW"]).fillna(0.0)

    d["DG_Mobile_MW"] = d["DG_Mobile_MW"].clip(lower=0.0, upper=2.0)

    # DG_Total fallback if missing (should not happen)
    if "DG_Total_MW" not in df.columns or df["DG_Total_MW"].isna().all():
        d["DG_Total_MW"] = d["DG_NamSaeng_MW"] + d["DG_PEA_MW"] + d["DG_Mobile_MW"]

    return d


# -----------------------------
# Monthly aggregations
# -----------------------------
def _monthly_from_dispatch(df: pd.DataFrame, dt_h: float, allin_fallback: float, allin_month_override: dict[int, float], cost_mode: str) -> pd.DataFrame:
    """
    df: Profiles_15min (EMS)
    cost_mode:
      - "proxy_allin": DG_kWh * All-in
      - "from_dispatch": use FuelCost_THB column if present, else fallback to proxy
    """
    d = df.copy()
    d["Datetime"] = pd.to_datetime(d["Datetime"])
    d["Month"] = d["Datetime"].dt.month.astype(int)

    d = _ensure_dg_split(d)

    # energy per step
    d["DG_kWh_step"] = d["DG_Total_MW"].to_numpy(float) * 1000.0 * float(dt_h)
    d["NS_kWh_step"] = d["DG_NamSaeng_MW"].to_numpy(float) * 1000.0 * float(dt_h)
    d["PEA_kWh_step"] = d["DG_PEA_MW"].to_numpy(float) * 1000.0 * float(dt_h)
    d["Mobile_kWh_step"] = d["DG_Mobile_MW"].to_numpy(float) * 1000.0 * float(dt_h)

    # all-in
    allin = _month_series_from_dt(d["Datetime"], allin_month_override, float(allin_fallback))
    d["AllIn_THB_per_kWh"] = allin

    d["FuelCost_proxy_THB"] = d["DG_kWh_step"] * allin

    if cost_mode == "from_dispatch" and "FuelCost_THB" in d.columns:
        d["FuelCost_THB"] = pd.to_numeric(d["FuelCost_THB"], errors="coerce").fillna(0.0)
    else:
        d["FuelCost_THB"] = d["FuelCost_proxy_THB"]

    if "Fuel_L" in d.columns:
        d["Fuel_L"] = pd.to_numeric(d["Fuel_L"], errors="coerce").fillna(0.0)
    else:
        d["Fuel_L"] = 0.0

    g = d.groupby("Month", as_index=False).agg(
        DG_kWh=("DG_kWh_step", "sum"),
        DG_MWh=("DG_kWh_step", lambda x: float(np.nansum(x)) / 1000.0),
        NS_MWh=("NS_kWh_step", lambda x: float(np.nansum(x)) / 1000.0),
        PEA_MWh=("PEA_kWh_step", lambda x: float(np.nansum(x)) / 1000.0),
        Mobile_MWh=("Mobile_kWh_step", lambda x: float(np.nansum(x)) / 1000.0),
        Fuel_L=("Fuel_L", "sum"),
        FuelCost_THB=("FuelCost_THB", "sum"),
    )

    g["AllIn_THB_per_kWh"] = [float(allin_month_override.get(int(m), float(allin_fallback))) for m in g["Month"].to_numpy(int)]
    g["FuelIntensity_L_per_kWh"] = np.where(g["DG_kWh"] > 0, g["Fuel_L"] / g["DG_kWh"], 0.0)
    g["TotalCost_MTHB"] = g["FuelCost_THB"] / 1_000_000.0

    # add labels
    g["Month_TH"] = g["Month"].map(lambda m: MONTH_LABELS_TH.get(int(m), str(m)))
    g["Month_EN"] = g["Month"].map(lambda m: MONTH_LABELS_EN.get(int(m), str(m)))

    # ensure full 12 months (fill 0)
    full = pd.DataFrame({"Month": MONTH_ORDER})
    out = full.merge(g, on="Month", how="left").fillna(0.0)
    out["Month_TH"] = out["Month"].map(lambda m: MONTH_LABELS_TH[int(m)])
    out["Month_EN"] = out["Month"].map(lambda m: MONTH_LABELS_EN[int(m)])
    return out


def _monthly_base_from_standard(std2568: pd.DataFrame) -> pd.DataFrame:
    d = std2568.copy()
    out = pd.DataFrame({
        "Month": d["Month"].astype(int),
        "Month_TH": d["Month"].map(lambda m: MONTH_LABELS_TH[int(m)]),
        "Month_EN": d["Month"].map(lambda m: MONTH_LABELS_EN[int(m)]),
        "DG_kWh_base": pd.to_numeric(d["Energy_kWh_2568"], errors="coerce").fillna(0.0),
        "DG_MWh_base": pd.to_numeric(d["Energy_kWh_2568"], errors="coerce").fillna(0.0) / 1000.0,
        "Fuel_L_base": pd.to_numeric(d["Fuel_L_2568"], errors="coerce").fillna(0.0),
        "FuelCost_THB_base": pd.to_numeric(d["FuelCost_THB_2568"], errors="coerce").fillna(0.0),
        "AllIn_base_2568": pd.to_numeric(d["AllIn_THB_per_kWh_2568"], errors="coerce").fillna(0.0),
        "FuelIntensity_base_2568": np.where(
            pd.to_numeric(d["Energy_kWh_2568"], errors="coerce").fillna(0.0) > 0,
            pd.to_numeric(d["Fuel_L_2568"], errors="coerce").fillna(0.0) / pd.to_numeric(d["Energy_kWh_2568"], errors="coerce").fillna(0.0),
            0.0
        ),
    }).sort_values("Month")

    # ensure 12 months (fill missing)
    full = pd.DataFrame({"Month": MONTH_ORDER})
    out = full.merge(out, on="Month", how="left").fillna(0.0)
    out["Month_TH"] = out["Month"].map(lambda m: MONTH_LABELS_TH[int(m)])
    out["Month_EN"] = out["Month"].map(lambda m: MONTH_LABELS_EN[int(m)])
    return out


# -----------------------------
# Charts (Altair with legend-toggle)
# -----------------------------
def _line_chart_legend_toggle(df_long: pd.DataFrame, title: str, y_title: str = "") -> alt.Chart:
    """
    df_long: columns: x (Month or Datetime), metric, value
    Uses legend-click selection to toggle series.
    """
    sel = alt.selection_point(fields=["metric"], bind="legend")
    base = alt.Chart(df_long).mark_line().encode(
        x=alt.X("x:O", sort=MONTH_ORDER, title="Month"),
        y=alt.Y("value:Q", title=y_title),
        color=alt.Color("metric:N", legend=alt.Legend(orient="bottom")),
        opacity=alt.condition(sel, alt.value(1.0), alt.value(0.12)),
        tooltip=["x:O", "metric:N", alt.Tooltip("value:Q", format=",.2f")],
    ).add_params(sel).properties(height=320, title=title)
    return base


def _stacked_bar(df_wide: pd.DataFrame, title: str, cols: list[str]) -> alt.Chart:
    d = df_wide[["Month"] + cols].copy()
    long = d.melt("Month", var_name="metric", value_name="value")
    return (
        alt.Chart(long)
        .mark_bar()
        .encode(
            x=alt.X("Month:O", sort=MONTH_ORDER, title="Month"),
            y=alt.Y("value:Q", title="Hours", stack=True),
            color=alt.Color("metric:N", legend=alt.Legend(orient="bottom")),
            tooltip=["Month:O", "metric:N", alt.Tooltip("value:Q", format=",.1f")],
        )
        .properties(height=320, title=title)
    )


def _build_mode_segments(df: pd.DataFrame, dt_h: float) -> pd.DataFrame:
    d = df[["Datetime", "Mode_final"]].copy()
    d["Datetime"] = pd.to_datetime(d["Datetime"])
    d["Mode_final"] = d["Mode_final"].astype(str).str.upper().fillna("A")
    d["chg"] = (d["Mode_final"] != d["Mode_final"].shift(1)).cumsum()
    seg = d.groupby("chg").agg(
        start=("Datetime", "min"),
        end=("Datetime", "max"),
        Mode=("Mode_final", "first"),
    ).reset_index(drop=True)
    seg["end"] = seg["end"] + pd.to_timedelta(dt_h, unit="h")
    return seg


def _ts_chart_with_mode_bg(df: pd.DataFrame, metrics: list[str], title: str, mode_colors: dict[str, str], mode_opacity: float) -> alt.Chart:
    d = df.copy()
    d["Datetime"] = pd.to_datetime(d["Datetime"])
    dt_h = _infer_dt_h(d["Datetime"], default="15min")

    layers = []
    if "Mode_final" in d.columns:
        seg = _build_mode_segments(d, dt_h)
        mode_scale = alt.Scale(domain=list(mode_colors.keys()), range=list(mode_colors.values()))
        rect = alt.Chart(seg).mark_rect(opacity=float(mode_opacity)).encode(
            x="start:T",
            x2="end:T",
            color=alt.Color("Mode:N", scale=mode_scale, legend=alt.Legend(title="Group", orient="top")),
            tooltip=["Mode:N", "start:T", "end:T"],
        )
        layers.append(rect)

    cols = [c for c in metrics if c in d.columns]
    if not cols:
        return alt.Chart(pd.DataFrame({"Datetime": [], "metric": [], "value": []})).mark_line()

    long = d[["Datetime"] + cols].melt("Datetime", var_name="metric", value_name="value")

    sel = alt.selection_point(fields=["metric"], bind="legend")
    line = alt.Chart(long).mark_line().encode(
        x=alt.X("Datetime:T", title=None),
        y=alt.Y("value:Q", title=None),
        color=alt.Color("metric:N", legend=alt.Legend(orient="bottom")),
        opacity=alt.condition(sel, alt.value(1.0), alt.value(0.12)),
        tooltip=["Datetime:T", "metric:N", alt.Tooltip("value:Q", format=",.3f")],
    ).add_params(sel)

    layers.append(line)
    chart = alt.layer(*layers).properties(height=360, title=title).resolve_scale(color="independent")
    return chart.interactive()


# -----------------------------
# Gen schedule (hours)
# -----------------------------
def _monthly_group_hours(df: pd.DataFrame, dt_h: float) -> pd.DataFrame:
    d = df.copy()
    d["Datetime"] = pd.to_datetime(d["Datetime"])
    d["Month"] = d["Datetime"].dt.month.astype(int)
    if "Mode_final" not in d.columns:
        return pd.DataFrame({"Month": MONTH_ORDER, "A_hours": 0.0, "B_hours": 0.0})
    mf = d["Mode_final"].astype(str).str.upper().fillna("A")
    out = d.assign(mf=mf).groupby("Month", as_index=False).agg(
        A_hours=("mf", lambda x: float((x == "A").sum()) * dt_h),
        B_hours=("mf", lambda x: float((x == "B").sum()) * dt_h),
    )
    full = pd.DataFrame({"Month": MONTH_ORDER})
    out = full.merge(out, on="Month", how="left").fillna(0.0)
    return out


# -----------------------------
# App
# -----------------------------
def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)

    with st.sidebar:
        st.header("Data sources")

        exec_path = st.text_input("Executive pack (xlsx)", value=_default_exec_path())
        dispatch_path = st.text_input("Dispatch output (xlsx)", value=_default_dispatch_path())
        std_path = st.text_input("StandardValue (Base 2568) (xlsx)", value=_default_standard_path())

        st.caption("Tip: ถ้าใช้ 'Copy as path' แล้วมีเครื่องหมาย quote ระบบจะตัดออกให้อัตโนมัติ")
        exec_path = _strip_quotes(exec_path)
        dispatch_path = _strip_quotes(dispatch_path)
        std_path = _strip_quotes(std_path)

        with st.expander("Files found in out_kotao_2569 (ช่วยเลือกชื่อไฟล์)"):
            files = _list_xlsx_in_outdir(DEFAULT_OUT_DIR)
            if files:
                st.code("\n".join(files))
            else:
                st.write("ไม่พบโฟลเดอร์หรือไม่มีไฟล์ .xlsx ในโฟลเดอร์นี้:", DEFAULT_OUT_DIR)

        st.divider()
        st.header("Fuel cost settings (EMS)")

        st.caption("EMS Fuel cost มี 2 โหมด:\n- **proxy_allin**: DG_kWh × All-in(THB/kWh)\n- **from_dispatch**: ใช้คอลัมน์ FuelCost_THB จากไฟล์ dispatch (ถ้ามี) ไม่งั้น fallback ไป proxy")
        cost_mode = st.selectbox("EMS cost mode", ["proxy_allin", "from_dispatch"], index=0)

        allin_ems = st.number_input("All-in EMS (THB/kWh) – default", value=10.12, step=0.01, format="%.2f")
        st.caption("Optional: ใส่ All-in รายเดือน (override) เช่น 1=10.10,2=10.08,...")
        month_text_ems = st.text_area("All-in monthly EMS (THB/kWh)", value="", height=110)
        month_values_ems = _parse_month_prices(month_text_ems)

        st.divider()
        st.header("Investment & Payback")
        capex_thb = float(CAPEX_FIXED_THB)
        opex_delta_thb_y = 0.0
        st.metric("CAPEX (THB) – fixed", _fmt_thb(capex_thb, 0))
        st.caption("Simple Payback = CAPEX / Annual saving (FuelCost Base − FuelCost EMS).")

        st.divider()
        st.header("Chart styling (Group background)")
        col_a = st.color_picker("Group A background", value="#FFE066")  # yellow
        col_b = st.color_picker("Group B background", value="#B197FC")  # purple
        bg_opacity = st.slider("Background opacity", min_value=0.0, max_value=0.35, value=0.12, step=0.01)

        st.divider()
        st.header("Dispatch view controls")
        month_opts = ["All"] + [f"{m:02d} ({MONTH_LABELS_TH[m]})" for m in range(1, 13)]
        sel_month = st.selectbox("เลือกเดือน (สำหรับกราฟ 15-min)", month_opts, index=0)

    # -----------------------------
    # Load data
    # -----------------------------
    exec_data: dict[str, pd.DataFrame] = {}
    dispatch_data: dict[str, pd.DataFrame] = {}
    std2568: Optional[pd.DataFrame] = None

    if exec_path and Path(exec_path).exists():
        exec_data = load_exec_pack(exec_path)

    if dispatch_path and Path(dispatch_path).exists():
        dispatch_data = load_dispatch_pack(dispatch_path)

    std_error = None
    if std_path and Path(std_path).exists():
        try:
            std2568 = load_standard_value_2568(std_path)
        except Exception as e:
            std_error = str(e)
            std2568 = None

    # -----------------------------
    # Tabs
    # -----------------------------
    tab_exec, tab_dispatch, tab_cost = st.tabs(["📊 Executive pack", "🧭 Dispatch (15-min)", "💸 DG energy & Fuel cost"])

    # =============================
    # Executive pack
    # =============================
    with tab_exec:
        if not exec_data:
            st.info("Executive pack ไม่จำเป็นสำหรับการเทียบ DG Base vs EMS (คุณสามารถเว้นได้) แต่ถ้ามีจะโชว์ KPI เพิ่มเติม")
        else:
            summary = exec_data.get("Exec_Summary", pd.DataFrame())
            monthly = exec_data.get("Monthly", pd.DataFrame())
            typical = exec_data.get("TypicalDay_PeakMonth", pd.DataFrame())

            if not summary.empty:
                st.subheader("Executive KPIs (พร้อมคำอธิบาย)")
                summary_th = _add_exec_kpi_details_th(summary)
                st.dataframe(summary_th, use_container_width=True)

            if not monthly.empty:
                st.subheader("Monthly (Executive pack)")
                st.dataframe(monthly, use_container_width=True)

            if not typical.empty:
                st.subheader("Typical day (peak month)")
                st.dataframe(typical, use_container_width=True)

            with st.expander("Inputs"):
                if "Inputs" in exec_data:
                    st.dataframe(exec_data["Inputs"], use_container_width=True)

    # =============================
    # Dispatch viewer
    # =============================
    with tab_dispatch:
        if not dispatch_data or "Profiles_15min" not in dispatch_data or dispatch_data["Profiles_15min"].empty:
            st.error("ต้องระบุ Dispatch output ที่มี sheet 'Profiles_15min' (EMS dispatch) ก่อน")
            st.stop()

        prof = _ensure_dt(dispatch_data["Profiles_15min"], "Datetime")
        prof = _ensure_dg_split(prof)

        # dt
        dt_h = _infer_dt_h(prof["Datetime"], default="15min")

        # month filter
        prof["Month"] = prof["Datetime"].dt.month.astype(int)
        if sel_month == "All":
            prof_m = prof.copy()
        else:
            mm = int(sel_month.split()[0])
            prof_m = prof[prof["Month"] == mm].copy()

        # day selector
        prof_m["Date"] = prof_m["Datetime"].dt.date
        dates = sorted(prof_m["Date"].dropna().unique().tolist())
        if not dates:
            st.warning("ไม่มีข้อมูลในช่วงเดือนที่เลือก")
            st.stop()

        left, right = st.columns([1, 3])
        with left:
            day_mode = st.radio("มุมมองเวลา", ["ทั้งช่วงที่เลือก", "เลือกวัน"], index=0, horizontal=True)
            if day_mode == "เลือกวัน":
                picked = st.date_input("เลือกวันที่", value=dates[0], min_value=min(dates), max_value=max(dates))
                if picked not in dates:
                    ords = np.array([d.toordinal() for d in dates], dtype=int)
                    picked_ord = int(picked.toordinal())
                    nearest = dates[int(np.argmin(np.abs(ords - picked_ord)))]
                    st.info(f"วันที่ {picked} ไม่มีข้อมูล → เลือกวันใกล้สุด: {nearest}")
                    picked = nearest
                prof_d = prof_m[prof_m["Date"] == picked].copy()
            else:
                prof_d = prof_m.copy()

            # metric selector
            default_metrics = [
                "DG_Total_MW", "DG_NamSaeng_MW", "DG_PEA_MW", "DG_Mobile_MW",
                "Sub_MW", "Load_KoTao_MW", "Headroom_115kV_MW", "LoadShed_MW",
            ]
            available = [m for m in default_metrics if m in prof_d.columns]
            sel_metrics = st.multiselect("เลือกเส้นที่จะโชว์ (Legend คลิกซ่อน/แสดงได้)", available, default=available)

        # KPI for the selected window
        dg_kwh = float(np.nansum(prof_d["DG_Total_MW"].to_numpy(float) * 1000.0 * dt_h))
        allin_row = _month_series_from_dt(prof_d["Datetime"], month_values_ems, float(allin_ems))
        fuelcost_proxy = float(np.nansum((prof_d["DG_Total_MW"].to_numpy(float) * 1000.0 * dt_h) * allin_row))

        fuel_l = float(np.nansum(pd.to_numeric(prof_d.get("Fuel_L", 0.0), errors="coerce").fillna(0.0))) if isinstance(prof_d, pd.DataFrame) else 0.0
        # If user selects from_dispatch AND dispatch has FuelCost_THB, use it; otherwise use proxy.
        if str(cost_mode) == "from_dispatch" and "FuelCost_THB" in prof_d.columns:
            fuelcost_ems = float(np.nansum(pd.to_numeric(prof_d["FuelCost_THB"], errors="coerce").fillna(0.0)))
            cost_label = "Fuel cost (THB) – from dispatch"
        else:
            fuelcost_ems = float(fuelcost_proxy)
            cost_label = "Fuel cost (THB) – proxy"

        allin_eff = _safe_div(fuelcost_ems, dg_kwh, 0.0)
        thb_per_l = _safe_div(fuelcost_ems, fuel_l, 0.0)
        l_per_kwh = _safe_div(fuel_l, dg_kwh, 0.0)
        thb_per_mwh = _safe_div(fuelcost_ems, _safe_div(dg_kwh, 1000.0, 0.0), 0.0)

        r1c1, r1c2, r1c3, r1c4 = st.columns(4)
        r1c1.metric("DG Energy (MWh)", _fmt_num(dg_kwh/1000.0, 2))
        r1c2.metric("Fuel (L) – from sim", _fmt_num(fuel_l, 0))
        r1c3.metric(cost_label, _fmt_thb(fuelcost_ems, 0))
        r1c4.metric("All-in effective (THB/kWh)", _fmt_num(allin_eff, 2))

        r2c1, r2c2, r2c3, r2c4 = st.columns(4)
        r2c1.metric("Cost (MTHB)", _fmt_mthb(fuelcost_ems, 2))
        r2c2.metric("THB/L (effective)", _fmt_num(thb_per_l, 2))
        r2c3.metric("Fuel intensity (L/kWh)", _fmt_num(l_per_kwh, 4))
        r2c4.metric("THB/MWh (effective)", _fmt_num(thb_per_mwh, 0))

        st.subheader("Dispatch time series (with 115kV headroom + Group A/B background)")
        chart = _ts_chart_with_mode_bg(
            prof_d,
            metrics=sel_metrics,
            title="(Legend คลิกซ่อน/แสดง series ได้)",
            mode_colors={"A": col_a, "B": col_b},
            mode_opacity=float(bg_opacity),
        )
        st.altair_chart(chart, use_container_width=True)

        with st.expander("ดูข้อมูล 15-min (raw)"):
            st.dataframe(prof_d.drop(columns=["Month", "Date"], errors="ignore"), use_container_width=True)

    # =============================
    # Fuel cost tab
    # =============================
    with tab_cost:
        if std_error:
            st.error(f"อ่าน StandardValue ไม่ได้: {std_error}")
        if std2568 is None:
            st.warning("เพื่อให้ Base(2568) ถูกต้อง (Fuel cost 399,200,313) กรุณาระบุไฟล์ KoTao_StandardValue_Charts.xlsx ให้ถูกต้อง")
        if not dispatch_data or "Profiles_15min" not in dispatch_data or dispatch_data["Profiles_15min"].empty:
            st.error("ต้องระบุ Dispatch output (Profiles_15min) ก่อน")
            st.stop()

        prof = _ensure_dt(dispatch_data["Profiles_15min"], "Datetime")
        dt_h = _infer_dt_h(prof["Datetime"], default="15min")

        ems_month = _monthly_from_dispatch(
            prof,
            dt_h=dt_h,
            allin_fallback=float(allin_ems),
            allin_month_override=month_values_ems,
            cost_mode=str(cost_mode),
        )

        # base monthly from standard
        if std2568 is not None:
            base_month = _monthly_base_from_standard(std2568)
        else:
            base_month = pd.DataFrame({"Month": MONTH_ORDER})
            base_month["DG_MWh_base"] = 0.0
            base_month["FuelCost_THB_base"] = 0.0
            base_month["Fuel_L_base"] = 0.0
            base_month["Month_TH"] = base_month["Month"].map(lambda m: MONTH_LABELS_TH[int(m)])
            base_month["Month_EN"] = base_month["Month"].map(lambda m: MONTH_LABELS_EN[int(m)])

        # merge on Month only (critical)
        wide = base_month.merge(
            ems_month.rename(columns={
                "DG_MWh": "DG_MWh_ems",
                "DG_kWh": "DG_kWh_ems",
                "Fuel_L": "Fuel_L_ems",
                "FuelCost_THB": "FuelCost_THB_ems",
                "AllIn_THB_per_kWh": "AllIn_ems",
                "NS_MWh": "NS_MWh_ems",
                "PEA_MWh": "PEA_MWh_ems",
                "Mobile_MWh": "Mobile_MWh_ems",
            }),
            on="Month",
            how="left",
            suffixes=("", "_dup"),
        ).fillna(0.0)

        wide["Month_TH"] = wide["Month"].map(lambda m: MONTH_LABELS_TH[int(m)])
        wide["Month_EN"] = wide["Month"].map(lambda m: MONTH_LABELS_EN[int(m)])

        # savings
        wide["Saving_THB"] = wide["FuelCost_THB_base"] - wide["FuelCost_THB_ems"]
        wide["Saving_pct"] = np.where(wide["FuelCost_THB_base"] > 0, wide["Saving_THB"] / wide["FuelCost_THB_base"] * 100.0, 0.0)

        # derived finance metrics (per month)
        if "DG_kWh_base" not in wide.columns:
            wide["DG_kWh_base"] = wide["DG_MWh_base"].astype(float) * 1000.0
        wide["AllIn_eff_base"] = np.where(wide["DG_kWh_base"] > 0, wide["FuelCost_THB_base"] / wide["DG_kWh_base"], 0.0)
        wide["AllIn_eff_ems"]  = np.where(wide["DG_kWh_ems"]  > 0, wide["FuelCost_THB_ems"]  / wide["DG_kWh_ems"],  0.0)
        wide["THB_per_L_base"] = np.where(wide["Fuel_L_base"] > 0, wide["FuelCost_THB_base"] / wide["Fuel_L_base"], 0.0)
        wide["THB_per_L_ems"]  = np.where(wide["Fuel_L_ems"]  > 0, wide["FuelCost_THB_ems"]  / wide["Fuel_L_ems"],  0.0)
        wide["Saving_MTHB"] = wide["Saving_THB"] / 1_000_000.0

        # KPIs year
        tot_base = float(np.nansum(wide["FuelCost_THB_base"]))
        tot_ems = float(np.nansum(wide["FuelCost_THB_ems"]))
        saving = tot_base - tot_ems
        saving_pct = (saving / tot_base * 100.0) if tot_base > 0 else 0.0

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Fuel cost Base 2568 (THB)", help="ค่าใช้จ่ายเชื้อเพลิงปีที่ยังไม่ได้ติดตั้ง EMS (THB)", value=_fmt_thb(tot_base, 0))
        k2.metric("Fuel cost EMS 2569 (THB)", help="ค่าใช้จ่ายเชื้อเพลิงปีที่ติดตั้ง EMS (THB)", value=_fmt_thb(tot_ems, 0))
        k3.metric("Saving (THB)", help="การประหยัดค่าใช้จ่ายเชื้อเพลิง = Fuel cost Base 2568 (THB) - Fuel cost EMS 2569 (THB)", value=_fmt_thb(saving, 0))
        k4.metric("Saving (%)", help="เปอร์เซ็นต์การประหยัดค่าใช้จ่ายเชื้อเพลิง (%)", value=_fmt_num(saving_pct, 1) + "%")

        # additional yearly finance KPIs
        tot_dg_kwh_base = float(np.nansum(wide.get("DG_kWh_base", wide["DG_MWh_base"] * 1000.0)))
        tot_dg_kwh_ems  = float(np.nansum(wide.get("DG_kWh_ems", 0.0)))
        tot_dg_mwh_base = _safe_div(tot_dg_kwh_base, 1000.0, 0.0)
        tot_dg_mwh_ems  = _safe_div(tot_dg_kwh_ems, 1000.0, 0.0)

        tot_fuel_l_base = float(np.nansum(wide.get("Fuel_L_base", 0.0)))
        tot_fuel_l_ems  = float(np.nansum(wide.get("Fuel_L_ems", 0.0)))

        allin_eff_base = _safe_div(tot_base, tot_dg_kwh_base, 0.0)
        allin_eff_ems  = _safe_div(tot_ems,  tot_dg_kwh_ems,  0.0)
        thb_per_l_base = _safe_div(tot_base, tot_fuel_l_base, 0.0)
        thb_per_l_ems  = _safe_div(tot_ems,  tot_fuel_l_ems,  0.0)


        st.markdown("### DG (Diesel Generator) – พลังงานและเชื้อเพลิง")
        st.caption(
            "นิยาม: DG = เครื่องกำเนิดไฟฟ้าดีเซลทั้งหมดในระบบเกาะเต่า (รวม NamSaeng / PEA / Mobile ตามที่อยู่ในไฟล์ dispatch). "
            "พลังงาน DG (kWh/MWh) ของปีฐานอ่านจากไฟล์ StandardValue; ส่วนของ EMS คำนวณจาก Dispatch (Profiles_15min) ด้วยสูตร Σ(DG_Total_MW×1000×dt_h)."
        )
        r2c1, r2c2, r2c3, r2c4 = st.columns(4)
        r2c1.metric("DG energy Base 2568 (MWh)", help="พลังงานโหลดของ DG ปีที่ยังไม่ได้ติดตั้ง EMS (MWh)", value=_fmt_num(tot_dg_mwh_base, 1))
        r2c2.metric("DG energy EMS 2569 (MWh)", help="พลังงานโหลดของ DG ปีที่ติดตั้ง EMS (MWh)", value=_fmt_num(tot_dg_mwh_ems, 1))
        r2c3.metric("All-in effective Base (THB/kWh)", help="ค่า All-in เฉลี่ยของปีที่ยังไม่ได้ติดตั้ง EMS (THB/kWh)", value=_fmt_num(allin_eff_base, 2))
        r2c4.metric("All-in effective EMS (THB/kWh)", help="ค่า All-in เฉลี่ยของปีที่ติดตั้ง EMS (THB/kWh)", value=_fmt_num(allin_eff_ems, 2))

        r3c1, r3c2, r3c3, r3c4 = st.columns(4)
        r3c1.metric("Fuel Base 2568 (L)", help="ปริมาณเชื้อเพลิงปีที่ยังไม่ได้ติดตั้ง EMS (L)", value=_fmt_num(tot_fuel_l_base, 0))
        r3c2.metric("Fuel EMS 2569 (L)", help="ปริมาณเชื้อเพลิงปีที่ติดตั้ง EMS (L)", value=_fmt_num(tot_fuel_l_ems, 0))
        r3c3.metric("THB/L [Base] ", help="ต้นทุนค่าใช้จ่ายเฉลี่ยต่อลิตรของปีที่ยังไม่ได้ติดตั้ง EMS (THB/L)", value=_fmt_num(thb_per_l_base, 2))
        r3c4.metric("THB/L [EMS] ", help="ต้นทุนค่าใช้จ่ายเฉลี่ยต่อลิตรของปีที่ติดตั้ง EMS (THB/L)", value=_fmt_num(thb_per_l_ems, 2))

        # -----------------------------
        # Payback (simple)
        # -----------------------------
        if float(capex_thb) > 0:
            st.subheader("Payback (simple) – based on annual saving")
            net_annual_saving = float(saving)

            pb1, pb2, pb3, pb4 = st.columns(4)
            pb1.metric("CAPEX (THB)", help="ต้นทุนการลงทุน (THB)", value=_fmt_thb(capex_thb, 0))
            pb2.metric("Annual saving (THB/yr)", help="ค่าใช้จ่ายลดลงต่อปี (THB/yr)", value=_fmt_thb(net_annual_saving, 0))

            if net_annual_saving > 0:
                payback_years = float(capex_thb) / float(net_annual_saving)
                pb3.metric("Simple payback (years)", help="ระยะเวลาคืนทุนแบบง่าย (ปี)", value=_fmt_num(payback_years, 2))
                pb4.metric("Simple payback (months)", help="ระยะเวลาคืนทุนแบบง่าย (เดือน)", value=_fmt_num(payback_years * 12.0, 1))

                # Cumulative net saving within the year (month-by-month)
                cum = wide.sort_values("Month")[["Month", "Saving_THB"]].copy()
                cum["NetSaving_THB"] = cum["Saving_THB"].astype(float)
                cum["CumNetSaving_THB"] = cum["NetSaving_THB"].cumsum()

                hit = cum[cum["CumNetSaving_THB"] >= float(capex_thb)]
                if not hit.empty:
                    m_hit = int(hit.iloc[0]["Month"])
                    st.success(
                        f"คืนทุนภายในปีนี้ประมาณ {payback_years:.2f} ปี  — ภายใต้การคิดบนข้อมูลจาก Fuel Cost จากทาง PEA"
                    )
                else:
                    st.info("ยังไม่คืนทุนภายใน 12 เดือน (คำนวณแบบ simple; สมมติ saving ต่อปีคงที่)")
                # (Removed) Cumulative net saving vs CAPEX chart – per user request
            else:
                pb3.metric("Simple payback (years)", "N/A")
                pb4.metric("หมายเหตุ", "Net annual saving ≤ 0")

        with st.expander("ที่มาของตัวเลข / วิธีคำนวณ (Base 2568 vs EMS 2569)"):
            st.markdown(
                """
**1) ขอบเขตคำว่า “DG”**
- **DG (Diesel Generator)** ใน Dashboard นี้หมายถึงพลังงานที่ผลิตจากเครื่องยนต์ดีเซลทั้งหมดในระบบ (รวม NamSaeng / PEA / Mobile ตามคอลัมน์ในไฟล์ Dispatch)
- พลังงาน DG แสดงได้ทั้ง **kWh** และ **MWh** (1 MWh = 1,000 kWh)

**2) ที่มาข้อมูลปีฐาน (Base 2568)**
อ่านจากไฟล์ **KoTao_StandardValue_Charts.xlsx** → sheet **`Inputs`**
- `Energy_kWh_2568` → ใช้เป็น **DG_kWh_base** (รวมรายเดือนเป็นทั้งปี)
- `FuelCost_THB_2568` → ใช้เป็น **FuelCost_THB_base**
- `Fuel_L_2568` → ใช้เป็น **Fuel_L_base**
- `AllIn_THB_per_kWh_2568` → ใช้เป็นค่าอ้างอิง “all-in” รายเดือนของปีฐาน (ถ้ามี)

**3) ที่มาข้อมูลกรณี EMS (ปี 2569)**
อ่านจากไฟล์ Dispatch → sheet **`Profiles_15min`**
- ใช้คอลัมน์หลัก: `Datetime`, `DG_Total_MW` (และถ้ามี `DG_NamSaeng_MW`, `DG_PEA_MW`, `DG_Mobile_MW`)
- คำนวณช่วงเวลา `dt_h` จากความถี่ของ Datetime (เช่น 15 นาที → 0.25 ชั่วโมง)

**4) สูตรคำนวณพลังงาน DG (EMS)**
- `DG_kWh_ems = Σ_t (DG_Total_MW(t) × 1000 × dt_h)`
- `DG_MWh_ems = DG_kWh_ems / 1000`

**5) สูตรคำนวณ Fuel cost (EMS) – เลือกได้ 2 โหมด**
- **from_dispatch** (ถ้าไฟล์มีคอลัมน์ `FuelCost_THB`):  
  `FuelCost_THB_ems = Σ_t FuelCost_THB(t)`
- **proxy_allin** (หรือกรณีไม่มี `FuelCost_THB`):  
  `FuelCost_THB_ems = Σ_t (DG_Total_MW(t) × 1000 × dt_h × All-in(t))`  
  โดย `All-in(t)` มาจากค่า **All-in default** หรือ **All-in รายเดือน (override)** ที่ตั้งใน Sidebar

**6) นิยามตัวชี้วัดที่แสดงบนหน้า**
- `All-in effective (THB/kWh) = FuelCost_THB / DG_kWh`
- `THB/L (effective) = FuelCost_THB / Fuel_L`
- `Saving (THB) = FuelCost_THB_base − FuelCost_THB_ems`
- `Saving (%) = Saving / FuelCost_THB_base × 100`

**7) Simple Payback**
- กำหนด **CAPEX = 28,547,600 บาท**
- `Payback (years) = CAPEX / Annual saving`
- `Payback (months) = Payback (years) × 12`
                """
            )

        # monthly charts
        c1, c2 = st.columns(2)
        with c1:
            long = pd.DataFrame({
                "x": wide["Month"].astype(int),
                "DG_MWh_base": wide["DG_MWh_base"].astype(float),
                "DG_MWh_ems": wide["DG_MWh_ems"].astype(float),
            }).melt("x", var_name="metric", value_name="value").rename(columns={"x": "x"})
            st.altair_chart(_line_chart_legend_toggle(long, "DG energy (MWh): Base vs EMS", "MWh"), use_container_width=True)

        with c2:
            long = pd.DataFrame({
                "x": wide["Month"].astype(int),
                "FuelCost_THB_base": wide["FuelCost_THB_base"].astype(float),
                "FuelCost_THB_ems": wide["FuelCost_THB_ems"].astype(float),
            }).melt("x", var_name="metric", value_name="value")
            st.altair_chart(_line_chart_legend_toggle(long, "Fuel cost (THB): Base vs EMS", "THB"), use_container_width=True)
        # (Removed) Monthly saving chart – per user request

        # group hours chart from EMS dispatch
        gh = _monthly_group_hours(_ensure_dt(dispatch_data["Profiles_15min"], "Datetime"), dt_h)
        st.altair_chart(_stacked_bar(gh, "Group A vs Group B (hours / month) – EMS", ["A_hours", "B_hours"]), use_container_width=True)

        # monthly table
        st.subheader("Monthly table (Base 2568 vs EMS 2569)")
        # downloads
        with st.expander("Download monthly table"):
            csv_bytes = _to_bytes_csv(wide.sort_values("Month"))
            st.download_button("Download CSV", data=csv_bytes, file_name="KoTao_Base2568_vs_EMS2569_monthly.csv", mime="text/csv")
            xlsx_bytes = _to_bytes_xlsx({"Base_vs_EMS_Monthly": wide.sort_values("Month")})
            st.download_button("Download Excel", data=xlsx_bytes, file_name="KoTao_Base2568_vs_EMS2569_monthly.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        cols = [
            "Month_TH", "Month_EN", "Month",
            "DG_kWh_base", "DG_kWh_ems",
            "DG_MWh_base", "DG_MWh_ems",
            "NS_MWh_ems", "PEA_MWh_ems", "Mobile_MWh_ems",
            "Fuel_L_base", "Fuel_L_ems",
            "FuelCost_THB_base", "FuelCost_THB_ems",
            "AllIn_base_2568", "AllIn_ems",
            "AllIn_eff_base", "AllIn_eff_ems",
            "THB_per_L_base", "THB_per_L_ems",
            "Saving_THB", "Saving_MTHB", "Saving_pct",
        ]
        cols = [c for c in cols if c in wide.columns]
        df_show = wide[cols].sort_values("Month").copy()

        # human-friendly rounding for finance columns
        fmt_map = {}
        for c in df_show.columns:
            if c in ["Month"]:
                continue
            if "pct" in c.lower():
                fmt_map[c] = "{:,.2f}"
            elif ("THB" in c) or ("Cost" in c):
                fmt_map[c] = "{:,.0f}"
            elif ("MWh" in c):
                fmt_map[c] = "{:,.2f}"
            elif ("kWh" in c):
                fmt_map[c] = "{:,.0f}"
            elif ("AllIn" in c) or ("THB_per" in c) or ("eff" in c):
                fmt_map[c] = "{:,.2f}"
            elif ("Fuel" in c) or (c.endswith("_L")):
                fmt_map[c] = "{:,.0f}"

        st.dataframe(df_show.style.format(fmt_map), use_container_width=True, height=480)

        # split share chart (MWh) – EMS
        st.subheader("DG split energy (MWh) – EMS")
        split = wide[["Month", "NS_MWh_ems", "PEA_MWh_ems", "Mobile_MWh_ems"]].copy()
        split_long = split.melt("Month", var_name="metric", value_name="value").rename(columns={"Month": "x"})
        st.altair_chart(_line_chart_legend_toggle(split_long, "DG split (MWh) – EMS", "MWh"), use_container_width=True)

    st.caption("หมายเหตุ: Legend คลิกเพื่อซ่อน/แสดงเส้น (Altair selection)")

if __name__ == "__main__":
    main()
