from fastapi import APIRouter, Request, Query
from fastapi.templating import Jinja2Templates
from datetime import date
import pandas as pd
import numpy as np
import re

from db import query_df, table_exists

router = APIRouter(tags=["mac_address"])
templates = Jinja2Templates(directory="templates")

BASE_CHARGE_URL = "https://elto.nidec-asi-online.com/Charge/detail?id="


def _fmt_mac(mac: str) -> str:
    if pd.isna(mac) or not mac:
        return ""
    s = str(mac).strip().lower().replace("0x", "")
    s = re.sub(r"[^0-9a-f]", "", s)
    if len(s) >= 12:
        return ":".join([s[i:i+2] for i in range(0, 12, 2)]).upper()
    return mac.upper()


def _format_soc_evolution(s0, s1):
    if pd.notna(s0) and pd.notna(s1):
        try:
            return f"{int(round(s0))}% → {int(round(s1))}%"
        except Exception:
            return ""
    return ""


def _build_conditions(sites: str, date_debut: date | None, date_fin: date | None):
    conditions = ["1=1"]
    params = {}

    if date_debut:
        conditions.append("`Datetime start` >= :date_debut")
        params["date_debut"] = str(date_debut)
    if date_fin:
        conditions.append("`Datetime start` < DATE_ADD(:date_fin, INTERVAL 1 DAY)")
        params["date_fin"] = str(date_fin)
    if sites:
        site_list = [s.strip() for s in sites.split(",") if s.strip()]
        if site_list:
            placeholders = ",".join([f":site_{i}" for i in range(len(site_list))])
            conditions.append(f"Site IN ({placeholders})")
            for i, s in enumerate(site_list):
                params[f"site_{i}"] = s

    return " AND ".join(conditions), params


@router.get("/mac-address/search")
async def search_mac(
    request: Request,
    sites: str = Query(default=""),
    date_debut: date = Query(default=None),
    date_fin: date = Query(default=None),
    mac_query: str = Query(default=""),
):
    if not table_exists("kpi_charges_mac"):
        return templates.TemplateResponse(
            "partials/mac_search.html",
            {
                "request": request,
                "error": "Table kpi_charges_mac non disponible",
                "mac_query": mac_query,
            }
        )

    if not mac_query or len(mac_query.strip()) < 2:
        return templates.TemplateResponse(
            "partials/mac_search.html",
            {
                "request": request,
                "prompt": "Saisissez au moins 2 caractères d'une adresse MAC",
                "mac_query": mac_query,
            }
        )

    mac_norm = mac_query.strip().lower().replace("0x", "")
    mac_norm = re.sub(r"[^0-9a-f]", "", mac_norm)

    where_clause, params = _build_conditions(sites, date_debut, date_fin)

    sql = f"""
        SELECT
            c.ID,
            c.Site,
            c.PDC,
            c.`Datetime start`,
            c.`Datetime end`,
            c.`Energy (Kwh)`,
            c.`MAC Address` as mac,
            c.Vehicle,
            c.`SOC Start`,
            c.`SOC End`,
            s.`State of charge(0:good, 1:error)` as state
        FROM kpi_charges_mac c
        LEFT JOIN kpi_sessions s ON c.ID = s.ID
        WHERE {where_clause}
    """

    df = query_df(sql, params)

    if df.empty:
        return templates.TemplateResponse(
            "partials/mac_search.html",
            {
                "request": request,
                "no_data": True,
                "mac_query": mac_query,
            }
        )

    df["mac_norm"] = (
        df["mac"].astype(str).str.lower()
        .str.replace("0x", "", regex=False)
        .str.replace(r"[^0-9a-f]", "", regex=True)
    )

    df = df[df["mac_norm"].str.contains(mac_norm, na=False)].copy()

    if df.empty:
        return templates.TemplateResponse(
            "partials/mac_search.html",
            {
                "request": request,
                "no_results": True,
                "mac_query": mac_query,
            }
        )

    for col in ["Datetime start", "Datetime end"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in ["Energy (Kwh)", "SOC Start", "SOC End"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["is_ok"] = pd.to_numeric(df["state"], errors="coerce").fillna(0).astype(int).eq(0)
    df["mac_formatted"] = df["mac"].apply(_fmt_mac)
    df["evolution_soc"] = df.apply(
        lambda r: _format_soc_evolution(r.get("SOC Start"), r.get("SOC End")), axis=1
    )
    df["elto_link"] = BASE_CHARGE_URL + df["ID"].astype(str)

    total = len(df)
    ok_count = int(df["is_ok"].sum())
    nok_count = total - ok_count
    success_rate = round(ok_count / total * 100, 1) if total else 0

    df_ok = df[df["is_ok"]].copy()
    df_nok = df[~df["is_ok"]].copy()

    if "Datetime start" in df_ok.columns:
        df_ok = df_ok.sort_values("Datetime start", ascending=False)
    if "Datetime start" in df_nok.columns:
        df_nok = df_nok.sort_values("Datetime start", ascending=False)

    display_cols = [
        "Site", "PDC", "Datetime start", "Datetime end",
        "evolution_soc", "mac_formatted", "Vehicle", "Energy (Kwh)", "elto_link"
    ]

    ok_rows = df_ok[display_cols].to_dict("records") if not df_ok.empty else []
    nok_rows = df_nok[display_cols].to_dict("records") if not df_nok.empty else []

    return templates.TemplateResponse(
        "partials/mac_search.html",
        {
            "request": request,
            "mac_query": mac_query,
            "total": total,
            "ok_count": ok_count,
            "nok_count": nok_count,
            "success_rate": success_rate,
            "ok_rows": ok_rows,
            "nok_rows": nok_rows,
        }
    )


@router.get("/mac-address/top10")
async def get_top10_unidentified(
    request: Request,
    sites: str = Query(default=""),
    date_debut: date = Query(default=None),
    date_fin: date = Query(default=None),
):
    if not table_exists("kpi_mac_id"):
        return templates.TemplateResponse(
            "partials/mac_top10.html",
            {
                "request": request,
                "error": "Table kpi_mac_id non disponible",
            }
        )

    sql = """
        SELECT Mac, nombre_de_charges
        FROM kpi_mac_id
        ORDER BY nombre_de_charges DESC
        LIMIT 10
    """

    df = query_df(sql)

    if df.empty:
        return templates.TemplateResponse(
            "partials/mac_top10.html",
            {
                "request": request,
                "no_data": True,
            }
        )

    df["Mac"] = df["Mac"].apply(_fmt_mac)
    df.insert(0, "Rang", range(1, len(df) + 1))

    rows = df.to_dict("records")

    return templates.TemplateResponse(
        "partials/mac_top10.html",
        {
            "request": request,
            "rows": rows,
        }
    )


@router.get("/mac-address")
async def get_mac_address_tab(
    request: Request,
    sites: str = Query(default=""),
    date_debut: date = Query(default=None),
    date_fin: date = Query(default=None),
):
    return templates.TemplateResponse(
        "partials/mac_address.html",
        {
            "request": request,
        }
    )
