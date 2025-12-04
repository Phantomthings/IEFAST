"""
Router pour les filtres dynamiques
Les options de Type d'erreur et Moment dépendent des données filtrées par sites/période
"""

from fastapi import APIRouter, Request, Query
from fastapi.responses import JSONResponse
from datetime import date
import pandas as pd

from db import query_df

router = APIRouter(tags=["filters"])

MOMENT_ORDER = ["Init", "Lock Connector", "CableCheck", "Charge", "Fin de charge", "Unknown"]


@router.get("/filters/options")
async def get_filter_options(
    sites: str = Query(default=""),
    date_debut: date = Query(default=None),
    date_fin: date = Query(default=None),
):
    """
    Retourne les options dynamiques pour Type d'erreur et Moment d'erreur
    basées sur les données filtrées par sites/période
    """
    # Construire la requête avec filtres
    conditions = ["1=1"]
    
    if date_debut:
        conditions.append(f"`Datetime start` >= '{date_debut}'")
    if date_fin:
        conditions.append(f"`Datetime start` < DATE_ADD('{date_fin}', INTERVAL 1 DAY)")
    
    site_list = [s.strip() for s in sites.split(",") if s.strip()] if sites else []
    if site_list:
        sites_str = "','".join(site_list)
        conditions.append(f"Site IN ('{sites_str}')")
    
    where_clause = " AND ".join(conditions)
    
    # Récupérer les valeurs uniques de type_erreur et moment
    sql = f"""
        SELECT DISTINCT type_erreur, moment
        FROM kpi_sessions
        WHERE {where_clause}
          AND (type_erreur IS NOT NULL OR moment IS NOT NULL)
    """
    
    df = query_df(sql)
    
    # Options type_erreur
    error_types = []
    if "type_erreur" in df.columns:
        error_types = sorted(df["type_erreur"].dropna().unique().tolist())
    
    # Options moment (ordonnées selon MOMENT_ORDER)
    moments = []
    if "moment" in df.columns:
        raw_moments = df["moment"].dropna().unique().tolist()
        # Ordonner selon MOMENT_ORDER, puis ajouter les autres
        moments = [m for m in MOMENT_ORDER if m in raw_moments]
        moments += [m for m in raw_moments if m not in MOMENT_ORDER]
    
    return JSONResponse({
        "error_types": error_types,
        "moments": moments,
    })


@router.get("/filters/sites")
async def get_sites():
    """
    Retourne la liste de tous les sites disponibles
    """
    sql = """
        SELECT DISTINCT Site 
        FROM kpi_sessions 
        WHERE Site IS NOT NULL 
        ORDER BY Site
    """
    df = query_df(sql)
    sites = df["Site"].tolist() if not df.empty else []
    
    return JSONResponse({"sites": sites})
