"""
Router pour les défauts actifs
Endpoint: GET /api/defauts-actifs
"""

from fastapi import APIRouter, Request, Query
from fastapi.templating import Jinja2Templates
from datetime import datetime
import pandas as pd

from db import query_df

router = APIRouter(tags=["defauts"])
templates = Jinja2Templates(directory="templates")


@router.get("/defauts-actifs")
async def get_defauts_actifs(
    request: Request,
    sites: str = Query(default="", description="Sites séparés par virgule"),
):
    """
    Retourne le fragment HTML des défauts actifs (KPI card + liste)
    """
    # Requête SQL filtrée
    sql = """
        SELECT
            site,
            date_debut,
            defaut,
            eqp
        FROM kpi_defauts_log
        WHERE date_fin IS NULL
        ORDER BY date_debut DESC
    """
    
    df = query_df(sql)
    
    # Filtrer par sites si spécifié
    if sites:
        site_list = [s.strip() for s in sites.split(",") if s.strip()]
        if site_list:
            df = df[df["site"].isin(site_list)]
    
    # Calculs
    nb_defauts = len(df)
    nb_sites = df["site"].nunique() if not df.empty else 0
    
    # Déterminer le statut de la carte
    if nb_defauts > 5:
        status = "danger"
    elif nb_defauts > 0:
        status = "warning"
    else:
        status = "success"
    
    # Calcul de la durée
    defauts_list = []
    if not df.empty:
        df["date_debut"] = pd.to_datetime(df["date_debut"], errors="coerce")
        now = pd.Timestamp.now()
        
        for _, row in df.iterrows():
            delta_days = (now - row["date_debut"]).days if pd.notna(row["date_debut"]) else 0
            is_recent = delta_days < 1
            
            defauts_list.append({
                "site": row["site"],
                "defaut": row["defaut"],
                "eqp": row["eqp"],
                "depuis_jours": delta_days,
                "is_recent": is_recent,
                "card_class": "critical" if delta_days > 7 else "warning",
            })
    
    # Sites à surveiller (défauts < 24h)
    sites_recent = list(set(d["site"] for d in defauts_list if d["is_recent"]))
    
    # Grouper par site
    defauts_par_site = {}
    for d in defauts_list:
        site = d["site"]
        if site not in defauts_par_site:
            defauts_par_site[site] = []
        defauts_par_site[site].append(d)
    
    return templates.TemplateResponse(
        "partials/defauts_actifs.html",
        {
            "request": request,
            "nb_defauts": nb_defauts,
            "nb_sites": nb_sites,
            "status": status,
            "sites_recent": sites_recent,
            "defauts_par_site": defauts_par_site,
        }
    )
