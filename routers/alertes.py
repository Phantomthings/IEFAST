"""
Router pour les alertes
Endpoint: GET /api/alertes
"""

from fastapi import APIRouter, Request, Query
from fastapi.templating import Jinja2Templates
from datetime import date
import pandas as pd

from db import query_df

router = APIRouter(tags=["alertes"])
templates = Jinja2Templates(directory="templates")


@router.get("/alertes")
async def get_alertes(
    request: Request,
    sites: str = Query(default=""),
    date_debut: date = Query(default=None),
    date_fin: date = Query(default=None),
):
    """
    Retourne le fragment HTML des alertes actives
    """
    sql = """
        SELECT
            Site,
            PDC,
            type_erreur,
            detection,
            occurrences_12h,
            moment,
            evi_code,
            downstream_code_pc
        FROM kpi_alertes
        ORDER BY detection DESC
    """
    
    df = query_df(sql)
    
    if not df.empty:
        df["detection"] = pd.to_datetime(df["detection"], errors="coerce")
        
        # Filtrer par dates
        if date_debut:
            df = df[df["detection"] >= pd.Timestamp(date_debut)]
        if date_fin:
            df = df[df["detection"] < pd.Timestamp(date_fin) + pd.Timedelta(days=1)]
        
        # Filtrer par sites
        if sites:
            site_list = [s.strip() for s in sites.split(",") if s.strip()]
            if site_list:
                df = df[df["Site"].isin(site_list)]
    
    nb_alertes = len(df)
    
    if nb_alertes > 10:
        status = "danger"
    elif nb_alertes > 0:
        status = "warning"
    else:
        status = "success"
    
    # Top 5 sites en alerte
    top_sites = []
    if not df.empty:
        top = df.groupby("Site").size().sort_values(ascending=False).head(5)
        top_sites = [{"site": site, "count": count} for site, count in top.items()]
    
    return templates.TemplateResponse(
        "partials/alertes.html",
        {
            "request": request,
            "nb_alertes": nb_alertes,
            "status": status,
            "top_sites": top_sites,
        }
    )
