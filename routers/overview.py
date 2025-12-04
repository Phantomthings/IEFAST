"""
Router pour l'onglet Vue d'ensemble (tab0_overview)
Endpoint: GET /api/tab/overview
"""

from fastapi import APIRouter, Request, Query
from fastapi.templating import Jinja2Templates
from datetime import date, datetime
import pandas as pd
import numpy as np

from db import query_df

router = APIRouter(tags=["overview"])
templates = Jinja2Templates(directory="templates")


def get_status(value: int, thresholds: tuple = (0, 5)) -> str:
    """Retourne le statut CSS basé sur les seuils"""
    low, high = thresholds
    if value > high:
        return "danger"
    elif value > low:
        return "warning"
    return "success"


@router.get("/tab/overview")
async def get_overview(
    request: Request,
    sites: str = Query(default=""),
    date_debut: date = Query(default=None),
    date_fin: date = Query(default=None),
    pdc_only: bool = Query(default=False),
    error_types: str = Query(default=""),
    moments: str = Query(default=""),
):
    """
    Retourne le fragment HTML complet de l'onglet Vue d'ensemble
    """
    site_list = [s.strip() for s in sites.split(",") if s.strip()] if sites else []
    error_type_list = [e.strip() for e in error_types.split(",") if e.strip()] if error_types else []
    moment_list = [m.strip() for m in moments.split(",") if m.strip()] if moments else []
    
    # ============================================================
    # 1. DÉFAUTS ACTIFS
    # ============================================================
    sql_defauts = """
        SELECT site, date_debut, defaut, eqp
        FROM kpi_defauts_log
        WHERE date_fin IS NULL
        ORDER BY date_debut DESC
    """
    df_defauts = query_df(sql_defauts)
    
    if not df_defauts.empty:
        df_defauts["date_debut"] = pd.to_datetime(df_defauts["date_debut"], errors="coerce")
        
        # Filtre sites
        if site_list:
            df_defauts = df_defauts[df_defauts["site"].isin(site_list)]
        
        # Filtre PDC uniquement
        if pdc_only:
            df_defauts = df_defauts[df_defauts["eqp"].str.contains("PDC", case=False, na=False)]
    
    nb_defauts = len(df_defauts)
    nb_sites_defauts = df_defauts["site"].nunique() if not df_defauts.empty else 0
    defauts_status = get_status(nb_defauts, (0, 5))
    
    # Calcul durée et sites récents
    defauts_par_site = {}
    sites_recent = []
    
    if not df_defauts.empty:
        now = pd.Timestamp.now()
        df_defauts["depuis_jours"] = (now - df_defauts["date_debut"]).dt.days
        df_defauts["is_recent"] = (now - df_defauts["date_debut"]) < pd.Timedelta(days=1)
        
        # Sites à surveiller (défauts < 24h)
        sites_recent = df_defauts[df_defauts["is_recent"]]["site"].unique().tolist()
        
        # Grouper par site avec patterns équipement
        equip_patterns = [
            ("PDC1", r"PDC1"),
            ("PDC2", r"PDC2"),
            ("PDC3", r"PDC3"),
            ("PDC4", r"PDC4"),
            ("Variateur HC1", r"Variateur.*HC1|HC1.*Variateur"),
            ("Variateur HC2", r"Variateur.*HC2|HC2.*Variateur"),
            ("Variateur HB1", r"Variateur.*HB1|HB1.*Variateur"),
            ("Variateur HB2", r"Variateur.*HB2|HB2.*Variateur"),
            ("Autres", None),  # Catch-all
        ]
        
        for site_name, df_site in df_defauts.groupby("site"):
            site_data = {"name": site_name, "count": len(df_site), "equipements": []}
            handled_indices = set()
            
            for label, pattern in equip_patterns:
                if pattern:
                    mask = df_site["eqp"].str.contains(pattern, case=False, na=False, regex=True)
                    df_eqp = df_site[mask & ~df_site.index.isin(handled_indices)]
                else:
                    # Autres = tout ce qui n'a pas été traité
                    df_eqp = df_site[~df_site.index.isin(handled_indices)]
                
                if df_eqp.empty:
                    continue
                
                handled_indices.update(df_eqp.index.tolist())
                
                defects = []
                for _, row in df_eqp.iterrows():
                    defects.append({
                        "defaut": row["defaut"],
                        "eqp": row["eqp"],
                        "depuis_jours": int(row["depuis_jours"]),
                        "card_class": "critical" if row["depuis_jours"] > 7 else "warning",
                    })
                
                if defects:
                    site_data["equipements"].append({
                        "label": label,
                        "defects": defects,
                    })
            
            defauts_par_site[site_name] = site_data
    
    # ============================================================
    # 2. TRANSACTIONS SUSPECTES (<1 kWh)
    # ============================================================
    sql_suspicious = "SELECT * FROM kpi_suspicious_under_1kwh"
    df_suspicious = query_df(sql_suspicious)
    
    if not df_suspicious.empty and "Datetime start" in df_suspicious.columns:
        df_suspicious["Datetime start"] = pd.to_datetime(df_suspicious["Datetime start"], errors="coerce")
        
        if date_debut:
            df_suspicious = df_suspicious[df_suspicious["Datetime start"] >= pd.Timestamp(date_debut)]
        if date_fin:
            df_suspicious = df_suspicious[df_suspicious["Datetime start"] < pd.Timestamp(date_fin) + pd.Timedelta(days=1)]
        if site_list and "Site" in df_suspicious.columns:
            df_suspicious = df_suspicious[df_suspicious["Site"].isin(site_list)]
    
    nb_suspicious = len(df_suspicious)
    suspicious_status = get_status(nb_suspicious, (0, 5))
    
    # ============================================================
    # 3. TENTATIVES MULTIPLES
    # ============================================================
    sql_multi = "SELECT * FROM kpi_multi_attempts_hour"
    df_multi = query_df(sql_multi)
    
    if not df_multi.empty and "Date_heure" in df_multi.columns:
        df_multi["Date_heure"] = pd.to_datetime(df_multi["Date_heure"], errors="coerce")
        
        if date_debut:
            df_multi = df_multi[df_multi["Date_heure"] >= pd.Timestamp(date_debut)]
        if date_fin:
            df_multi = df_multi[df_multi["Date_heure"] < pd.Timestamp(date_fin) + pd.Timedelta(days=1)]
        if site_list and "Site" in df_multi.columns:
            df_multi = df_multi[df_multi["Site"].isin(site_list)]
    
    nb_multi = len(df_multi)
    multi_status = get_status(nb_multi, (0, 5))
    
    # ============================================================
    # 4. ALERTES ACTIVES
    # ============================================================
    sql_alertes = """
        SELECT Site, PDC, type_erreur, detection, occurrences_12h, moment
        FROM kpi_alertes
        ORDER BY detection DESC
    """
    df_alertes = query_df(sql_alertes)
    
    if not df_alertes.empty:
        df_alertes["detection"] = pd.to_datetime(df_alertes["detection"], errors="coerce")
        
        if date_debut:
            df_alertes = df_alertes[df_alertes["detection"] >= pd.Timestamp(date_debut)]
        if date_fin:
            df_alertes = df_alertes[df_alertes["detection"] < pd.Timestamp(date_fin) + pd.Timedelta(days=1)]
        if site_list:
            df_alertes = df_alertes[df_alertes["Site"].isin(site_list)]
    
    nb_alertes = len(df_alertes)
    alertes_status = get_status(nb_alertes, (0, 10))
    
    # Top 5 sites en alerte (pour le graphique)
    top_sites_alertes = []
    if not df_alertes.empty:
        top = df_alertes.groupby("Site").size().sort_values(ascending=False).head(5)
        max_val = top.max() if len(top) > 0 else 1
        for site, count in top.items():
            top_sites_alertes.append({
                "site": site,
                "count": int(count),
                "percent": round(count / max_val * 100, 1),
            })
    
    # ============================================================
    # 5. STATS SESSIONS (Taux réussite / Échecs)
    # Applique les filtres error_types et moments sur is_ok
    # ============================================================
    # Construire la requête avec filtres
    conditions = ["1=1"]
    
    if date_debut:
        conditions.append(f"`Datetime start` >= '{date_debut}'")
    if date_fin:
        conditions.append(f"`Datetime start` < DATE_ADD('{date_fin}', INTERVAL 1 DAY)")
    if site_list:
        sites_str = "','".join(site_list)
        conditions.append(f"Site IN ('{sites_str}')")
    
    where_clause = " AND ".join(conditions)
    
    sql_sessions = f"""
        SELECT 
            Site,
            `State of charge(0:good, 1:error)` as state,
            type_erreur,
            moment
        FROM kpi_sessions
        WHERE {where_clause}
    """
    df_sessions = query_df(sql_sessions)
    
    top_sites_reussite = []
    top_sites_echecs = []
    
    if not df_sessions.empty:
        # is_ok de base : state == 0
        df_sessions["is_ok"] = pd.to_numeric(df_sessions["state"], errors="coerce").fillna(0).astype(int).eq(0)
        
        # Appliquer les filtres error_types et moments sur les échecs
        # mask_nok = sessions en erreur
        # mask_type = type_erreur dans la liste sélectionnée
        # mask_moment = moment dans la liste sélectionnée
        # is_ok_filt = True SAUF si (not is_ok AND mask_type AND mask_moment)
        
        mask_nok = ~df_sessions["is_ok"]
        
        # Filtre type_erreur
        if error_type_list and "type_erreur" in df_sessions.columns:
            mask_type = df_sessions["type_erreur"].isin(error_type_list)
        else:
            mask_type = True
        
        # Filtre moment
        if moment_list and "moment" in df_sessions.columns:
            mask_moment = df_sessions["moment"].isin(moment_list)
        else:
            mask_moment = True
        
        # is_ok_filt : on garde l'erreur seulement si elle match les filtres
        mask_nok_keep = mask_nok & mask_type & mask_moment
        df_sessions["is_ok_filt"] = np.where(mask_nok_keep, False, True)
        
        stats = (
            df_sessions.groupby("Site")
            .agg(
                total=("is_ok_filt", "count"),
                ok=("is_ok_filt", "sum"),
            )
            .reset_index()
        )
        stats["nok"] = stats["total"] - stats["ok"]
        stats["taux_ok"] = np.where(
            stats["total"] > 0,
            (stats["ok"] / stats["total"] * 100).round(1),
            0
        )
        
        # Top 10 par volume, triés par taux de réussite
        top_by_volume = stats.sort_values("total", ascending=False).head(10)
        top_by_success = top_by_volume.sort_values("taux_ok", ascending=False)
        
        for _, row in top_by_success.iterrows():
            top_sites_reussite.append({
                "site": row["Site"],
                "taux_ok": float(row["taux_ok"]),
                "total": int(row["total"]),
            })
        
        # Top 10 par échecs
        top_by_fails = stats.sort_values("nok", ascending=False).head(10)
        max_nok = top_by_fails["nok"].max() if len(top_by_fails) > 0 else 1
        
        for _, row in top_by_fails.iterrows():
            top_sites_echecs.append({
                "site": row["Site"],
                "nok": int(row["nok"]),
                "total": int(row["total"]),
                "percent": round(row["nok"] / max_nok * 100, 1) if max_nok > 0 else 0,
            })
    
    # ============================================================
    # RENDER
    # ============================================================
    return templates.TemplateResponse(
        "partials/tab_overview.html",
        {
            "request": request,
            # Défauts
            "nb_defauts": nb_defauts,
            "nb_sites_defauts": nb_sites_defauts,
            "defauts_status": defauts_status,
            "sites_recent": sites_recent,
            "defauts_par_site": defauts_par_site,
            "pdc_only": pdc_only,
            # Suspicious
            "nb_suspicious": nb_suspicious,
            "suspicious_status": suspicious_status,
            # Multi-attempts
            "nb_multi": nb_multi,
            "multi_status": multi_status,
            # Alertes
            "nb_alertes": nb_alertes,
            "alertes_status": alertes_status,
            "top_sites_alertes": top_sites_alertes,
            # Sessions stats
            "top_sites_reussite": top_sites_reussite,
            "top_sites_echecs": top_sites_echecs,
        }
    )
