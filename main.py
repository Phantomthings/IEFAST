"""
IE Charge Dashboard - FastAPI + HTMX
Architecture découplée : chaque onglet charge ses données au clic (lazy load)
"""

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager

from db import engine, get_sites, get_date_range
from routers import defauts, alertes, sessions, kpis, overview, filters

# Lifespan pour initialiser/fermer la connexion DB
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("🚀 Démarrage IE Charge Dashboard")
    yield
    # Shutdown
    engine.dispose()
    print("👋 Arrêt propre")

app = FastAPI(
    title="IE Charge Dashboard",
    lifespan=lifespan
)

# Static files (CSS, JS)
app.mount("/static", StaticFiles(directory="static"), name="static")

# Templates Jinja2
templates = Jinja2Templates(directory="templates")

# Inclure les routers
app.include_router(filters.router, prefix="/api")
app.include_router(overview.router, prefix="/api")
app.include_router(defauts.router, prefix="/api")
app.include_router(alertes.router, prefix="/api")
app.include_router(sessions.router, prefix="/api")
app.include_router(kpis.router, prefix="/api")


@app.get("/")
async def index(request: Request):
    """Page principale avec layout et onglets (lazy load)"""
    sites = get_sites()
    date_range = get_date_range()
    
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "sites": sites,
            "date_min": date_range["min"],
            "date_max": date_range["max"],
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
