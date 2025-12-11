"""
ELTO Dashboard - Module Export
Application FastAPI minimale sans authentification

Modules inclus:
- Filtres (barre globale + filtres site/date/etc.)
- Onglet Générale
- Onglet Détails Site
- Onglet Analyse Erreur (Top 3 EVI/Downstream + par site)
"""

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager

from db import engine, get_sites, get_date_range
from routers import filters, sessions


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gestion du cycle de vie de l'application"""
    print("Démarrage ELTO Dashboard - Module Export")
    yield
    engine.dispose()
    print("Arrêt ELTO Dashboard - Module Export")


app = FastAPI(
    title="ELTO Dashboard - Module Export",
    description="Dashboard Erreurs de Charge - Filtres + Générale + Détails Site + Analyse Erreur",
    version="1.0.0",
    lifespan=lifespan,
)

# Montage des fichiers statiques
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/assets", StaticFiles(directory="assets"), name="assets")

# Configuration des templates Jinja2
templates = Jinja2Templates(directory="templates")

# Inclusion des routers (sans authentification)
app.include_router(filters.router, prefix="/api")
app.include_router(sessions.router, prefix="/api")


@app.get("/")
async def index(request: Request):
    """Page principale du dashboard"""
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


@app.get("/health")
async def health():
    """Endpoint de vérification de santé"""
    return {"status": "ok", "module": "ELTO Export"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
