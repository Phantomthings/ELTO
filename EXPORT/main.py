"""
ELTO Dashboard Export - Version sans authentification
Point d'entrée minimal pour lancer l'application
"""

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse

from db import get_sites, get_date_range
from routers import filters, sessions

app = FastAPI(
    title="ELTO Dashboard Export",
    description="Dashboard d'analyse des erreurs de charge",
    version="1.0.0",
)

# Montage des fichiers statiques
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/assets", StaticFiles(directory="assets"), name="assets")

# Templates Jinja2
templates = Jinja2Templates(directory="templates")

# Enregistrement des routers (sans authentification)
app.include_router(filters.router, prefix="/api")
app.include_router(sessions.router, prefix="/api")


@app.get("/")
async def root():
    """Redirection vers le dashboard"""
    return RedirectResponse(url="/dashboard")


@app.get("/dashboard")
async def dashboard(request: Request):
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
        },
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
