from fastapi import Depends, FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager

from db import engine, get_sites, get_date_range
from routers import defauts, alertes, sessions, kpis, overview, filters, mac_address
from routers.auth import (
    get_current_user,
    router as auth_router,
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Démarrage ELTO Dashboard")
    yield
    engine.dispose()
    print("Arrêt ")

app = FastAPI(
    title="ELTO Dashboard",
    lifespan=lifespan
)

# Static files
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/assets", StaticFiles(directory="assets"), name="assets")  
# Templates Jinja2
templates = Jinja2Templates(directory="templates")

# Inclure les routers
app.include_router(auth_router)
protected_dependency = [Depends(get_current_user)]

app.include_router(filters.router, prefix="/api", dependencies=protected_dependency)
app.include_router(overview.router, prefix="/api", dependencies=protected_dependency)
app.include_router(defauts.router, prefix="/api", dependencies=protected_dependency)
app.include_router(alertes.router, prefix="/api", dependencies=protected_dependency)
app.include_router(sessions.router, prefix="/api", dependencies=protected_dependency)
app.include_router(kpis.router, prefix="/api", dependencies=protected_dependency)
app.include_router(mac_address.router, prefix="/api", dependencies=protected_dependency)


@app.get("/dashboard")
async def index(request: Request, current_user: dict = Depends(get_current_user)):
    sites = get_sites()
    date_range = get_date_range()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "user": current_user,
            "sites": sites,
            "date_min": date_range["min"],
            "date_max": date_range["max"],
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)