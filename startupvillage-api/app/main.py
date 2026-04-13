from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import settings
from app.routers.space import router as space_router
from app.routers.tickets import router as tickets_router

app = FastAPI(title="Startup Village Serving API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health():
    return {"status": "ok"}

app.include_router(space_router, prefix=settings.api_prefix)
app.include_router(tickets_router, prefix=settings.api_prefix)