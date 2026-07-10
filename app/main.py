from fastapi import FastAPI

from .futures import router as futures_router

app = FastAPI(title="FirstRate Data API", version="0.1.0")
app.include_router(futures_router)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
