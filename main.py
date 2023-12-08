from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from db.base import Base
from db.session import engine
from core.config import settings
from apis.base import api_router


def include_router(app):
    app.include_router(api_router)


def start_application():
    app = FastAPI(title=settings.PROJECT_NAME, version=settings.PROJECT_VERSION, root_path="/statistics")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    include_router(app)
    return app


app = start_application()
