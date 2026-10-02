"""API routers."""

from .agent import router as agent_router
from .documents import router as documents_router
from .health import router as health_router
from .pwa import router as pwa_router
from .ui import router as ui_router
from .workshop import router as workshop_router

ROUTERS = [health_router, ui_router, agent_router, documents_router, workshop_router, pwa_router]
