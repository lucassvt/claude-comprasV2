"""
Punto de entrada - Agente de Compras La Mascotera v2
Inicia el servidor FastAPI con uvicorn.
"""

import uvicorn
from app.core.config import settings


def main():
    """Inicia el servidor web"""
    print(f"""
    ============================================
        {settings.app_name}
        Version: {settings.app_version}
    ============================================

    Iniciando servidor en http://localhost:8000

    Presione Ctrl+C para detener.
    """)

    uvicorn.run(
        "app.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug
    )


if __name__ == "__main__":
    main()
