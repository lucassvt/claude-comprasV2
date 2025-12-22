"""
API FastAPI - Agente de Compras La Mascotera v2
Endpoints para gestión de stock, distribución y compras.
"""

import logging
from datetime import datetime
from typing import List, Optional
from pathlib import Path

from fastapi import FastAPI, Depends, HTTPException, Query, Body
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.requests import Request
from sqlalchemy.orm import Session
from pydantic import BaseModel

from app.core.config import settings
from app.core.database import get_db
from app.services.config_service import ConfigService
from app.services.stock_calculator import StockCalculator
from app.services.distribution_service import DistributionService
from app.services.purchase_service import PurchaseService
from app.services.sync_status_service import get_sync_status_service
from app.services.dux_sync_service import DuxSyncService
from app.services.dux_sales_sync_service import DuxSalesSyncService

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Crear aplicación FastAPI
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="Sistema de Optimización de Compras y Distribución"
)

# Templates
templates_dir = Path(__file__).parent.parent / "templates"
templates = Jinja2Templates(directory=str(templates_dir.resolve()))

# Archivos estáticos (imágenes) - usar ruta absoluta
static_dir = Path(__file__).parent.parent.parent / "imagenes"
static_dir = static_dir.resolve()
logger.info(f"Directorio de imágenes: {static_dir}")
logger.info(f"Existe: {static_dir.exists()}")

# Crear carpeta exports si no existe
exports_dir = Path("exports")
exports_dir.mkdir(exist_ok=True)

# Cache de niveles de stock (se actualiza con el botón)
stock_levels_cache = []


# ==================== MODELOS PYDANTIC ====================

class GlobalParamsRequest(BaseModel):
    dias_stock_default: int = 30
    factor_ideal: float = 2.0
    factor_maximo: float = 4.0
    periodo_ventas_dias: int = 365


class RubroConfigRequest(BaseModel):
    rubro: str
    dias_stock: int


class MarcaConfigRequest(BaseModel):
    marca: str
    dias_stock: int


class ExclusionsRequest(BaseModel):
    excluded_deposits: List[str] = []
    excluded_brands: List[str] = []


class DemandMethodRequest(BaseModel):
    metodo: str


# ==================== PÁGINAS HTML ====================

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Página principal - Dashboard"""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/config", response_class=HTMLResponse)
async def config_page(request: Request):
    """Página de configuración"""
    return templates.TemplateResponse("config.html", {"request": request})


# ==================== API DE CONFIGURACIÓN ====================

@app.get("/api/config")
async def get_all_config(db: Session = Depends(get_db)):
    """Obtiene toda la configuración del sistema"""
    config_service = ConfigService(db)
    return config_service.get_all_config()


@app.post("/api/config/global-params")
async def save_global_params(
    params: GlobalParamsRequest,
    db: Session = Depends(get_db)
):
    """Guarda los parámetros globales"""
    config_service = ConfigService(db)
    success = config_service.save_global_params(
        dias_stock_default=params.dias_stock_default,
        factor_ideal=params.factor_ideal,
        factor_maximo=params.factor_maximo,
        periodo_ventas_dias=params.periodo_ventas_dias
    )
    if success:
        return {"status": "ok", "message": "Parámetros guardados"}
    raise HTTPException(status_code=500, detail="Error guardando parámetros")


@app.post("/api/config/rubro")
async def save_rubro_config(
    config: RubroConfigRequest,
    db: Session = Depends(get_db)
):
    """Guarda configuración de días de stock para un rubro"""
    config_service = ConfigService(db)
    success = config_service.save_rubro_config(config.rubro, config.dias_stock)
    if success:
        return {"status": "ok", "message": f"Rubro '{config.rubro}' configurado"}
    raise HTTPException(status_code=500, detail="Error guardando configuración")


@app.delete("/api/config/rubro/{rubro}")
async def delete_rubro_config(rubro: str, db: Session = Depends(get_db)):
    """Elimina configuración de un rubro"""
    config_service = ConfigService(db)
    success = config_service.delete_rubro_config(rubro)
    if success:
        return {"status": "ok", "message": f"Configuración de rubro '{rubro}' eliminada"}
    raise HTTPException(status_code=500, detail="Error eliminando configuración")


@app.post("/api/config/marca")
async def save_marca_config(
    config: MarcaConfigRequest,
    db: Session = Depends(get_db)
):
    """Guarda configuración de días de stock para una marca"""
    config_service = ConfigService(db)
    success = config_service.save_marca_config(config.marca, config.dias_stock)
    if success:
        return {"status": "ok", "message": f"Marca '{config.marca}' configurada"}
    raise HTTPException(status_code=500, detail="Error guardando configuración")


@app.delete("/api/config/marca/{marca}")
async def delete_marca_config(marca: str, db: Session = Depends(get_db)):
    """Elimina configuración de una marca"""
    config_service = ConfigService(db)
    success = config_service.delete_marca_config(marca)
    if success:
        return {"status": "ok", "message": f"Configuración de marca '{marca}' eliminada"}
    raise HTTPException(status_code=500, detail="Error eliminando configuración")


@app.post("/api/config/exclusions")
async def save_exclusions(
    exclusions: ExclusionsRequest,
    db: Session = Depends(get_db)
):
    """Guarda las exclusiones de depósitos y marcas"""
    config_service = ConfigService(db)

    success_deposits = config_service.save_excluded_deposits(exclusions.excluded_deposits)
    success_brands = config_service.save_excluded_brands(exclusions.excluded_brands)

    if success_deposits and success_brands:
        return {"status": "ok", "message": "Exclusiones guardadas"}
    raise HTTPException(status_code=500, detail="Error guardando exclusiones")


# ==================== API DE STOCK ====================

@app.post("/api/stock/update-references")
async def update_stock_references(db: Session = Depends(get_db)):
    """
    Actualiza las referencias de stock (mín/ideal/máx).
    Recalcula basado en la demanda de los últimos 365 días.
    """
    global stock_levels_cache

    try:
        config_service = ConfigService(db)
        excluded_deposits = config_service.get_excluded_deposits()
        excluded_brands = config_service.get_excluded_brands()
        excluded_products = config_service.get_excluded_products()

        calculator = StockCalculator(db)
        stock_levels = calculator.calculate_all_stock_levels(
            excluded_deposits=excluded_deposits,
            excluded_brands=excluded_brands,
            excluded_products=excluded_products
        )

        # Guardar en cache
        stock_levels_cache = stock_levels

        summary = calculator.get_summary(stock_levels)

        logger.info(f"Referencias actualizadas: {summary['total']} productos")

        return {
            "status": "ok",
            "message": f"Se actualizaron {summary['total']} referencias de stock",
            "summary": summary,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error actualizando referencias: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stock/summary")
async def get_stock_summary(db: Session = Depends(get_db)):
    """Obtiene el resumen del estado de stock"""
    global stock_levels_cache

    if not stock_levels_cache:
        return {
            "status": "empty",
            "message": "No hay datos. Por favor actualice las referencias de stock.",
            "summary": {
                "total": 0,
                "bajo_minimo": 0,
                "sin_stock": 0,
                "excedente": 0,
                "ok": 0
            },
            "extended": {
                "valor_stock_total": 0,
                "skus_total": 0,
                "skus_bajo_minimo": 0,
                "skus_top_bajo_minimo": 0
            }
        }

    calculator = StockCalculator(db)
    summary = calculator.get_summary(stock_levels_cache)

    # Agregar TOP 200 bajo mínimo
    top_200 = calculator.get_top_200_products(stock_levels_cache)

    # Obtener resumen extendido con valor del stock y SKUs
    extended = calculator.get_extended_summary(stock_levels_cache)

    return {
        "status": "ok",
        "summary": summary,
        "extended": extended,
        "top_200_bajo_minimo": len(top_200),
        "negativos": len(calculator.get_negative_stock(stock_levels_cache))
    }


@app.get("/api/stock/top200")
async def get_top200_below_minimum(db: Session = Depends(get_db)):
    """Obtiene los TOP 200 productos por ventas que están bajo mínimo"""
    global stock_levels_cache

    if not stock_levels_cache:
        raise HTTPException(status_code=400, detail="No hay datos. Actualice las referencias primero.")

    calculator = StockCalculator(db)
    top_200 = calculator.get_top_200_products(stock_levels_cache)

    return {
        "status": "ok",
        "count": len(top_200),
        "products": [p.to_dict() for p in top_200[:50]]  # Limitar respuesta
    }


@app.get("/api/stock/negative")
async def get_negative_stock(db: Session = Depends(get_db)):
    """Obtiene productos con stock negativo"""
    global stock_levels_cache

    if not stock_levels_cache:
        raise HTTPException(status_code=400, detail="No hay datos. Actualice las referencias primero.")

    calculator = StockCalculator(db)
    negative = calculator.get_negative_stock(stock_levels_cache)

    # Agrupar por depósito
    by_deposit = {}
    for n in negative:
        if n.deposito_nombre not in by_deposit:
            by_deposit[n.deposito_nombre] = []
        by_deposit[n.deposito_nombre].append(n.to_dict())

    return {
        "status": "ok",
        "total_count": len(negative),
        "by_deposit": by_deposit
    }


# ==================== API DE DISTRIBUCIÓN ====================

@app.post("/api/distribution/generate")
async def generate_distribution(
    target_level: str = Query("ideal", regex="^(minimo|ideal|maximo)$"),
    db: Session = Depends(get_db)
):
    """
    Genera propuesta de distribución desde el depósito central.

    Args:
        target_level: Nivel objetivo ('minimo', 'ideal', 'maximo')
    """
    global stock_levels_cache

    if not stock_levels_cache:
        raise HTTPException(status_code=400, detail="No hay datos. Actualice las referencias primero.")

    try:
        config_service = ConfigService(db)
        excluded_deposits = config_service.get_excluded_deposits()
        excluded_brands = config_service.get_excluded_brands()

        distribution_service = DistributionService(db)
        result = distribution_service.generate_distribution(
            stock_levels=stock_levels_cache,
            target_level=target_level,
            excluded_deposits=excluded_deposits,
            excluded_brands=excluded_brands
        )

        return {
            "status": "ok",
            "summary": result.summary,
            "transfers_count": len(result.transfers),
            "purchase_needs_count": len(result.purchase_needs)
        }
    except Exception as e:
        logger.error(f"Error generando distribución: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/distribution/redistribution-opportunities")
async def get_redistribution_opportunities(db: Session = Depends(get_db)):
    """Obtiene oportunidades de redistribución desde sucursales con excedente"""
    global stock_levels_cache

    if not stock_levels_cache:
        raise HTTPException(status_code=400, detail="No hay datos. Actualice las referencias primero.")

    distribution_service = DistributionService(db)
    opportunities = distribution_service.get_redistribution_opportunities(stock_levels_cache)

    return {
        "status": "ok",
        "count": len(opportunities),
        "opportunities": opportunities[:100]  # Limitar respuesta
    }


# ==================== API DE EXPORTACIÓN ====================

@app.get("/api/export/distribution")
async def export_distribution(
    target_level: str = Query("ideal", regex="^(minimo|ideal|maximo)$"),
    sync_stock: bool = Query(True, description="Sincronizar stock desde DUX antes de generar"),
    db: Session = Depends(get_db)
):
    """
    Exporta propuesta de distribución a Excel.

    IMPORTANTE: Por defecto sincroniza stock desde DUX API antes de generar
    para asegurar datos actualizados.
    """
    global stock_levels_cache

    try:
        # PASO 1: Sincronizar stock desde DUX (si está habilitado)
        if sync_stock:
            logger.info("Sincronizando stock desde DUX antes de generar distribución...")
            sync_service = DuxSyncService(db)
            sync_result = sync_service.sync_stock()
            logger.info(f"Stock sincronizado: {sync_result['products_processed']} productos procesados")

        # PASO 2: Recalcular niveles de stock con datos frescos
        config_service = ConfigService(db)
        excluded_deposits = config_service.get_excluded_deposits()
        excluded_brands = config_service.get_excluded_brands()
        excluded_products = config_service.get_excluded_products()

        calculator = StockCalculator(db)
        stock_levels = calculator.calculate_all_stock_levels(
            excluded_deposits=excluded_deposits,
            excluded_brands=excluded_brands,
            excluded_products=excluded_products
        )

        # Actualizar cache
        stock_levels_cache = stock_levels

        # PASO 3: Generar distribución
        distribution_service = DistributionService(db)
        result = distribution_service.generate_distribution(
            stock_levels=stock_levels,
            target_level=target_level,
            excluded_deposits=excluded_deposits,
            excluded_brands=excluded_brands
        )

        file_path = distribution_service.export_distribution_excel(result)

        return FileResponse(
            file_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=Path(file_path).name
        )
    except Exception as e:
        logger.error(f"Error exportando distribución: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/export/purchases")
async def export_purchases(
    target_level: str = Query("ideal", regex="^(minimo|ideal|maximo)$"),
    sync_stock: bool = Query(True, description="Sincronizar stock desde DUX antes de generar"),
    db: Session = Depends(get_db)
):
    """
    Exporta propuesta de compras a Excel.

    IMPORTANTE: Por defecto sincroniza stock desde DUX API antes de generar
    para asegurar datos actualizados.
    """
    global stock_levels_cache

    try:
        # PASO 1: Sincronizar stock desde DUX (si está habilitado)
        if sync_stock:
            logger.info("Sincronizando stock desde DUX antes de generar compras...")
            sync_service = DuxSyncService(db)
            sync_result = sync_service.sync_stock()
            logger.info(f"Stock sincronizado: {sync_result['products_processed']} productos procesados")

        # PASO 2: Recalcular niveles de stock con datos frescos
        config_service = ConfigService(db)
        excluded_deposits = config_service.get_excluded_deposits()
        excluded_brands = config_service.get_excluded_brands()
        excluded_products = config_service.get_excluded_products()

        calculator = StockCalculator(db)
        stock_levels = calculator.calculate_all_stock_levels(
            excluded_deposits=excluded_deposits,
            excluded_brands=excluded_brands,
            excluded_products=excluded_products
        )

        # Actualizar cache
        stock_levels_cache = stock_levels

        # PASO 3: Generar distribución para obtener necesidades de compra
        distribution_service = DistributionService(db)
        result = distribution_service.generate_distribution(
            stock_levels=stock_levels,
            target_level=target_level,
            excluded_deposits=excluded_deposits,
            excluded_brands=excluded_brands
        )

        purchase_service = PurchaseService(db)
        file_path = purchase_service.export_purchases_excel(result.purchase_needs)

        return FileResponse(
            file_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=Path(file_path).name
        )
    except Exception as e:
        logger.error(f"Error exportando compras: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/export/stock-references")
async def export_stock_references(db: Session = Depends(get_db)):
    """Exporta referencias de stock (mín/ideal/máx) a Excel"""
    global stock_levels_cache

    if not stock_levels_cache:
        raise HTTPException(status_code=400, detail="No hay datos. Actualice las referencias primero.")

    try:
        purchase_service = PurchaseService(db)
        file_path = purchase_service.export_stock_references_excel(stock_levels_cache)

        return FileResponse(
            file_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=Path(file_path).name
        )
    except Exception as e:
        logger.error(f"Error exportando referencias: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/export/calculation-detail")
async def export_calculation_detail(db: Session = Depends(get_db)):
    """Exporta detalle de cálculo de stock por depósito a Excel"""
    global stock_levels_cache

    if not stock_levels_cache:
        raise HTTPException(status_code=400, detail="No hay datos. Actualice las referencias primero.")

    try:
        purchase_service = PurchaseService(db)
        file_path = purchase_service.export_calculation_detail_excel(stock_levels_cache)

        return FileResponse(
            file_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=Path(file_path).name
        )
    except Exception as e:
        logger.error(f"Error exportando detalle: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/export/top200-below-minimum")
async def export_top200_below_minimum(db: Session = Depends(get_db)):
    """Exporta TOP 200 productos bajo mínimo a Excel"""
    global stock_levels_cache

    if not stock_levels_cache:
        raise HTTPException(status_code=400, detail="No hay datos. Actualice las referencias primero.")

    try:
        purchase_service = PurchaseService(db)
        file_path = purchase_service.export_top200_below_minimum_excel(stock_levels_cache)

        return FileResponse(
            file_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=Path(file_path).name
        )
    except Exception as e:
        logger.error(f"Error exportando TOP 200: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/export/negative-stock")
async def export_negative_stock(db: Session = Depends(get_db)):
    """Exporta productos con stock negativo para auditoría a Excel"""
    global stock_levels_cache

    if not stock_levels_cache:
        raise HTTPException(status_code=400, detail="No hay datos. Actualice las referencias primero.")

    try:
        purchase_service = PurchaseService(db)
        file_path = purchase_service.export_negative_stock_excel(stock_levels_cache)

        return FileResponse(
            file_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=Path(file_path).name
        )
    except Exception as e:
        logger.error(f"Error exportando stock negativo: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/export/excess-redistribution")
async def export_excess_redistribution(
    target_level: str = Query("ideal", regex="^(minimo|ideal|maximo)$"),
    sync_stock: bool = Query(True, description="Sincronizar stock desde DUX antes de generar"),
    db: Session = Depends(get_db)
):
    """
    Exporta propuesta de redistribución de excedentes entre sucursales.

    Toma stock de sucursales con excedente (stock > máximo) y propone
    transferirlo a sucursales con faltante (stock < ideal).

    Diferente a Reparto Central que solo mueve desde DEPOSITO RUTA 9.
    """
    global stock_levels_cache

    try:
        # PASO 1: Sincronizar stock desde DUX (si está habilitado)
        if sync_stock:
            logger.info("Sincronizando stock desde DUX antes de generar redistribución...")
            sync_service = DuxSyncService(db)
            sync_result = sync_service.sync_stock()
            logger.info(f"Stock sincronizado: {sync_result['products_processed']} productos procesados")

        # PASO 2: Recalcular niveles de stock con datos frescos
        config_service = ConfigService(db)
        excluded_deposits = config_service.get_excluded_deposits()
        excluded_brands = config_service.get_excluded_brands()
        excluded_products = config_service.get_excluded_products()

        calculator = StockCalculator(db)
        stock_levels = calculator.calculate_all_stock_levels(
            excluded_deposits=excluded_deposits,
            excluded_brands=excluded_brands,
            excluded_products=excluded_products
        )

        # Actualizar cache
        stock_levels_cache = stock_levels

        # PASO 3: Generar redistribución de excedentes
        distribution_service = DistributionService(db)
        result = distribution_service.generate_excess_redistribution(
            stock_levels=stock_levels,
            target_level=target_level,
            excluded_deposits=excluded_deposits
        )

        # PASO 4: Exportar a Excel
        file_path = distribution_service.export_excess_redistribution_excel(result)

        return FileResponse(
            file_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=Path(file_path).name
        )
    except Exception as e:
        logger.error(f"Error exportando redistribución de excedentes: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/export/immobilized-stock")
async def export_immobilized_stock(
    sync_stock: bool = Query(True, description="Sincronizar stock desde DUX antes de generar"),
    db: Session = Depends(get_db)
):
    """
    Exporta reporte de stock inmovilizado (excedente sobre máximo).

    Incluye:
    - Productos con excedente
    - Unidades excedentes
    - Costo unitario
    - Valor total inmovilizado
    - Ventas de 90 días (para contexto de rotación)
    """
    global stock_levels_cache

    try:
        # PASO 1: Sincronizar stock desde DUX (si está habilitado)
        if sync_stock:
            logger.info("Sincronizando stock desde DUX antes de generar reporte de stock inmovilizado...")
            sync_service = DuxSyncService(db)
            sync_result = sync_service.sync_stock()
            logger.info(f"Stock sincronizado: {sync_result['products_processed']} productos procesados")

        # PASO 2: Recalcular niveles de stock con datos frescos
        config_service = ConfigService(db)
        excluded_deposits = config_service.get_excluded_deposits()
        excluded_brands = config_service.get_excluded_brands()
        excluded_products = config_service.get_excluded_products()

        calculator = StockCalculator(db)
        stock_levels = calculator.calculate_all_stock_levels(
            excluded_deposits=excluded_deposits,
            excluded_brands=excluded_brands,
            excluded_products=excluded_products
        )

        # Actualizar cache
        stock_levels_cache = stock_levels

        # PASO 3: Exportar stock inmovilizado
        purchase_service = PurchaseService(db)
        file_path = purchase_service.export_immobilized_stock_excel(stock_levels)

        return FileResponse(
            file_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=Path(file_path).name
        )
    except Exception as e:
        logger.error(f"Error exportando stock inmovilizado: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stock/immobilized-summary")
async def get_immobilized_stock_summary(db: Session = Depends(get_db)):
    """
    Obtiene resumen de stock inmovilizado para el dashboard.

    Returns:
        Totales de productos, unidades y valor inmovilizado
    """
    global stock_levels_cache

    if not stock_levels_cache:
        return {
            "status": "empty",
            "message": "No hay datos. Por favor actualice las referencias de stock.",
            "summary": {
                "total_productos": 0,
                "total_unidades": 0,
                "valor_total": 0
            }
        }

    purchase_service = PurchaseService(db)
    summary = purchase_service.get_immobilized_stock_summary(stock_levels_cache)

    return {
        "status": "ok",
        "summary": summary
    }


# ==================== SYNC DUX ====================

@app.post("/api/sync/stock")
async def sync_stock_from_dux(db: Session = Depends(get_db)):
    """
    Sincroniza stock desde la API DUX.
    Actualiza los valores de stock_disponible de todos los productos.
    Además recalcula el cache de stock_levels con los filtros de configuración.
    """
    global stock_levels_cache

    try:
        logger.info("Iniciando sincronización de stock desde DUX...")
        sync_service = DuxSyncService(db)
        result = sync_service.sync_stock()

        # Recalcular stock_levels_cache con los filtros configurados
        logger.info("Recalculando niveles de stock con filtros configurados...")
        config_service = ConfigService(db)
        excluded_deposits = config_service.get_excluded_deposits()
        excluded_brands = config_service.get_excluded_brands()
        excluded_products = config_service.get_excluded_products()

        calculator = StockCalculator(db)
        stock_levels_cache = calculator.calculate_all_stock_levels(
            excluded_deposits=excluded_deposits,
            excluded_brands=excluded_brands,
            excluded_products=excluded_products
        )

        logger.info(f"Cache actualizado con {len(stock_levels_cache)} productos-depósito")

        # Actualizar estado de sync
        sync_status_service = get_sync_status_service()
        sync_status_service.update_sync_stock(records_processed=result.get('products_processed', 0))

        return {
            "status": "ok",
            "message": "Stock sincronizado correctamente",
            "stats": result,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error sincronizando stock: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/sync/ventas")
async def sync_ventas_from_dux(
    dias: int = Query(365, ge=1, le=730, description="Dias hacia atras (usado si no hay datos previos o incremental=False)"),
    incremental: bool = Query(True, description="Si True, sincroniza solo desde la ultima venta registrada"),
    fecha_desde: Optional[str] = Query(None, description="Fecha especifica desde la cual sincronizar (YYYY-MM-DD)"),
    db: Session = Depends(get_db)
):
    """
    Sincroniza ventas desde la API DUX.

    Modos de sincronizacion:
    - **Incremental (default)**: Solo sincroniza desde la ultima venta registrada en BD
    - **Completo**: Sincroniza los ultimos N dias completos (incremental=False)
    - **Manual**: Desde una fecha especifica (fecha_desde=YYYY-MM-DD)
    """
    try:
        sync_service = DuxSalesSyncService(db)
        result = sync_service.sync_ventas(
            dias_atras=dias,
            incremental=incremental,
            fecha_desde_override=fecha_desde
        )

        # Actualizar estado de sync
        sync_status_service = get_sync_status_service()
        sync_status_service.update_sync_ventas(
            records_processed=result.get('items_processed', 0)
        )

        modo = result.get('modo', 'desconocido')
        logger.info(f"Sincronizacion de ventas completada - Modo: {modo}")

        return {
            "status": "ok",
            "message": f"Ventas sincronizadas - {modo}",
            "stats": result,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error sincronizando ventas: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/sync/recalculate-stock")
async def recalculate_stock_levels(db: Session = Depends(get_db)):
    """
    Recalcula los niveles de stock minimo/ideal/maximo para todos los productos.
    Usa las ventas actuales y la configuracion de dias de stock para calcular.
    """
    global stock_levels_cache

    try:
        logger.info("Recalculando niveles de stock...")

        # Obtener configuracion de exclusiones
        config_service = ConfigService(db)
        excluded_deposits = config_service.get_excluded_deposits()
        excluded_brands = config_service.get_excluded_brands()
        excluded_products = config_service.get_excluded_products()

        # Recalcular
        calculator = StockCalculator(db)
        stock_levels_cache = calculator.calculate_all_stock_levels(
            excluded_deposits=excluded_deposits,
            excluded_brands=excluded_brands,
            excluded_products=excluded_products
        )

        # Actualizar estado de sync
        sync_status_service = get_sync_status_service()
        sync_status_service.update_stock_ideal()

        logger.info(f"Niveles recalculados: {len(stock_levels_cache)} productos-deposito")

        return {
            "status": "ok",
            "message": "Niveles de stock recalculados correctamente",
            "total_registros": len(stock_levels_cache),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error recalculando stock: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== UMBRALES POR SUB-RUBRO ====================

@app.get("/api/config/subrubro-thresholds")
async def get_subrubro_thresholds(db: Session = Depends(get_db)):
    """
    Obtiene todos los umbrales mínimos de ventas por sub-rubro.

    Estos umbrales definen las ventas mínimas (en el período) que debe
    tener un producto para que el sistema le calcule stock min/ideal/max.
    """
    config_service = ConfigService(db)
    return {
        "status": "ok",
        "default_threshold": settings.min_sales_threshold,
        "subrubro_thresholds": config_service.get_subrubro_thresholds(),
        "available_subrubros": config_service.get_available_subrubros()
    }


@app.post("/api/config/subrubro-threshold")
async def set_subrubro_threshold(
    subrubro: str = Body(..., description="Nombre del sub-rubro"),
    umbral: int = Body(..., ge=1, le=100, description="Umbral mínimo de ventas"),
    db: Session = Depends(get_db)
):
    """
    Guarda umbral mínimo de ventas para un sub-rubro.

    Si un producto de este sub-rubro vendió menos que el umbral en el período,
    no se le calculará stock mínimo/ideal/máximo (quedará en 0).
    """
    config_service = ConfigService(db)
    success = config_service.save_subrubro_threshold(subrubro, umbral)

    if success:
        return {
            "status": "ok",
            "message": f"Umbral para '{subrubro}' = {umbral} ventas"
        }
    else:
        raise HTTPException(status_code=500, detail="Error guardando umbral")


@app.delete("/api/config/subrubro-threshold/{subrubro}")
async def delete_subrubro_threshold(subrubro: str, db: Session = Depends(get_db)):
    """
    Elimina configuración de umbral para un sub-rubro.
    El sub-rubro usará el umbral default global.
    """
    config_service = ConfigService(db)
    success = config_service.delete_subrubro_threshold(subrubro)

    if success:
        return {
            "status": "ok",
            "message": f"Umbral de '{subrubro}' eliminado (usará default: {settings.min_sales_threshold})"
        }
    else:
        raise HTTPException(status_code=500, detail="Error eliminando umbral")


# ==================== MÉTODO DE CÁLCULO DE DEMANDA ====================

@app.get("/api/config/demand-method")
async def get_demand_method(db: Session = Depends(get_db)):
    """
    Obtiene el método de cálculo de demanda configurado.

    Métodos disponibles:
    - promedio_simple: ventas_totales / dias (sensible a picos)
    - mediana: mediana ajustada por proporción de días con ventas (robusto a picos)
    - combinado: usa promedio móvil + ML cuando hay datos suficientes
    """
    config_service = ConfigService(db)
    metodo_actual = config_service.get_demand_method()

    return {
        "status": "ok",
        "metodo_actual": metodo_actual,
        "metodos_disponibles": [
            {
                "id": "promedio_simple",
                "nombre": "Promedio Simple",
                "descripcion": "ventas_totales / dias. Sensible a picos de ventas atipicos."
            },
            {
                "id": "mediana",
                "nombre": "Mediana Ajustada (Recomendado)",
                "descripcion": "Usa la mediana de ventas diarias. Robusto a picos y outliers."
            },
            {
                "id": "combinado",
                "nombre": "Combinado (ML + Movil)",
                "descripcion": "Combina promedio movil y ML con tendencia cuando hay datos suficientes."
            }
        ]
    }


@app.post("/api/config/demand-method")
async def set_demand_method(
    request: DemandMethodRequest,
    db: Session = Depends(get_db)
):
    """
    Configura el método de cálculo de demanda.

    El cambio se aplica en el próximo recálculo de stock.
    """
    metodo = request.metodo
    metodos_validos = ['promedio_simple', 'mediana', 'combinado']
    if metodo not in metodos_validos:
        raise HTTPException(
            status_code=400,
            detail=f"Método inválido. Opciones: {', '.join(metodos_validos)}"
        )

    config_service = ConfigService(db)
    success = config_service.set_demand_method(metodo)

    if success:
        nombres = {
            'promedio_simple': 'Promedio Simple',
            'mediana': 'Mediana Ajustada',
            'combinado': 'Combinado (ML + Movil)'
        }
        return {
            "status": "ok",
            "message": f"Método de cálculo cambiado a: {nombres[metodo]}",
            "metodo": metodo
        }
    else:
        raise HTTPException(status_code=500, detail="Error guardando configuración")


# ==================== SYNC STATUS ====================

@app.get("/api/sync-status")
async def get_sync_status():
    """Obtiene el estado de todas las sincronizaciones"""
    sync_service = get_sync_status_service()
    return {
        "status": "ok",
        "sync_status": sync_service.get_all_status(),
        "timestamp": datetime.now().isoformat()
    }


# ==================== HEALTH CHECK ====================

@app.get("/api/health")
async def health_check():
    """Health check del sistema"""
    return {
        "status": "ok",
        "app": settings.app_name,
        "version": settings.app_version,
        "timestamp": datetime.now().isoformat()
    }


# ==================== ARCHIVOS ESTÁTICOS ====================

# Montar carpeta de imágenes
app.mount("/imagenes", StaticFiles(directory=str(static_dir)), name="imagenes")
