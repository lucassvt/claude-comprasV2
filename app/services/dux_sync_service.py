"""
Servicio de Sincronización de Stock desde API DUX
Sincroniza SOLO stock disponible para optimizar velocidad.

Uso principal: Sincronizar stock antes de generar distribución/compras.
"""

import logging
from typing import Dict, Optional, Callable
from datetime import datetime
from decimal import Decimal

from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import settings
from app.services.dux_api_client import DuxAPIClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DuxSyncService:
    """
    Servicio para sincronizar stock desde la API DUX.
    Optimizado para sincronizar SOLO stock_disponible (más rápido).
    """

    # Mapeo de sucursal_id de DUX -> deposit_id en BD local
    SUCURSAL_TO_DEPOSIT = {
        1: 17,   # SUCURSAL ALEM -> DEPOSITO ALEM
        2: 27,   # SUCURSAL LAPRIDA -> DEPOSITO LAPRIDA
        3: 18,   # SUCURSAL BELGRANO -> DEPOSITO BELGRANO
        4: 28,   # SUCURSAL PARQUE -> DEPOSITO PARQUE
        5: 19,   # SUCURSAL CONGRESO -> DEPOSITO CONGRESO
        6: 20,   # SUCURSAL MUÑECAS -> DEPOSITO MUÑECAS
        8: 26,   # SUCURSAL BANDA -> DEPOSITO BANDA
        9: 24,   # SUCURSAL CATAMARCA -> DEPOSITO CATAMARCA
        10: 29,  # SUCURSAL REYES CATOLICOS -> DEPOSITO REYES CATOLICOS
        11: 23,  # SUCURSAL ARENALES -> DEPOSITO ARENALES
        12: 32,  # SUCURSAL LEGUIZAMON -> DEPOSITO LEGUIZAMON
        14: 22,  # SUCURSAL BELGRANO SUR -> DEPOSITO BELGRANO SUR
        15: 34,  # SUCURSAL NEUQUEN OLASCOAGA -> DEPOSITO OLASCOAGA
        17: 25,  # SUCURSAL CONCEPCION -> DEPOSITO CONCEPCION
        18: 16,  # DEPOSITO RUTA 9 -> DEPOSITO RUTA 9
        25: 30,  # PETS PLUS MIGUEL LILLO -> DEPOSITO PETS PLUS MIGUEL LILLO
        32: 31,  # SUCURSAL PINAR I -> DEPOSITO PINAR
    }

    def __init__(self, db: Session):
        """
        Args:
            db: Sesión de base de datos SQLAlchemy
        """
        self.db = db
        self.client = DuxAPIClient(
            base_url=settings.dux_api_base_url,
            token=settings.dux_api_token,
            requests_per_minute=6,     # 1 cada 10 segundos = 6 por minuto (más conservador)
            requests_per_second=0.1,   # 1 cada 10 segundos
            max_retries=10             # Más reintentos antes de fallar
        )

        self.stats = {
            'products_processed': 0,
            'stock_records_updated': 0,
            'stock_records_created': 0,
            'errors': 0,
            'negative_stock_detected': 0
        }

    def sync_stock(
        self,
        max_pages: Optional[int] = None,
        progress_callback: Optional[Callable[[int, int, str], None]] = None
    ) -> Dict:
        """
        Sincroniza stock disponible desde la API DUX.
        OPTIMIZADO: Solo actualiza stock_disponible para mayor velocidad.

        Args:
            max_pages: Máximo de páginas a sincronizar (None = todas)
            progress_callback: Callback para reportar progreso (current, total, message)

        Returns:
            Estadísticas de la sincronización
        """
        logger.info("=" * 70)
        logger.info("INICIANDO SINCRONIZACIÓN DE STOCK DESDE DUX")
        logger.info("=" * 70)

        start_time = datetime.now()

        try:
            # Reportar inicio
            if progress_callback:
                progress_callback(0, 100, "Obteniendo productos desde API DUX...")

            # Obtener todos los items (incluyen stock) - page_size=50 por defecto
            items_data = self.client.get_all_items(
                max_pages=max_pages,
                progress_callback=self._api_progress_callback
            )

            total_items = len(items_data)
            logger.info(f"\n📊 Procesando stock de {total_items} productos...")

            if progress_callback:
                progress_callback(30, 100, f"Procesando {total_items} productos...")

            # Crear mapeos
            products_map = self._get_products_map()
            deposits_map = self._get_deposits_map()

            # Procesar cada producto y su array de stock
            for idx, item_data in enumerate(items_data, 1):
                try:
                    cod_item = item_data.get('cod_item', '').strip()
                    if not cod_item:
                        continue

                    # Obtener el array de stock del item
                    stock_array = item_data.get('stock', [])
                    if not stock_array:
                        continue

                    # Procesar cada registro de stock (uno por depósito)
                    for stock_entry in stock_array:
                        self._update_stock_disponible(
                            stock_entry, cod_item, products_map, deposits_map
                        )

                    self.stats['products_processed'] += 1

                    # Commit y progreso cada 100 productos
                    if idx % 100 == 0:
                        self.db.commit()
                        progress_pct = 30 + int((idx / total_items) * 60)
                        logger.info(f"   Procesados {idx}/{total_items} productos...")
                        if progress_callback:
                            progress_callback(
                                progress_pct, 100,
                                f"Procesados {idx}/{total_items} productos..."
                            )

                except Exception as e:
                    self.stats['errors'] += 1
                    logger.error(f"Error procesando stock de {cod_item}: {e}")

            # Commit final
            self.db.commit()

            # Calcular duración
            duration = (datetime.now() - start_time).total_seconds()

            if progress_callback:
                progress_callback(100, 100, "Sincronización completada")

            logger.info("\n" + "=" * 70)
            logger.info("SINCRONIZACIÓN DE STOCK COMPLETADA")
            logger.info("=" * 70)
            logger.info(f"   📦 Productos procesados:     {self.stats['products_processed']}")
            logger.info(f"   ✅ Registros actualizados:   {self.stats['stock_records_updated']}")
            logger.info(f"   🆕 Registros creados:        {self.stats['stock_records_created']}")
            logger.info(f"   ⚠️  Stock negativo:          {self.stats['negative_stock_detected']}")
            logger.info(f"   ❌ Errores:                  {self.stats['errors']}")
            logger.info(f"   ⏱️  Duración:                 {duration:.1f} segundos")
            logger.info("=" * 70)

            return {
                **self.stats,
                'duration_seconds': duration,
                'success': True
            }

        except Exception as e:
            logger.error(f"❌ Error en sincronización de stock: {e}")
            self.db.rollback()
            if progress_callback:
                progress_callback(0, 100, f"Error: {str(e)}")
            raise

    def _update_stock_disponible(
        self,
        stock_entry: Dict,
        cod_item: str,
        products_map: Dict,
        deposits_map: Dict
    ):
        """
        Actualiza SOLO el stock_disponible de un producto-depósito.
        Optimizado para velocidad (solo un campo).

        Args:
            stock_entry: Objeto de stock del array (contiene: id, nombre, stock_disponible)
            cod_item: Código del producto
            products_map: Mapeo de cod_item -> product_id
            deposits_map: Mapeo de dux_id -> deposit_id
        """
        # Obtener IDs
        deposit_dux_id = stock_entry.get('id')
        deposit_nombre = stock_entry.get('nombre', '').strip()

        if not cod_item or not deposit_dux_id:
            return

        product_id = products_map.get(cod_item)
        deposit_id = deposits_map.get(deposit_dux_id)

        # Si no encontramos por DUX ID, intentar por nombre
        if not deposit_id and deposit_nombre:
            deposit_nombre_upper = deposit_nombre.upper()
            # Buscar en BD por nombre exacto primero
            result = self.db.execute(text("""
                SELECT id FROM deposits WHERE UPPER(nombre) = :nombre
            """), {"nombre": deposit_nombre_upper})
            row = result.fetchone()
            if row:
                deposit_id = row[0]
                deposits_map[deposit_dux_id] = deposit_id
            else:
                # Buscar por coincidencia parcial (ej: "SUCURSAL PINAR I" -> "DEPOSITO PINAR")
                # Extraer la palabra clave del nombre (última palabra antes de número romano)
                palabras = deposit_nombre_upper.replace('SUCURSAL', '').replace('DEPOSITO', '').strip().split()
                if palabras:
                    keyword = palabras[0]  # Primera palabra significativa
                    result = self.db.execute(text("""
                        SELECT id FROM deposits WHERE UPPER(nombre) LIKE :pattern
                    """), {"pattern": f"%{keyword}%"})
                    row = result.fetchone()
                    if row:
                        deposit_id = row[0]
                        deposits_map[deposit_dux_id] = deposit_id

        if not product_id or not deposit_id:
            return

        # Obtener stock_disponible (es lo único que necesitamos)
        stock_disponible = stock_entry.get('stock_disponible')
        if stock_disponible is None:
            # Calcular si no viene directamente
            stock_real = stock_entry.get('stock_real', 0) or 0
            stock_reservado = stock_entry.get('stock_reservado', 0) or 0
            stock_disponible = stock_real - stock_reservado

        stock_disponible = Decimal(str(stock_disponible))

        # Detectar stock negativo
        if stock_disponible < 0:
            self.stats['negative_stock_detected'] += 1

        # Verificar si existe registro
        check_result = self.db.execute(text("""
            SELECT id FROM stock
            WHERE product_id = :product_id AND deposit_id = :deposit_id
        """), {"product_id": product_id, "deposit_id": deposit_id})

        existing = check_result.fetchone()

        if existing:
            # Actualizar solo stock_disponible
            self.db.execute(text("""
                UPDATE stock
                SET stock_disponible = :stock_disponible,
                    updated_at = :updated_at
                WHERE product_id = :product_id AND deposit_id = :deposit_id
            """), {
                "product_id": product_id,
                "deposit_id": deposit_id,
                "stock_disponible": stock_disponible,
                "updated_at": datetime.now()
            })
            self.stats['stock_records_updated'] += 1
        else:
            # Insertar nuevo registro
            stock_real = stock_entry.get('stock_real')
            stock_reservado = stock_entry.get('stock_reservado')
            stock_real = Decimal(str(stock_real)) if stock_real is not None else Decimal('0')
            stock_reservado = Decimal(str(stock_reservado)) if stock_reservado is not None else Decimal('0')

            self.db.execute(text("""
                INSERT INTO stock (product_id, deposit_id, stock_real, stock_reservado, stock_disponible, updated_at)
                VALUES (:product_id, :deposit_id, :stock_real, :stock_reservado, :stock_disponible, :updated_at)
            """), {
                "product_id": product_id,
                "deposit_id": deposit_id,
                "stock_real": stock_real,
                "stock_reservado": stock_reservado,
                "stock_disponible": stock_disponible,
                "updated_at": datetime.now()
            })
            self.stats['stock_records_created'] += 1

    def _get_products_map(self) -> Dict[str, int]:
        """Retorna mapeo de cod_item -> product_id"""
        result = self.db.execute(text("SELECT cod_item, id FROM products"))
        return {row[0]: row[1] for row in result}

    def _get_deposits_map(self) -> Dict[int, int]:
        """Retorna mapeo de dux_id -> deposit_id basado en SUCURSAL_TO_DEPOSIT"""
        # Usar el mapeo manual como base
        deposits_map = dict(self.SUCURSAL_TO_DEPOSIT)

        # También intentar obtener desde BD si hay dux_id
        try:
            result = self.db.execute(text("""
                SELECT dux_id, id FROM deposits WHERE dux_id IS NOT NULL
            """))
            for row in result:
                if row[0]:  # dux_id no es null
                    deposits_map[row[0]] = row[1]
        except Exception:
            pass  # La columna dux_id puede no existir

        return deposits_map

    def _api_progress_callback(self, current_page: int, total_pages: Optional[int], items_count: int):
        """Callback para mostrar progreso de paginación de API"""
        if total_pages:
            percentage = (current_page / total_pages) * 100
            logger.info(f"   📄 Página {current_page}/{total_pages} ({percentage:.1f}%) - Items: {items_count}")
        else:
            logger.info(f"   📄 Página {current_page} - Items: {items_count}")

    def get_stats(self) -> Dict:
        """Retorna estadísticas de la última sincronización"""
        return self.stats
