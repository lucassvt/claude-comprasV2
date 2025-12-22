"""
Servicio de Sincronizacion de Ventas desde API DUX
Sincroniza ventas/facturas de los ultimos N dias.

Uso principal: Actualizar historial de ventas antes de recalcular stock.
"""

import logging
from typing import Dict, Optional, Callable
from datetime import datetime, timedelta
from decimal import Decimal

from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import settings
from app.services.dux_api_client import DuxAPIClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DuxSalesSyncService:
    """
    Servicio para sincronizar ventas desde la API DUX.
    Obtiene facturas/ventas y las guarda en sales_history.
    """

    # Mapeo de sucursal_id de DUX -> deposit_id en BD local
    SUCURSAL_TO_DEPOSIT = {
        1: 17,   # SUCURSAL ALEM -> DEPOSITO ALEM
        2: 27,   # SUCURSAL LAPRIDA -> DEPOSITO LAPRIDA
        3: 18,   # SUCURSAL BELGRANO -> DEPOSITO BELGRANO
        4: 28,   # SUCURSAL PARQUE -> DEPOSITO PARQUE
        5: 19,   # SUCURSAL CONGRESO -> DEPOSITO CONGRESO
        6: 20,   # SUCURSAL MUNECAS -> DEPOSITO MUNECAS
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
            db: Sesion de base de datos SQLAlchemy
        """
        self.db = db
        self.client = DuxAPIClient(
            base_url=settings.dux_api_base_url,
            token=settings.dux_api_token,
            empresa_id=settings.dux_empresa_id,
            requests_per_minute=12,
            requests_per_second=0.2
        )

        self.stats = {
            'ventas_processed': 0,
            'items_processed': 0,
            'records_inserted': 0,
            'records_updated': 0,
            'errors': 0,
            'sucursales_not_found': 0,
            'products_not_found': 0,
            'notas_credito_processed': 0  # Notas de credito que restan ventas
        }

    def get_last_sale_date(self) -> Optional[datetime]:
        """
        Obtiene la fecha de la ultima venta registrada en la BD.
        Util para sincronizacion incremental.

        Returns:
            Fecha de la ultima venta o None si no hay ventas
        """
        try:
            result = self.db.execute(text("""
                SELECT MAX(fecha) FROM sales_history
            """))
            row = result.fetchone()
            if row and row[0]:
                return row[0] if isinstance(row[0], datetime) else datetime.combine(row[0], datetime.min.time())
        except Exception as e:
            logger.error(f"Error obteniendo ultima fecha de venta: {e}")
        return None

    def sync_ventas(
        self,
        dias_atras: int = 30,
        max_pages: Optional[int] = None,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        fecha_desde_override: Optional[str] = None,
        incremental: bool = True
    ) -> Dict:
        """
        Sincroniza ventas desde la API DUX.

        Args:
            dias_atras: Cantidad de dias hacia atras (usado si no hay datos previos o incremental=False)
            max_pages: Maximo de paginas a sincronizar (None = todas)
            progress_callback: Callback para reportar progreso
            fecha_desde_override: Fecha especifica desde la cual sincronizar (YYYY-MM-DD)
            incremental: Si True, sincroniza solo desde la ultima venta registrada

        Returns:
            Estadisticas de la sincronizacion
        """
        start_time = datetime.now()

        # Determinar fecha_desde
        if fecha_desde_override:
            # Usar fecha especificada manualmente
            fecha_desde = fecha_desde_override
            modo = "manual"
        elif incremental:
            # Modo incremental: desde la ultima venta registrada
            last_sale = self.get_last_sale_date()
            if last_sale:
                # Restar 1 dia por seguridad (por si hay ventas del mismo dia no procesadas)
                fecha_desde = (last_sale - timedelta(days=1)).strftime('%Y-%m-%d')
                dias_reales = (datetime.now() - last_sale).days + 1
                modo = f"incremental (desde {fecha_desde})"
            else:
                # No hay ventas previas, usar dias_atras completo
                fecha_desde = (datetime.now() - timedelta(days=dias_atras)).strftime('%Y-%m-%d')
                modo = f"inicial ({dias_atras} dias)"
        else:
            # Modo completo: usar dias_atras
            fecha_desde = (datetime.now() - timedelta(days=dias_atras)).strftime('%Y-%m-%d')
            modo = f"completo ({dias_atras} dias)"

        logger.info("=" * 70)
        logger.info(f"SINCRONIZACION DE VENTAS - Modo: {modo}")
        logger.info(f"Fecha desde: {fecha_desde}")
        logger.info("=" * 70)

        try:
            if progress_callback:
                progress_callback(0, 100, "Obteniendo ventas desde API DUX...")

            # Calcular fecha_hasta (hoy)
            fecha_hasta = datetime.now().strftime('%Y-%m-%d')

            # Obtener lista de sucursales a sincronizar
            sucursales = settings.sucursales_list
            logger.info(f"Sincronizando {len(sucursales)} sucursales: {sucursales}")

            # Filtros base con nombres correctos de API DUX
            base_filters = {
                'fechaDesde': fecha_desde,
                'fechaHasta': fecha_hasta,
                'idEmpresa': settings.dux_empresa_id
            }

            # Recolectar ventas de todas las sucursales (con su ID)
            ventas_data = []  # Lista de tuplas (factura, sucursal_id)
            for idx, sucursal_id in enumerate(sucursales, 1):
                if progress_callback:
                    progress_callback(
                        int((idx / len(sucursales)) * 25),
                        100,
                        f"Obteniendo ventas sucursal {sucursal_id} ({idx}/{len(sucursales)})..."
                    )

                filters = {**base_filters, 'idSucursal': sucursal_id}
                logger.info(f"Sucursal {sucursal_id}: Solicitando ventas con filtros {filters}")

                try:
                    ventas_sucursal = self.client.get_all_ventas(
                        max_pages=max_pages,
                        filters=filters,
                        progress_callback=self._api_progress_callback
                    )
                    logger.info(f"Sucursal {sucursal_id}: {len(ventas_sucursal)} facturas obtenidas")
                    # Guardar cada factura con su sucursal_id
                    for factura in ventas_sucursal:
                        ventas_data.append((factura, sucursal_id))
                except Exception as e:
                    logger.error(f"Error obteniendo ventas de sucursal {sucursal_id}: {e}")

            total_ventas = len(ventas_data)
            logger.info(f"\n Procesando {total_ventas} facturas de todas las sucursales...")

            if progress_callback:
                progress_callback(30, 100, f"Procesando {total_ventas} facturas...")

            # Crear mapeos
            products_map = self._get_products_map()
            deposits_map = self._get_deposits_map()

            # Procesar cada factura con su sucursal_id
            for idx, (factura, sucursal_id) in enumerate(ventas_data, 1):
                try:
                    self._process_factura(factura, products_map, deposits_map, sucursal_id)
                    self.stats['ventas_processed'] += 1

                    # Commit y progreso cada 50 facturas
                    if idx % 50 == 0:
                        self.db.commit()
                        progress_pct = 30 + int((idx / total_ventas) * 60)
                        logger.info(f"   Procesadas {idx}/{total_ventas} facturas...")
                        if progress_callback:
                            progress_callback(
                                progress_pct, 100,
                                f"Procesadas {idx}/{total_ventas} facturas..."
                            )

                except Exception as e:
                    self.stats['errors'] += 1
                    logger.error(f"Error procesando factura: {e}")

            # Commit final
            self.db.commit()

            # Calcular duracion
            duration = (datetime.now() - start_time).total_seconds()

            if progress_callback:
                progress_callback(100, 100, "Sincronizacion completada")

            logger.info("\n" + "=" * 70)
            logger.info("SINCRONIZACION DE VENTAS COMPLETADA")
            logger.info("=" * 70)
            logger.info(f"   Facturas procesadas:    {self.stats['ventas_processed']}")
            logger.info(f"   Items procesados:       {self.stats['items_processed']}")
            logger.info(f"   Notas de credito:       {self.stats['notas_credito_processed']} (restan ventas)")
            logger.info(f"   Registros insertados:   {self.stats['records_inserted']}")
            logger.info(f"   Registros actualizados: {self.stats['records_updated']}")
            logger.info(f"   Sucursales no encontradas: {self.stats['sucursales_not_found']}")
            logger.info(f"   Productos no encontrados:  {self.stats['products_not_found']}")
            logger.info(f"   Errores:                {self.stats['errors']}")
            logger.info(f"   Duracion:               {duration:.1f} segundos")
            logger.info("=" * 70)

            return {
                **self.stats,
                'duration_seconds': duration,
                'fecha_desde': fecha_desde,
                'modo': modo,
                'success': True
            }

        except Exception as e:
            logger.error(f"Error en sincronizacion de ventas: {e}")
            self.db.rollback()
            if progress_callback:
                progress_callback(0, 100, f"Error: {str(e)}")
            raise

    def _process_factura(
        self,
        factura: Dict,
        products_map: Dict,
        deposits_map: Dict,
        sucursal_id_param: int = None
    ):
        """
        Procesa una factura y sus items.

        Args:
            factura: Datos de la factura desde DUX
            products_map: Mapeo cod_item -> product_id
            deposits_map: Mapeo dux_id -> deposit_id
            sucursal_id_param: ID de sucursal pasado como parametro en la solicitud
        """
        import json as json_module

        # Obtener sucursal/deposito (priorizar el parametro de la solicitud)
        sucursal_id = (
            sucursal_id_param or
            factura.get('id_sucursal') or
            factura.get('sucursal_id') or
            factura.get('sucursal', {}).get('id')
        )
        if not sucursal_id:
            self.stats['sucursales_not_found'] += 1
            return

        deposit_id = deposits_map.get(sucursal_id)
        if not deposit_id:
            self.stats['sucursales_not_found'] += 1
            return

        # Obtener fecha
        fecha_str = factura.get('fecha_comp') or factura.get('fecha')
        if not fecha_str:
            return

        try:
            if 'T' in str(fecha_str):
                fecha = datetime.fromisoformat(fecha_str.replace('Z', ''))
            elif ',' in str(fecha_str):
                # Formato "Dec 20, 2025 3:00:00 AM"
                fecha = datetime.strptime(fecha_str, '%b %d, %Y %I:%M:%S %p')
            else:
                fecha = datetime.strptime(str(fecha_str)[:10], '%Y-%m-%d')
        except Exception as e:
            logger.debug(f"Error parseando fecha '{fecha_str}': {e}")
            return

        # Datos del vendedor (usando id_vendedor) - convertir a string
        vendedor_id = factura.get('id_vendedor')
        vendedor_usuario = str(vendedor_id) if vendedor_id else None
        vendedor_nombre = factura.get('apellido_razon_soc')  # Nombre del cliente como referencia

        # Detectar si es nota de credito (NCA, NCX) para restar ventas
        tipo_comprobante = (
            factura.get('tipo_comp', '') or
            factura.get('tipo_comprobante', '') or
            factura.get('comprobante_tipo', '') or
            ''
        ).upper().strip()
        es_nota_credito = tipo_comprobante in ('NCA', 'NCX', 'NC', 'NOTA DE CREDITO', 'NOTA CREDITO', 'NOTA CRED')

        # Procesar items de la factura
        # La API DUX puede devolver los items en 'detalles_json' como string JSON
        # o en 'items', 'detalle', 'detalles'
        items = factura.get('items', []) or factura.get('detalle', []) or factura.get('detalles', [])

        # Parsear detalles_json si existe
        if not items and 'detalles_json' in factura:
            try:
                detalles_json = factura.get('detalles_json', '[]')
                if isinstance(detalles_json, str):
                    items = json_module.loads(detalles_json)
                else:
                    items = detalles_json
            except Exception as e:
                logger.debug(f"Error parseando detalles_json: {e}")
                items = []

        for item in items:
            try:
                # Obtener codigo de item
                cod_item = (
                    item.get('cod_item', '') or
                    item.get('producto', {}).get('cod_item', '') or
                    ''
                ).strip()

                if not cod_item:
                    continue

                product_id = products_map.get(cod_item)
                if not product_id:
                    self.stats['products_not_found'] += 1
                    continue

                # Obtener cantidad (API DUX usa 'ctd' o 'cantidad')
                cantidad = float(item.get('ctd', 0) or item.get('cantidad', 0) or 0)
                if cantidad == 0:  # Solo ignorar si es exactamente 0
                    continue

                # Si es nota de credito, invertir signo (resta de ventas)
                if es_nota_credito:
                    cantidad = -abs(cantidad)

                # Calcular monto (API DUX usa 'precio_uni' o 'precio_unitario')
                monto = float(item.get('subtotal', 0) or 0)
                if not monto:
                    precio = float(
                        item.get('precio_uni', 0) or
                        item.get('precio', 0) or
                        item.get('precio_unitario', 0) or
                        0
                    )
                    monto = precio * abs(cantidad)  # Usar abs para calcular monto base

                # Si es nota de credito, el monto tambien debe ser negativo
                if es_nota_credito:
                    monto = -abs(monto)

                # Insertar o actualizar registro
                self._upsert_sale(
                    product_id=product_id,
                    deposit_id=deposit_id,
                    fecha=fecha,
                    cantidad=cantidad,
                    monto=monto,
                    vendedor_usuario=vendedor_usuario,
                    vendedor_nombre=vendedor_nombre
                )

                self.stats['items_processed'] += 1
                if es_nota_credito:
                    self.stats['notas_credito_processed'] += 1

            except Exception as e:
                self.stats['errors'] += 1
                # Log detallado del error para diagnóstico
                if self.stats['errors'] <= 5:  # Solo los primeros 5 errores con detalle
                    logger.error(f"Error procesando item (cod_item={cod_item}): {type(e).__name__}: {e}")

    def _upsert_sale(
        self,
        product_id: int,
        deposit_id: int,
        fecha: datetime,
        cantidad: float,
        monto: float,
        vendedor_usuario: str = None,
        vendedor_nombre: str = None
    ):
        """
        Inserta o actualiza un registro de venta.
        Si ya existe un registro para producto-deposito-fecha, suma las cantidades.

        Nota: Los campos vendedor_usuario y vendedor_nombre se ignoran por ahora
        ya que la tabla no los tiene. Se pueden agregar en el futuro si es necesario.
        """
        fecha_date = fecha.date() if isinstance(fecha, datetime) else fecha

        # Verificar si existe
        result = self.db.execute(text("""
            SELECT id, cantidad, monto FROM sales_history
            WHERE product_id = :product_id
              AND deposit_id = :deposit_id
              AND fecha = :fecha
        """), {
            "product_id": product_id,
            "deposit_id": deposit_id,
            "fecha": fecha_date
        })

        existing = result.fetchone()

        if existing:
            # Actualizar sumando cantidades
            new_cantidad = float(existing[1]) + cantidad
            new_monto = float(existing[2]) + monto

            self.db.execute(text("""
                UPDATE sales_history
                SET cantidad = :cantidad,
                    monto = :monto
                WHERE id = :id
            """), {
                "id": existing[0],
                "cantidad": Decimal(str(new_cantidad)),
                "monto": Decimal(str(new_monto))
            })
            self.stats['records_updated'] += 1
        else:
            # Insertar nuevo
            self.db.execute(text("""
                INSERT INTO sales_history
                    (product_id, deposit_id, fecha, cantidad, monto, created_at)
                VALUES
                    (:product_id, :deposit_id, :fecha, :cantidad, :monto, :created_at)
            """), {
                "product_id": product_id,
                "deposit_id": deposit_id,
                "fecha": fecha_date,
                "cantidad": Decimal(str(cantidad)),
                "monto": Decimal(str(monto)),
                "created_at": datetime.now()
            })
            self.stats['records_inserted'] += 1

    def _get_products_map(self) -> Dict[str, int]:
        """Retorna mapeo de cod_item -> product_id"""
        result = self.db.execute(text("SELECT cod_item, id FROM products"))
        return {row[0]: row[1] for row in result}

    def _get_deposits_map(self) -> Dict[int, int]:
        """Retorna mapeo de dux_id -> deposit_id basado en SUCURSAL_TO_DEPOSIT"""
        deposits_map = dict(self.SUCURSAL_TO_DEPOSIT)

        try:
            result = self.db.execute(text("""
                SELECT dux_id, id FROM deposits WHERE dux_id IS NOT NULL
            """))
            for row in result:
                if row[0]:
                    deposits_map[row[0]] = row[1]
        except Exception:
            pass

        return deposits_map

    def _api_progress_callback(self, current_page: int, total_pages: Optional[int], items_count: int):
        """Callback para mostrar progreso de paginacion de API"""
        if total_pages:
            percentage = (current_page / total_pages) * 100
            logger.info(f"   Pagina {current_page}/{total_pages} ({percentage:.1f}%) - Facturas: {items_count}")
        else:
            logger.info(f"   Pagina {current_page} - Facturas: {items_count}")

    def get_stats(self) -> Dict:
        """Retorna estadisticas de la ultima sincronizacion"""
        return self.stats
