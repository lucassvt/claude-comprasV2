"""
Servicio de Cálculo de Stock
Calcula stock mínimo, ideal y máximo basado en la demanda.

Fórmulas:
- stock_minimo = demanda_diaria * dias_stock_configurados
- stock_ideal = stock_minimo * factor_ideal (default: 2)
- stock_maximo = stock_minimo * factor_maximo (default: 4)
"""

import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from sqlalchemy.orm import Session
from sqlalchemy import text
import pandas as pd

from app.core.config import settings
from app.services.demand_forecaster import DemandForecaster, ForecastResult

logger = logging.getLogger(__name__)


@dataclass
class StockLevel:
    """Niveles de stock calculados para un producto-depósito"""
    product_id: int
    deposit_id: int
    cod_item: str
    producto_nombre: str
    marca: str
    rubro: str
    subrubro: str
    deposito_nombre: str
    stock_actual: float  # stock_disponible (para cálculos de reposición)
    stock_real: float    # stock físico real (para auditoría)
    stock_reservado: float  # stock reservado
    stock_minimo: float
    stock_ideal: float
    stock_maximo: float
    demanda_diaria: float
    dias_cobertura: int
    metodo_forecast: str
    tendencia: str
    ventas_30_dias: float
    ventas_60_dias: float
    ventas_90_dias: float
    ventas_365_dias: float
    monto_90_dias: float  # Monto de ventas 90 días (para ranking TOP)
    estado: str  # 'ok', 'bajo_minimo', 'excedente', 'sin_stock'

    def to_dict(self) -> Dict:
        return {
            'product_id': self.product_id,
            'deposit_id': self.deposit_id,
            'cod_item': self.cod_item,
            'producto_nombre': self.producto_nombre,
            'marca': self.marca,
            'rubro': self.rubro,
            'subrubro': self.subrubro,
            'deposito_nombre': self.deposito_nombre,
            'stock_actual': round(self.stock_actual, 2),
            'stock_real': round(self.stock_real, 2),
            'stock_reservado': round(self.stock_reservado, 2),
            'stock_minimo': round(self.stock_minimo, 2),
            'stock_ideal': round(self.stock_ideal, 2),
            'stock_maximo': round(self.stock_maximo, 2),
            'demanda_diaria': round(self.demanda_diaria, 4),
            'dias_cobertura': self.dias_cobertura,
            'metodo_forecast': self.metodo_forecast,
            'tendencia': self.tendencia,
            'ventas_30_dias': self.ventas_30_dias,
            'ventas_60_dias': self.ventas_60_dias,
            'ventas_90_dias': self.ventas_90_dias,
            'ventas_365_dias': self.ventas_365_dias,
            'monto_90_dias': round(self.monto_90_dias, 2),
            'estado': self.estado
        }


class StockCalculator:
    """
    Calcula los niveles de stock para todos los productos-depósitos.
    """

    def __init__(self, db: Session):
        self.db = db
        self.config_cache = {}  # Cache de configuraciones por rubro/marca
        self.global_config = {}  # Cache de parámetros globales desde BD

        # Cargar método de cálculo desde BD (con fallback a settings)
        metodo_calculo = self._get_metodo_calculo_demanda()
        self.forecaster = DemandForecaster(metodo_preferido=metodo_calculo)

    def _get_metodo_calculo_demanda(self) -> str:
        """
        Obtiene el método de cálculo de demanda desde la BD.
        Fallback a settings.demand_calculation_method si no está configurado.
        """
        try:
            result = self.db.execute(text("""
                SELECT value FROM system_config WHERE key = 'metodo_calculo_demanda'
            """))
            row = result.fetchone()
            if row and row[0]:
                # La columna es JSONB, puede devolver string directamente
                value = row[0]
                if isinstance(value, str):
                    metodo = value.strip().lower()
                else:
                    metodo = str(value).strip().lower()
                if metodo in ('promedio_simple', 'mediana', 'combinado'):
                    logger.info(f"Método de cálculo de demanda desde BD: {metodo}")
                    return metodo
        except Exception as e:
            logger.warning(f"Error leyendo método de cálculo: {e}")
        return settings.demand_calculation_method

    def calculate_all_stock_levels(
        self,
        excluded_deposits: Optional[List[str]] = None,
        excluded_brands: Optional[List[str]] = None,
        excluded_products: Optional[List[str]] = None
    ) -> List[StockLevel]:
        """
        Calcula los niveles de stock para todos los productos-depósitos.

        Args:
            excluded_deposits: Lista de nombres de depósitos a excluir
            excluded_brands: Lista de nombres de marcas a excluir
            excluded_products: Lista de códigos de productos a excluir

        Returns:
            Lista de StockLevel con los niveles calculados
        """
        excluded_deposits = excluded_deposits or []
        excluded_brands = excluded_brands or []
        excluded_products = excluded_products or []

        # Cargar configuraciones
        self._load_configurations()

        # Obtener productos y stock
        products_stock = self._get_products_with_stock(excluded_deposits, excluded_brands, excluded_products)

        # Obtener historial de ventas
        sales_history = self._get_sales_history()

        results = []

        for ps in products_stock:
            product_id = ps['product_id']
            deposit_id = ps['deposit_id']

            # Obtener días de stock configurados
            dias_stock = self._get_dias_stock(
                ps['marca'],
                ps['rubro'],
                ps['subrubro']
            )

            # Obtener historial de ventas para este producto-depósito
            key = (product_id, deposit_id)
            sales_df = sales_history.get(key, pd.DataFrame())

            # Calcular demanda
            forecast = self.forecaster.calculate_demand(
                sales_df,
                product_id,
                deposit_id,
                days_back=settings.sales_period_days
            )

            # Calcular niveles de stock
            # CRITERIO: Si vendió menos del umbral mínimo en el período, stock_minimo = 0
            # Esto evita calcular stock para productos con venta casi nula
            # El umbral puede ser diferenciado por sub-rubro
            ventas_periodo = forecast.ventas_365_dias
            umbral_minimo = self._get_umbral_minimo(ps['subrubro'], ps['rubro'])

            if ventas_periodo < umbral_minimo:
                # Ventas insuficientes para justificar stock mínimo
                stock_minimo = 0.0
                stock_ideal = 0.0
                stock_maximo = 0.0
            else:
                stock_minimo = forecast.demanda_diaria * dias_stock
                # Usar parámetros globales de la BD (no de settings)
                stock_ideal = stock_minimo * self.global_config['factor_ideal']
                stock_maximo = stock_minimo * self.global_config['factor_maximo']

            # Determinar estado
            stock_actual = float(ps['stock_disponible'])
            stock_real = float(ps['stock_real'])
            stock_reservado = float(ps['stock_reservado'])

            # Si stock_minimo = 0 (ventas bajas), el producto no requiere reposición
            # No marcarlo como bajo_minimo ni excedente
            if stock_minimo == 0:
                # Producto con ventas insuficientes - no requiere gestión de stock
                if stock_actual <= 0:
                    estado = 'sin_stock'
                else:
                    estado = 'ok'  # Tiene stock pero no requiere reposición
            elif stock_actual <= 0:
                estado = 'sin_stock'
            elif stock_actual < stock_minimo:
                estado = 'bajo_minimo'
            elif stock_actual > stock_maximo:
                estado = 'excedente'
            else:
                estado = 'ok'

            results.append(StockLevel(
                product_id=product_id,
                deposit_id=deposit_id,
                cod_item=ps['cod_item'],
                producto_nombre=ps['nombre'],
                marca=ps['marca'] or '',
                rubro=ps['rubro'] or '',
                subrubro=ps['subrubro'] or '',
                deposito_nombre=ps['deposito_nombre'],
                stock_actual=stock_actual,
                stock_real=stock_real,
                stock_reservado=stock_reservado,
                stock_minimo=round(stock_minimo, 2),
                stock_ideal=round(stock_ideal, 2),
                stock_maximo=round(stock_maximo, 2),
                demanda_diaria=forecast.demanda_diaria,
                dias_cobertura=dias_stock,
                metodo_forecast=forecast.metodo_usado,
                tendencia=forecast.tendencia,
                ventas_30_dias=forecast.ventas_30_dias,
                ventas_60_dias=forecast.ventas_60_dias,
                ventas_90_dias=forecast.ventas_90_dias,
                ventas_365_dias=forecast.ventas_365_dias,
                monto_90_dias=forecast.monto_90_dias,
                estado=estado
            ))

        logger.info(f"Calculados {len(results)} niveles de stock")
        return results

    def _load_configurations(self):
        """Carga las configuraciones de días de stock por rubro/marca y parámetros globales"""

        # Cargar parámetros globales desde BD (con fallback a settings)
        self.global_config = {
            'dias_stock_default': settings.default_stock_days,
            'factor_ideal': settings.factor_ideal,
            'factor_maximo': settings.factor_maximo,
            'periodo_ventas_dias': settings.sales_period_days,
            'umbral_minimo_ventas': settings.min_sales_threshold
        }

        result = self.db.execute(text("""
            SELECT key, value FROM system_config
            WHERE key IN ('dias_stock_default', 'factor_ideal', 'factor_maximo', 'periodo_ventas_dias', 'umbral_minimo_ventas')
        """))

        for row in result:
            key = row[0]
            value = row[1]
            if key == 'dias_stock_default':
                self.global_config['dias_stock_default'] = int(value)
            elif key == 'factor_ideal':
                self.global_config['factor_ideal'] = float(value)
            elif key == 'factor_maximo':
                self.global_config['factor_maximo'] = float(value)
            elif key == 'periodo_ventas_dias':
                self.global_config['periodo_ventas_dias'] = int(value)
            elif key == 'umbral_minimo_ventas':
                self.global_config['umbral_minimo_ventas'] = int(value)

        logger.info(f"Parámetros globales cargados: {self.global_config}")

        # Cargar configuración por marca
        result = self.db.execute(text("""
            SELECT key, value FROM system_config
            WHERE key LIKE 'dias_stock_marca_%'
        """))

        for row in result:
            marca = row[0].replace('dias_stock_marca_', '')
            self.config_cache[f'marca_{marca.upper()}'] = int(row[1])

        # Cargar configuración por rubro
        result = self.db.execute(text("""
            SELECT key, value FROM system_config
            WHERE key LIKE 'dias_stock_rubro_%'
        """))

        for row in result:
            rubro = row[0].replace('dias_stock_rubro_', '')
            self.config_cache[f'rubro_{rubro.upper()}'] = int(row[1])

        # Cargar configuración por subrubro
        result = self.db.execute(text("""
            SELECT key, value FROM system_config
            WHERE key LIKE 'dias_stock_subrubro_%'
        """))

        for row in result:
            subrubro = row[0].replace('dias_stock_subrubro_', '')
            self.config_cache[f'subrubro_{subrubro.upper()}'] = int(row[1])

        # Cargar umbrales mínimos de ventas por sub-rubro
        self.subrubro_thresholds = {}
        result = self.db.execute(text("""
            SELECT key, value FROM system_config
            WHERE key LIKE 'umbral_subrubro_%'
        """))

        for row in result:
            subrubro = row[0].replace('umbral_subrubro_', '')
            try:
                self.subrubro_thresholds[subrubro] = int(row[1])
            except (ValueError, TypeError):
                pass

        if self.subrubro_thresholds:
            logger.info(f"Umbrales por sub-rubro cargados: {len(self.subrubro_thresholds)} configurados")

    def _get_dias_stock(self, marca: str, rubro: str, subrubro: str) -> int:
        """
        Obtiene los días de stock configurados.
        Prioridad: Marca > Subrubro > Rubro > Default
        """
        # Buscar por marca (mayor prioridad)
        if marca:
            key = f'marca_{marca.upper()}'
            if key in self.config_cache:
                return self.config_cache[key]

        # Buscar por subrubro
        if subrubro:
            key = f'subrubro_{subrubro.upper()}'
            if key in self.config_cache:
                return self.config_cache[key]

        # Buscar por rubro
        if rubro:
            key = f'rubro_{rubro.upper()}'
            if key in self.config_cache:
                return self.config_cache[key]

        # Retornar default desde BD (no desde settings)
        return self.global_config['dias_stock_default']

    def _get_umbral_minimo(self, subrubro: str, rubro: str = None) -> int:
        """
        Obtiene el umbral mínimo de ventas para un producto.

        Si el producto vendió menos de este umbral en el período,
        no se le calculará stock mínimo/ideal/máximo (stock_min = 0).

        Prioridad: sub-rubro específico > default global

        Args:
            subrubro: Nombre del sub-rubro del producto
            rubro: Nombre del rubro (para futuras extensiones)

        Returns:
            Umbral mínimo de ventas configurado
        """
        # Buscar por sub-rubro específico
        if subrubro and subrubro in self.subrubro_thresholds:
            return self.subrubro_thresholds[subrubro]

        # En el futuro se podría agregar por rubro:
        # if rubro and rubro in self.rubro_thresholds:
        #     return self.rubro_thresholds[rubro]

        # Default global desde BD (con fallback a settings)
        return self.global_config.get('umbral_minimo_ventas', settings.min_sales_threshold)

    def _get_products_with_stock(
        self,
        excluded_deposits: List[str],
        excluded_brands: List[str],
        excluded_products: List[str] = None
    ) -> List[Dict]:
        """Obtiene productos con su stock actual, excluyendo fraccionados, servicios y productos específicos"""

        excluded_products = excluded_products or []

        # Construir condiciones de exclusión
        deposit_condition = ""
        if excluded_deposits:
            deposits_str = ", ".join([f"'{d}'" for d in excluded_deposits])
            deposit_condition = f"AND d.nombre NOT IN ({deposits_str})"

        brand_condition = ""
        if excluded_brands:
            brands_str = ", ".join([f"'{b}'" for b in excluded_brands])
            brand_condition = f"AND p.marca_nombre NOT IN ({brands_str})"

        product_condition = ""
        if excluded_products:
            products_str = ", ".join([f"'{c}'" for c in excluded_products])
            product_condition = f"AND p.cod_item NOT IN ({products_str})"

        query = text(f"""
            SELECT
                p.id as product_id,
                p.cod_item,
                p.nombre,
                p.marca_nombre as marca,
                p.rubro_nombre as rubro,
                p.sub_rubro_nombre as subrubro,
                d.id as deposit_id,
                d.nombre as deposito_nombre,
                COALESCE(s.stock_disponible, 0) as stock_disponible,
                COALESCE(s.stock_real, 0) as stock_real,
                COALESCE(s.stock_reservado, 0) as stock_reservado
            FROM products p
            CROSS JOIN deposits d
            LEFT JOIN stock s ON s.product_id = p.id AND s.deposit_id = d.id
            WHERE 1=1
                AND p.cod_item NOT LIKE '%X%'
                AND UPPER(COALESCE(p.rubro_nombre, '')) NOT LIKE '%SERVICIO%'
                AND UPPER(COALESCE(p.sub_rubro_nombre, '')) NOT LIKE '%SERVICIO%'
                AND d.activo = true
                {deposit_condition}
                {brand_condition}
                {product_condition}
            ORDER BY p.cod_item, d.nombre
        """)

        result = self.db.execute(query)
        return [dict(row._mapping) for row in result]

    def _get_sales_history(self) -> Dict[Tuple[int, int], pd.DataFrame]:
        """Obtiene el historial de ventas agrupado por producto-depósito"""

        query = text("""
            SELECT
                product_id,
                deposit_id,
                fecha,
                cantidad,
                monto
            FROM sales_history
            WHERE fecha >= CURRENT_DATE - INTERVAL '365 days'
            ORDER BY product_id, deposit_id, fecha
        """)

        result = self.db.execute(query)
        rows = [dict(row._mapping) for row in result]

        if not rows:
            return {}

        df = pd.DataFrame(rows)

        # Convertir Decimal a float
        df['cantidad'] = df['cantidad'].astype(float)
        df['monto'] = df['monto'].astype(float)

        # Agrupar por producto-depósito
        grouped = {}
        for (pid, did), group_df in df.groupby(['product_id', 'deposit_id']):
            grouped[(pid, did)] = group_df[['fecha', 'cantidad', 'monto']]

        return grouped

    def get_summary(self, stock_levels: List[StockLevel]) -> Dict:
        """Genera un resumen de los niveles de stock"""
        total = len(stock_levels)
        bajo_minimo = sum(1 for s in stock_levels if s.estado == 'bajo_minimo')
        sin_stock = sum(1 for s in stock_levels if s.estado == 'sin_stock')
        excedente = sum(1 for s in stock_levels if s.estado == 'excedente')
        ok = sum(1 for s in stock_levels if s.estado == 'ok')

        return {
            'total': total,
            'bajo_minimo': bajo_minimo,
            'sin_stock': sin_stock,
            'excedente': excedente,
            'ok': ok,
            'porcentaje_bajo_minimo': round(bajo_minimo / max(1, total) * 100, 1),
            'porcentaje_excedente': round(excedente / max(1, total) * 100, 1)
        }

    def get_top_200_products(self, stock_levels: List[StockLevel]) -> List[StockLevel]:
        """
        Obtiene los TOP 200 productos por MONTO de ventas (importe $)
        que están bajo el stock mínimo en al menos un depósito.

        Lógica:
        1. Agrupa por producto único (suma montos de todos los depósitos)
        2. Ordena por monto total del producto
        3. Toma TOP 200 productos únicos
        4. Retorna todos los registros producto-depósito que están bajo mínimo

        IMPORTANTE: Solo incluye depósitos con stock_minimo > 0
        (si stock_minimo = 0 significa que ese depósito no requiere stock de ese producto)
        """
        # 1. Agrupar por producto para calcular monto total
        product_totals = {}
        for sl in stock_levels:
            if sl.product_id not in product_totals:
                product_totals[sl.product_id] = {
                    'monto_total': 0,
                    'registros': []
                }
            product_totals[sl.product_id]['monto_total'] += sl.monto_90_dias
            product_totals[sl.product_id]['registros'].append(sl)

        # 2. Ordenar productos por monto total y tomar TOP 200
        sorted_products = sorted(
            product_totals.items(),
            key=lambda x: x[1]['monto_total'],
            reverse=True
        )[:200]

        # 3. Obtener los product_ids del TOP 200
        top_200_ids = set(pid for pid, _ in sorted_products)

        # 4. Retornar todos los registros producto-depósito que:
        #    - Pertenecen al TOP 200
        #    - Están bajo mínimo
        #    - Tienen stock_minimo > 0
        bajo_minimo = [
            sl for sl in stock_levels
            if sl.product_id in top_200_ids
            and sl.estado in ('bajo_minimo', 'sin_stock')
            and sl.stock_minimo > 0
        ]

        return bajo_minimo

    def get_negative_stock(self, stock_levels: List[StockLevel]) -> List[StockLevel]:
        """
        Obtiene productos con stock REAL negativo (para auditoría).
        Solo incluye productos con stock < -0.5 para evitar falsos positivos
        por redondeo (productos con stock = 0 o muy cercano a 0).
        """
        return [s for s in stock_levels if s.stock_real < -0.5]

    def get_extended_summary(self, stock_levels: List[StockLevel]) -> Dict:
        """
        Genera un resumen extendido con valor del stock y conteo de SKUs.

        Returns:
            Dict con:
            - valor_stock_total: Suma de stock_real × costo para todos los productos
            - skus_total: Cantidad de SKUs únicos en el sistema
            - skus_bajo_minimo: Cantidad de SKUs bajo mínimo
            - skus_top_bajo_minimo: Cantidad de SKUs TOP 200 bajo mínimo
        """
        # Obtener costos de productos desde la BD
        result = self.db.execute(text("""
            SELECT id, costo FROM products WHERE costo IS NOT NULL AND costo > 0
        """))
        product_costs = {row[0]: float(row[1]) for row in result}

        # Calcular valor total del stock
        valor_stock_total = 0.0
        skus_vistos = set()
        skus_bajo_minimo = set()

        for sl in stock_levels:
            # Agregar al set de SKUs únicos
            skus_vistos.add(sl.product_id)

            # Calcular valor del stock si hay costo
            costo = product_costs.get(sl.product_id, 0)
            if costo > 0 and sl.stock_real > 0:
                valor_stock_total += sl.stock_real * costo

            # Contar SKUs bajo mínimo
            if sl.estado in ('bajo_minimo', 'sin_stock'):
                skus_bajo_minimo.add(sl.product_id)

        # Obtener SKUs TOP bajo mínimo
        top_200 = self.get_top_200_products(stock_levels)
        skus_top_bajo_minimo = len(set(s.product_id for s in top_200))

        return {
            'valor_stock_total': round(valor_stock_total, 2),
            'skus_total': len(skus_vistos),
            'skus_bajo_minimo': len(skus_bajo_minimo),
            'skus_top_bajo_minimo': skus_top_bajo_minimo
        }
