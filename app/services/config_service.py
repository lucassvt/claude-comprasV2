"""
Servicio de Configuración
Gestiona las configuraciones del sistema:
- Días de stock por rubro/marca
- Exclusiones de depósitos y marcas
- Parámetros globales
"""

import json
import logging
from typing import Dict, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import settings

logger = logging.getLogger(__name__)


class ConfigService:
    """Gestiona las configuraciones del sistema"""

    def __init__(self, db: Session):
        self.db = db

    # ==================== PARÁMETROS GLOBALES ====================

    def get_global_params(self) -> Dict:
        """Obtiene los parámetros globales de configuración"""
        params = {
            'dias_stock_default': settings.default_stock_days,
            'factor_ideal': settings.factor_ideal,
            'factor_maximo': settings.factor_maximo,
            'periodo_ventas_dias': settings.sales_period_days
        }

        # Intentar obtener valores personalizados de la BD
        result = self.db.execute(text("""
            SELECT key, value FROM system_config
            WHERE key IN ('dias_stock_default', 'factor_ideal', 'factor_maximo', 'periodo_ventas_dias')
        """))

        for row in result:
            key = row[0]
            value = row[1]
            if key == 'dias_stock_default':
                params['dias_stock_default'] = int(value)
            elif key == 'factor_ideal':
                params['factor_ideal'] = float(value)
            elif key == 'factor_maximo':
                params['factor_maximo'] = float(value)
            elif key == 'periodo_ventas_dias':
                params['periodo_ventas_dias'] = int(value)

        return params

    def save_global_params(
        self,
        dias_stock_default: int,
        factor_ideal: float,
        factor_maximo: float,
        periodo_ventas_dias: int
    ) -> bool:
        """Guarda los parámetros globales"""
        try:
            params = [
                ('dias_stock_default', str(dias_stock_default)),
                ('factor_ideal', str(factor_ideal)),
                ('factor_maximo', str(factor_maximo)),
                ('periodo_ventas_dias', str(periodo_ventas_dias))
            ]

            for key, value in params:
                self.db.execute(text("""
                    INSERT INTO system_config (key, value, updated_at)
                    VALUES (:key, :value, NOW())
                    ON CONFLICT (key) DO UPDATE SET value = :value, updated_at = NOW()
                """), {'key': key, 'value': value})

            self.db.commit()
            logger.info("Parámetros globales guardados")
            return True
        except Exception as e:
            logger.error(f"Error guardando parámetros globales: {e}")
            self.db.rollback()
            return False

    # ==================== CONFIGURACIÓN POR RUBRO ====================

    def get_rubro_configs(self) -> List[Dict]:
        """Obtiene configuraciones de días de stock por rubro"""
        result = self.db.execute(text("""
            SELECT key, value FROM system_config
            WHERE key LIKE 'dias_stock_rubro_%'
            ORDER BY key
        """))

        configs = []
        for row in result:
            rubro = row[0].replace('dias_stock_rubro_', '')
            configs.append({
                'rubro': rubro,
                'dias_stock': int(row[1])
            })

        return configs

    def save_rubro_config(self, rubro: str, dias_stock: int) -> bool:
        """Guarda configuración de días de stock para un rubro"""
        try:
            key = f'dias_stock_rubro_{rubro.upper()}'
            self.db.execute(text("""
                INSERT INTO system_config (key, value, updated_at)
                VALUES (:key, :value, NOW())
                ON CONFLICT (key) DO UPDATE SET value = :value, updated_at = NOW()
            """), {'key': key, 'value': str(dias_stock)})

            self.db.commit()
            logger.info(f"Configuración de rubro '{rubro}' guardada: {dias_stock} días")
            return True
        except Exception as e:
            logger.error(f"Error guardando config rubro: {e}")
            self.db.rollback()
            return False

    def delete_rubro_config(self, rubro: str) -> bool:
        """Elimina configuración de un rubro"""
        try:
            key = f'dias_stock_rubro_{rubro.upper()}'
            self.db.execute(text("DELETE FROM system_config WHERE key = :key"), {'key': key})
            self.db.commit()
            return True
        except Exception as e:
            logger.error(f"Error eliminando config rubro: {e}")
            self.db.rollback()
            return False

    # ==================== CONFIGURACIÓN POR MARCA ====================

    def get_marca_configs(self) -> List[Dict]:
        """Obtiene configuraciones de días de stock por marca"""
        result = self.db.execute(text("""
            SELECT key, value FROM system_config
            WHERE key LIKE 'dias_stock_marca_%'
            ORDER BY key
        """))

        configs = []
        for row in result:
            marca = row[0].replace('dias_stock_marca_', '')
            configs.append({
                'marca': marca,
                'dias_stock': int(row[1])
            })

        return configs

    def save_marca_config(self, marca: str, dias_stock: int) -> bool:
        """Guarda configuración de días de stock para una marca"""
        try:
            key = f'dias_stock_marca_{marca.upper()}'
            self.db.execute(text("""
                INSERT INTO system_config (key, value, updated_at)
                VALUES (:key, :value, NOW())
                ON CONFLICT (key) DO UPDATE SET value = :value, updated_at = NOW()
            """), {'key': key, 'value': str(dias_stock)})

            self.db.commit()
            logger.info(f"Configuración de marca '{marca}' guardada: {dias_stock} días")
            return True
        except Exception as e:
            logger.error(f"Error guardando config marca: {e}")
            self.db.rollback()
            return False

    def delete_marca_config(self, marca: str) -> bool:
        """Elimina configuración de una marca"""
        try:
            key = f'dias_stock_marca_{marca.upper()}'
            self.db.execute(text("DELETE FROM system_config WHERE key = :key"), {'key': key})
            self.db.commit()
            return True
        except Exception as e:
            logger.error(f"Error eliminando config marca: {e}")
            self.db.rollback()
            return False

    # ==================== EXCLUSIONES ====================

    # Depósitos excluidos por defecto del análisis de stock
    DEFAULT_EXCLUDED_DEPOSITS = [
        "DEPOSITO OLASCOAGA",
        "DEPOSITO REYES CATOLICOS",
        "DEPOSITO PETS PLUS MIGUEL LILLO",
        "DEPOSITO ADMINISTRACION / MARKETING"
    ]

    # Códigos de productos excluidos por defecto (servicios, canjes, pagos, etc.)
    DEFAULT_EXCLUDED_PRODUCTS = [
        "0000099",    # BENEFICIOS A FRANQUICIAS
        "01225",      # ARTRIN PLUS X18 COMP
        "01431",      # VARIOS
        "8888",       # CANJE REGALO ESTRELLA
        "GO CUOTAS FRANQUICIAS",
        "MERCADO PAGO",
        "TARJETA NARANJA",
        "TRANSFERENCIAS FRANQUICIAS",
        "VENTAS WEB FRANQUICIAS"
    ]

    def get_excluded_deposits(self) -> List[str]:
        """Obtiene lista de depósitos excluidos (incluye defaults + configurados)"""
        # Empezar con los excluidos por defecto
        excluded = list(self.DEFAULT_EXCLUDED_DEPOSITS)

        # Agregar los configurados en BD (si hay)
        result = self.db.execute(text("""
            SELECT value FROM system_config WHERE key = 'excluded_deposits'
        """))
        row = result.fetchone()
        if row and row[0]:
            # La columna es JSONB, puede venir ya deserializada como lista
            value = row[0]
            if isinstance(value, str):
                try:
                    db_excluded = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    db_excluded = []
            elif isinstance(value, list):
                db_excluded = value
            else:
                db_excluded = []

            # Combinar sin duplicados
            for dep in db_excluded:
                if dep not in excluded:
                    excluded.append(dep)

        logger.info(f"Depósitos excluidos: {excluded}")
        return excluded

    def save_excluded_deposits(self, deposits: List[str]) -> bool:
        """Guarda lista de depósitos excluidos"""
        try:
            value = json.dumps(deposits)
            self.db.execute(text("""
                INSERT INTO system_config (key, value, updated_at)
                VALUES ('excluded_deposits', :value, NOW())
                ON CONFLICT (key) DO UPDATE SET value = :value, updated_at = NOW()
            """), {'value': value})

            self.db.commit()
            logger.info(f"Depósitos excluidos guardados: {deposits}")
            return True
        except Exception as e:
            logger.error(f"Error guardando depósitos excluidos: {e}")
            self.db.rollback()
            return False

    def get_excluded_brands(self) -> List[str]:
        """Obtiene lista de marcas excluidas"""
        result = self.db.execute(text("""
            SELECT value FROM system_config WHERE key = 'excluded_brands'
        """))
        row = result.fetchone()
        if row and row[0]:
            # La columna es JSONB, puede venir ya deserializada como lista
            value = row[0]
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    return []
            elif isinstance(value, list):
                return value
        return []

    def save_excluded_brands(self, brands: List[str]) -> bool:
        """Guarda lista de marcas excluidas"""
        try:
            value = json.dumps(brands)
            self.db.execute(text("""
                INSERT INTO system_config (key, value, updated_at)
                VALUES ('excluded_brands', :value, NOW())
                ON CONFLICT (key) DO UPDATE SET value = :value, updated_at = NOW()
            """), {'value': value})

            self.db.commit()
            logger.info(f"Marcas excluidas guardadas: {brands}")
            return True
        except Exception as e:
            logger.error(f"Error guardando marcas excluidas: {e}")
            self.db.rollback()
            return False

    def get_excluded_products(self) -> List[str]:
        """Obtiene lista de códigos de productos excluidos (incluye defaults + configurados)"""
        # Empezar con los excluidos por defecto
        excluded = list(self.DEFAULT_EXCLUDED_PRODUCTS)

        # Agregar los configurados en BD (si hay)
        result = self.db.execute(text("""
            SELECT value FROM system_config WHERE key = 'excluded_products'
        """))
        row = result.fetchone()
        if row and row[0]:
            # La columna es JSONB, puede venir ya deserializada como lista
            value = row[0]
            if isinstance(value, str):
                try:
                    db_excluded = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    db_excluded = []
            elif isinstance(value, list):
                db_excluded = value
            else:
                db_excluded = []

            # Combinar sin duplicados
            for cod in db_excluded:
                if cod not in excluded:
                    excluded.append(cod)

        logger.info(f"Productos excluidos: {excluded}")
        return excluded

    # ==================== LISTAS DISPONIBLES ====================

    def get_available_deposits(self) -> List[Dict]:
        """Obtiene lista de depósitos disponibles"""
        result = self.db.execute(text("""
            SELECT id, nombre, es_central FROM deposits
            WHERE activo = true
            ORDER BY es_central DESC, nombre
        """))
        return [{'id': r[0], 'nombre': r[1], 'es_central': r[2]} for r in result]

    def get_available_brands(self) -> List[str]:
        """Obtiene lista de marcas disponibles"""
        result = self.db.execute(text("""
            SELECT DISTINCT marca_nombre FROM products
            WHERE marca_nombre IS NOT NULL AND marca_nombre != ''
            ORDER BY marca_nombre
        """))
        return [r[0] for r in result]

    def get_available_rubros(self) -> List[str]:
        """Obtiene lista de rubros disponibles"""
        result = self.db.execute(text("""
            SELECT DISTINCT rubro_nombre FROM products
            WHERE rubro_nombre IS NOT NULL AND rubro_nombre != ''
            ORDER BY rubro_nombre
        """))
        return [r[0] for r in result]

    def get_available_subrubros(self) -> List[str]:
        """Obtiene lista de subrubros disponibles"""
        result = self.db.execute(text("""
            SELECT DISTINCT sub_rubro_nombre FROM products
            WHERE sub_rubro_nombre IS NOT NULL AND sub_rubro_nombre != ''
            ORDER BY sub_rubro_nombre
        """))
        return [r[0] for r in result]

    # ==================== UMBRALES POR SUB-RUBRO ====================

    def get_subrubro_thresholds(self) -> Dict[str, int]:
        """
        Obtiene umbrales mínimos de ventas por sub-rubro.

        Estos umbrales definen las ventas mínimas (en el período) que debe
        tener un producto para que el sistema le calcule stock min/ideal/max.

        Returns:
            Dict con subrubro -> umbral
        """
        result = self.db.execute(text("""
            SELECT key, value FROM system_config
            WHERE key LIKE 'umbral_subrubro_%'
            ORDER BY key
        """))

        thresholds = {}
        for row in result:
            # key: 'umbral_subrubro_COMEDEROS' -> subrubro: 'COMEDEROS'
            subrubro = row[0].replace('umbral_subrubro_', '')
            try:
                thresholds[subrubro] = int(row[1])
            except (ValueError, TypeError):
                thresholds[subrubro] = settings.min_sales_threshold

        return thresholds

    def get_subrubro_threshold(self, subrubro: str) -> Optional[int]:
        """Obtiene el umbral para un sub-rubro específico"""
        if not subrubro:
            return None

        key = f'umbral_subrubro_{subrubro}'
        result = self.db.execute(text("""
            SELECT value FROM system_config WHERE key = :key
        """), {'key': key})

        row = result.fetchone()
        if row and row[0]:
            try:
                return int(row[0])
            except (ValueError, TypeError):
                return None
        return None

    def save_subrubro_threshold(self, subrubro: str, umbral: int) -> bool:
        """
        Guarda umbral mínimo de ventas para un sub-rubro.

        Args:
            subrubro: Nombre del sub-rubro
            umbral: Ventas mínimas para calcular stock (1-100)
        """
        try:
            key = f'umbral_subrubro_{subrubro}'
            self.db.execute(text("""
                INSERT INTO system_config (key, value, updated_at)
                VALUES (:key, :value, NOW())
                ON CONFLICT (key) DO UPDATE SET value = :value, updated_at = NOW()
            """), {'key': key, 'value': str(umbral)})

            self.db.commit()
            logger.info(f"Umbral de sub-rubro '{subrubro}' guardado: {umbral} ventas")
            return True
        except Exception as e:
            logger.error(f"Error guardando umbral sub-rubro: {e}")
            self.db.rollback()
            return False

    def delete_subrubro_threshold(self, subrubro: str) -> bool:
        """Elimina configuración de umbral para un sub-rubro (usará default)"""
        try:
            key = f'umbral_subrubro_{subrubro}'
            self.db.execute(text("DELETE FROM system_config WHERE key = :key"), {'key': key})
            self.db.commit()
            logger.info(f"Umbral de sub-rubro '{subrubro}' eliminado")
            return True
        except Exception as e:
            logger.error(f"Error eliminando umbral sub-rubro: {e}")
            self.db.rollback()
            return False

    # ==================== MÉTODO DE CÁLCULO DE DEMANDA ====================

    def get_demand_method(self) -> str:
        """
        Obtiene el método de cálculo de demanda configurado.

        Métodos disponibles:
        - 'promedio_simple': ventas_totales / dias
        - 'mediana': mediana ajustada por proporción de días con ventas
        - 'combinado': promedio móvil + ML cuando hay datos suficientes

        Returns:
            Método configurado o default 'mediana'
        """
        result = self.db.execute(text("""
            SELECT value FROM system_config WHERE key = 'metodo_calculo_demanda'
        """))
        row = result.fetchone()

        if row and row[0]:
            # El valor puede venir como JSON string, intentar deserializar
            raw_value = row[0]
            if isinstance(raw_value, str):
                try:
                    metodo = json.loads(raw_value)
                except (json.JSONDecodeError, TypeError):
                    metodo = raw_value.strip()
            else:
                metodo = str(raw_value)

            metodo = metodo.lower()
            if metodo in ('promedio_simple', 'mediana', 'combinado'):
                return metodo

        # Default desde settings
        return settings.demand_calculation_method

    def set_demand_method(self, metodo: str) -> bool:
        """
        Configura el método de cálculo de demanda.

        Args:
            metodo: 'promedio_simple', 'mediana', o 'combinado'

        Returns:
            True si se guardó correctamente
        """
        try:
            # Convertir a JSON string válido para columna JSONB
            value = json.dumps(metodo)
            self.db.execute(text("""
                INSERT INTO system_config (key, value, updated_at)
                VALUES ('metodo_calculo_demanda', :value, NOW())
                ON CONFLICT (key) DO UPDATE SET value = :value, updated_at = NOW()
            """), {'value': value})

            self.db.commit()
            logger.info(f"Método de cálculo de demanda cambiado a: {metodo}")
            return True
        except Exception as e:
            logger.error(f"Error guardando método de cálculo: {e}")
            self.db.rollback()
            return False

    # ==================== CONFIGURACIÓN COMPLETA ====================

    def get_all_config(self) -> Dict:
        """Obtiene toda la configuración del sistema"""
        return {
            'global_params': self.get_global_params(),
            'rubro_configs': self.get_rubro_configs(),
            'marca_configs': self.get_marca_configs(),
            'subrubro_thresholds': self.get_subrubro_thresholds(),
            'excluded_deposits': self.get_excluded_deposits(),
            'excluded_brands': self.get_excluded_brands(),
            'available_deposits': self.get_available_deposits(),
            'available_brands': self.get_available_brands(),
            'available_rubros': self.get_available_rubros(),
            'available_subrubros': self.get_available_subrubros(),
            'default_threshold': settings.min_sales_threshold,
            'demand_method': self.get_demand_method()
        }
