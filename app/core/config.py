"""
Configuración de la aplicación usando Pydantic Settings
Agente de Compras La Mascotera v2
"""

from pydantic_settings import BaseSettings
from typing import Optional, List


class Settings(BaseSettings):
    """Configuración de la aplicación"""

    # API Dux
    dux_api_token: str
    dux_api_base_url: str
    dux_empresa_id: int
    dux_sucursales_ids: str  # IDs separados por comas (e.g., "1,2,3,4")

    # Database
    database_url: str = "postgresql://postgres:mascotera2025@localhost:5432/mascotera_compras"

    # App
    debug: bool = True
    secret_key: str = "mascotera-secret-key"
    app_name: str = "Agente de Compras La Mascotera v2"
    app_version: str = "2.0.0"

    # Stock Calculation Config
    default_stock_days: int = 30  # Días de stock mínimo por defecto
    factor_ideal: float = 2.0     # stock_ideal = stock_minimo * factor_ideal
    factor_maximo: float = 4.0    # stock_maximo = stock_minimo * factor_maximo
    sales_period_days: int = 365  # Período de ventas para calcular demanda
    min_sales_threshold: int = 5  # Umbral mínimo de ventas en el período para calcular stock
                                  # Si vendió menos de este valor, stock_minimo = 0

    # Método de cálculo de demanda: 'promedio_simple', 'mediana', 'combinado'
    # - promedio_simple: ventas_totales / dias (sensible a picos)
    # - mediana: mediana_dias_venta * proporcion_dias_con_ventas (robusto a picos)
    # - combinado: usa promedio móvil + ML cuando hay datos suficientes
    demand_calculation_method: str = 'mediana'

    # Sync Config
    sync_rate_limit_per_second: int = 2
    sync_rate_limit_per_minute: int = 30

    @property
    def sucursales_list(self) -> List[int]:
        """Retorna lista de IDs de sucursales"""
        return [int(x.strip()) for x in self.dux_sucursales_ids.split(',') if x.strip()]

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


# Instancia global de settings
settings = Settings()
