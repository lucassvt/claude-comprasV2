"""
Servicio de Forecasting de Demanda
Implementa 3 métodos para calcular la demanda diaria y selecciona el mejor.

Métodos:
1. Promedio Simple
2. Promedio Móvil Ponderado
3. Machine Learning (Regresión + Tendencia)
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
import logging

logger = logging.getLogger(__name__)


@dataclass
class ForecastResult:
    """Resultado del forecasting para un producto"""
    product_id: int
    deposit_id: int
    demanda_diaria: float
    metodo_usado: str  # 'promedio_simple', 'promedio_movil', 'ml_tendencia'
    confianza: float  # 0-1, qué tan confiable es el resultado
    tendencia: str  # 'creciente', 'decreciente', 'estable'
    ventas_30_dias: float
    ventas_60_dias: float
    ventas_90_dias: float
    ventas_365_dias: float
    monto_90_dias: float  # Monto de ventas en los últimos 90 días (para ranking TOP)

    def to_dict(self) -> Dict:
        return {
            'product_id': self.product_id,
            'deposit_id': self.deposit_id,
            'demanda_diaria': round(self.demanda_diaria, 4),
            'metodo_usado': self.metodo_usado,
            'confianza': round(self.confianza, 2),
            'tendencia': self.tendencia,
            'ventas_30_dias': self.ventas_30_dias,
            'ventas_60_dias': self.ventas_60_dias,
            'ventas_90_dias': self.ventas_90_dias,
            'ventas_365_dias': self.ventas_365_dias,
            'monto_90_dias': round(self.monto_90_dias, 2)
        }


class DemandForecaster:
    """
    Calcula la demanda diaria usando múltiples métodos y selecciona el mejor.
    """

    def __init__(self, metodo_preferido: str = 'mediana'):
        """
        Args:
            metodo_preferido: Método de cálculo preferido.
                - 'promedio_simple': ventas/dias (sensible a picos)
                - 'mediana': mediana * proporcion_dias_con_ventas (robusto a picos)
                - 'combinado': usa promedio móvil + ML cuando hay datos suficientes
        """
        self.min_days_for_ml = 30  # Mínimo de días para usar ML
        self.min_days_for_movil = 14  # Mínimo de días para promedio móvil
        self.metodo_preferido = metodo_preferido

    def calculate_demand(
        self,
        sales_history: pd.DataFrame,
        product_id: int,
        deposit_id: int,
        days_back: int = 365
    ) -> ForecastResult:
        """
        Calcula la demanda diaria para un producto-depósito.

        Args:
            sales_history: DataFrame con columnas ['fecha', 'cantidad', 'monto']
            product_id: ID del producto
            deposit_id: ID del depósito
            days_back: Días hacia atrás para considerar

        Returns:
            ForecastResult con la demanda calculada y métricas
        """
        if sales_history.empty:
            return ForecastResult(
                product_id=product_id,
                deposit_id=deposit_id,
                demanda_diaria=0.0,
                metodo_usado='sin_datos',
                confianza=0.0,
                tendencia='estable',
                ventas_30_dias=0,
                ventas_60_dias=0,
                ventas_90_dias=0,
                ventas_365_dias=0,
                monto_90_dias=0.0
            )

        # Preparar datos
        df = sales_history.copy()
        df['fecha'] = pd.to_datetime(df['fecha'])
        df = df.sort_values('fecha')

        # Calcular fecha de corte
        fecha_fin = df['fecha'].max()
        fecha_inicio = fecha_fin - timedelta(days=days_back)
        df = df[df['fecha'] >= fecha_inicio]

        if df.empty:
            return ForecastResult(
                product_id=product_id,
                deposit_id=deposit_id,
                demanda_diaria=0.0,
                metodo_usado='sin_datos',
                confianza=0.0,
                tendencia='estable',
                ventas_30_dias=0,
                ventas_60_dias=0,
                ventas_90_dias=0,
                ventas_365_dias=0,
                monto_90_dias=0.0
            )

        # Calcular ventas por período (cantidad)
        ventas_30 = df[df['fecha'] >= (fecha_fin - timedelta(days=30))]['cantidad'].sum()
        ventas_60 = df[df['fecha'] >= (fecha_fin - timedelta(days=60))]['cantidad'].sum()
        ventas_90 = df[df['fecha'] >= (fecha_fin - timedelta(days=90))]['cantidad'].sum()
        ventas_365 = df['cantidad'].sum()

        # Calcular monto de ventas (para ranking TOP por importe)
        monto_90 = df[df['fecha'] >= (fecha_fin - timedelta(days=90))]['monto'].sum()

        # Calcular demanda con cada método
        dias_con_datos = (fecha_fin - df['fecha'].min()).days + 1

        # 1. Promedio Simple
        demanda_simple = self._promedio_simple(df, dias_con_datos)

        # 2. Mediana Ajustada (robusto a outliers)
        demanda_mediana = self._mediana_ajustada(df, dias_con_datos)

        # 3. Promedio Móvil Ponderado
        demanda_movil = self._promedio_movil_ponderado(df, fecha_fin)

        # 4. ML con Tendencia
        demanda_ml, tendencia, confianza_ml = self._ml_tendencia(df, fecha_fin)

        # Seleccionar el mejor método según configuración
        demanda_final, metodo, confianza = self._seleccionar_mejor_metodo(
            demanda_simple, demanda_mediana, demanda_movil, demanda_ml,
            confianza_ml, dias_con_datos, tendencia
        )

        return ForecastResult(
            product_id=product_id,
            deposit_id=deposit_id,
            demanda_diaria=max(0, demanda_final),
            metodo_usado=metodo,
            confianza=confianza,
            tendencia=tendencia,
            ventas_30_dias=float(ventas_30),
            ventas_60_dias=float(ventas_60),
            ventas_90_dias=float(ventas_90),
            ventas_365_dias=float(ventas_365),
            monto_90_dias=float(monto_90)
        )

    def _promedio_simple(self, df: pd.DataFrame, dias: int) -> float:
        """
        Calcula el promedio simple de ventas diarias.
        demanda = total_vendido / dias
        """
        total_vendido = df['cantidad'].sum()
        return float(total_vendido / max(1, dias))

    def _mediana_ajustada(self, df: pd.DataFrame, dias: int) -> float:
        """
        Calcula la demanda usando mediana ajustada por proporción de días con ventas.

        Fórmula: demanda = mediana_dias_con_venta * (dias_con_ventas / dias_periodo)

        Este método es robusto a picos de ventas atípicos (outliers).
        La mediana representa el valor "típico" de un día con ventas,
        y se ajusta por la proporción de días en que realmente hubo ventas.

        Ejemplo:
        - Producto vendió 7 días en 365, con cantidades [1, 1, 1, 1, 2, 3, 6]
        - Mediana = 1 (valor típico, ignora el pico de 6)
        - Proporción = 7/365 = 0.0192
        - Demanda = 1 * 0.0192 = 0.0192 u/día

        Comparación con promedio simple:
        - Promedio simple = 15/365 = 0.0411 u/día (distorsionado por el pico de 6)
        - Mediana ajustada = 0.0192 u/día (más representativo del comportamiento normal)
        """
        # Agrupar por día
        df_diario = df.groupby(df['fecha'].dt.date)['cantidad'].sum()

        if df_diario.empty:
            return 0.0

        # Calcular mediana de las cantidades diarias
        mediana = float(df_diario.median())

        # Calcular proporción de días con ventas
        dias_con_ventas = len(df_diario)
        proporcion = dias_con_ventas / max(1, dias)

        # Demanda = mediana * proporción
        return mediana * proporcion

    def _promedio_movil_ponderado(
        self,
        df: pd.DataFrame,
        fecha_fin: datetime,
        ventana_dias: int = 90
    ) -> float:
        """
        Calcula el promedio móvil ponderado.
        Los días más recientes tienen mayor peso.
        """
        # Filtrar últimos N días
        fecha_inicio = fecha_fin - timedelta(days=ventana_dias)
        df_reciente = df[df['fecha'] >= fecha_inicio].copy()

        if df_reciente.empty:
            return 0.0

        # Agrupar por día (usando solo la fecha, sin timezone)
        df_diario = df_reciente.groupby(df_reciente['fecha'].dt.date)['cantidad'].sum().reset_index()
        df_diario.columns = ['fecha', 'cantidad']
        # Convertir a datetime sin timezone para poder hacer merge
        df_diario['fecha'] = pd.to_datetime(df_diario['fecha']).dt.tz_localize(None)

        # Crear serie completa de fechas (incluir días sin ventas como 0)
        # Normalizar fechas sin timezone
        fecha_inicio_norm = pd.Timestamp(fecha_inicio).tz_localize(None).normalize()
        fecha_fin_norm = pd.Timestamp(fecha_fin).tz_localize(None).normalize()
        todas_fechas = pd.date_range(start=fecha_inicio_norm, end=fecha_fin_norm, freq='D')
        df_completo = pd.DataFrame({'fecha': todas_fechas})
        df_completo = df_completo.merge(df_diario, on='fecha', how='left')
        df_completo['cantidad'] = df_completo['cantidad'].fillna(0).astype(float)

        # Calcular pesos (más reciente = más peso)
        n = len(df_completo)
        pesos = np.linspace(1, 2, n)  # Pesos de 1 a 2

        # Promedio ponderado
        suma_ponderada = (df_completo['cantidad'].values * pesos).sum()
        suma_pesos = pesos.sum()

        return float(suma_ponderada / suma_pesos) if suma_pesos > 0 else 0.0

    def _ml_tendencia(
        self,
        df: pd.DataFrame,
        fecha_fin: datetime
    ) -> Tuple[float, str, float]:
        """
        Usa regresión lineal para detectar tendencia y proyectar demanda.

        Returns:
            (demanda_proyectada, tendencia, confianza)
        """
        # Agrupar por día
        df_diario = df.groupby(df['fecha'].dt.date)['cantidad'].sum().reset_index()
        df_diario.columns = ['fecha', 'cantidad']
        df_diario['fecha'] = pd.to_datetime(df_diario['fecha'])

        if len(df_diario) < self.min_days_for_ml:
            # No hay suficientes datos para ML
            # Calcular promedio simple: total vendido / días del período
            # NO usar mean() de cantidades porque eso da el promedio por día CON ventas
            total_vendido = df_diario['cantidad'].sum()
            dias_periodo = (df_diario['fecha'].max() - df_diario['fecha'].min()).days + 1
            promedio = total_vendido / max(1, dias_periodo)
            return float(promedio), 'estable', 0.3

        # Crear features (días desde inicio)
        fecha_min = df_diario['fecha'].min()
        df_diario['dias'] = (df_diario['fecha'] - fecha_min).dt.days

        # Preparar datos para regresión
        X = df_diario['dias'].values.reshape(-1, 1)
        y = df_diario['cantidad'].values

        # Ajustar modelo lineal
        modelo = LinearRegression()
        modelo.fit(X, y)

        # Calcular R² como medida de confianza
        r2 = modelo.score(X, y)
        confianza = max(0.3, min(0.95, r2))  # Limitar entre 0.3 y 0.95

        # Obtener pendiente para determinar tendencia
        pendiente = modelo.coef_[0]
        promedio_diario = y.mean()

        # Determinar tendencia basada en la pendiente relativa al promedio
        ratio_pendiente = pendiente / max(0.01, promedio_diario)

        if ratio_pendiente > 0.01:  # Más de 1% de crecimiento diario
            tendencia = 'creciente'
        elif ratio_pendiente < -0.01:  # Más de 1% de decrecimiento diario
            tendencia = 'decreciente'
        else:
            tendencia = 'estable'

        # Proyectar demanda para los próximos 15 días (promedio)
        dias_futuro = (fecha_fin - fecha_min).days
        X_futuro = np.array([[dias_futuro + i] for i in range(15)])
        predicciones = modelo.predict(X_futuro)
        demanda_proyectada = float(np.mean(predicciones))

        # No permitir demanda negativa
        demanda_proyectada = max(0, demanda_proyectada)

        return demanda_proyectada, tendencia, confianza

    def _seleccionar_mejor_metodo(
        self,
        demanda_simple: float,
        demanda_mediana: float,
        demanda_movil: float,
        demanda_ml: float,
        confianza_ml: float,
        dias_con_datos: int,
        tendencia: str
    ) -> Tuple[float, str, float]:
        """
        Selecciona el método de cálculo basado en la configuración y calidad de datos.

        Métodos disponibles:
        - 'promedio_simple': ventas/dias (sensible a picos)
        - 'mediana': mediana ajustada (robusto a picos, recomendado)
        - 'combinado': usa promedio móvil + ML cuando hay datos suficientes

        Returns:
            (demanda_seleccionada, metodo_usado, confianza)
        """
        # Si el método preferido es 'promedio_simple', usarlo siempre
        if self.metodo_preferido == 'promedio_simple':
            return demanda_simple, 'promedio_simple', 0.6

        # Si el método preferido es 'mediana', usarlo siempre
        # (es robusto incluso con pocos datos)
        if self.metodo_preferido == 'mediana':
            return demanda_mediana, 'mediana', 0.7

        # Método 'combinado' (comportamiento anterior mejorado)
        # Si hay pocos datos, usar mediana (más robusta que promedio simple)
        if dias_con_datos < self.min_days_for_movil:
            return demanda_mediana, 'mediana', 0.5

        # Si hay datos suficientes pero no para ML, usar promedio móvil
        if dias_con_datos < self.min_days_for_ml:
            return demanda_movil, 'promedio_movil', 0.6

        # Si el ML tiene buena confianza y hay tendencia clara, usarlo
        if confianza_ml > 0.6 and tendencia != 'estable':
            return demanda_ml, 'ml_tendencia', confianza_ml

        # Si la tendencia es estable o baja confianza, combinar métodos
        # Usar promedio ponderado incluyendo mediana
        peso_mediana = 0.3
        peso_movil = 0.4
        peso_ml = 0.3

        demanda_combinada = (
            demanda_mediana * peso_mediana +
            demanda_movil * peso_movil +
            demanda_ml * peso_ml
        )

        return demanda_combinada, 'combinado', 0.7

    def calculate_demand_batch(
        self,
        sales_data: Dict[Tuple[int, int], pd.DataFrame],
        days_back: int = 365
    ) -> Dict[Tuple[int, int], ForecastResult]:
        """
        Calcula la demanda para múltiples productos-depósitos.

        Args:
            sales_data: Diccionario {(product_id, deposit_id): DataFrame}
            days_back: Días hacia atrás

        Returns:
            Diccionario con resultados de forecasting
        """
        results = {}

        for (product_id, deposit_id), df in sales_data.items():
            try:
                result = self.calculate_demand(df, product_id, deposit_id, days_back)
                results[(product_id, deposit_id)] = result
            except Exception as e:
                logger.error(f"Error calculando demanda para producto {product_id}, depósito {deposit_id}: {e}")
                results[(product_id, deposit_id)] = ForecastResult(
                    product_id=product_id,
                    deposit_id=deposit_id,
                    demanda_diaria=0.0,
                    metodo_usado='error',
                    confianza=0.0,
                    tendencia='estable',
                    ventas_30_dias=0,
                    ventas_60_dias=0,
                    ventas_90_dias=0,
                    ventas_365_dias=0,
                    monto_90_dias=0.0
                )

        return results
