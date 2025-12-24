"""
Servicio de Compras y Exportaciones
Genera propuestas de compra y exporta diversos reportes a Excel.

Funcionalidades:
- Propuesta de compras (productos sin stock global suficiente)
- Exportar referencias de stock (mín/ideal/máx)
- Exportar detalle de cálculo por depósito
- Exportar TOP 200 productos bajo mínimo
- Exportar stock negativo para auditoría
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass
from sqlalchemy.orm import Session
from sqlalchemy import text
import pandas as pd
from pathlib import Path

from app.core.config import settings
from app.services.stock_calculator import StockCalculator, StockLevel
from app.services.distribution_service import DistributionResult, PurchaseNeed

logger = logging.getLogger(__name__)


class PurchaseService:
    """
    Genera reportes de compras y exportaciones Excel.
    """

    def __init__(self, db: Session):
        self.db = db

    def _get_global_config(self) -> Dict:
        """Obtiene los parámetros globales desde la BD (con fallback a settings)"""
        config = {
            'factor_ideal': settings.factor_ideal,
            'factor_maximo': settings.factor_maximo,
            'dias_stock_default': settings.default_stock_days
        }

        result = self.db.execute(text("""
            SELECT key, value FROM system_config
            WHERE key IN ('factor_ideal', 'factor_maximo', 'dias_stock_default')
        """))

        for row in result:
            key = row[0]
            value = row[1]
            if key == 'factor_ideal':
                config['factor_ideal'] = float(value)
            elif key == 'factor_maximo':
                config['factor_maximo'] = float(value)
            elif key == 'dias_stock_default':
                config['dias_stock_default'] = int(value)

        return config

    def export_purchases_excel(
        self,
        purchase_needs: List[PurchaseNeed],
        output_path: Optional[str] = None
    ) -> str:
        """
        Exporta las necesidades de compra a Excel.

        Args:
            purchase_needs: Lista de necesidades de compra
            output_path: Ruta opcional para el archivo

        Returns:
            Ruta del archivo generado
        """
        if not output_path:
            exports_dir = Path("exports")
            exports_dir.mkdir(exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = str(exports_dir / f"compras_proveedores_{timestamp}.xlsx")

        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            workbook = writer.book

            # Formatos
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#4472C4',
                'font_color': 'white',
                'border': 1,
                'align': 'center'
            })
            currency_format = workbook.add_format({'num_format': '$#,##0.00'})
            number_format = workbook.add_format({'num_format': '#,##0'})

            if purchase_needs:
                data = []
                for p in purchase_needs:
                    data.append({
                        'Fecha': datetime.now().strftime("%Y-%m-%d"),
                        'Código': p.cod_item,
                        'Producto': p.producto_nombre,
                        'Cantidad': int(round(p.cantidad_necesaria)),
                        'Depósito Destino': p.deposit_destino_nombre,
                        'Origen Necesidad': p.origen_necesidad,
                        'Costo Unitario': round(p.costo_unitario, 2),
                        'Costo Total': round(p.costo_total, 2),
                        'Marca': p.marca,
                        'Rubro': p.rubro,
                        'Subrubro': p.subrubro,
                        'Stock Actual': int(round(p.stock_actual_destino)),
                        'Stock Objetivo': int(round(p.stock_objetivo_destino)),
                        'Stock Central': int(round(p.stock_central_actual)),
                        'Mínimo Central': int(round(p.stock_central_minimo))
                    })

                df = pd.DataFrame(data)
                df.to_excel(writer, sheet_name='Compras', index=False, startrow=1, header=False)

                worksheet = writer.sheets['Compras']
                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(0, col_num, value, header_format)

                worksheet.set_column('A:A', 12)  # Fecha
                worksheet.set_column('B:B', 12)  # Código
                worksheet.set_column('C:C', 45)  # Producto
                worksheet.set_column('D:D', 10, number_format)  # Cantidad
                worksheet.set_column('E:E', 20)  # Depósito
                worksheet.set_column('F:F', 22)  # Origen
                worksheet.set_column('G:H', 15, currency_format)  # Costos
                worksheet.set_column('I:K', 18)  # Marca, Rubro, Subrubro
                worksheet.set_column('L:O', 12, number_format)  # Stocks

                # Resumen
                resumen_data = [{
                    'Métrica': 'Total Productos',
                    'Valor': len(purchase_needs)
                }, {
                    'Métrica': 'Total Unidades',
                    'Valor': int(round(sum(p.cantidad_necesaria for p in purchase_needs)))
                }, {
                    'Métrica': 'Costo Total Estimado',
                    'Valor': f"${sum(p.costo_total for p in purchase_needs):,.2f}"
                }]
                df_resumen = pd.DataFrame(resumen_data)
                df_resumen.to_excel(writer, sheet_name='Resumen', index=False, startrow=1, header=False)

                worksheet = writer.sheets['Resumen']
                for col_num, value in enumerate(df_resumen.columns.values):
                    worksheet.write(0, col_num, value, header_format)

            else:
                # Hoja vacía con mensaje
                df = pd.DataFrame([{'Mensaje': 'No hay necesidades de compra'}])
                df.to_excel(writer, sheet_name='Compras', index=False)

        logger.info(f"Excel de compras exportado: {output_path}")
        return output_path

    def export_stock_references_excel(
        self,
        stock_levels: List[StockLevel],
        output_path: Optional[str] = None
    ) -> str:
        """
        Exporta las referencias de stock (mín/ideal/máx) a Excel.

        Args:
            stock_levels: Lista de niveles de stock
            output_path: Ruta opcional para el archivo

        Returns:
            Ruta del archivo generado
        """
        if not output_path:
            exports_dir = Path("exports")
            exports_dir.mkdir(exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = str(exports_dir / f"referencias_stock_{timestamp}.xlsx")

        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            workbook = writer.book

            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#4472C4',
                'font_color': 'white',
                'border': 1,
                'align': 'center'
            })

            # Estado colores
            estado_ok = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
            estado_bajo = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
            estado_excedente = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C6500'})

            data = []
            for sl in stock_levels:
                estado_texto = {
                    'ok': 'OK',
                    'bajo_minimo': 'Bajo Mínimo',
                    'sin_stock': 'Sin Stock',
                    'excedente': 'Excedente'
                }.get(sl.estado, sl.estado)

                data.append({
                    'Código': sl.cod_item,
                    'Producto': sl.producto_nombre,
                    'Marca': sl.marca,
                    'Rubro': sl.rubro,
                    'Subrubro': sl.subrubro,
                    'Depósito': sl.deposito_nombre,
                    'Stock Actual': int(round(sl.stock_actual)),
                    'Stock Mínimo': int(round(sl.stock_minimo)),
                    'Stock Ideal': int(round(sl.stock_ideal)),
                    'Stock Máximo': int(round(sl.stock_maximo)),
                    'Estado': estado_texto
                })

            df = pd.DataFrame(data)
            df.to_excel(writer, sheet_name='Referencias', index=False, startrow=1, header=False)

            worksheet = writer.sheets['Referencias']
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)

            worksheet.set_column('A:A', 12)
            worksheet.set_column('B:B', 45)
            worksheet.set_column('C:E', 20)
            worksheet.set_column('F:F', 18)
            worksheet.set_column('G:J', 12)
            worksheet.set_column('K:K', 14)

            # Aplicar formato condicional para estado
            worksheet.conditional_format('K2:K' + str(len(data) + 1), {
                'type': 'text',
                'criteria': 'containing',
                'value': 'OK',
                'format': estado_ok
            })
            worksheet.conditional_format('K2:K' + str(len(data) + 1), {
                'type': 'text',
                'criteria': 'containing',
                'value': 'Bajo',
                'format': estado_bajo
            })
            worksheet.conditional_format('K2:K' + str(len(data) + 1), {
                'type': 'text',
                'criteria': 'containing',
                'value': 'Sin Stock',
                'format': estado_bajo
            })
            worksheet.conditional_format('K2:K' + str(len(data) + 1), {
                'type': 'text',
                'criteria': 'containing',
                'value': 'Excedente',
                'format': estado_excedente
            })

        logger.info(f"Excel de referencias exportado: {output_path}")
        return output_path

    def export_calculation_detail_excel(
        self,
        stock_levels: List[StockLevel],
        output_path: Optional[str] = None
    ) -> str:
        """
        Exporta el detalle de cálculo de stock con una hoja por depósito.

        Args:
            stock_levels: Lista de niveles de stock
            output_path: Ruta opcional para el archivo

        Returns:
            Ruta del archivo generado
        """
        if not output_path:
            exports_dir = Path("exports")
            exports_dir.mkdir(exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = str(exports_dir / f"detalle_calculo_stock_{timestamp}.xlsx")

        # Agrupar por depósito
        by_deposit = {}
        for sl in stock_levels:
            if sl.deposito_nombre not in by_deposit:
                by_deposit[sl.deposito_nombre] = []
            by_deposit[sl.deposito_nombre].append(sl)

        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            workbook = writer.book

            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#4472C4',
                'font_color': 'white',
                'border': 1,
                'align': 'center',
                'text_wrap': True
            })

            # Obtener parámetros globales desde BD
            global_config = self._get_global_config()

            for deposit_name, levels in sorted(by_deposit.items()):
                # Limpiar nombre para hoja de Excel (max 31 caracteres)
                sheet_name = deposit_name[:31].replace('/', '-').replace('\\', '-')

                data = []
                umbral_ventas = settings.min_sales_threshold
                for sl in levels:
                    diferencia = sl.stock_actual - sl.stock_minimo
                    # Detectar si fue excluido por ventas bajas
                    excluido_ventas = sl.ventas_365_dias < umbral_ventas
                    data.append({
                        'Código': sl.cod_item,
                        'Producto': sl.producto_nombre,
                        'Marca': sl.marca,
                        'Monto 90 días ($)': round(sl.monto_90_dias, 2),
                        'Ventas 30 días': int(round(sl.ventas_30_dias)),
                        'Ventas 60 días': int(round(sl.ventas_60_dias)),
                        'Ventas 90 días': int(round(sl.ventas_90_dias)),
                        'Ventas 365 días': int(round(sl.ventas_365_dias)),
                        'Umbral Mín Ventas': umbral_ventas,
                        'Excluido Ventas Bajas': 'Sí' if excluido_ventas else 'No',
                        'Demanda Diaria': round(sl.demanda_diaria, 4),
                        'Método Forecast': sl.metodo_forecast,
                        'Tendencia': sl.tendencia,
                        'Días Cobertura': sl.dias_cobertura,
                        'Factor Ideal': global_config['factor_ideal'],
                        'Factor Máximo': global_config['factor_maximo'],
                        'Stock Mínimo': int(round(sl.stock_minimo)),
                        'Stock Ideal': int(round(sl.stock_ideal)),
                        'Stock Máximo': int(round(sl.stock_maximo)),
                        'Stock Actual': int(round(sl.stock_actual)),
                        'Diferencia vs Mín': int(round(diferencia)),
                        'Estado': sl.estado
                    })

                df = pd.DataFrame(data)
                df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1, header=False)

                worksheet = writer.sheets[sheet_name]
                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(0, col_num, value, header_format)

                worksheet.set_column('A:A', 12)   # Código
                worksheet.set_column('B:B', 40)   # Producto
                worksheet.set_column('C:C', 18)   # Marca
                worksheet.set_column('D:D', 18)   # Monto 90 días ($)
                worksheet.set_column('E:H', 14)   # Ventas 30/60/90/365
                worksheet.set_column('I:I', 16)   # Umbral Mín Ventas
                worksheet.set_column('J:J', 20)   # Excluido Ventas Bajas
                worksheet.set_column('K:K', 14)   # Demanda Diaria
                worksheet.set_column('L:M', 16)   # Método/Tendencia
                worksheet.set_column('N:V', 14)   # Resto de columnas

        logger.info(f"Excel de detalle cálculo exportado: {output_path}")
        return output_path

    def export_top200_below_minimum_excel(
        self,
        stock_levels: List[StockLevel],
        output_path: Optional[str] = None
    ) -> str:
        """
        Exporta los TOP 200 productos por MONTO de ventas (importe $) que están bajo el mínimo.

        Args:
            stock_levels: Lista de niveles de stock
            output_path: Ruta opcional para el archivo

        Returns:
            Ruta del archivo generado
        """
        if not output_path:
            exports_dir = Path("exports")
            exports_dir.mkdir(exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = str(exports_dir / f"productos_top_bajo_minimo_{timestamp}.xlsx")

        # Obtener productos únicos con su monto de ventas total (90 días)
        product_sales = {}
        for sl in stock_levels:
            if sl.product_id not in product_sales:
                product_sales[sl.product_id] = {
                    'cod_item': sl.cod_item,
                    'producto': sl.producto_nombre,
                    'marca': sl.marca,
                    'rubro': sl.rubro,
                    'monto_90_dias': 0,
                    'ventas_90_dias': 0,
                    'depositos_bajo_minimo': []
                }
            product_sales[sl.product_id]['monto_90_dias'] += sl.monto_90_dias
            product_sales[sl.product_id]['ventas_90_dias'] += sl.ventas_90_dias

            # Solo incluir si:
            # 1. stock_minimo > 0 (el depósito requiere stock de este producto)
            # 2. stock_actual < stock_minimo (está bajo el mínimo)
            # NO depender de sl.estado porque el caché puede tener datos viejos
            if sl.stock_minimo > 0 and sl.stock_actual < sl.stock_minimo:
                product_sales[sl.product_id]['depositos_bajo_minimo'].append({
                    'deposito': sl.deposito_nombre,
                    'stock_actual': sl.stock_actual,
                    'stock_minimo': sl.stock_minimo,
                    'faltante': sl.stock_minimo - sl.stock_actual
                })

        # Ordenar por MONTO de ventas (importe $) y tomar TOP 200
        sorted_products = sorted(
            product_sales.items(),
            key=lambda x: x[1]['monto_90_dias'],
            reverse=True
        )[:200]

        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            workbook = writer.book

            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#C00000',
                'font_color': 'white',
                'border': 1,
                'align': 'center'
            })

            alert_format = workbook.add_format({
                'bg_color': '#FFC7CE',
                'font_color': '#9C0006'
            })

            data = []
            ranking = 0
            for product_id, info in sorted_products:
                ranking += 1
                if info['depositos_bajo_minimo']:
                    for dep in info['depositos_bajo_minimo']:
                        stock_min_redondeado = int(round(dep['stock_minimo']))
                        # Solo incluir si stock_minimo redondeado > 0
                        if stock_min_redondeado > 0:
                            data.append({
                                'Ranking': ranking,
                                'Código': info['cod_item'],
                                'Producto': info['producto'],
                                'Depósito': dep['deposito'],
                                'Stock Actual': int(round(dep['stock_actual'])),
                                'Stock Mínimo': stock_min_redondeado,
                                'Faltante': int(round(dep['faltante'])),
                                'Marca': info['marca']
                            })

            if data:
                df = pd.DataFrame(data)
                df.to_excel(writer, sheet_name='TOP Bajo Mínimo', index=False, startrow=1, header=False)

                worksheet = writer.sheets['TOP Bajo Mínimo']
                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(0, col_num, value, header_format)

                worksheet.set_column('A:A', 10)   # Ranking
                worksheet.set_column('B:B', 12)   # Código
                worksheet.set_column('C:C', 45)   # Producto
                worksheet.set_column('D:D', 20)   # Depósito
                worksheet.set_column('E:G', 14)   # Stock Actual, Stock Mínimo, Faltante
                worksheet.set_column('H:H', 18)   # Marca

                # Resumen en otra hoja
                total_faltante = sum(d['Faltante'] for d in data)
                resumen = [{
                    'Métrica': 'Productos TOP con faltante',
                    'Valor': len(set(d['Código'] for d in data))
                }, {
                    'Métrica': 'Total registros (producto-depósito)',
                    'Valor': len(data)
                }, {
                    'Métrica': 'Unidades faltantes total',
                    'Valor': int(round(total_faltante))
                }]
                df_resumen = pd.DataFrame(resumen)
                df_resumen.to_excel(writer, sheet_name='Resumen', index=False, startrow=1, header=False)

                ws_resumen = writer.sheets['Resumen']
                for col_num, value in enumerate(df_resumen.columns.values):
                    ws_resumen.write(0, col_num, value, header_format)
            else:
                df = pd.DataFrame([{'Mensaje': 'No hay productos TOP bajo mínimo'}])
                df.to_excel(writer, sheet_name='TOP Bajo Mínimo', index=False)

        logger.info(f"Excel de TOP 200 bajo mínimo exportado: {output_path}")
        return output_path

    def export_negative_stock_excel(
        self,
        stock_levels: List[StockLevel],
        output_path: Optional[str] = None
    ) -> str:
        """
        Exporta productos con stock negativo separados por depósito (para auditoría).

        Args:
            stock_levels: Lista de niveles de stock
            output_path: Ruta opcional para el archivo

        Returns:
            Ruta del archivo generado
        """
        if not output_path:
            exports_dir = Path("exports")
            exports_dir.mkdir(exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = str(exports_dir / f"stock_negativo_auditoria_{timestamp}.xlsx")

        # Filtrar productos con stock REAL negativo y agrupar por depósito
        # Usamos stock_real (físico) para auditoría, no stock_disponible
        # Solo incluye productos con stock < -0.5 para evitar falsos positivos
        # por redondeo (productos con stock = 0 o muy cercano a 0)
        negative_by_deposit = {}
        for sl in stock_levels:
            if sl.stock_real < -0.5:
                if sl.deposito_nombre not in negative_by_deposit:
                    negative_by_deposit[sl.deposito_nombre] = []
                negative_by_deposit[sl.deposito_nombre].append(sl)

        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            workbook = writer.book

            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#C00000',
                'font_color': 'white',
                'border': 1,
                'align': 'center'
            })

            negative_format = workbook.add_format({
                'bg_color': '#FFC7CE',
                'font_color': '#9C0006',
                'num_format': '#,##0.00'
            })

            if negative_by_deposit:
                for deposit_name, levels in sorted(negative_by_deposit.items()):
                    sheet_name = deposit_name[:31].replace('/', '-').replace('\\', '-')

                    data = []
                    for sl in levels:
                        data.append({
                            'Código': sl.cod_item,
                            'Producto': sl.producto_nombre,
                            'Stock Real': int(round(sl.stock_real)),
                            'Stock Reservado': int(round(sl.stock_reservado)),
                            'Stock Disponible': int(round(sl.stock_actual)),
                            'Marca': sl.marca,
                            'Rubro': sl.rubro,
                            'Subrubro': sl.subrubro
                        })

                    df = pd.DataFrame(data)
                    df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1, header=False)

                    worksheet = writer.sheets[sheet_name]
                    for col_num, value in enumerate(df.columns.values):
                        worksheet.write(0, col_num, value, header_format)

                    worksheet.set_column('A:A', 12)  # Código
                    worksheet.set_column('B:B', 45)  # Producto
                    worksheet.set_column('C:C', 14, negative_format)  # Stock Real
                    worksheet.set_column('D:D', 16)  # Stock Reservado
                    worksheet.set_column('E:E', 16)  # Stock Disponible
                    worksheet.set_column('F:H', 20)  # Marca, Rubro, Subrubro

                # Hoja de resumen
                resumen_data = []
                total_negativos = 0
                for dep, levels in negative_by_deposit.items():
                    total_negativos += len(levels)
                    resumen_data.append({
                        'Depósito': dep,
                        'Productos con Stock Negativo': len(levels)
                    })

                resumen_data.append({
                    'Depósito': 'TOTAL',
                    'Productos con Stock Negativo': total_negativos
                })

                df_resumen = pd.DataFrame(resumen_data)
                df_resumen.to_excel(writer, sheet_name='Resumen', index=False, startrow=1, header=False)

                ws_resumen = writer.sheets['Resumen']
                for col_num, value in enumerate(df_resumen.columns.values):
                    ws_resumen.write(0, col_num, value, header_format)
                ws_resumen.set_column('A:A', 25)
                ws_resumen.set_column('B:B', 30)

            else:
                df = pd.DataFrame([{'Mensaje': 'No hay productos con stock negativo'}])
                df.to_excel(writer, sheet_name='Sin Negativos', index=False)

        logger.info(f"Excel de stock negativo exportado: {output_path}")
        return output_path

    def get_purchase_summary(self, purchase_needs: List[PurchaseNeed]) -> Dict:
        """Genera un resumen de las necesidades de compra"""
        if not purchase_needs:
            return {
                'total_productos': 0,
                'total_unidades': 0,
                'costo_total': 0,
                'por_origen': {},
                'por_marca': {}
            }

        por_origen = {}
        por_marca = {}

        for p in purchase_needs:
            # Por origen
            if p.origen_necesidad not in por_origen:
                por_origen[p.origen_necesidad] = {'count': 0, 'units': 0, 'cost': 0}
            por_origen[p.origen_necesidad]['count'] += 1
            por_origen[p.origen_necesidad]['units'] += p.cantidad_necesaria
            por_origen[p.origen_necesidad]['cost'] += p.costo_total

            # Por marca
            marca = p.marca or 'Sin Marca'
            if marca not in por_marca:
                por_marca[marca] = {'count': 0, 'units': 0, 'cost': 0}
            por_marca[marca]['count'] += 1
            por_marca[marca]['units'] += p.cantidad_necesaria
            por_marca[marca]['cost'] += p.costo_total

        return {
            'total_productos': len(purchase_needs),
            'total_unidades': sum(p.cantidad_necesaria for p in purchase_needs),
            'costo_total': sum(p.costo_total for p in purchase_needs),
            'por_origen': por_origen,
            'por_marca': dict(sorted(por_marca.items(), key=lambda x: x[1]['cost'], reverse=True)[:10])
        }

    def _get_product_costs(self) -> Dict[int, float]:
        """Obtiene los costos de los productos"""
        result = self.db.execute(text("""
            SELECT id, COALESCE(costo, 0) as costo FROM products
        """))
        return {row[0]: float(row[1]) for row in result}

    def export_immobilized_stock_excel(
        self,
        stock_levels: List[StockLevel],
        output_path: Optional[str] = None
    ) -> str:
        """
        Exporta reporte de stock inmovilizado (excedente sobre máximo).

        El stock inmovilizado es capital "dormido" que excede las necesidades máximas.

        Args:
            stock_levels: Lista de niveles de stock
            output_path: Ruta opcional para el archivo

        Returns:
            Ruta del archivo generado
        """
        if not output_path:
            exports_dir = Path("exports")
            exports_dir.mkdir(exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = str(exports_dir / f"stock_inmovilizado_{timestamp}.xlsx")

        # Obtener costos de productos
        product_costs = self._get_product_costs()

        # Filtrar productos con excedente (stock_actual > stock_maximo) y agrupar por depósito
        excess_by_deposit = {}
        total_stats = {
            'total_productos': 0,
            'total_unidades_excedentes': 0,
            'valor_total_inmovilizado': 0
        }

        for sl in stock_levels:
            # Solo productos con excedente real (estado = 'excedente')
            if sl.estado == 'excedente' and sl.stock_maximo > 0:
                unidades_excedentes = sl.stock_actual - sl.stock_maximo
                if unidades_excedentes > 0:
                    costo_unitario = product_costs.get(sl.product_id, 0)
                    valor_inmovilizado = unidades_excedentes * costo_unitario

                    if sl.deposito_nombre not in excess_by_deposit:
                        excess_by_deposit[sl.deposito_nombre] = []

                    excess_by_deposit[sl.deposito_nombre].append({
                        'cod_item': sl.cod_item,
                        'producto': sl.producto_nombre,
                        'marca': sl.marca,
                        'rubro': sl.rubro,
                        'stock_actual': int(round(sl.stock_actual)),
                        'stock_maximo': int(round(sl.stock_maximo)),
                        'unidades_excedentes': int(round(unidades_excedentes)),
                        'costo_unitario': costo_unitario,
                        'valor_inmovilizado': valor_inmovilizado,
                        'ventas_90_dias': int(round(sl.ventas_90_dias)),
                        'monto_90_dias': sl.monto_90_dias
                    })

                    total_stats['total_productos'] += 1
                    total_stats['total_unidades_excedentes'] += unidades_excedentes
                    total_stats['valor_total_inmovilizado'] += valor_inmovilizado

        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            workbook = writer.book

            # Formatos
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#FF9800',
                'font_color': 'white',
                'border': 1,
                'align': 'center'
            })
            currency_format = workbook.add_format({'num_format': '$#,##0.00'})
            number_format = workbook.add_format({'num_format': '#,##0'})
            warning_format = workbook.add_format({
                'bg_color': '#FFEB9C',
                'font_color': '#9C6500'
            })

            if excess_by_deposit:
                # Hoja consolidada con todos los depósitos
                all_data = []
                for deposit_name, items in sorted(excess_by_deposit.items()):
                    for item in items:
                        all_data.append({
                            'Código': item['cod_item'],
                            'Producto': item['producto'],
                            'Marca': item['marca'],
                            'Rubro': item['rubro'],
                            'Depósito': deposit_name,
                            'Stock Actual': item['stock_actual'],
                            'Stock Máximo': item['stock_maximo'],
                            'Unidades Excedentes': item['unidades_excedentes'],
                            'Costo Unitario': item['costo_unitario'],
                            'Valor Inmovilizado': item['valor_inmovilizado'],
                            'Ventas 90 días': item['ventas_90_dias'],
                            'Monto 90 días ($)': round(item['monto_90_dias'], 2)
                        })

                # Ordenar por valor inmovilizado descendente
                all_data.sort(key=lambda x: x['Valor Inmovilizado'], reverse=True)

                df = pd.DataFrame(all_data)
                df.to_excel(writer, sheet_name='Stock Inmovilizado', index=False, startrow=1, header=False)

                worksheet = writer.sheets['Stock Inmovilizado']
                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(0, col_num, value, header_format)

                worksheet.set_column('A:A', 12)   # Código
                worksheet.set_column('B:B', 45)   # Producto
                worksheet.set_column('C:D', 18)   # Marca, Rubro
                worksheet.set_column('E:E', 20)   # Depósito
                worksheet.set_column('F:H', 14, number_format)  # Stocks
                worksheet.set_column('I:J', 16, currency_format)  # Costos
                worksheet.set_column('K:L', 16, number_format)  # Ventas

                # Hoja de resumen por depósito
                resumen_depositos = []
                for deposit_name, items in sorted(excess_by_deposit.items()):
                    total_unidades = sum(i['unidades_excedentes'] for i in items)
                    total_valor = sum(i['valor_inmovilizado'] for i in items)
                    resumen_depositos.append({
                        'Depósito': deposit_name,
                        'Productos con Excedente': len(items),
                        'Total Unidades Excedentes': int(round(total_unidades)),
                        'Valor Inmovilizado ($)': round(total_valor, 2)
                    })

                # Agregar fila de totales
                resumen_depositos.append({
                    'Depósito': 'TOTAL GENERAL',
                    'Productos con Excedente': total_stats['total_productos'],
                    'Total Unidades Excedentes': int(round(total_stats['total_unidades_excedentes'])),
                    'Valor Inmovilizado ($)': round(total_stats['valor_total_inmovilizado'], 2)
                })

                df_resumen = pd.DataFrame(resumen_depositos)
                df_resumen.to_excel(writer, sheet_name='Resumen por Depósito', index=False, startrow=1, header=False)

                ws_resumen = writer.sheets['Resumen por Depósito']
                for col_num, value in enumerate(df_resumen.columns.values):
                    ws_resumen.write(0, col_num, value, header_format)
                ws_resumen.set_column('A:A', 25)
                ws_resumen.set_column('B:B', 22, number_format)
                ws_resumen.set_column('C:C', 24, number_format)
                ws_resumen.set_column('D:D', 22, currency_format)

            else:
                # No hay stock inmovilizado
                df = pd.DataFrame([{'Mensaje': 'No hay productos con stock inmovilizado (excedente)'}])
                df.to_excel(writer, sheet_name='Sin Excedentes', index=False)

        logger.info(f"Excel de stock inmovilizado exportado: {output_path}")
        return output_path

    def get_immobilized_stock_summary(self, stock_levels: List[StockLevel]) -> Dict:
        """
        Obtiene un resumen del stock inmovilizado para mostrar en el dashboard.

        Returns:
            Dict con totales de productos, unidades y valor inmovilizado
        """
        product_costs = self._get_product_costs()

        total_productos = 0
        total_unidades = 0
        total_valor = 0

        for sl in stock_levels:
            if sl.estado == 'excedente' and sl.stock_maximo > 0:
                unidades_excedentes = sl.stock_actual - sl.stock_maximo
                if unidades_excedentes > 0:
                    costo_unitario = product_costs.get(sl.product_id, 0)
                    total_productos += 1
                    total_unidades += unidades_excedentes
                    total_valor += unidades_excedentes * costo_unitario

        return {
            'total_productos': total_productos,
            'total_unidades': int(round(total_unidades)),
            'valor_total': round(total_valor, 2)
        }
