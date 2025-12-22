"""
Servicio de Distribución
Genera propuestas de transferencia desde el depósito central (Ruta 9) hacia sucursales.

Lógica:
1. Ruta 9 solo puede distribuir stock por encima de su mínimo
2. Si Ruta 9 tiene stock <= mínimo, el producto va a lista de compras
3. Las sucursales reciben hasta alcanzar el nivel objetivo (mínimo/ideal/máximo)
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from sqlalchemy.orm import Session
from sqlalchemy import text
import pandas as pd
from pathlib import Path

from app.core.config import settings
from app.services.stock_calculator import StockCalculator, StockLevel

logger = logging.getLogger(__name__)


@dataclass
class TransferProposal:
    """Propuesta de transferencia de un producto entre depósitos"""
    product_id: int
    cod_item: str
    producto_nombre: str
    marca: str
    rubro: str
    deposit_origen_id: int
    deposit_origen_nombre: str
    deposit_destino_id: int
    deposit_destino_nombre: str
    cantidad_transferir: int
    stock_origen_antes: float
    stock_origen_despues: float
    stock_destino_antes: float
    stock_destino_despues: float
    stock_minimo_destino: float
    stock_ideal_destino: float
    stock_objetivo_destino: float

    def to_dict(self) -> Dict:
        return {
            'product_id': self.product_id,
            'cod_item': self.cod_item,
            'producto_nombre': self.producto_nombre,
            'marca': self.marca,
            'rubro': self.rubro,
            'deposit_origen_id': self.deposit_origen_id,
            'deposit_origen_nombre': self.deposit_origen_nombre,
            'deposit_destino_id': self.deposit_destino_id,
            'deposit_destino_nombre': self.deposit_destino_nombre,
            'cantidad_transferir': int(round(self.cantidad_transferir)),
            'stock_origen_antes': int(round(self.stock_origen_antes)),
            'stock_origen_despues': int(round(self.stock_origen_despues)),
            'stock_destino_antes': int(round(self.stock_destino_antes)),
            'stock_destino_despues': int(round(self.stock_destino_despues)),
            'stock_minimo_destino': int(round(self.stock_minimo_destino)),
            'stock_ideal_destino': int(round(self.stock_ideal_destino)),
            'stock_objetivo_destino': int(round(self.stock_objetivo_destino))
        }


@dataclass
class PurchaseNeed:
    """Necesidad de compra cuando el central no puede cubrir"""
    product_id: int
    cod_item: str
    producto_nombre: str
    marca: str
    rubro: str
    subrubro: str
    deposit_destino_id: int
    deposit_destino_nombre: str
    cantidad_necesaria: int
    stock_actual_destino: float
    stock_objetivo_destino: float
    stock_central_actual: float
    stock_central_minimo: float
    costo_unitario: float
    costo_total: float
    origen_necesidad: str  # "Central sin stock" o "Sucursal sin cobertura"

    def to_dict(self) -> Dict:
        return {
            'product_id': self.product_id,
            'cod_item': self.cod_item,
            'producto_nombre': self.producto_nombre,
            'marca': self.marca,
            'rubro': self.rubro,
            'subrubro': self.subrubro,
            'deposit_destino_id': self.deposit_destino_id,
            'deposit_destino_nombre': self.deposit_destino_nombre,
            'cantidad_necesaria': int(round(self.cantidad_necesaria)),
            'stock_actual_destino': int(round(self.stock_actual_destino)),
            'stock_objetivo_destino': int(round(self.stock_objetivo_destino)),
            'stock_central_actual': int(round(self.stock_central_actual)),
            'stock_central_minimo': int(round(self.stock_central_minimo)),
            'costo_unitario': round(self.costo_unitario, 2),
            'costo_total': round(self.costo_total, 2),
            'origen_necesidad': self.origen_necesidad
        }


@dataclass
class DistributionResult:
    """Resultado completo del proceso de distribución"""
    transfers: List[TransferProposal] = field(default_factory=list)
    purchase_needs: List[PurchaseNeed] = field(default_factory=list)
    summary: Dict = field(default_factory=dict)


class DistributionService:
    """
    Genera propuestas de distribución desde el depósito central hacia sucursales.
    """

    CENTRAL_DEPOSIT_NAME = "DEPOSITO RUTA 9"  # Nombre del depósito central

    def __init__(self, db: Session):
        self.db = db
        self.stock_calculator = StockCalculator(db)

    def generate_distribution(
        self,
        stock_levels: List[StockLevel],
        target_level: str = 'ideal',  # 'minimo', 'ideal', 'maximo'
        excluded_deposits: Optional[List[str]] = None,
        excluded_brands: Optional[List[str]] = None
    ) -> DistributionResult:
        """
        Genera propuestas de distribución y necesidades de compra.

        Args:
            stock_levels: Lista de niveles de stock calculados
            target_level: Nivel objetivo ('minimo', 'ideal', 'maximo')
            excluded_deposits: Depósitos a excluir
            excluded_brands: Marcas a excluir

        Returns:
            DistributionResult con transferencias y necesidades de compra
        """
        excluded_deposits = excluded_deposits or []
        excluded_brands = excluded_brands or []

        # Filtrar niveles de stock según exclusiones
        filtered_levels = [
            sl for sl in stock_levels
            if sl.deposito_nombre not in excluded_deposits
            and sl.marca not in excluded_brands
        ]

        # Organizar por producto
        products_by_id = self._group_by_product(filtered_levels)

        transfers = []
        purchase_needs = []

        # Obtener costos de productos
        product_costs = self._get_product_costs()

        for product_id, deposits_data in products_by_id.items():
            # Buscar datos del depósito central
            central_data = None
            sucursales_data = []

            for deposit_name, stock_level in deposits_data.items():
                if deposit_name == self.CENTRAL_DEPOSIT_NAME:
                    central_data = stock_level
                else:
                    sucursales_data.append(stock_level)

            if not central_data:
                # No hay datos del central para este producto
                continue

            # Calcular disponible en central (sobre su mínimo)
            disponible_central = max(0, central_data.stock_actual - central_data.stock_minimo)

            # Procesar cada sucursal que necesita stock
            for sucursal in sucursales_data:
                # Determinar stock objetivo según el nivel seleccionado
                if target_level == 'minimo':
                    stock_objetivo = sucursal.stock_minimo
                elif target_level == 'maximo':
                    stock_objetivo = sucursal.stock_maximo
                else:  # ideal
                    stock_objetivo = sucursal.stock_ideal

                # Calcular faltante
                faltante = stock_objetivo - sucursal.stock_actual

                if faltante <= 0:
                    # No necesita reposición
                    continue

                # Redondear a entero
                faltante_int = int(round(faltante))

                if faltante_int <= 0:
                    continue

                # Verificar si el central puede cubrir
                if disponible_central >= faltante_int:
                    # El central puede cubrir completamente
                    cantidad_transferir = faltante_int
                    disponible_central -= cantidad_transferir

                    transfers.append(TransferProposal(
                        product_id=product_id,
                        cod_item=central_data.cod_item,
                        producto_nombre=central_data.producto_nombre,
                        marca=central_data.marca,
                        rubro=central_data.rubro,
                        deposit_origen_id=central_data.deposit_id,
                        deposit_origen_nombre=central_data.deposito_nombre,
                        deposit_destino_id=sucursal.deposit_id,
                        deposit_destino_nombre=sucursal.deposito_nombre,
                        cantidad_transferir=cantidad_transferir,
                        stock_origen_antes=central_data.stock_actual,
                        stock_origen_despues=central_data.stock_actual - cantidad_transferir,
                        stock_destino_antes=sucursal.stock_actual,
                        stock_destino_despues=sucursal.stock_actual + cantidad_transferir,
                        stock_minimo_destino=sucursal.stock_minimo,
                        stock_ideal_destino=sucursal.stock_ideal,
                        stock_objetivo_destino=stock_objetivo
                    ))

                elif disponible_central > 0:
                    # El central puede cubrir parcialmente
                    cantidad_transferir = int(disponible_central)
                    faltante_restante = faltante_int - cantidad_transferir
                    disponible_central = 0

                    # Transferencia parcial
                    transfers.append(TransferProposal(
                        product_id=product_id,
                        cod_item=central_data.cod_item,
                        producto_nombre=central_data.producto_nombre,
                        marca=central_data.marca,
                        rubro=central_data.rubro,
                        deposit_origen_id=central_data.deposit_id,
                        deposit_origen_nombre=central_data.deposito_nombre,
                        deposit_destino_id=sucursal.deposit_id,
                        deposit_destino_nombre=sucursal.deposito_nombre,
                        cantidad_transferir=cantidad_transferir,
                        stock_origen_antes=central_data.stock_actual,
                        stock_origen_despues=central_data.stock_minimo,
                        stock_destino_antes=sucursal.stock_actual,
                        stock_destino_despues=sucursal.stock_actual + cantidad_transferir,
                        stock_minimo_destino=sucursal.stock_minimo,
                        stock_ideal_destino=sucursal.stock_ideal,
                        stock_objetivo_destino=stock_objetivo
                    ))

                    # Agregar el restante a compras
                    costo = product_costs.get(product_id, 0)
                    purchase_needs.append(PurchaseNeed(
                        product_id=product_id,
                        cod_item=sucursal.cod_item,
                        producto_nombre=sucursal.producto_nombre,
                        marca=sucursal.marca,
                        rubro=sucursal.rubro,
                        subrubro=sucursal.subrubro,
                        deposit_destino_id=sucursal.deposit_id,
                        deposit_destino_nombre=sucursal.deposito_nombre,
                        cantidad_necesaria=faltante_restante,
                        stock_actual_destino=sucursal.stock_actual + cantidad_transferir,
                        stock_objetivo_destino=stock_objetivo,
                        stock_central_actual=central_data.stock_actual,
                        stock_central_minimo=central_data.stock_minimo,
                        costo_unitario=costo,
                        costo_total=costo * faltante_restante,
                        origen_necesidad="Sucursal sin cobertura"
                    ))

                else:
                    # El central no tiene disponible
                    costo = product_costs.get(product_id, 0)
                    purchase_needs.append(PurchaseNeed(
                        product_id=product_id,
                        cod_item=sucursal.cod_item,
                        producto_nombre=sucursal.producto_nombre,
                        marca=sucursal.marca,
                        rubro=sucursal.rubro,
                        subrubro=sucursal.subrubro,
                        deposit_destino_id=sucursal.deposit_id,
                        deposit_destino_nombre=sucursal.deposito_nombre,
                        cantidad_necesaria=faltante_int,
                        stock_actual_destino=sucursal.stock_actual,
                        stock_objetivo_destino=stock_objetivo,
                        stock_central_actual=central_data.stock_actual,
                        stock_central_minimo=central_data.stock_minimo,
                        costo_unitario=costo,
                        costo_total=costo * faltante_int,
                        origen_necesidad="Central sin stock"
                    ))

            # Verificar si el central mismo está bajo mínimo
            if central_data.stock_actual < central_data.stock_minimo:
                # Determinar stock objetivo para el central
                if target_level == 'minimo':
                    stock_objetivo_central = central_data.stock_minimo
                elif target_level == 'maximo':
                    stock_objetivo_central = central_data.stock_maximo
                else:
                    stock_objetivo_central = central_data.stock_ideal

                faltante_central = stock_objetivo_central - central_data.stock_actual
                faltante_central_int = int(round(faltante_central))

                if faltante_central_int > 0:
                    costo = product_costs.get(product_id, 0)
                    purchase_needs.append(PurchaseNeed(
                        product_id=product_id,
                        cod_item=central_data.cod_item,
                        producto_nombre=central_data.producto_nombre,
                        marca=central_data.marca,
                        rubro=central_data.rubro,
                        subrubro=central_data.subrubro,
                        deposit_destino_id=central_data.deposit_id,
                        deposit_destino_nombre=central_data.deposito_nombre,
                        cantidad_necesaria=faltante_central_int,
                        stock_actual_destino=central_data.stock_actual,
                        stock_objetivo_destino=stock_objetivo_central,
                        stock_central_actual=central_data.stock_actual,
                        stock_central_minimo=central_data.stock_minimo,
                        costo_unitario=costo,
                        costo_total=costo * faltante_central_int,
                        origen_necesidad="Central bajo mínimo"
                    ))

        # Generar resumen
        summary = {
            'total_transfers': len(transfers),
            'total_purchase_needs': len(purchase_needs),
            'total_units_to_transfer': sum(t.cantidad_transferir for t in transfers),
            'total_units_to_purchase': sum(p.cantidad_necesaria for p in purchase_needs),
            'total_cost_purchases': sum(p.costo_total for p in purchase_needs),
            'target_level': target_level,
            'generated_at': datetime.now().isoformat()
        }

        logger.info(f"Distribución generada: {summary['total_transfers']} transferencias, "
                   f"{summary['total_purchase_needs']} necesidades de compra")

        return DistributionResult(
            transfers=transfers,
            purchase_needs=purchase_needs,
            summary=summary
        )

    def _group_by_product(self, stock_levels: List[StockLevel]) -> Dict[int, Dict[str, StockLevel]]:
        """Agrupa los niveles de stock por producto"""
        grouped = {}
        for sl in stock_levels:
            if sl.product_id not in grouped:
                grouped[sl.product_id] = {}
            grouped[sl.product_id][sl.deposito_nombre] = sl
        return grouped

    def _get_product_costs(self) -> Dict[int, float]:
        """Obtiene los costos de los productos"""
        result = self.db.execute(text("""
            SELECT id, COALESCE(costo, 0) as costo FROM products
        """))
        return {row[0]: float(row[1]) for row in result}

    def export_distribution_excel(
        self,
        result: DistributionResult,
        output_path: Optional[str] = None
    ) -> str:
        """
        Exporta las propuestas de distribución a Excel.

        Args:
            result: Resultado de la distribución
            output_path: Ruta opcional para el archivo

        Returns:
            Ruta del archivo generado
        """
        if not output_path:
            exports_dir = Path("exports")
            exports_dir.mkdir(exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = str(exports_dir / f"distribucion_{timestamp}.xlsx")

        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            workbook = writer.book

            # Formato de encabezado
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#4472C4',
                'font_color': 'white',
                'border': 1,
                'align': 'center'
            })

            # Formato numérico
            number_format = workbook.add_format({'num_format': '#,##0'})
            currency_format = workbook.add_format({'num_format': '$#,##0.00'})

            # Hoja de Transferencias
            if result.transfers:
                transfers_data = [t.to_dict() for t in result.transfers]
                df_transfers = pd.DataFrame(transfers_data)
                df_transfers = df_transfers[[
                    'cod_item', 'producto_nombre', 'marca', 'rubro',
                    'deposit_origen_nombre', 'deposit_destino_nombre',
                    'cantidad_transferir', 'stock_origen_antes', 'stock_origen_despues',
                    'stock_destino_antes', 'stock_destino_despues',
                    'stock_minimo_destino', 'stock_ideal_destino'
                ]]
                df_transfers.columns = [
                    'Código', 'Producto', 'Marca', 'Rubro',
                    'Desde (Origen)', 'Hacia (Destino)',
                    'Cantidad', 'Stock Origen Antes', 'Stock Origen Después',
                    'Stock Destino Antes', 'Stock Destino Después',
                    'Stock Mín. Destino', 'Stock Ideal Destino'
                ]
                df_transfers.to_excel(writer, sheet_name='Transferencias', index=False, startrow=1, header=False)

                worksheet = writer.sheets['Transferencias']
                for col_num, value in enumerate(df_transfers.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                worksheet.set_column('A:A', 12)
                worksheet.set_column('B:B', 40)
                worksheet.set_column('C:F', 20)
                worksheet.set_column('G:M', 15)

            # Hoja de Resumen
            summary_df = pd.DataFrame([{
                'Métrica': 'Total Transferencias',
                'Valor': result.summary.get('total_transfers', 0)
            }, {
                'Métrica': 'Total Unidades a Transferir',
                'Valor': result.summary.get('total_units_to_transfer', 0)
            }, {
                'Métrica': 'Total Necesidades de Compra',
                'Valor': result.summary.get('total_purchase_needs', 0)
            }, {
                'Métrica': 'Total Unidades a Comprar',
                'Valor': result.summary.get('total_units_to_purchase', 0)
            }, {
                'Métrica': 'Costo Total Estimado Compras',
                'Valor': f"${result.summary.get('total_cost_purchases', 0):,.2f}"
            }, {
                'Métrica': 'Nivel Objetivo',
                'Valor': result.summary.get('target_level', 'ideal').capitalize()
            }, {
                'Métrica': 'Generado',
                'Valor': result.summary.get('generated_at', '')
            }])
            summary_df.to_excel(writer, sheet_name='Resumen', index=False, startrow=1, header=False)

            worksheet = writer.sheets['Resumen']
            for col_num, value in enumerate(summary_df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            worksheet.set_column('A:A', 30)
            worksheet.set_column('B:B', 25)

        logger.info(f"Excel de distribución exportado: {output_path}")
        return output_path

    def get_redistribution_opportunities(
        self,
        stock_levels: List[StockLevel]
    ) -> List[Dict]:
        """
        Identifica oportunidades de redistribución desde sucursales con excedente.

        Args:
            stock_levels: Lista de niveles de stock

        Returns:
            Lista de oportunidades de redistribución
        """
        opportunities = []

        # Agrupar por producto
        products_by_id = self._group_by_product(stock_levels)

        for product_id, deposits_data in products_by_id.items():
            # Buscar depósitos con excedente y con faltante
            excedentes = []
            faltantes = []

            for deposit_name, sl in deposits_data.items():
                if sl.stock_actual > sl.stock_maximo:
                    excedente = sl.stock_actual - sl.stock_ideal
                    excedentes.append({
                        'deposito': deposit_name,
                        'deposit_id': sl.deposit_id,
                        'excedente': excedente,
                        'stock_level': sl
                    })
                elif sl.stock_actual < sl.stock_minimo:
                    faltante = sl.stock_ideal - sl.stock_actual
                    faltantes.append({
                        'deposito': deposit_name,
                        'deposit_id': sl.deposit_id,
                        'faltante': faltante,
                        'stock_level': sl
                    })

            # Si hay excedente y faltante, crear oportunidad
            if excedentes and faltantes:
                for exc in excedentes:
                    for fal in faltantes:
                        cantidad_posible = min(exc['excedente'], fal['faltante'])
                        if cantidad_posible >= 1:
                            opportunities.append({
                                'product_id': product_id,
                                'cod_item': exc['stock_level'].cod_item,
                                'producto': exc['stock_level'].producto_nombre,
                                'desde': exc['deposito'],
                                'hacia': fal['deposito'],
                                'cantidad_sugerida': int(cantidad_posible),
                                'excedente_origen': round(exc['excedente'], 2),
                                'faltante_destino': round(fal['faltante'], 2)
                            })

        logger.info(f"Encontradas {len(opportunities)} oportunidades de redistribución")
        return opportunities

    def generate_excess_redistribution(
        self,
        stock_levels: List[StockLevel],
        target_level: str = 'ideal',
        excluded_deposits: Optional[List[str]] = None
    ) -> DistributionResult:
        """
        Genera propuestas de redistribución desde sucursales con EXCEDENTE
        hacia sucursales con FALTANTE (por debajo del stock ideal).

        A diferencia de generate_distribution() que solo mueve desde el central,
        este método redistribuye entre CUALQUIER sucursal con excedente hacia
        cualquier sucursal con faltante.

        Args:
            stock_levels: Lista de niveles de stock calculados
            target_level: Nivel objetivo ('minimo', 'ideal', 'maximo')
            excluded_deposits: Depósitos a excluir

        Returns:
            DistributionResult con transferencias de redistribución
        """
        excluded_deposits = excluded_deposits or []

        # Filtrar niveles de stock
        filtered_levels = [
            sl for sl in stock_levels
            if sl.deposito_nombre not in excluded_deposits
        ]

        # Agrupar por producto
        products_by_id = self._group_by_product(filtered_levels)

        transfers = []

        for product_id, deposits_data in products_by_id.items():
            # Identificar depósitos con excedente (stock > máximo)
            excedentes = []
            # Identificar depósitos con faltante (stock < ideal)
            faltantes = []

            for deposit_name, sl in deposits_data.items():
                if sl.stock_actual > sl.stock_maximo and sl.stock_maximo > 0:
                    # Excedente = lo que sobra sobre el stock ideal (para mantener un nivel razonable)
                    excedente_disponible = sl.stock_actual - sl.stock_ideal
                    if excedente_disponible >= 1:
                        excedentes.append({
                            'deposito': deposit_name,
                            'stock_level': sl,
                            'excedente_disponible': excedente_disponible
                        })

                # Determinar stock objetivo según el nivel seleccionado
                if target_level == 'minimo':
                    stock_objetivo = sl.stock_minimo
                elif target_level == 'maximo':
                    stock_objetivo = sl.stock_maximo
                else:  # ideal
                    stock_objetivo = sl.stock_ideal

                # Faltante = cuánto necesita para llegar al objetivo
                faltante = stock_objetivo - sl.stock_actual
                if faltante >= 1 and sl.stock_actual < sl.stock_ideal:
                    faltantes.append({
                        'deposito': deposit_name,
                        'stock_level': sl,
                        'faltante': faltante,
                        'stock_objetivo': stock_objetivo
                    })

            # Ordenar excedentes de mayor a menor
            excedentes.sort(key=lambda x: x['excedente_disponible'], reverse=True)
            # Ordenar faltantes de mayor a menor (priorizar los más urgentes)
            faltantes.sort(key=lambda x: x['faltante'], reverse=True)

            # Generar transferencias
            for exc in excedentes:
                if exc['excedente_disponible'] <= 0:
                    continue

                for fal in faltantes:
                    if fal['faltante'] <= 0:
                        continue
                    if exc['excedente_disponible'] <= 0:
                        break

                    # Calcular cantidad a transferir
                    cantidad = min(exc['excedente_disponible'], fal['faltante'])
                    cantidad_int = int(round(cantidad))

                    if cantidad_int < 1:
                        continue

                    # Crear propuesta de transferencia
                    sl_origen = exc['stock_level']
                    sl_destino = fal['stock_level']

                    transfers.append(TransferProposal(
                        product_id=product_id,
                        cod_item=sl_origen.cod_item,
                        producto_nombre=sl_origen.producto_nombre,
                        marca=sl_origen.marca,
                        rubro=sl_origen.rubro,
                        deposit_origen_id=sl_origen.deposit_id,
                        deposit_origen_nombre=sl_origen.deposito_nombre,
                        deposit_destino_id=sl_destino.deposit_id,
                        deposit_destino_nombre=sl_destino.deposito_nombre,
                        cantidad_transferir=cantidad_int,
                        stock_origen_antes=sl_origen.stock_actual,
                        stock_origen_despues=sl_origen.stock_actual - cantidad_int,
                        stock_destino_antes=sl_destino.stock_actual,
                        stock_destino_despues=sl_destino.stock_actual + cantidad_int,
                        stock_minimo_destino=sl_destino.stock_minimo,
                        stock_ideal_destino=sl_destino.stock_ideal,
                        stock_objetivo_destino=fal['stock_objetivo']
                    ))

                    # Actualizar disponibilidad
                    exc['excedente_disponible'] -= cantidad_int
                    fal['faltante'] -= cantidad_int

        # Generar resumen
        summary = {
            'total_transfers': len(transfers),
            'total_units_to_transfer': sum(t.cantidad_transferir for t in transfers),
            'unique_products': len(set(t.product_id for t in transfers)),
            'origen_deposits': len(set(t.deposit_origen_nombre for t in transfers)),
            'destino_deposits': len(set(t.deposit_destino_nombre for t in transfers)),
            'target_level': target_level,
            'generated_at': datetime.now().isoformat()
        }

        logger.info(f"Redistribución de excedentes generada: {summary['total_transfers']} transferencias, "
                   f"{summary['total_units_to_transfer']} unidades")

        return DistributionResult(
            transfers=transfers,
            purchase_needs=[],  # No hay necesidades de compra en redistribución
            summary=summary
        )

    def export_excess_redistribution_excel(
        self,
        result: DistributionResult,
        output_path: Optional[str] = None
    ) -> str:
        """
        Exporta las propuestas de redistribución de excedentes a Excel.

        Args:
            result: Resultado de la redistribución
            output_path: Ruta opcional para el archivo

        Returns:
            Ruta del archivo generado
        """
        if not output_path:
            exports_dir = Path("exports")
            exports_dir.mkdir(exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = str(exports_dir / f"redistribucion_excedentes_{timestamp}.xlsx")

        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            workbook = writer.book

            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#F59E0B',  # Naranja/warning
                'font_color': 'white',
                'border': 1,
                'align': 'center'
            })

            number_format = workbook.add_format({'num_format': '#,##0'})

            # Hoja de Redistribuciones
            if result.transfers:
                data = []
                for t in result.transfers:
                    data.append({
                        'Código': t.cod_item,
                        'Producto': t.producto_nombre,
                        'Marca': t.marca,
                        'Rubro': t.rubro,
                        'Desde (Sucursal)': t.deposit_origen_nombre,
                        'Hacia (Sucursal)': t.deposit_destino_nombre,
                        'Cantidad': int(round(t.cantidad_transferir)),
                        'Stock Origen Antes': int(round(t.stock_origen_antes)),
                        'Stock Origen Después': int(round(t.stock_origen_despues)),
                        'Stock Destino Antes': int(round(t.stock_destino_antes)),
                        'Stock Destino Después': int(round(t.stock_destino_despues)),
                        'Stock Ideal Destino': int(round(t.stock_ideal_destino))
                    })

                df = pd.DataFrame(data)
                df.to_excel(writer, sheet_name='Redistribución', index=False, startrow=1, header=False)

                worksheet = writer.sheets['Redistribución']
                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(0, col_num, value, header_format)

                worksheet.set_column('A:A', 12)  # Código
                worksheet.set_column('B:B', 45)  # Producto
                worksheet.set_column('C:D', 18)  # Marca, Rubro
                worksheet.set_column('E:F', 22)  # Desde, Hacia
                worksheet.set_column('G:L', 16, number_format)  # Cantidades

            else:
                df = pd.DataFrame([{'Mensaje': 'No hay redistribuciones sugeridas'}])
                df.to_excel(writer, sheet_name='Redistribución', index=False)

            # Hoja de Resumen
            summary_data = [{
                'Métrica': 'Total Transferencias Propuestas',
                'Valor': result.summary.get('total_transfers', 0)
            }, {
                'Métrica': 'Total Unidades a Redistribuir',
                'Valor': result.summary.get('total_units_to_transfer', 0)
            }, {
                'Métrica': 'Productos Únicos',
                'Valor': result.summary.get('unique_products', 0)
            }, {
                'Métrica': 'Sucursales Origen (con excedente)',
                'Valor': result.summary.get('origen_deposits', 0)
            }, {
                'Métrica': 'Sucursales Destino (con faltante)',
                'Valor': result.summary.get('destino_deposits', 0)
            }, {
                'Métrica': 'Nivel Objetivo',
                'Valor': result.summary.get('target_level', 'ideal').capitalize()
            }, {
                'Métrica': 'Generado',
                'Valor': result.summary.get('generated_at', '')
            }]

            df_summary = pd.DataFrame(summary_data)
            df_summary.to_excel(writer, sheet_name='Resumen', index=False, startrow=1, header=False)

            ws_summary = writer.sheets['Resumen']
            for col_num, value in enumerate(df_summary.columns.values):
                ws_summary.write(0, col_num, value, header_format)
            ws_summary.set_column('A:A', 35)
            ws_summary.set_column('B:B', 25)

            # Hoja por Sucursal Origen (agrupado)
            if result.transfers:
                by_origen = {}
                for t in result.transfers:
                    if t.deposit_origen_nombre not in by_origen:
                        by_origen[t.deposit_origen_nombre] = {
                            'total_transferencias': 0,
                            'total_unidades': 0,
                            'destinos': set()
                        }
                    by_origen[t.deposit_origen_nombre]['total_transferencias'] += 1
                    by_origen[t.deposit_origen_nombre]['total_unidades'] += t.cantidad_transferir
                    by_origen[t.deposit_origen_nombre]['destinos'].add(t.deposit_destino_nombre)

                origen_data = []
                for deposito, stats in sorted(by_origen.items()):
                    origen_data.append({
                        'Sucursal con Excedente': deposito,
                        'Total Transferencias': stats['total_transferencias'],
                        'Total Unidades': int(stats['total_unidades']),
                        'Destinos Diferentes': len(stats['destinos'])
                    })

                df_origen = pd.DataFrame(origen_data)
                df_origen.to_excel(writer, sheet_name='Por Sucursal Origen', index=False, startrow=1, header=False)

                ws_origen = writer.sheets['Por Sucursal Origen']
                for col_num, value in enumerate(df_origen.columns.values):
                    ws_origen.write(0, col_num, value, header_format)
                ws_origen.set_column('A:A', 30)
                ws_origen.set_column('B:D', 20)

        logger.info(f"Excel de redistribución de excedentes exportado: {output_path}")
        return output_path
