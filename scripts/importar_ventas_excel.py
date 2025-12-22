"""
Script para importar ventas desde Excel a la base de datos.
Archivo: utilidades/ventas de junio 2025 a 10-12-2025.xlsx

Este script:
1. Lee el archivo Excel con ventas por comprobante
2. Agrupa las ventas por producto-deposito-fecha
3. Inserta los registros en la tabla sales_history
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import logging
from datetime import datetime
from decimal import Decimal
from sqlalchemy import text

from app.core.database import SessionLocal

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Mapeo de nombres de sucursal del Excel a deposit_id en la BD
# Basado en la estructura de la BD existente
SUCURSAL_TO_DEPOSIT = {
    'SUCURSAL  ALEM': 17,                              # DEPOSITO ALEM
    'SUCURSAL ALEM': 17,                               # variante
    'SUCURSAL LAPRIDA': 27,                            # DEPOSITO LAPRIDA
    'SUCURSAL BELGRANO': 18,                           # DEPOSITO BELGRANO
    'SUCURSAL PARQUE': 28,                             # DEPOSITO PARQUE
    'SUCURSAL CONGRESO': 19,                           # DEPOSITO CONGRESO
    'SUCURSAL MUÑECAS': 20,                            # DEPOSITO MUÑECAS
    'SUCURSAL MU�ECAS': 20,                            # con encoding issue
    'SUCURSAL BANDA': 26,                              # DEPOSITO BANDA
    'SUCURSAL CATAMARCA': 24,                          # DEPOSITO CATAMARCA
    'SUCURSAL REYES CATOLICOS': 29,                    # DEPOSITO REYES CATOLICOS
    'SUCURSAL ARENALES': 23,                           # DEPOSITO ARENALES
    'SUCURSAL LEGUIZAMON': 32,                         # DEPOSITO LEGUIZAMON
    'SUCURSAL BELGRANO SUR': 22,                       # DEPOSITO BELGRANO SUR
    'SUCURSAL NEUQUEN OLASCOAGA': 34,                  # DEPOSITO OLASCOAGA
    'SUCURSAL CONCEPCION': 25,                         # DEPOSITO CONCEPCION
    'DEPOSITO RUTA 9': 16,                             # DEPOSITO RUTA 9
    'PETS PLUS MIGUEL LILLO': 30,                      # DEPOSITO PETS PLUS MIGUEL LILLO
    'SUCURSAL PINAR I': 31,                            # DEPOSITO PINAR
    'SUCURSAL PINAR': 31,                              # variante
    # Sucursales que no tienen deposito directo - se asignan al mas cercano o se excluyen
    'SUCURSAL TESORERIA CENTRAL / VENTA INTERNA': 16,  # Se asigna a RUTA 9 (central)
    'SUCURSAL CONTACT CENTER': 16,                     # Se asigna a RUTA 9 (ventas online)
    'SUCURSAL YERBA BUENA': None,                      # No hay deposito - EXCLUIR
    'PETS PLUS CONCEPCION': 25,                        # Asignar a CONCEPCION
    'SUCURSAL NEUQUEN ALCORTA': 34,                    # Asignar a OLASCOAGA (Neuquen)
    'SUCURSAL SOLIS': None,                            # No hay deposito - EXCLUIR
    'STUDIO KAI': None,                                # No hay deposito - EXCLUIR
    'PETS PLUS AGUAS BLANCAS': None,                   # No hay deposito - EXCLUIR
    'PETS PLUS NEUQUEN': 34,                           # Asignar a OLASCOAGA (Neuquen)
}


def importar_ventas_desde_excel(excel_path: str, dry_run: bool = False, clear_existing: bool = False):
    """
    Importa ventas desde el Excel a la tabla sales_history.

    Args:
        excel_path: Ruta al archivo Excel
        dry_run: Si es True, solo muestra lo que haria sin hacer cambios
        clear_existing: Si es True, borra las ventas existentes en el rango de fechas
    """
    logger.info(f"Leyendo archivo Excel: {excel_path}")

    # Leer Excel
    df = pd.read_excel(excel_path, sheet_name=0)

    # Normalizar nombres de columnas
    df.columns = [c.replace('Código', 'Codigo').replace('Teléfono', 'Telefono').replace('Dirección', 'Direccion') for c in df.columns]

    logger.info(f"Excel tiene {len(df)} registros de ventas")
    logger.info(f"Rango de fechas: {df['Fecha Comp'].min()} a {df['Fecha Comp'].max()}")

    # Filtrar registros con datos validos
    df = df[df['Codigo Producto'].notna()]
    df = df[df['Cantidad'].notna()]
    df = df[df['Cantidad'] > 0]  # Solo ventas positivas

    logger.info(f"Registros validos (con codigo y cantidad > 0): {len(df)}")

    # Estadisticas de sucursales
    logger.info("\n=== SUCURSALES EN EL ARCHIVO ===")
    for sucursal in df['Sucursal'].unique():
        deposit_id = SUCURSAL_TO_DEPOSIT.get(sucursal)
        status = f"-> Deposito {deposit_id}" if deposit_id else "-> EXCLUIDO (sin deposito)"
        count = len(df[df['Sucursal'] == sucursal])
        logger.info(f"  {sucursal}: {count} registros {status}")

    if dry_run:
        logger.info("\n=== DRY RUN - No se harán cambios ===")
        # Mostrar muestra de datos
        logger.info("\nMuestra de datos a importar:")
        sample = df.head(10)
        for _, row in sample.iterrows():
            logger.info(f"  {row['Fecha Comp'].date()} | {row['Sucursal']} | {row['Codigo Producto']} | Cant: {row['Cantidad']} | Total: {row['Total']}")
        return

    # Conectar a la BD
    db = SessionLocal()

    try:
        # Obtener mapeo de cod_item a product_id
        logger.info("\nObteniendo mapeo de productos...")
        result = db.execute(text("SELECT id, cod_item FROM products"))
        product_map = {row[1].strip(): row[0] for row in result}
        logger.info(f"Se encontraron {len(product_map)} productos en la BD")

        # Obtener rango de fechas para limpieza
        fecha_min = df['Fecha Comp'].min()
        fecha_max = df['Fecha Comp'].max()

        if clear_existing:
            logger.info(f"\nBorrando ventas existentes entre {fecha_min} y {fecha_max}...")
            result = db.execute(text("""
                DELETE FROM sales_history
                WHERE fecha >= :fecha_min AND fecha <= :fecha_max
            """), {"fecha_min": fecha_min, "fecha_max": fecha_max})
            deleted_count = result.rowcount
            logger.info(f"Se borraron {deleted_count} registros existentes")
            db.commit()

        # Contadores
        insertados = 0
        no_encontrados_producto = 0
        no_encontrados_deposito = 0
        errores = 0
        productos_no_encontrados = set()

        # Procesar cada registro
        total_rows = len(df)
        logger.info(f"\nProcesando {total_rows} registros de ventas...")

        for idx, row in df.iterrows():
            if idx % 5000 == 0 and idx > 0:
                logger.info(f"  Procesados {idx}/{total_rows} registros...")
                db.commit()

            try:
                # Obtener datos
                sucursal = row['Sucursal']
                cod_producto = str(row['Codigo Producto']).strip()
                cantidad = float(row['Cantidad'])
                total = float(row['Total']) if pd.notna(row['Total']) else 0
                fecha = row['Fecha Comp']

                # Mapear sucursal a deposito
                deposit_id = SUCURSAL_TO_DEPOSIT.get(sucursal)
                if not deposit_id:
                    no_encontrados_deposito += 1
                    continue

                # Buscar product_id
                product_id = product_map.get(cod_producto)
                if not product_id:
                    no_encontrados_producto += 1
                    productos_no_encontrados.add(cod_producto)
                    continue

                # Insertar registro
                db.execute(text("""
                    INSERT INTO sales_history (product_id, deposit_id, fecha, cantidad, monto, created_at)
                    VALUES (:product_id, :deposit_id, :fecha, :cantidad, :monto, :created_at)
                """), {
                    "product_id": product_id,
                    "deposit_id": deposit_id,
                    "fecha": fecha,
                    "cantidad": Decimal(str(cantidad)),
                    "monto": Decimal(str(total)),
                    "created_at": datetime.now()
                })
                insertados += 1

            except Exception as e:
                errores += 1
                if errores <= 10:
                    logger.error(f"Error procesando fila {idx}: {e}")

        # Commit final
        db.commit()

        logger.info("\n" + "=" * 70)
        logger.info("IMPORTACION DE VENTAS COMPLETADA")
        logger.info("=" * 70)
        logger.info(f"Registros insertados:           {insertados}")
        logger.info(f"Productos no encontrados en BD: {no_encontrados_producto}")
        logger.info(f"Sucursales sin deposito:        {no_encontrados_deposito}")
        logger.info(f"Errores:                        {errores}")
        logger.info("=" * 70)

        if productos_no_encontrados and len(productos_no_encontrados) <= 20:
            logger.info(f"\nCodigos de productos no encontrados: {sorted(productos_no_encontrados)}")
        elif productos_no_encontrados:
            logger.info(f"\nTotal de codigos de productos no encontrados: {len(productos_no_encontrados)}")

    except Exception as e:
        logger.error(f"Error durante la importacion: {e}")
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Importar ventas desde Excel')
    parser.add_argument('--dry-run', action='store_true',
                        help='Solo muestra lo que haria sin hacer cambios')
    parser.add_argument('--clear-existing', action='store_true',
                        help='Borra las ventas existentes en el rango de fechas antes de importar')
    parser.add_argument('--file', type=str,
                        default='utilidades/ventas de junio 2025 a  10-12-2025.xlsx',
                        help='Ruta al archivo Excel')

    args = parser.parse_args()

    # Construir ruta completa
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    excel_path = os.path.join(base_dir, args.file)

    if not os.path.exists(excel_path):
        logger.error(f"Archivo no encontrado: {excel_path}")
        sys.exit(1)

    importar_ventas_desde_excel(excel_path, dry_run=args.dry_run, clear_existing=args.clear_existing)
