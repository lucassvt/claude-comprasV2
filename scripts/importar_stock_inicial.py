"""
Script para importar valores iniciales de stock mínimo e ideal desde Excel.
Archivo: utilidades/stock minimo e ideal por deposito.xlsx

Este script carga los valores del Excel a la tabla depot_config.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import text

from app.core.database import SessionLocal

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Mapeo de nombres de columnas del Excel a IDs de depósitos en la BD
# Excel usa nombres cortos, BD usa "DEPOSITO NOMBRE"
EXCEL_TO_DEPOSIT_ID = {
    'ALEM': 17,
    'BELGRANO': 18,
    'CONGRESO': 19,
    'MUÑECAS': 20,
    'MU�ECAS': 20,  # Para encoding issues
    'PERON': 21,
    'BELGRANO SUR': 22,
    'ARENALES': 23,
    'CATAMARCA': 24,
    'CONCEPCION': 25,
    'BANDA': 26,
    'LAPRIDA': 27,
    'RUTA 9': 16,
    'PARQUE': 28,
    'LEGUIZAMON': 32,
    'PINAR I': 31,
    'PINAR': 31,
}


def extraer_nombre_deposito(columna: str) -> str:
    """Extrae el nombre del depósito de la columna del Excel."""
    # Columnas tienen formato: " NOMBRE-MINIMO" o " NOMBRE-IDEAL"
    # Limpiar espacios y caracteres especiales
    col_clean = columna.strip().replace('\xa0', ' ')

    # Remover -MINIMO o -IDEAL
    if '-MINIMO' in col_clean:
        nombre = col_clean.replace('-MINIMO', '').strip()
    elif '-IDEAL' in col_clean:
        nombre = col_clean.replace('-IDEAL', '').strip()
    else:
        return None

    return nombre


def importar_stock_desde_excel(excel_path: str, dry_run: bool = False):
    """
    Importa los valores de stock mínimo e ideal desde el Excel.

    Args:
        excel_path: Ruta al archivo Excel
        dry_run: Si es True, solo muestra lo que haría sin hacer cambios
    """
    logger.info(f"Leyendo archivo Excel: {excel_path}")

    # Leer Excel
    df = pd.read_excel(excel_path, sheet_name='Mnimos')

    # Normalizar nombres de columnas (encoding)
    df.columns = [c.replace('Código', 'Codigo').replace('�', 'Ñ') for c in df.columns]

    logger.info(f"Excel tiene {len(df)} productos")
    logger.info(f"Columnas: {df.columns.tolist()[:10]}...")

    # Identificar columnas de MINIMO e IDEAL
    columnas_minimo = [c for c in df.columns if '-MINIMO' in c]
    columnas_ideal = [c for c in df.columns if '-IDEAL' in c]

    logger.info(f"Columnas MINIMO encontradas: {len(columnas_minimo)}")
    logger.info(f"Columnas IDEAL encontradas: {len(columnas_ideal)}")

    # Crear mapeo de columnas a deposit_id
    col_to_deposit_minimo = {}
    col_to_deposit_ideal = {}

    for col in columnas_minimo:
        nombre = extraer_nombre_deposito(col)
        if nombre and nombre in EXCEL_TO_DEPOSIT_ID:
            col_to_deposit_minimo[col] = EXCEL_TO_DEPOSIT_ID[nombre]
            logger.info(f"  MINIMO: '{col}' -> Depósito ID {EXCEL_TO_DEPOSIT_ID[nombre]}")
        else:
            logger.warning(f"  MINIMO: '{col}' -> No se encontró mapeo para '{nombre}'")

    for col in columnas_ideal:
        nombre = extraer_nombre_deposito(col)
        if nombre and nombre in EXCEL_TO_DEPOSIT_ID:
            col_to_deposit_ideal[col] = EXCEL_TO_DEPOSIT_ID[nombre]
        else:
            logger.warning(f"  IDEAL: '{col}' -> No se encontró mapeo para '{nombre}'")

    if dry_run:
        logger.info("\n=== DRY RUN - No se harán cambios ===\n")
        # Mostrar muestra de datos
        for idx, row in df.head(5).iterrows():
            cod_producto = str(row.iloc[0]).strip()
            logger.info(f"Producto: {cod_producto}")
            for col, dep_id in list(col_to_deposit_minimo.items())[:3]:
                val_min = row[col] if pd.notna(row[col]) else 0
                col_ideal = col.replace('-MINIMO', '-IDEAL')
                val_ideal = row[col_ideal] if col_ideal in df.columns and pd.notna(row[col_ideal]) else 0
                logger.info(f"  Dep {dep_id}: min={val_min}, ideal={val_ideal}")
        return

    # Conectar a la BD
    db = SessionLocal()

    try:
        # Obtener mapeo de cod_item a product_id
        logger.info("Obteniendo mapeo de productos...")
        result = db.execute(text("SELECT id, cod_item FROM products"))
        product_map = {row[1].strip(): row[0] for row in result}
        logger.info(f"Se encontraron {len(product_map)} productos en la BD")

        # Contadores
        insertados = 0
        actualizados = 0
        no_encontrados = 0
        errores = 0

        # Procesar cada producto
        total_rows = len(df)
        for idx, row in df.iterrows():
            if idx % 500 == 0:
                logger.info(f"Procesando producto {idx}/{total_rows}...")

            # Obtener código de producto (primera columna)
            cod_producto = str(row.iloc[0]).strip()

            # Buscar product_id
            if cod_producto not in product_map:
                no_encontrados += 1
                continue

            product_id = product_map[cod_producto]

            # Procesar cada depósito
            for col_minimo, deposit_id in col_to_deposit_minimo.items():
                try:
                    # Obtener valores
                    stock_minimo = float(row[col_minimo]) if pd.notna(row[col_minimo]) else 0

                    # Buscar columna ideal correspondiente
                    col_ideal = col_minimo.replace('-MINIMO', '-IDEAL')
                    stock_ideal = 0
                    if col_ideal in df.columns:
                        stock_ideal = float(row[col_ideal]) if pd.notna(row[col_ideal]) else 0

                    # Calcular stock máximo (2x ideal por defecto)
                    stock_maximo = stock_ideal * 2 if stock_ideal > 0 else stock_minimo * 4

                    # Verificar si ya existe registro
                    check_result = db.execute(text("""
                        SELECT id FROM depot_config
                        WHERE product_id = :product_id AND deposit_id = :deposit_id
                    """), {"product_id": product_id, "deposit_id": deposit_id})

                    existing = check_result.fetchone()

                    if existing:
                        # Actualizar
                        db.execute(text("""
                            UPDATE depot_config
                            SET stock_minimo = :stock_minimo,
                                stock_ideal = :stock_ideal,
                                stock_maximo = :stock_maximo,
                                updated_at = :updated_at
                            WHERE product_id = :product_id AND deposit_id = :deposit_id
                        """), {
                            "product_id": product_id,
                            "deposit_id": deposit_id,
                            "stock_minimo": stock_minimo,
                            "stock_ideal": stock_ideal,
                            "stock_maximo": stock_maximo,
                            "updated_at": datetime.now()
                        })
                        actualizados += 1
                    else:
                        # Insertar (sin created_at ya que la tabla no lo tiene)
                        db.execute(text("""
                            INSERT INTO depot_config
                            (product_id, deposit_id, stock_minimo, stock_ideal, stock_maximo, updated_at, activo)
                            VALUES (:product_id, :deposit_id, :stock_minimo, :stock_ideal, :stock_maximo, :updated_at, true)
                        """), {
                            "product_id": product_id,
                            "deposit_id": deposit_id,
                            "stock_minimo": stock_minimo,
                            "stock_ideal": stock_ideal,
                            "stock_maximo": stock_maximo,
                            "updated_at": datetime.now()
                        })
                        insertados += 1

                except Exception as e:
                    errores += 1
                    if errores <= 10:
                        logger.error(f"Error procesando {cod_producto} - Dep {deposit_id}: {e}")

            # Commit cada 1000 productos
            if idx % 1000 == 0 and idx > 0:
                db.commit()
                logger.info(f"Commit parcial - {insertados} insertados, {actualizados} actualizados")

        # Commit final
        db.commit()

        logger.info("\n" + "="*60)
        logger.info("IMPORTACIÓN COMPLETADA")
        logger.info("="*60)
        logger.info(f"Registros insertados: {insertados}")
        logger.info(f"Registros actualizados: {actualizados}")
        logger.info(f"Productos no encontrados en BD: {no_encontrados}")
        logger.info(f"Errores: {errores}")
        logger.info("="*60)

    except Exception as e:
        logger.error(f"Error durante la importación: {e}")
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Importar stock mínimo/ideal desde Excel')
    parser.add_argument('--dry-run', action='store_true',
                        help='Solo muestra lo que haría sin hacer cambios')
    parser.add_argument('--file', type=str,
                        default='utilidades/stock minimo e ideal por deposito.xlsx',
                        help='Ruta al archivo Excel')

    args = parser.parse_args()

    # Construir ruta completa
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    excel_path = os.path.join(base_dir, args.file)

    if not os.path.exists(excel_path):
        logger.error(f"Archivo no encontrado: {excel_path}")
        sys.exit(1)

    importar_stock_desde_excel(excel_path, dry_run=args.dry_run)
