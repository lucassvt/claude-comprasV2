"""
Script para importar notas de crédito desde Excel a la base de datos.
Las notas de crédito se insertan como ventas con cantidad negativa.
"""
import sys
sys.path.insert(0, 'c:/Users/54381/Desktop/claude agente de compras 2')

import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text

# Mapeo de sucursal a deposit_id
SUCURSAL_TO_DEPOSIT = {
    'SUCURSAL ALEM': 17,
    'SUCURSAL  ALEM': 17,  # Con espacio extra
    'SUCURSAL LAPRIDA': 27,
    'SUCURSAL BELGRANO': 18,
    'SUCURSAL PARQUE': 28,
    'SUCURSAL CONGRESO': 19,
    'SUCURSAL MUÑECAS': 20,
    'SUCURSAL BANDA': 26,
    'SUCURSAL CATAMARCA': 24,
    'SUCURSAL REYES CATOLICOS': 29,
    'SUCURSAL ARENALES': 23,
    'SUCURSAL LEGUIZAMON': 32,
    'SUCURSAL BELGRANO SUR': 22,
    'SUCURSAL NEUQUEN OLASCOAGA': 34,
    'SUCURSAL CONCEPCION': 25,
    'DEPOSITO RUTA 9': 16,
    'PETS PLUS MIGUEL LILLO': 30,
    'SUCURSAL PINAR I': 31,
}

def importar_notas_credito(excel_path: str):
    """Importa notas de crédito desde Excel"""

    # Leer Excel
    print(f"Leyendo archivo: {excel_path}")
    df = pd.read_excel(excel_path)
    print(f"Total registros en Excel: {len(df)}")

    # Conectar a BD
    engine = create_engine('postgresql://postgres:mascotera2025@localhost/mascotera_compras')

    stats = {
        'insertados': 0,
        'productos_no_encontrados': 0,
        'sucursales_no_encontradas': 0,
        'errores': 0
    }

    productos_no_encontrados = set()
    sucursales_no_encontradas = set()

    with engine.connect() as conn:
        # Obtener mapeo de cod_item -> product_id
        result = conn.execute(text("SELECT id, cod_item FROM products"))
        product_map = {row[1]: row[0] for row in result}
        print(f"Productos en BD: {len(product_map)}")

        for idx, row in df.iterrows():
            try:
                sucursal = str(row['Sucursal']).strip()
                cod_item = str(row['Código Producto']).strip() if pd.notna(row['Código Producto']) else None
                cantidad = float(row['Cantidad']) if pd.notna(row['Cantidad']) else 0
                monto = float(row['Total']) if pd.notna(row['Total']) else 0
                fecha = row['Fecha Comp']

                # Saltar si no hay código de producto
                if not cod_item:
                    continue

                # Buscar deposit_id
                deposit_id = SUCURSAL_TO_DEPOSIT.get(sucursal)
                if not deposit_id:
                    sucursales_no_encontradas.add(sucursal)
                    stats['sucursales_no_encontradas'] += 1
                    continue

                # Buscar product_id
                product_id = product_map.get(cod_item)
                if not product_id:
                    productos_no_encontrados.add(cod_item)
                    stats['productos_no_encontrados'] += 1
                    continue

                # Convertir fecha
                if isinstance(fecha, str):
                    fecha = pd.to_datetime(fecha)
                elif isinstance(fecha, datetime):
                    pass
                else:
                    fecha = pd.to_datetime(fecha)

                # Asegurar que cantidad y monto sean negativos (son notas de crédito)
                if cantidad > 0:
                    cantidad = -cantidad
                if monto > 0:
                    monto = -monto

                # Insertar en sales_history
                conn.execute(text("""
                    INSERT INTO sales_history (product_id, deposit_id, fecha, cantidad, monto, created_at)
                    VALUES (:product_id, :deposit_id, :fecha, :cantidad, :monto, NOW())
                """), {
                    'product_id': product_id,
                    'deposit_id': deposit_id,
                    'fecha': fecha,
                    'cantidad': cantidad,
                    'monto': monto
                })

                stats['insertados'] += 1

                if stats['insertados'] % 100 == 0:
                    print(f"  Procesados: {stats['insertados']}...")

            except Exception as e:
                stats['errores'] += 1
                print(f"Error en fila {idx}: {e}")

        conn.commit()

    print("\n" + "=" * 60)
    print("IMPORTACIÓN COMPLETADA")
    print("=" * 60)
    print(f"Notas de crédito insertadas: {stats['insertados']}")
    print(f"Productos no encontrados:    {stats['productos_no_encontrados']}")
    print(f"Sucursales no encontradas:   {stats['sucursales_no_encontradas']}")
    print(f"Errores:                     {stats['errores']}")

    if productos_no_encontrados:
        print(f"\nProductos no encontrados (primeros 10):")
        for cod in list(productos_no_encontrados)[:10]:
            print(f"  - {cod}")

    if sucursales_no_encontradas:
        print(f"\nSucursales no encontradas:")
        for suc in sucursales_no_encontradas:
            print(f"  - {suc}")

    return stats

if __name__ == "__main__":
    excel_path = "c:/Users/54381/Desktop/claude agente de compras 2/utilidades/notas de credito.xlsx"
    importar_notas_credito(excel_path)
