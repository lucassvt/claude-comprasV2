"""Verificar cálculo de demanda para BOZAL CUERO N3"""
import sys
sys.path.insert(0, 'c:/Users/54381/Desktop/claude agente de compras 2')

from sqlalchemy import create_engine, text
import pandas as pd
from app.services.demand_forecaster import DemandForecaster

engine = create_engine('postgresql://postgres:mascotera2025@localhost/mascotera_compras')

with engine.connect() as conn:
    # Obtener ventas del BOZAL CUERO N3 por depósito
    result = conn.execute(text("""
        SELECT
            p.cod_item,
            p.nombre,
            d.nombre as deposito,
            d.id as deposit_id,
            p.id as product_id,
            COUNT(sh.id) as num_ventas,
            SUM(sh.cantidad) as total_cantidad,
            MIN(sh.fecha) as primera_venta,
            MAX(sh.fecha) as ultima_venta
        FROM sales_history sh
        JOIN products p ON sh.product_id = p.id
        JOIN deposits d ON sh.deposit_id = d.id
        WHERE p.cod_item = '00158'
        GROUP BY p.cod_item, p.nombre, d.nombre, d.id, p.id
        ORDER BY total_cantidad DESC
        LIMIT 5
    """))

    depositos = [dict(r._mapping) for r in result]

    print("=" * 80)
    print("VENTAS DE BOZAL CUERO N3 (00158) POR DEPOSITO")
    print("=" * 80)

    for dep in depositos:
        print(f"\nDeposito: {dep['deposito']}")
        print(f"  Ventas: {dep['num_ventas']} registros")
        print(f"  Cantidad total: {dep['total_cantidad']}")
        print(f"  Periodo: {dep['primera_venta']} a {dep['ultima_venta']}")

        # Obtener ventas detalladas
        ventas = conn.execute(text("""
            SELECT fecha, cantidad, monto
            FROM sales_history
            WHERE product_id = :pid AND deposit_id = :did
            ORDER BY fecha
        """), {'pid': dep['product_id'], 'did': dep['deposit_id']})

        df = pd.DataFrame([dict(r._mapping) for r in ventas])

        if not df.empty:
            df['cantidad'] = df['cantidad'].astype(float)
            df['monto'] = df['monto'].astype(float)

            # Calcular demanda con mediana
            forecaster = DemandForecaster(metodo_preferido='mediana')
            result = forecaster.calculate_demand(df, dep['product_id'], dep['deposit_id'], days_back=180)

            esperado = float(dep['total_cantidad']) / 180

            print(f"  Demanda calculada: {result.demanda_diaria:.6f} u/dia (metodo: {result.metodo_usado})")
            print(f"  Demanda esperada (simple): {esperado:.6f} u/dia ({dep['total_cantidad']}/180)")
