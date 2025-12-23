"""Test completo de todos los métodos de forecasting"""
import sys
sys.path.insert(0, 'c:/Users/54381/Desktop/claude agente de compras 2')

import pandas as pd
from datetime import datetime, timedelta
from app.services.demand_forecaster import DemandForecaster

# Caso 1: Solo 1 venta en 180 días
print("=" * 70)
print("CASO 1: 1 venta de 6 unidades en 180 días")
print("=" * 70)

df1 = pd.DataFrame({
    'fecha': [datetime(2025, 12, 19)],
    'cantidad': [6.0],
    'monto': [29752.08]
})

for metodo in ['promedio_simple', 'mediana', 'combinado']:
    forecaster = DemandForecaster(metodo_preferido=metodo)
    result = forecaster.calculate_demand(df1, 1, 1, days_back=180)
    esperado = 6.0 / 180
    ok = "OK" if abs(result.demanda_diaria - esperado) < 0.001 else "FAIL"
    print(f"  {metodo:20}: {result.demanda_diaria:.6f} (esperado: {esperado:.6f}) {ok}")

# Caso 2: 10 ventas de 1 unidad cada una, distribuidas en 180 días
print("\n" + "=" * 70)
print("CASO 2: 10 ventas de 1 unidad en 180 días")
print("=" * 70)

fechas = [datetime(2025, 6, 25) + timedelta(days=i*18) for i in range(10)]
df2 = pd.DataFrame({
    'fecha': fechas,
    'cantidad': [1.0] * 10,
    'monto': [100.0] * 10
})

for metodo in ['promedio_simple', 'mediana', 'combinado']:
    forecaster = DemandForecaster(metodo_preferido=metodo)
    result = forecaster.calculate_demand(df2, 1, 1, days_back=180)
    esperado = 10.0 / 180
    ok = "OK" if abs(result.demanda_diaria - esperado) < 0.01 else "~"
    print(f"  {metodo:20}: {result.demanda_diaria:.6f} (esperado ~{esperado:.6f}) {ok}")

# Caso 3: Venta con pico (outlier)
print("\n" + "=" * 70)
print("CASO 3: 7 ventas con un pico (outlier) de 100 unidades")
print("=" * 70)

fechas3 = [datetime(2025, 6, 25) + timedelta(days=i*25) for i in range(7)]
cantidades3 = [1.0, 1.0, 1.0, 100.0, 1.0, 1.0, 1.0]  # Total: 106
df3 = pd.DataFrame({
    'fecha': fechas3,
    'cantidad': cantidades3,
    'monto': [c * 100 for c in cantidades3]
})

for metodo in ['promedio_simple', 'mediana', 'combinado']:
    forecaster = DemandForecaster(metodo_preferido=metodo)
    result = forecaster.calculate_demand(df3, 1, 1, days_back=180)
    print(f"  {metodo:20}: {result.demanda_diaria:.6f}")

print(f"\n  Promedio simple esperado: {106/180:.6f} (106 unidades / 180 días)")
print(f"  Mediana esperada: mediana(1,1,1,100,1,1,1)=1 * (7/180) = {1 * 7/180:.6f}")

# Caso 4: Sin ventas
print("\n" + "=" * 70)
print("CASO 4: Sin ventas")
print("=" * 70)

df4 = pd.DataFrame({'fecha': [], 'cantidad': [], 'monto': []})

for metodo in ['promedio_simple', 'mediana', 'combinado']:
    forecaster = DemandForecaster(metodo_preferido=metodo)
    result = forecaster.calculate_demand(df4, 1, 1, days_back=180)
    ok = "OK" if result.demanda_diaria == 0 else "FAIL"
    print(f"  {metodo:20}: {result.demanda_diaria:.6f} (esperado: 0) {ok}")

print("\n" + "=" * 70)
print("TODOS LOS TESTS COMPLETADOS")
print("=" * 70)
