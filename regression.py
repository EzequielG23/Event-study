import pandas as pd
import numpy as np
import statsmodels.api as sm

# ruta del archivo excel
file_path = r'ruta'

# cargar el archivo excel
df = pd.read_excel(file_path)

# asegurar que las fechas estén en formato datetime
df['FechaEvento'] = pd.to_datetime(df['FechaEvento'], errors='coerce')
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

# establecer 'Date' como índice si no lo es ya
if 'Date' in df.columns and df.index.name != 'Date':
    df.set_index('Date', inplace=True)

# inicializar listas para resultados y eventos omitidos
resultados_regresion = []
eventos_omitidos = []

# número total de eventos originales
total_eventos_originales = df['ID_Evento'].nunique()

# bucle sobre cada evento
for id_evento in sorted(df['ID_Evento'].unique()):
    datos_evento = df[df['ID_Evento'] == id_evento]
    datos_evento = datos_evento.sort_index()
    fecha_evento = datos_evento['FechaEvento'].iloc[0]
    datos_pre_evento = datos_evento[datos_evento.index < fecha_evento]
    datos_pre_evento = datos_pre_evento.tail(200)  # hasta 200 días antes

    # eliminar valores faltantes o infinitos
    datos_pre_evento = datos_pre_evento.replace([np.inf, -np.inf], np.nan)
    datos_pre_evento = datos_pre_evento.dropna(subset=['Rendimiento_activo', 'Rendimiento_indice'])

    # verificar que haya al menos 2 datos para la regresión
    if len(datos_pre_evento) >= 2:
        y = datos_pre_evento['Rendimiento_activo']
        X = datos_pre_evento['Rendimiento_indice']
        X = sm.add_constant(X)  # agregar constante

        # realizar regresión lineal
        modelo = sm.OLS(y, X).fit()
        alpha = modelo.params['const']
        beta = modelo.params['Rendimiento_indice']
        r_squared = modelo.rsquared
        error_estandar = modelo.bse['Rendimiento_indice']

        # mostrar resultados
        print(f"ID_Evento {id_evento} - Alpha: {alpha}, Beta: {beta}, R²: {r_squared}, Días usados: {len(datos_pre_evento)}")

        # guardar resultados
        resultados_regresion.append({
            'ID_Evento': id_evento,
            'Alpha (α)': alpha,
            'Beta (β)': beta,
            'R²': r_squared,
            'Error estándar': error_estandar,
            'Días usados': len(datos_pre_evento)
        })
    else:
        print(f"ID_Evento {id_evento} tiene menos de 2 días de datos válidos después de eliminar NaNs. Se omite.")
        eventos_omitidos.append(id_evento)
        resultados_regresion.append({
            'ID_Evento': id_evento,
            'Alpha (α)': np.nan,
            'Beta (β)': np.nan,
            'R²': np.nan,
            'Error estándar': np.nan,
            'Días usados': len(datos_pre_evento)
        })

# convertir resultados a DataFrame
df_regresion = pd.DataFrame(resultados_regresion)

# guardar resultados en nueva hoja del excel
with pd.ExcelWriter(file_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
    df_regresion.to_excel(writer, sheet_name='Resultados_Regresion', index=False)

# resumen del procesamiento
total_eventos_procesados = df_regresion['ID_Evento'].nunique()

print("\nResumen del procesamiento:")
print(f"Total de eventos originales: {total_eventos_originales}")
print(f"Total de eventos procesados: {total_eventos_procesados}")
print(f"Total de eventos omitidos: {len(eventos_omitidos)}")

if len(eventos_omitidos) > 0:
    print(f"Eventos omitidos debido a datos insuficientes: {eventos_omitidos}")

print("Regresión lineal completada y resultados guardados.")
