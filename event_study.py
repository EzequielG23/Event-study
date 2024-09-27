import pandas as pd

# ruta del archivo excel con datos de precios y resultados de regresión
file_path = r'ruta'

# cargar datos de precios y rendimientos
df_precios = pd.read_excel(file_path, sheet_name='Sheet1')
df_regresion = pd.read_excel(file_path, sheet_name='Resultados_Regresion')

resultados = []

# asegurar que 'FechaEvento' está en formato de fecha
df_precios['FechaEvento'] = pd.to_datetime(df_precios['FechaEvento'])

# iterar sobre cada evento
for id_evento in df_regresion['ID_Evento'].unique():
    datos_evento = df_precios[df_precios['ID_Evento'] == id_evento].copy()
    alpha = df_regresion.loc[df_regresion['ID_Evento'] == id_evento, 'Alpha (α)'].values[0]
    beta = df_regresion.loc[df_regresion['ID_Evento'] == id_evento, 'Beta (β)'].values[0]
    fecha_evento = datos_evento['FechaEvento'].iloc[0]
    datos_evento.set_index('Date', inplace=True)
    
    if fecha_evento not in datos_evento.index:
        print(f"Advertencia: La fecha del evento {fecha_evento} no está en los datos para el ID_Evento {id_evento}.")
        # buscar siguiente día hábil disponible
        fecha_evento = datos_evento.index[datos_evento.index.searchsorted(fecha_evento)]
        print(f"Usando la siguiente fecha hábil disponible: {fecha_evento}")

    idx_evento = datos_evento.index.get_loc(fecha_evento)
    inicio_idx = max(0, idx_evento - 10)
    fin_idx = min(len(datos_evento), idx_evento + 10 + 1)
    datos_evento_ventana = datos_evento.iloc[inicio_idx:fin_idx].copy()
    
    if len(datos_evento_ventana) < 21:
        print(f"Advertencia: Para el ID_Evento {id_evento}, se encontraron {len(datos_evento_ventana)} sesiones en lugar de 21. Ajustando ventana.")

    if 'Rendimiento_activo' not in datos_evento_ventana.columns or 'Rendimiento_indice' not in datos_evento_ventana.columns:
        print(f"Error: Columnas de rendimientos no encontradas para el ID_Evento {id_evento}.")
        continue

    datos_evento_ventana['Rendimiento_esperado'] = alpha + beta * datos_evento_ventana['Rendimiento_indice']
    datos_evento_ventana['Rendimiento_anormal'] = datos_evento_ventana['Rendimiento_activo'] - datos_evento_ventana['Rendimiento_esperado']
    datos_evento_ventana['Es_dia_evento'] = datos_evento_ventana.index == fecha_evento

    for fecha, fila in datos_evento_ventana.iterrows():
        resultados.append({
            'ID_Evento': id_evento,
            'Fecha': fecha,
            'Es_dia_evento': fila['Es_dia_evento'],
            'Rentabilidad_activo': fila['Rendimiento_activo'],
            'Rentabilidad_indice': fila['Rendimiento_indice'],
            'Rentabilidad_esperada': fila['Rendimiento_esperado'],
            'Rentabilidad_anormal': fila['Rendimiento_anormal']
        })

df_resultados = pd.DataFrame(resultados)

with pd.ExcelWriter(file_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
    df_resultados.to_excel(writer, sheet_name='Resultados_Completos_Evento', index=False)

print("Cálculo completado y resultados guardados en una única pestaña.")

# definir ventanas de análisis
ventanas = [(-10, 10), (-5, 5), (-2, 2), (-1, 1)]
resultados_car = []
df_resultados.set_index(['ID_Evento', 'Fecha'], inplace=True)

# iterar sobre cada evento
for id_evento in df_resultados.index.get_level_values('ID_Evento').unique():
    datos_evento = df_resultados.xs(id_evento, level='ID_Evento').copy()
    idx_evento = datos_evento[datos_evento['Es_dia_evento']].index[0]
    
    for ventana in ventanas:
        inicio_ventana = max(0, datos_evento.index.get_loc(idx_evento) + ventana[0])
        fin_ventana = min(len(datos_evento), datos_evento.index.get_loc(idx_evento) + ventana[1] + 1)
        car = datos_evento['Rentabilidad_anormal'].iloc[inicio_ventana:fin_ventana].sum()
        resultados_car.append({
            'ID_Evento': id_evento,
            'Ventana': f'CAR_{ventana[0]}_a_{ventana[1]}',
            'CAR': car
        })

df_car = pd.DataFrame(resultados_car)

with pd.ExcelWriter(file_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
    df_car.to_excel(writer, sheet_name='Resultados_CAR', index=False)

print("Cálculo de CAR completado y resultados guardados en una nueva pestaña.")
