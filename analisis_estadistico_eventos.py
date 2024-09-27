import pandas as pd
import numpy as np
from scipy import stats
import shutil
import os

# crear copia de seguridad
original_file = r'ruta excel'
backup_file = r'crear respaldo'
resultados_file = r'resultados del segundo script'

try:
    shutil.copyfile(original_file, backup_file)
    print("Copia de seguridad creada exitosamente.")
except Exception as e:
    print(f"Error al crear la copia de seguridad: {e}")

# definir ventanas de análisis
ventanas = [(-10, 10), (-5, 5), (-2, 2), (-1, 1)]

# cargar datos
try:
    df_resultados = pd.read_excel(original_file, sheet_name='Resultados_Completos_Evento', engine='openpyxl')
    df_car = pd.read_excel(original_file, sheet_name='Resultados_CAR', engine='openpyxl')
    print("Datos cargados exitosamente.")
except Exception as e:
    print(f"Error al cargar los datos: {e}")
    exit()

# asegurar fechas en formato datetime
df_resultados['Fecha'] = pd.to_datetime(df_resultados['Fecha'], errors='coerce')

# verificar días de evento
event_counts = df_resultados.groupby('ID_Evento')['Es_dia_evento'].sum()

multiple_event_days = event_counts[event_counts > 1]
if not multiple_event_days.empty:
    print("Eventos con múltiples días marcados como 'Es_dia_evento = True':")
    print(multiple_event_days)
else:
    print("Todos los eventos tienen correctamente un solo día marcado como 'Es_dia_evento = True'.")

no_event_days = event_counts[event_counts == 0]
if not no_event_days.empty:
    print("\nEventos sin ningún día marcado como 'Es_dia_evento = True':")
    print(no_event_days)
else:
    print("\nTodos los eventos tienen al menos un día marcado como 'Es_dia_evento = True'.")

# verificar cobertura de ventana temporal por evento
ventana_principal = (-10, 10)
ra_counts = {}
ra_faltantes = {}

df_resultados = df_resultados.sort_values(by=['ID_Evento', 'Fecha']).reset_index(drop=True)
grouped = df_resultados.groupby('ID_Evento')

for id_evento, group in grouped:
    eventos_dia = group[group['Es_dia_evento'] == True]
    if eventos_dia.empty:
        print(f"Evento {id_evento} no tiene una fecha de evento marcada.")
        continue
    elif len(eventos_dia) > 1:
        print(f"Evento {id_evento} tiene múltiples días marcados como 'Es_dia_evento = True'.")
        fecha_evento = eventos_dia['Fecha'].iloc[0]
    else:
        fecha_evento = eventos_dia['Fecha'].iloc[0]
    
    idx_evento = group.index[group['Fecha'] == fecha_evento].tolist()
    if not idx_evento:
        print(f"Evento {id_evento}: No se encontró la fecha del evento en el grupo.")
        continue
    idx_evento = idx_evento[0]
    
    inicio_idx = idx_evento + ventana_principal[0]
    fin_idx = idx_evento + ventana_principal[1]
    
    if inicio_idx < group.index.min():
        inicio_idx = group.index.min()
    if fin_idx > group.index.max():
        fin_idx = group.index.max()
    
    ventana_group = group.loc[inicio_idx:fin_idx]
    
    count_ra = ventana_group['Rentabilidad_anormal'].count()
    ra_counts[id_evento] = count_ra
    
    if count_ra < 21:
        ra_faltantes[id_evento] = 21 - count_ra
        print(f"Evento {id_evento} tiene {count_ra} RA en la ventana {ventana_principal[0]} a {ventana_principal[1]} sesiones (faltan {21 - count_ra}).")

df_ra_counts = pd.DataFrame(list(ra_counts.items()), columns=['ID_Evento', 'RA_Count'])
print("\nResumen de la cantidad de RA por evento:")
print(df_ra_counts.describe())

if ra_faltantes:
    df_ra_faltantes = pd.DataFrame(list(ra_faltantes.items()), columns=['ID_Evento', 'RA_Faltantes'])
    with pd.ExcelWriter(resultados_file, engine='openpyxl') as writer:
        df_ra_faltantes.to_excel(writer, sheet_name='Eventos_RA_Faltantes', index=False)
    print("\nDetalles de eventos con RA faltantes guardados en la hoja 'Eventos_RA_Faltantes' del archivo de resultados.")
else:
    print("\nNo hay eventos con RA faltantes.")

resultados_estadisticos_ra = []
resultados_estadisticos_car = []

# pruebas sobre rendimientos anormales diarios (RA)
print("\nIniciando pruebas estadísticas sobre Rendimientos Anormales Diarios...")

for ventana in ventanas:
    etiqueta_ventana = f'{ventana[0]} a {ventana[1]}'
    ra_ventana = []
    
    for id_evento, group in grouped:
        eventos_dia = group[group['Es_dia_evento'] == True]
        if eventos_dia.empty:
            continue
        elif len(eventos_dia) > 1:
            fecha_evento = eventos_dia['Fecha'].iloc[0]
        else:
            fecha_evento = eventos_dia['Fecha'].iloc[0]
        
        idx_evento = group.index[group['Fecha'] == fecha_evento].tolist()
        if not idx_evento:
            continue
        idx_evento = idx_evento[0]
        
        inicio_idx = idx_evento + ventana[0]
        fin_idx = idx_evento + ventana[1]
        
        if inicio_idx < group.index.min():
            inicio_idx = group.index.min()
        if fin_idx > group.index.max():
            fin_idx = group.index.max()
        
        ventana_group = group.loc[inicio_idx:fin_idx]
        
        ra = ventana_group['Rentabilidad_anormal'].values
        ra_ventana.extend(ra)
    
    ra_ventana = np.array(ra_ventana)
    ra_ventana = ra_ventana[~np.isnan(ra_ventana)]
    
    numero_eventos = grouped.ngroups
    dias_en_ventana = ventana[1] - ventana[0] + 1
    N_esperado = numero_eventos * dias_en_ventana
    N_real = len(ra_ventana)
    
    if N_real < N_esperado:
        print(f"\nVentana {etiqueta_ventana}: N real = {N_real}, N esperado = {N_esperado}. Faltan {N_esperado - N_real} RA.")
    else:
        print(f"\nVentana {etiqueta_ventana}: N real = {N_real}, N esperado = {N_esperado}.")
    
    if len(ra_ventana) == 0:
        print(f"No hay rendimientos anormales disponibles para la ventana {etiqueta_ventana}.")
        continue
    
    t_statistic, p_value_t = stats.ttest_1samp(ra_ventana, 0)
    
    if len(ra_ventana) < 2:
        wilcoxon_statistic, p_value_wilcoxon = np.nan, np.nan
    else:
        try:
            wilcoxon_statistic, p_value_wilcoxon = stats.wilcoxon(ra_ventana)
        except ValueError as ve:
            print(f"Error en Wilcoxon para la ventana {etiqueta_ventana}: {ve}")
            wilcoxon_statistic, p_value_wilcoxon = np.nan, np.nan
    
    media_ra = np.mean(ra_ventana)
    mediana_ra = np.median(ra_ventana)
    
    resultados_estadisticos_ra.append({
        'Ventana': etiqueta_ventana,
        'Media_RA': media_ra,
        'Mediana_RA': mediana_ra,
        't_statistic': t_statistic,
        'p_value_t': p_value_t,
        'wilcoxon_statistic': wilcoxon_statistic,
        'p_value_wilcoxon': p_value_wilcoxon,
        'N': len(ra_ventana)
    })
    
    print(f"Pruebas completadas para Rendimientos Anormales en la ventana {etiqueta_ventana}.")

# pruebas sobre rendimientos anormales acumulados (CAR)
print("\nIniciando pruebas estadísticas sobre Rendimientos Anormales Acumulados (CAR)...")

for ventana in ventanas:
    etiqueta_ventana = f'CAR_{ventana[0]}_a_{ventana[1]}'
    car_values = df_car[df_car['Ventana'] == etiqueta_ventana]['CAR'].dropna().values
    
    numero_eventos = grouped.ngroups
    N_esperado_car = numero_eventos
    N_real_car = len(car_values)
    
    if N_real_car < N_esperado_car:
        print(f"Ventana {etiqueta_ventana}: N real CAR = {N_real_car}, N esperado CAR = {N_esperado_car}. Faltan {N_esperado_car - N_real_car} CAR.")
    else:
        print(f"Ventana {etiqueta_ventana}: N real CAR = {N_real_car}, N esperado CAR = {N_esperado_car}.")
    
    if len(car_values) == 0:
        print(f"No hay CAR disponibles para la ventana {etiqueta_ventana}.")
        continue
    
    t_statistic, p_value_t = stats.ttest_1samp(car_values, 0)
    
    if len(car_values) < 2:
        wilcoxon_statistic, p_value_wilcoxon = np.nan, np.nan
    else:
        try:
            wilcoxon_statistic, p_value_wilcoxon = stats.wilcoxon(car_values)
        except ValueError as ve:
            print(f"Error en Wilcoxon para la ventana {etiqueta_ventana}: {ve}")
            wilcoxon_statistic, p_value_wilcoxon = np.nan, np.nan
    
    media_car = np.mean(car_values)
    mediana_car = np.median(car_values)
    
    resultados_estadisticos_car.append({
        'Ventana': etiqueta_ventana,
        'Media_CAR': media_car,
        'Mediana_CAR': mediana_car,
        't_statistic': t_statistic,
        'p_value_t': p_value_t,
        'wilcoxon_statistic': wilcoxon_statistic,
        'p_value_wilcoxon': p_value_wilcoxon,
        'N': len(car_values)
    })
    
    print(f"Pruebas completadas para CAR en la ventana {etiqueta_ventana}.")

# guardar resultados en nuevo archivo excel
df_estadisticos_ra = pd.DataFrame(resultados_estadisticos_ra)
df_estadisticos_car = pd.DataFrame(resultados_estadisticos_car)

try:
    with pd.ExcelWriter(resultados_file, engine='openpyxl') as writer:
        df_estadisticos_ra.to_excel(writer, sheet_name='Resultados_Estadisticos_RA', index=False)
        df_estadisticos_car.to_excel(writer, sheet_name='Resultados_Estadisticos_CAR', index=False)
    print("\nPruebas estadísticas completadas y resultados guardados en el archivo 'resultados_estadisticos.xlsx'.")
except Exception as e:
    print(f"Error al guardar los resultados: {e}")
