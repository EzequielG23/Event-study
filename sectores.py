import pandas as pd
import numpy as np
from scipy import stats
import shutil

# definir rutas de archivos
eventos_file = r"eventoos generados"
datos_file = r"primer excel generadoo"
excel_copia_file = r"excel base"
resultados_file = r"resultados por sector"
backup_file = r"backup"
# crear copia de seguridad
try:
    shutil.copyfile(datos_file, backup_file)
    print("copia de seguridad creada exitosamente.")
except Exception as e:
    print(f"error al crear la copia de seguridad: {e}")

# cargar datos
try:
    df_enlace = pd.read_excel(excel_copia_file, engine='openpyxl')
    df_datos = pd.read_excel(datos_file, sheet_name='Resultados_Completos_Evento', engine='openpyxl')
    df_car = pd.read_excel(datos_file, sheet_name='Resultados_CAR', engine='openpyxl')
    print("datos cargados exitosamente.")
except Exception as e:
    print(f"error al cargar los datos: {e}")
    exit()

# verificar columnas
print("columnas de df_datos:", df_datos.columns)

# convertir fechas
df_datos['Fecha'] = pd.to_datetime(df_datos['Fecha'], errors='coerce')
df_enlace['Fecha'] = pd.to_datetime(df_enlace['Fecha'], errors='coerce')

# unir datos usando 'Número' y 'ID_Evento'
df_merged = pd.merge(df_datos, df_enlace[['Número', 'Categoría']], left_on='ID_Evento', right_on='Número', how='left')

# definir ventanas
ventanas = [(-10, 10), (-5, 5), (-2, 2), (-1, 1)]

# inicializar resultados por sector
sectores = df_enlace['Categoría'].unique()
resultados_sector = {}

for sector in sectores:
    print(f"\nprocesando sector: {sector}")
    df_sector = df_merged[df_merged['Categoría'] == sector]
    
    if df_sector.empty:
        print(f"no hay eventos para el sector {sector}.")
        continue
    
    resultados_estadisticos_ra = []
    resultados_estadisticos_car = []
    
    # análisis de ra
    for ventana in ventanas:
        etiqueta_ventana = f'{ventana[0]} a {ventana[1]}'
        ra_ventana = []
        
        for id_evento in df_sector['ID_Evento'].unique():
            datos_evento = df_sector[df_sector['ID_Evento'] == id_evento].sort_values(by='Fecha').reset_index(drop=True)
            fecha_evento = datos_evento[datos_evento['Es_dia_evento'] == True]['Fecha']
            if fecha_evento.empty:
                print(f"evento {id_evento} no tiene una fecha de evento marcada.")
                continue
            fecha_evento = fecha_evento.iloc[0]
            
            idx_evento = datos_evento.index[datos_evento['Fecha'] == fecha_evento].tolist()
            if not idx_evento:
                print(f"evento {id_evento}: no se encontró la fecha del evento en el grupo.")
                continue
            idx_evento = idx_evento[0]
            
            inicio_idx = idx_evento + ventana[0]
            fin_idx = idx_evento + ventana[1]
            
            if inicio_idx < 0:
                inicio_idx = 0
            if fin_idx >= len(datos_evento):
                fin_idx = len(datos_evento) - 1
            
            ventana_group = datos_evento.loc[inicio_idx:fin_idx]
            ra = ventana_group['Rentabilidad_anormal'].values
            ra_ventana.extend(ra)
        
        ra_ventana = np.array(ra_ventana)
        ra_ventana = ra_ventana[~np.isnan(ra_ventana)]
        
        numero_eventos = df_sector['ID_Evento'].nunique()
        dias_en_ventana = ventana[1] - ventana[0] + 1
        N_esperado = numero_eventos * dias_en_ventana
        N_real = len(ra_ventana)
        
        if N_real < N_esperado:
            print(f"ventana {etiqueta_ventana}: n real = {N_real}, n esperado = {N_esperado}. faltan {N_esperado - N_real} ra.")
        else:
            print(f"ventana {etiqueta_ventana}: n real = {N_real}, n esperado = {N_esperado}.")
        
        if len(ra_ventana) == 0:
            print(f"no hay rendimientos anormales disponibles para la ventana {etiqueta_ventana}.")
            continue
        
        t_statistic, p_value_t = stats.ttest_1samp(ra_ventana, 0)
        
        if len(ra_ventana) < 2:
            wilcoxon_statistic, p_value_wilcoxon = np.nan, np.nan
        else:
            try:
                wilcoxon_statistic, p_value_wilcoxon = stats.wilcoxon(ra_ventana)
            except ValueError as ve:
                print(f"error en wilcoxon para la ventana {etiqueta_ventana}: {ve}")
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
        
        print(f"pruebas completadas para rendimientos anormales en la ventana {etiqueta_ventana}.")
    
    # análisis de CAR
    for ventana in ventanas:
        etiqueta_ventana = f'CAR_{ventana[0]}_a_{ventana[1]}'
        car_values = df_car[(df_car['Ventana'] == etiqueta_ventana) & (df_car['ID_Evento'].isin(df_sector['ID_Evento']))]['CAR'].dropna().values
        
        if len(car_values) == 0:
            print(f"no hay CAR disponibles para la ventana {etiqueta_ventana}.")
            continue
        
        t_statistic, p_value_t = stats.ttest_1samp(car_values, 0)
        
        if len(car_values) < 2:
            wilcoxon_statistic, p_value_wilcoxon = np.nan, np.nan
        else:
            try:
                wilcoxon_statistic, p_value_wilcoxon = stats.wilcoxon(car_values)
            except ValueError as ve:
                print(f"error en wilcoxon para CAR en la ventana {etiqueta_ventana}: {ve}")
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
        
        print(f"pruebas completadas para CAR en la ventana {etiqueta_ventana}.")
    
    # convertir a dataframes
    df_estadisticos_ra = pd.DataFrame(resultados_estadisticos_ra)
    df_estadisticos_car = pd.DataFrame(resultados_estadisticos_car)
    
    # guardar resultados
    resultados_sector[f"{sector}_RA"] = df_estadisticos_ra
    resultados_sector[f"{sector}_CAR"] = df_estadisticos_car

# guardar en excel
try:
    with pd.ExcelWriter(resultados_file, engine='openpyxl') as writer:
        for sector, df in resultados_sector.items():
            df.to_excel(writer, sheet_name=sector, index=False)
    print(f"\nresultados por sector guardados exitosamente en '{resultados_file}'.")
except Exception as e:
    print(f"error al guardar los resultados: {e}")
