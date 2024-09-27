import yfinance as yf
import pandas as pd
import numpy as np

# archivos de entrada y salida
input_file = r'ruta'
output_file = r'ruuta'

df_events = pd.read_excel(input_file)
df_events['ID_Evento'] = df_events.index + 1

all_data = pd.DataFrame()
errores_tickers = []
eventos_ajustados = []
eventos_con_sesiones_reducidas = []

# función para descargar datos de un ticker entre fechas
def descargar_datos(ticker, fecha_inicio, fecha_fin):
    try:
        data = yf.download(ticker, start=fecha_inicio, end=fecha_fin, progress=False)[['Close']]
        data.index = pd.to_datetime(data.index)
        return data
    except Exception as e:
        print(f"Error al descargar datos para {ticker}: {e}")
        return pd.DataFrame()

# maneja la descarga y procesamiento de datos
def manejar_evento(ticker, ticker_indice, fecha_evento_original, id_evento):
    max_intentos = 30  # máximo intentos para ajustar fecha
    intentos = 0
    fecha_evento = pd.to_datetime(fecha_evento_original)
    evento_ajustado = False

    # extender rango de fechas para asegurar suficientes sesiones
    fecha_inicio = fecha_evento - pd.DateOffset(days=700)
    fecha_fin = fecha_evento + pd.DateOffset(days=300)

    # descargar datos para activo e índice
    data_activo = descargar_datos(ticker, fecha_inicio, fecha_fin)
    data_indice = descargar_datos(ticker_indice, fecha_inicio, fecha_fin)

    if data_activo.empty or data_indice.empty:
        print(f"Error: Datos vacíos para {ticker} o {ticker_indice}.")
        errores_tickers.append(f"{ticker} o {ticker_indice}")
        return

    # unir datos por fecha (intersección)
    data_combined = data_activo.join(data_indice, how='inner', lsuffix='_activo', rsuffix='_indice')
    data_combined.sort_index(inplace=True)

    while intentos < max_intentos:
        if fecha_evento not in data_combined.index:
            # buscar siguiente fecha donde ambos tienen datos
            fechas_posteriores = data_combined.index[data_combined.index > fecha_evento]
            if len(fechas_posteriores) == 0:
                print(f"No se encontró una fecha común después de {fecha_evento.date()} para {ticker} y {ticker_indice}.")
                errores_tickers.append(f"{ticker} o {ticker_indice}")
                return
            fecha_evento_anterior = fecha_evento
            fecha_evento = fechas_posteriores.min()
            intentos += 1
            evento_ajustado = True
            print(f"Fecha del evento ajustada de {fecha_evento_anterior.date()} a {fecha_evento.date()} para el evento {id_evento} ({ticker}) porque no hay sesión en la fecha original.")
            continue

        idx_evento = data_combined.index.get_loc(fecha_evento)

        # verificar si hay suficientes sesiones después del evento
        sesiones_necesarias_despues = 10

        if (len(data_combined) - idx_evento - 1) < sesiones_necesarias_despues:
            fecha_evento_anterior = fecha_evento
            fechas_posteriores = data_combined.index[data_combined.index > fecha_evento]
            if len(fechas_posteriores) == 0:
                print(f"No hay suficientes sesiones después del evento {id_evento} ({ticker}).")
                errores_tickers.append(f"{ticker} (ID Evento {id_evento}) - Insuficientes sesiones después del evento")
                return
            fecha_evento = fechas_posteriores.min()
            intentos += 1
            evento_ajustado = True
            print(f"Fecha del evento ajustada de {fecha_evento_anterior.date()} a {fecha_evento.date()} para el evento {id_evento} ({ticker}) porque no hay suficientes sesiones después.")
            continue

        break

    if intentos == max_intentos:
        print(f"No se pudo encontrar una fecha adecuada para el evento {id_evento} ({ticker}) después de {max_intentos} intentos.")
        errores_tickers.append(f"{ticker} (ID Evento {id_evento}) - No se encontró fecha adecuada")
        return

    if evento_ajustado:
        eventos_ajustados.append(f"Evento {id_evento} ({ticker}): Fecha ajustada a {fecha_evento.date()} después de {intentos} intentos.")
        df_events.loc[df_events['ID_Evento'] == id_evento, 'Fecha'] = fecha_evento

    sesiones_necesarias_antes = 200
    sesiones_adicionales = 1  # sesión extra para cálculo de rendimientos
    total_sesiones_antes = sesiones_necesarias_antes + sesiones_adicionales

    if idx_evento < total_sesiones_antes:
        print(f"Advertencia: El evento {id_evento} ({ticker}) no tiene suficientes sesiones antes del evento. Se requieren {total_sesiones_antes}, pero solo hay {idx_evento}. Se utilizarán las sesiones disponibles ({idx_evento}).")
        eventos_con_sesiones_reducidas.append(f"Evento {id_evento} ({ticker}): Solo hay {idx_evento} sesiones antes del evento.")
        total_sesiones_antes = idx_evento  # usar sesiones disponibles

    # obtener sesiones pre, día del evento y post evento
    data_pre_evento = data_combined.iloc[idx_evento - total_sesiones_antes:idx_evento]
    data_evento_session = data_combined.iloc[idx_evento:idx_evento + 1]
    data_post_evento = data_combined.iloc[idx_evento + 1:idx_evento + 1 + sesiones_necesarias_despues]

    data_evento = pd.concat([data_pre_evento, data_evento_session, data_post_evento])

    # calcular rendimientos logarítmicos
    data_evento['Rendimiento_activo'] = np.log(data_evento['Close_activo'] / data_evento['Close_activo'].shift(1))
    data_evento['Rendimiento_indice'] = np.log(data_evento['Close_indice'] / data_evento['Close_indice'].shift(1))

    # eliminar filas con NaN en rendimientos
    data_evento.dropna(subset=['Rendimiento_activo', 'Rendimiento_indice'], inplace=True)

    data_evento['ID_Evento'] = id_evento
    data_evento['Ticker'] = ticker
    data_evento['FechaEvento'] = fecha_evento.date()
    data_evento['Ticker_Indice'] = ticker_indice

    global all_data
    all_data = pd.concat([all_data, data_evento])

# bucle sobre los eventos y procesarlos
for _, row in df_events.iterrows():
    manejar_evento(
        row['Ticker'],
        row['Ticker índice'],
        pd.to_datetime(row['Fecha']),
        row['ID_Evento']
    )

# guardar resultados en excel
all_data.to_excel(output_file, sheet_name='Sheet1')

# guardar errores y ajustes en archivos de texto
with open('tickers_con_errores.txt', 'w') as f:
    f.write('\n'.join(errores_tickers))

with open('eventos_ajustados.txt', 'w') as f:
    f.write('\n'.join(eventos_ajustados))

with open('eventos_con_sesiones_reducidas.txt', 'w') as f:
    f.write('\n'.join(eventos_con_sesiones_reducidas))

df_events.to_excel('eventos_actualizados.xlsx', index=False)

print("Procesamiento completado. Datos guardados en 'Sheet1' del archivo Excel especificado.")
print(f"Eventos ajustados: {len(eventos_ajustados)}")
print(f"Eventos con sesiones pre-evento reducidas: {len(eventos_con_sesiones_reducidas)}")
print(f"Errores encontrados: {len(errores_tickers)}")
