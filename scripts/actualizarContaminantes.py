from datetime import datetime, timedelta
import json
import requests
import os 
import pandas as pd


dirContaminantes = '/home/thinking/raw/contaminantesData/'

'''
# Sacar la fecha del último día de datos
last_date = None
with open(dirContaminantes+'Alcantarilla.csv', 'rb') as f:
    try:  # catch OSError in case of a one line file 
        f.seek(-2, os.SEEK_END)
        while f.read(1) != b'\n':
            f.seek(-2, os.SEEK_CUR)
    except OSError:
        f.seek(0)
    last_line = f.readline().decode()
    last_date = datetime.strptime(last_line.split(',')[0], "%Y-%m-%d %H:%M:%S")
'''
f = open(dirContaminantes+'ultimaFecha.txt', 'r')
last_date = datetime.strptime(f.read().strip(), "%Y-%m-%d %H:%M:%S")

fechaInicio = last_date.strftime('%d/%m/%Y')
fechaFin = (datetime.today() + timedelta(days=1)).strftime('%d/%m/%Y')
print(fechaInicio, fechaFin)
url = 'https://sinqlair.carm.es/calidadaire/obtener_datos.aspx?tipo=tablaRedVigilancia&tipoConsulta=medias_horarias&estacionesSelec=e_alcantarilla_Alcantarilla-e_aljorra_Aljorra-e_alumbres_Alumbres-e_caravaca_Caravaca-e_lorca_Lorca-e_mompean_Mompe%C3%A1n-e_sanbasilio_San%20Basilio-e_valle_Valle%20de%20Escombreras&contaminantesSelec=O3-NO-NO2-NOX*NOx-NH3-NT-CO-SO2-PM10-C6H6*Ben-C7H8*Tol-XIL*Xil&periodoConsulta=sel_rango&fechaInicioConsulta='+fechaInicio+'&fechaFinConsulta='+fechaFin+'&tipo_dato=grid'
x = requests.post(url)

data = json.loads(x.text[3:])

# Leer y reestructurar los datos
columnas = ['Date','Comunidad', 'O3', 'NO', 'NO2', 'NOx', 'NH3', 'NT', 'CO', 'SO2', 'PM10', 'Benceno', 'Tolueno', 'Xileno']
df =  pd.json_normalize(data).drop(columns=['limites0','limites1'])
df.columns = columnas
df['Date'] = pd.to_datetime(df['Date'], format="%d/%m/%Y %H:%M:%S")
df.index = df['Date']
    
# Agrupar los datos segun la comunidad y escribirlos
dfs = [l[1] for l in list(df.groupby('Comunidad'))]
for i in range(len(dfs)):
    filepath= dirContaminantes+dfs[i].iloc[0]['Comunidad']+'.csv'
    d = dfs[i].sort_index()[dfs[i].index > last_date]

    # Cambiar los , por . (los datos crudos vienen , para decimales)
    for c in columnas[2:]:
        d[c] = d[c].apply(lambda x: x.replace(',','.'))
    d['Date'] = d['Date'].apply(lambda x: str(x))
    d.to_csv(filepath, mode='a', header=True if os.path.exists(filepath) == False else False, index=False)

    # si son las 12 (ya ha pasado el día), elminar el octavo día anterior (guardar solo 7 días)
    if datetime.now().hour == 0:
        df = pd.read_csv(filepath)
        df['Date'] = pd.to_datetime(df['Date'])
        df = df[df['Date'] >= (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d 00:00:00')]
        df.to_csv(filepath, index=False)

if len(d) > 0:
    f = open(dirContaminantes+'ultimaFecha.txt', 'w')
    f.write(str(d['Date'][len(d)-1]))
    f.close() 

print(datetime.today(), ' done. Quiting...')