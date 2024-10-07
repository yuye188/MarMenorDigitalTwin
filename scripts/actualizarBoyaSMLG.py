import requests 
import pandas as pd
import datetime
import time
import os

dir = '/home/thinking/raw/boyaSMLG/'

propiedades = ['Temperatura Aire', 'Humedad Relativa', 'Presión Vapor', 'Velocidad Viento', 'Temperatura Agua', 'Concentración O2',
                  'Saturación O2', 'Conductividad', 'Clorofila', 'Turbidez', 'Temperatura Agua SB', 'Conductividad SB', 'Presión SB',
                  'Temperatura Agua EC1550']

propiedades_sin_tilde = ['Temperatura Aire', 'Humedad Relativa', 'Presion Vapor', 'Velocidad Viento', 'Temperatura Agua', 'Concentracion O2',
                  'Saturacion O2', 'Conductividad', 'Clorofila', 'Turbidez', 'Temperatura Agua SB', 'Conductividad SB', 'Presion SB',
                  'Temperatura Agua EC1550']

toDate = str(int(datetime.datetime.now().timestamp())*1000) 
print(datetime.datetime.fromtimestamp(int(toDate)/1000))

for idx, propiedad in enumerate(propiedades):

    filepath = dir+propiedades_sin_tilde[idx]+'.csv'
    df = pd.read_csv(filepath)
    
    fromDate = datetime.datetime.fromisoformat(df.iloc[-1,]['Date']) + datetime.timedelta(minutes=1)
    fromDate = str(int(fromDate.timestamp())*1000) 

    url = 'https://platon.grc.upv.es/sensingtools-api/api/user-hard-sensors/Fko-LYcB0U_NvF-sOx7d/data?sensorTypeNames='+propiedad+ \
          '&fromDate='+fromDate+'&toDate='+toDate+'&locationIds=Fko-LYcB0U_NvF-sOx7d_Profundidad:%205m_37.70940_-0.78552_-5,Fko-LYcB0U_NvF-sOx7d_Profundidad:%200.5m_37.70940_-0.78552_-0.5,Fko-LYcB0U_NvF-sOx7d_Profundidad:%200m_37.70940_-0.78552_0,Fko-LYcB0U_NvF-sOx7d_Profundidad:%201.5m_37.70940_-0.78552_-1.5,Fko-LYcB0U_NvF-sOx7d_Profundidad:%201m_37.70940_-0.78552_-1,Fko-LYcB0U_NvF-sOx7d_Profundidad:%202.5m_37.70940_-0.78552_-2.5,Fko-LYcB0U_NvF-sOx7d_Profundidad:%202m_37.70940_-0.78552_-2,Fko-LYcB0U_NvF-sOx7d_Profundidad:%203m_37.70940_-0.78552_-3,Fko-LYcB0U_NvF-sOx7d_Profundidad:%204m_37.70940_-0.78552_-4,Fko-LYcB0U_NvF-sOx7d_Profundidad:%206.5m_37.70940_-0.78552_-6.5,Fko-LYcB0U_NvF-sOx7d_Profundidad:%206m_37.70940_-0.78552_-6&group=none'
    result = requests.get(url).json()

    dfs = []
    df_final = pd.DataFrame()
    for key, value in zip(result['series'][propiedad].keys(), result['series'][propiedad].values()):
        if len(value) > 0:
            profundidad = key.split(': ')[1].split('_')[0]
            df = pd.DataFrame(value, columns=['Date', profundidad])
            df['Date'] = df['Date'].apply( lambda x : datetime.datetime.fromtimestamp(x/1000)) # The time (in miliseconds) is divided by 1000 to convert in seconds
            dfs.append(df)
            df.drop_duplicates(subset='Date', inplace=True)

    if len(dfs) > 0:
        print('New data of ->', propiedad)
        df_final = dfs[0]
        for i in range(1, len(dfs)):
            df_final = pd.merge(df_final, dfs[i], on='Date', how='outer')

        df_final.to_csv(filepath, mode='a', header=True if os.path.exists(filepath) == False else False, index=False)
    print(propiedad, 'processed.')
    time.sleep(2)
    print()