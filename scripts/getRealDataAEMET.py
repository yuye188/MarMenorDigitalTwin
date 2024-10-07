# Para datos del día anterior
import requests
import pandas as pd
from os.path import exists
import datetime

codigos = ['7023X', '7026X', '7012D', '7031X']
nombre = ['FuenteAlamo', 'TorrePacheco', 'Cartagena', 'SanJavier',]

headers = {"accept": "application/json", 
           "api_key": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ5dS55ZUB1bS5lcyIsImp0aSI6IjNjNzZjZjlhLTZmNWEtNDgxOS1hNjEyLTQ5ZmM1NzVmNWM0NSIsImlzcyI6IkFFTUVUIiwiaWF0IjoxNjc3ODQ1OTAxLCJ1c2VySWQiOiIzYzc2Y2Y5YS02ZjVhLTQ4MTktYTYxMi00OWZjNTc1ZjVjNDUiLCJyb2xlIjoiIn0.nyxGpzZxgJOFUqYLdttY1qqIjREUrleAxctOJnF5-Os"}
print('Starting:', datetime.datetime.now())
for i in range(len(codigos)):

    file_path = "/home/thinking/raw/aemet/real/"+nombre[i]+'.csv'
    file_exists = exists(file_path)
    
    url = 'https://opendata.aemet.es/opendata/api/observacion/convencional/datos/estacion/'+codigos[i]
    # Obtener el url de los datos de la estación meteo seleccionada
    r = requests.get(url, headers=headers, timeout=30)
    url_datas = r.json()['datos']

    # Obtener los datos reales
    r = requests.get(url_datas, timeout=30)
    df = pd.DataFrame(r.json())[['fint', 'prec', 'hr', 'ta', 'tamin', 'tamax']]
    df['fint'] = pd.to_datetime(df['fint'])
    
    real = pd.read_csv(file_path)
    real['fint'] = real['fint'].apply(lambda x: x.split('+')[0])
    df = df[df['fint'] > real['fint'].iloc[-1]]
    if len(df) > 0:
        print('new data in', nombre[i],  real['fint'].iloc[-1])
        df.to_csv(file_path, mode='a', header=True if file_exists == False else False, index=False)

