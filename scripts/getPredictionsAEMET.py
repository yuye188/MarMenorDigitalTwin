import requests
import pandas as pd
from os.path import exists
import datetime

print('Starting:', datetime.datetime.now())

def extraerData(datosJson):


    start = pd.Timestamp(datosJson['fecha']).date()

    end = start + pd.DateOffset(days=1)
    # Create an hourly date range for start-end dates (00:00:00 to 23:00:00)
    horas = pd.date_range(start=start, end=end - pd.Timedelta(seconds=1), freq='h')

    df = pd.DataFrame(datosJson['precipitacion'])
    df['temperatura'] = pd.DataFrame(datosJson['temperatura'])['value']
    df['hr'] = pd.DataFrame(datosJson['humedadRelativa'])['value']
    df.drop(columns=['periodo'], inplace=True)
    df.columns = ['prec', 'temperatura', 'hr']
    df['Fecha'] = horas[:len(df)]
    df = df[['Fecha', 'prec', 'temperatura', 'hr']]
    return df


# Para predicción
codigos = ['30021', '30902', '30037', '30016', '30035', '30036', '30041']
nombres = ['FuenteAlamo', 'LosAlcazares', 'TorrePacheco', 'Cartagena', 'SanJavier', 'SanPedro', 'LaUnion']

headers = {"accept": "application/json", 
           "api_key": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ5dS55ZUB1bS5lcyIsImp0aSI6IjNjNzZjZjlhLTZmNWEtNDgxOS1hNjEyLTQ5ZmM1NzVmNWM0NSIsImlzcyI6IkFFTUVUIiwiaWF0IjoxNjc3ODQ1OTAxLCJ1c2VySWQiOiIzYzc2Y2Y5YS02ZjVhLTQ4MTktYTYxMi00OWZjNTc1ZjVjNDUiLCJyb2xlIjoiIn0.nyxGpzZxgJOFUqYLdttY1qqIjREUrleAxctOJnF5-Os"}

for i in range(len(codigos)):

    file_path_manana = "/home/thinking/raw/aemet/prediction/" + nombres[i] + '/' + nombres[i] + 'manana.csv'
    file_exists_manana = exists(file_path_manana)

    # Las predicciones del pasado mañana no se están guardando porque no dan el día completo (AEMET se van actualizando sus predicciones de 48 en ciertas horas del día)
    #file_path_pasado = "/Yu/aemet/prediction/"+nombres[i]+'pasado.csv'
    #file_exists_pasado = exists(file_path_pasado)
    
    url = 'https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/horaria/' + codigos[i]

    # Obtener el url de los datos de la estación meteo. seleccionada
    r = requests.get(url, headers=headers, timeout=30)
    url_datas = r.json()['datos']
    # Obtener los datos de predicción
    r = requests.get(url_datas, timeout=30)

    manana = extraerData(r.json()[0]['prediccion']['dia'][1])
    #pasadoManana = extraerData(r.json()[0]['prediccion']['dia'][2])

    manana.to_csv(file_path_manana, mode='a', header=True if file_exists_manana == False else False, index=False)
    #pasadoManana.to_csv(file_path_pasado, mode='a', header=True if file_exists_pasado == False else False, index=False)


print(datetime.datetime.now(), ' done...')