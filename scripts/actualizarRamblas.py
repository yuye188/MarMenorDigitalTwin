import time
import requests
import re
import json 
import pandas as pd
import datetime
import os 
import traceback

now = datetime.datetime.today() 
eliminarDia = now.hour == 0

def ActualizarDatosSAIH(dir, hora, eliminarDia): 
    print('----- ' + dir + ' -----')
    print(hora)
    dirRamblas = dir
    subdirRamblas = os.listdir(dirRamblas)
    f = open(dirRamblas+'ultimaFecha.txt', 'r')

    # Sumar 5 minutos para no repetir el ultimo valor
    ultimoDia = datetime.datetime.strptime(f.read().strip(), "%Y-%m-%d %H:%M:%S") + datetime.timedelta(minutes=5)
    ahora = hora


    # Obtener el cookie autorizado para cada punto
    s = requests.Session()
    try:
        s.get('https://saihweb.chsegura.es/apps/iVisor/index.php')
        if s.cookies.get_dict().get('SAIHSESSIONID') == None:
            print('*** Error: No hay cookie de SAIH')
            return
    except Exception as e:
        print('*** Error para conseguir el cookie de SAIH')
        traceback.print_exc()
        return

    for subdir in subdirRamblas:

        # Saltar el fichero 'ultimaFecha.txt'
        if 'txt' in subdir:
            continue

        # Obtener los ficheros de cada punto
        ficheros = os.listdir(dirRamblas+subdir)

        for f in ficheros:

            # Evitar que se duplicar la ejecucion para el mismo parametro (uno en .parquet y otro en .csv)
            if 'parquet' not in f:
                continue

            # Obtener el código del parámetro 
            codigo = f.split('-')[0]
            Finicio = ultimoDia
            Ffinal = ahora

            total = None
            param = f.split('-')[1].split('.')[0]

            rutaCompleta = dirRamblas+subdir+'/'+codigo

            try:
                url = 'https://saihweb.chsegura.es/apps/iVisor/graficas/graficaVar.php?puntos='+codigo+'&dfrom='+Finicio.strftime("%d/%m/%Y %H:%M")+'&dto='+Ffinal.strftime("%d/%m/%Y %H:%M:%S")+'&source=I'
                r = requests.get(url, cookies=s.cookies)

                rangos = re.search(r'data: \[(.*?)}]', r.text,  re.DOTALL).span()
                data = r.text[rangos[0]+6: rangos[1]]

                # Add double quotes to keys and string values using regular expressions
                # replace single with double quote
                data = data.replace("'", '"')

                # wrap key with double quotes
                data = re.sub(r"(\w+)\s?:", r'"\1":', data)

                # wrap value with double quotes
                # but not for interger or boolean
                data = re.sub(r":\s?(?!(\d+|true|false))(\w+)", r':"\2"', data)

                # Load the data as a Python JSON object
                json_obj = json.loads( data)

                # Crear el df extrayendo los datos
                df = pd.DataFrame(json_obj).drop(columns=['ac','fb','pr', 'y']).rename(columns={'x': 'Date', 'y0': param})
                df['Date'] = pd.to_datetime(df['Date'], unit='ms')
                total = pd.concat([total, df], ignore_index=True)

                # Guardar el df en el fichero
                filepath = rutaCompleta+'-'+param+'.csv'
                total.to_csv(filepath, mode='a', header=True if os.path.exists(filepath) == False else False, index=False)

                # si son las 12 (ya ha pasado el día), elminar el octavo día anterior (guardar solo 7 días)
                if eliminarDia:
                    df = pd.read_csv(filepath)
                    df['Date'] = pd.to_datetime(df['Date'])
                    df = df[df['Date'] >= (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d 00:00:00')]
                    df.to_csv(filepath, index=False)
                    
            except Exception as e:
                print('****** Error en ',rutaCompleta,', Fecha=', Finicio)
                traceback.print_exc()
                return
            
            print(codigo+' done, sleeping 1s...')
            time.sleep(1) 
        
    f = open(dirRamblas+'ultimaFecha.txt', 'w')
    f.write(str(total['Date'][len(total)-1]))
    f.close() 
    print(datetime.datetime.now(), ' Update finished.')

ActualizarDatosSAIH('/home/thinking/raw/SAIHdatasActualizados/Ramblas/', now - datetime.timedelta(minutes=10), eliminarDia) # Restar 10 minutos por si el valor más nuevo no está disponible aún

if now.hour == 9:
    ActualizarDatosSAIH('/home/thinking/raw/SAIHdatasActualizados/piezometros/', now - datetime.timedelta(hours=7, minutes=10), True) #Restar 12 horas para que sea justo a las 2:00 (cuando se actualiza los datos de piezometros)