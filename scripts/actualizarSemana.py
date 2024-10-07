from datetime import datetime, timedelta
import requests
import re
import json 
import pandas as pd
import os 
import traceback
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.csv as pv


last_sun = datetime.now() - timedelta(days=datetime.now().weekday()+1)

def actualizarContaminantes(last_sun):
    fechaInicio = (last_sun - timedelta(days=6)).strftime('%d/%m/%Y') # El lunes pasado, retrocediendo 6 dias
    fechaFin = (last_sun + timedelta(days=1)).strftime('%d/%m/%Y') # Domingo + 1 dia, p.e. queremos datos hasta el dia 29, esta fecha seria 30
    dir = '/home/thinking/raw/contaminantesData/'
    print(fechaInicio, fechaFin)
    try:
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
            filepath= dir+dfs[i].iloc[0]['Comunidad']
            d = dfs[i].sort_index()

            # Cambiar los , por . (los datos crudos vienen , para decimales)
            for c in columnas[2:]:
                d[c] = d[c].apply(lambda x: x.replace(',','.'))

            concated = pd.concat([pd.read_parquet(filepath+'.parquet'), d])
            concated.to_parquet(filepath+'.parquet', index=False)
            #os.remove(filepath+'.csv')
            
        # Actualizar la ultima fecha de los datos (para csv), que será el domingo pasado a las 23:00 -> no hace falta modificar la ultima fecha porque no toca los ficheros csv
        #f = open(dir+'ultimaFecha.txt', 'w')
        #f.write(last_sun.date().strftime('%Y-%m-%d 23:00:00'))
        #f.close() 
    except Exception as e:
        print("*** Error en actualizar contaminantes:")
        traceback.print_exc()
        return 


def actualizarPunto(dir, last_sun):
    dirRamblas = dir
    print('----- ' + dirRamblas + ' -----')
    subdirRamblas = os.listdir(dirRamblas)

    # Definir las fechas de inicio y final
    fechaInicio =  (last_sun - timedelta(days=6)).strftime('%d/%m/%Y') + ' 00:00:00'
    fechaFin = last_sun.strftime('%d/%m/%Y')+' 23:55:00'


    # Obtener el cookie autorizado para cada punto
    s = requests.Session()
    try:
        s.get('https://saihweb.chsegura.es/apps/iVisor/index.php')
    except Exception as e:
        print('*** Error para conseguir el cookie de SAIH')
        print('cookie:')
        print(s.cookies.get_dict())
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
            
            print(f)

            # Obtener el código del parámetro 
            codigo = f.split('-')[0]
            param = f.split('-')[1].split('.')[0]
            rutaCompleta = dirRamblas+subdir+'/'+codigo

            try:
                url = 'https://saihweb.chsegura.es/apps/iVisor/graficas/graficaVar.php?puntos='+codigo+'&dfrom='+fechaInicio+'&dto='+fechaFin+'&source=I'
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
                df['Date'] = pd.to_datetime(df['Date'], unit='ms').apply(lambda x: str(x))
                
                # Guardar el fichero de datos nuevos
                filepath = rutaCompleta+'-'+param
                df.to_csv(filepath+'.csv', index=False)

                # Leer el fichero parquet (antes) y el fichero csv (hoy)
                tableParquet = pq.read_table(filepath+'.parquet')
                tableCSV = pv.read_csv(filepath+'.csv', convert_options=pv.ConvertOptions(column_types=tableParquet.schema))

                # Combine the tables
                combined_table = pa.concat_tables([tableParquet, tableCSV])

                # Write the combined table to a new Parquet file
                pq.write_table(combined_table, filepath+'.parquet')

                
                #concated = pd.concat([pd.read_parquet(filepath+'.parquet'), df])
                #concated[param] = concated[param].apply(lambda x: str(x))
                #concated.to_parquet(filepath+'.parquet', index=False)
                
                #os.remove(filepath+'.csv')
                    
            except Exception as e:
                print('****** Error en ',rutaCompleta,', Fecha=', fechaInicio)
                traceback.print_exc()
                return

    # Actualizar la ultima fecha de los datos (para csv), que será el domingo pasado a las 23:55 -> quitar esto y descomentar os.remove(fichero_csv_temporal)
    f = open(dirRamblas+'ultimaFecha.txt', 'w')
    f.write(last_sun.date().strftime('%Y-%m-%d 23:55:00'))
    f.close() 

actualizarContaminantes(last_sun)

actualizarPunto('/home/thinking/raw/SAIHdatasActualizados/Ramblas/', last_sun)
actualizarPunto('/home/thinking/raw/SAIHdatasActualizados/piezometros/', last_sun)


def actualizarBoyaSMLG():
    
    dir = '/home/thinking/raw/boyaSMLG/'

    propiedades = ['Temperatura Aire', 'Humedad Relativa', 'Presión Vapor', 'Velocidad Viento', 'Temperatura Agua', 'Concentración O2',
                    'Saturación O2', 'Conductividad', 'Clorofila', 'Turbidez', 'Temperatura Agua SB', 'Conductividad SB', 'Presión SB',
                    'Temperatura Agua EC1550']

    propiedades_sin_tilde = ['Temperatura Aire', 'Humedad Relativa', 'Presion Vapor', 'Velocidad Viento', 'Temperatura Agua', 'Concentracion O2',
                    'Saturacion O2', 'Conductividad', 'Clorofila', 'Turbidez', 'Temperatura Agua SB', 'Conductividad SB', 'Presion SB',
                    'Temperatura Agua EC1550']

    for idx, propiedad in enumerate(propiedades):

        filepath = dir+propiedades_sin_tilde[idx]
        df_original = pd.read_csv(filepath+'.csv')
        
        # Coger la segunda fila del Date y sumarle 1 minutos
        fromDate = datetime.fromisoformat(df_original.iloc[1,]['Date']) + timedelta(minutes=1)
        fromDate = str(int(fromDate.timestamp())*1000) 

        # Coger la ultima fila del Date
        toDate = str(int(datetime.fromisoformat(df_original.iloc[-1,]['Date']).timestamp())*1000) 

        url = 'https://platon.grc.upv.es/sensingtools-api/api/user-hard-sensors/Fko-LYcB0U_NvF-sOx7d/data?sensorTypeNames='+propiedad+ \
            '&fromDate='+fromDate+'&toDate='+toDate+'&locationIds=Fko-LYcB0U_NvF-sOx7d_Profundidad:%205m_37.70940_-0.78552_-5,Fko-LYcB0U_NvF-sOx7d_Profundidad:%200.5m_37.70940_-0.78552_-0.5,Fko-LYcB0U_NvF-sOx7d_Profundidad:%200m_37.70940_-0.78552_0,Fko-LYcB0U_NvF-sOx7d_Profundidad:%201.5m_37.70940_-0.78552_-1.5,Fko-LYcB0U_NvF-sOx7d_Profundidad:%201m_37.70940_-0.78552_-1,Fko-LYcB0U_NvF-sOx7d_Profundidad:%202.5m_37.70940_-0.78552_-2.5,Fko-LYcB0U_NvF-sOx7d_Profundidad:%202m_37.70940_-0.78552_-2,Fko-LYcB0U_NvF-sOx7d_Profundidad:%203m_37.70940_-0.78552_-3,Fko-LYcB0U_NvF-sOx7d_Profundidad:%204m_37.70940_-0.78552_-4,Fko-LYcB0U_NvF-sOx7d_Profundidad:%206.5m_37.70940_-0.78552_-6.5,Fko-LYcB0U_NvF-sOx7d_Profundidad:%206m_37.70940_-0.78552_-6&group=none'
        result = requests.get(url).json()

        dfs = []
        df_final = pd.DataFrame()
        for key, value in zip(result['series'][propiedad].keys(), result['series'][propiedad].values()):
            # Si hay valor en una profundidad
            if len(value) > 0:
                profundidad = key.split(': ')[1].split('_')[0]
                df = pd.DataFrame(value, columns=['Date', profundidad])
                df['Date'] = df['Date'].apply( lambda x : datetime.fromtimestamp(x/1000)) # The time (in miliseconds) is divided by 1000 to convert in seconds
                df.drop_duplicates(subset='Date', inplace=True)
                dfs.append(df)

        # Si hay datos nuevos en alguna (o en todas) variables
        if len(dfs) > 0:
            print('New data of ->', propiedad)
            df_final = dfs[0]
            for i in range(1, len(dfs)):
                df_final = pd.merge(df_final, dfs[i], on='Date', how='outer')

            concated = pd.concat([pd.read_parquet(filepath+'.parquet'), df_final])
            concated.to_parquet(filepath+'.parquet', index=False)
            
            # Eliminar el archivo .csv original dejando solamente las 2 ultimas filas
            df_original = df_original.iloc[-2:,]
            df_original.to_csv(filepath+'.csv', index=False)

        print(propiedad, 'processed.')
        print()

actualizarBoyaSMLG()