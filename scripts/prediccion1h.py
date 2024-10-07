#!/usr/bin/env python
# coding: utf-8

# In[83]:



import os
import pandas as pd
import numpy as np
from glob import glob
import re
import random
import joblib
import datetime

# In[84]:


# Obtener la fecha y hora actual
fecha_hora_actual = datetime.datetime.now()

# Modificar los minutos y segundos
minutos_definidos = 40
segundos_definidos = 00

# Establecer los minutos y segundos definidos
fecha_hora_modificada = fecha_hora_actual.replace(minute=minutos_definidos, second=segundos_definidos)

# Formatear la fecha y hora modificada
fecha_hora_actual = fecha_hora_modificada.strftime("%Y-%m-%d %H:%M:%S")

dias_a_restar = 3
fecha_hora_atras = fecha_hora_modificada - datetime.timedelta(days=dias_a_restar)

fecha_hora_atras_ok = fecha_hora_atras.strftime("%Y-%m-%d %H:%M:%S")

#print(fecha_hora_actual)
#print(fecha_hora_atras_ok)


# In[85]:


# Esto es para obtener el path donde encontramos los datos de las ramblas
path = "/home/thinking/raw/SAIHdatasActualizados/Ramblas/*/*"  


# In[86]:


# Nombre de todos los archivos
files = glob(path, recursive=True)
files


# In[87]:


def cargar_csv(ruta_directorio, variable):
    archivos = {}
    for root, dirs, files in os.walk(ruta_directorio):
        for file in files:
            if re.search(fr'.*{variable}.csv', file, re.IGNORECASE):  # Utilizando expresión regular
                filepath = os.path.join(root, file)
                df = pd.read_csv(filepath)  # cargar el archivo csv
                # Obtener la última carpeta en la ruta como la clave
                _, ultima_carpeta = os.path.split(root)
                archivos[ultima_carpeta] = df  # guardar el dataframe en el diccionario

    return archivos


# In[88]:


caudales_prueba = cargar_csv('/home/thinking/raw/SAIHdatasActualizados/Ramblas/', "Caudal")

# Eliminar los espacios de los nombres
caudales = {clave.replace(' ', ''): valor for clave, valor in caudales_prueba.items()}


# In[59]:


# Piezometros
#Piezometros_prueba = cargar_csv('../raw/SAIHdatasActualizados/piezometros/', "Piezometrico")

# Eliminar los espacios de los nombres
#Piezometrico = {clave.replace(' ', ''): valor for clave, valor in Piezometros_prueba.items()}


# In[89]:


for key, dataframe in caudales.items():
    dataframe['Date'] = pd.to_datetime(dataframe['Date'])

for key, dataframe in caudales.items():
    caudales[key] = dataframe.replace('-', np.nan)

for key, dataframe in caudales.items():
    dataframe[dataframe.columns[1]] = dataframe[dataframe.columns[1]].astype(float)


# In[90]:


# Aqui debería tomar 3 días menos y actualizar esto como parametro junto con las horas disponibles
# Fecha de sistema y dos años 
DateIndex = pd.DataFrame(pd.date_range(start=pd.to_datetime(fecha_hora_atras_ok), # 2021-01-12 12:00:00
                                       end = (fecha_hora_actual), 
                                       freq = '5T'),
                        columns=['Date'])

resultado = DateIndex

for name, df in caudales.items():
        resultado = resultado.merge(df, on='Date', how='left')
        resultado = resultado.rename(columns={'Caudal':name})
        
caudales_ok = resultado.drop_duplicates(subset='Date')
caudales_ok = caudales_ok.dropna(how='all')
caudales_ok = caudales_ok.tail(576)
caudales_ok.fillna(method='ffill', inplace=True)


# In[62]:


# for key, dataframe in Piezometrico.items():
#     dataframe['Date'] = pd.to_datetime(dataframe['Date'])

# for key, dataframe in Piezometrico.items():
#     Piezometrico[key] = dataframe.replace('-', np.nan)

# for key, dataframe in Piezometrico.items():
#     dataframe[dataframe.columns[1]] = dataframe[dataframe.columns[1]].astype(float)

# # Aqui debería tomar 3 días menos y actualizar esto como parametro junto con las horas disponibles
# # La información piezo metrica falla
# DateIndex = pd.DataFrame(pd.date_range(start=pd.to_datetime('2023-11-18 12:00:00'), # 2021-01-12 12:00:00
#                                        end = ('2023-11-22 9:00:00'), 
#                                        freq = '5T'),
#                         columns=['Date'])

# resultado = DateIndex

# for name, df in Piezometrico.items():
#         resultado = resultado.merge(df, on='Date', how='left')
#         resultado = resultado.rename(columns={'Piezometrico':name})
        
# Piezometrico_ok = resultado.drop_duplicates(subset='Date')


# In[91]:


# Cargamos los pluviometros
pluvios = cargar_csv('/home/thinking/raw/SAIHdatasActualizados/Ramblas/', "Pluviometro")


# In[92]:


pluviometros = {clave.replace(' ', ''): valor for clave, valor in pluvios.items()}

for key, dataframe in pluviometros.items():
    dataframe['Date'] = pd.to_datetime(dataframe['Date'])

for key, dataframe in pluviometros.items():
    pluviometros[key] = dataframe.replace('-', np.nan)

for key, dataframe in pluviometros.items():
    dataframe[dataframe.columns[1]] = dataframe[dataframe.columns[1]].astype(float)

# Aqui debería tomar 3 días menos y actualizar esto como parametro junto con las horas disponibles
# La información piezo metrica falla
DateIndex = pd.DataFrame(pd.date_range(start=pd.to_datetime(fecha_hora_atras_ok), # 2021-01-12 12:00:00
                                       end = (fecha_hora_actual), 
                                       freq = '5T'),
                        columns=['Date'])


resultado = DateIndex

for name, df in pluviometros.items():
        resultado = resultado.merge(df, on='Date', how='left')
        resultado = resultado.rename(columns={'Pluviometro':name})
        
pluviometros_ok = resultado.drop_duplicates(subset='Date')
pluviometros_ok = pluviometros_ok.dropna(how='all')
pluviometros_ok = pluviometros_ok.tail(576)
pluviometros_ok.fillna(method='ffill', inplace=True)


# In[93]:


# Función agregado, media, max y min configurable
def resample_df(df, fecha, codigo_temporal):
    # Asegurémonos de que la fecha sea un datetime
    df[f'{fecha}'] = pd.to_datetime(df[f'{fecha}'])

    # Establecer fecha con hora como indice
    df = df.set_index(f'{fecha}')


    # Resampling a agregados, promedio, máximo y mínimo por código temporal
    df_sum = df.resample(f'{codigo_temporal}').sum().add_prefix(f'sum_{codigo_temporal}')
    df_mean = df.resample(f'{codigo_temporal}').mean().add_prefix(f'mean_{codigo_temporal}')
    df_max = df.resample(f'{codigo_temporal}').max().add_prefix(f'max_{codigo_temporal}')
    df_min = df.resample(f'{codigo_temporal}').min().add_prefix(f'min_{codigo_temporal}')
    df_std = df.resample(f'{codigo_temporal}').std().add_prefix(f'std{codigo_temporal}')


    df_resampled = pd.concat([df_sum, df_mean, df_max, df_min, df_std], axis=1)

    return df_resampled


# In[94]:


resampleado_pluviometros = resample_df(pluviometros_ok, "Date","H")
resampleado_caudal = resample_df(caudales_ok, "Date","H")
#resampleado_Piezometrico = resample_df(Piezometrico_ok, "Date","H")

resampleado_pluviometros.columns = resampleado_pluviometros.columns.str.replace('-', '')
resampleado_caudal.columns = resampleado_caudal.columns.str.replace('-', '')
#resampleado_Piezometrico.columns = resampleado_Piezometrico.columns.str.replace('-', '')


# In[95]:


# Función para shiftear modificada
def shift_df(df, num_shifts, name_variable):
    dfs = []
    for i in range(2, num_shifts+1):
        df_shifted = df.shift(i-1)
        df_shifted = df_shifted.add_prefix(f'shift_{i}_{name_variable}')
        dfs.append(df_shifted)
    df_shifted_all = pd.concat(dfs, axis=1)
    return df_shifted_all


# In[96]:


# Función para shiftear
def add_prefix_to_columns(df,i, name_variable):
    df.columns = [f'shift_{i}_{name_variable}' + str(col) for col in df.columns]
    return df


# In[97]:


resampleado_caudal_nombre = add_prefix_to_columns(resampleado_caudal, "1","caudal")
resampleado_pluviometros_nombre = add_prefix_to_columns(resampleado_pluviometros, "1","pluvios")
#resampleado_Piezometrico_nombre = add_prefix_to_columns(resampleado_Piezometrico, "1","Piezometrico")




resampleado_pluviometros = resample_df(pluviometros_ok, "Date","H")
resampleado_caudal = resample_df(caudales_ok, "Date","H")
#resampleado_Piezometrico = resample_df(Piezometrico_ok, "Date","H")

resampleado_pluviometros.columns = resampleado_pluviometros.columns.str.replace('-', '')
resampleado_caudal.columns = resampleado_caudal.columns.str.replace('-', '')
#resampleado_Piezometrico.columns = resampleado_Piezometrico.columns.str.replace('-', '')


# In[98]:


# Función de rolling
def rolling_df(df, fecha, codigo_temporal, variable):
    # Asegurémonos de que la fecha sea un datetime
    df[f'{fecha}'] = pd.to_datetime(df[f'{fecha}'])

    # Establecer fecha con hora como indice
    df = df.set_index(f'{fecha}')

    df_rolling_mean = df.rolling(window=codigo_temporal).mean().add_prefix(f'mean_rolling_{codigo_temporal}_{variable}')
    df_rolling_sum = df.rolling(window=codigo_temporal).sum().add_prefix(f'sum_rolling_{codigo_temporal}_{variable}')
    df_rolling_max = df.rolling(window=codigo_temporal).max().add_prefix(f'max_rolling_{codigo_temporal}_{variable}')
    df_rolling_min = df.rolling(window=codigo_temporal).min().add_prefix(f'min_rolling_{codigo_temporal}_{variable}')
    df_rolling_std = df.rolling(window=codigo_temporal).std().add_prefix(f'std_rolling_{codigo_temporal}_{variable}')

    df_rolling = pd.concat([df_rolling_mean, df_rolling_sum, df_rolling_max, df_rolling_min, df_rolling_std], axis=1)

    return df_rolling


# In[99]:


resampleado_caudal_nombre['Date'] = resampleado_caudal_nombre.index
resampleado_pluviometros_nombre['Date'] = resampleado_pluviometros_nombre.index
#resampleado_Piezometrico_nombre['Date'] = resampleado_Piezometrico_nombre.index

# Hacer un rolado 6, 12, 18 de caudales y pluvios (Esto serían valores sobre el dataset cada 5 min)
rolado_caudal_6 =rolling_df(resampleado_caudal_nombre, "Date", 6, "caudal")
rolado_caudal_12 =rolling_df(resampleado_caudal_nombre, "Date", 12, "caudal")
rolado_caudal_18 =rolling_df(resampleado_caudal_nombre, "Date", 18, "caudal")


rolado_pluvios_6 =rolling_df(resampleado_pluviometros_nombre, "Date", 6, "pluvios")
rolado_pluvios_12 =rolling_df(resampleado_pluviometros_nombre, "Date", 12, "pluvios")
rolado_pluvios_18 =rolling_df(resampleado_pluviometros_nombre, "Date", 18, "pluvios")

#rolado_Piezometrico_6 =rolling_df(resampleado_Piezometrico_nombre, "Date", 6, "Piezometrico")
#rolado_Piezometrico_12 =rolling_df(resampleado_Piezometrico_nombre, "Date", 12, "Piezometrico")
#rolado_Piezometrico_18 =rolling_df(resampleado_Piezometrico_nombre, "Date", 18, "Piezometrico")


caudal_shift_6 = shift_df(resampleado_caudal, 6, 'caudal')
pluviometros_shift_6 = shift_df(resampleado_pluviometros, 6, 'pluvios')
#Piezometrico_shift_6 = shift_df(resampleado_Piezometrico, 6, 'Piezometrico')


# In[100]:


#result = pd.concat([resampleado_caudal_nombre, resampleado_pluviometros_nombre, resampleado_Piezometrico_nombre,
#                    caudal_shift_6, pluviometros_shift_6, Piezometrico_shift_6, rolado_caudal_6, rolado_caudal_12, rolado_caudal_18,
#                    rolado_pluvios_6,  rolado_pluvios_12, rolado_pluvios_18, rolado_Piezometrico_6, rolado_Piezometrico_12, rolado_Piezometrico_18], axis=1)


result = pd.concat([resampleado_caudal_nombre, resampleado_pluviometros_nombre,
                   caudal_shift_6, pluviometros_shift_6, rolado_caudal_6, rolado_caudal_12, rolado_caudal_18,
                   rolado_pluvios_6,  rolado_pluvios_12, rolado_pluvios_18], axis=1)

# Pendiente de mejora
result_sinna = result.dropna()


# In[101]:


# Cargar el modelo
modela, ref_colsa = joblib.load("/home/thinking/src/model_for_yu_selected_server.pkl")


# In[102]:


df_pre_seleccion_predict = result[ref_colsa]

# seleccionar la última fila de un dataframe
last_row = df_pre_seleccion_predict.tail(1)


# In[103]:


predictions = modela.predict(last_row)





# In[ ]:


data = {'Date': last_row.index[0].strftime('%Y-%m-%d %H:%M:%S'),
        'Predicción_1H': predictions}

df_guardar = pd.DataFrame(data)

filepath = "/home/thinking/raw/salidaPredicciones/predicciones_06A01-La Puebla.csv"
df_guardar.to_csv(filepath, mode = "a", header=True if os.path.exists(filepath) == False else False, index=False)

import datetime
print(datetime.datetime.now(), ' prediction done.')
