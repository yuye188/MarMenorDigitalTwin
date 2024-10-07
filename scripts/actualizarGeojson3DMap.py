import json
import pandas as pd
import os
import datetime

f = open('/home/thinking/raw/3DGeojson/ramblas.json','r')
map = json.load(f)
f.close()

# Eliminar la boya SMLG
map['features'].pop()

# Actualizar los datos de las ramblas
for i in map['features']:
    dir = '/home/thinking/raw/SAIHdatasActualizados/Ramblas/'+i['id'] +'-'+ i['properties']['name'] + '/'
    print(dir)
    params_files = os.listdir(dir)
    for param in params_files:
        if 'csv' not in param:
            continue
        valor = pd.read_csv(dir+'/'+param).iloc[-1,1]
        i['properties'][param.split('-')[1].split('.')[0]] = valor if pd.notna(valor) else 'null'

boyaSMLG = {'type': 'Feature',
            'geometry': {'coordinates': [-0.78552, 37.7094],
            'type': 'Point'},
            'id': 'BuoySMLG'}

# Crear el objeto de la boya SMLG
dir = '/home/thinking/raw/boyaSMLG/'
params_files = os.listdir(dir)
properties = {'name': 'BuoySMLG'}
for param in params_files:
   if 'csv' not in param:
      continue
   profundidades = pd.read_csv(dir+'/'+param).iloc[-1,1:]
   for idx, p in enumerate(profundidades):
      properties[param.split('.')[0]+ '_' +profundidades.index.values[idx]] = str(p if pd.notna(p) else 'null')
boyaSMLG['properties'] = properties
print('Boya añadida')

# Añadir la boya SMLG la lista
map['features'].append(boyaSMLG)

# Escribir el obj a Json
with open('/home/thinking/raw/3DGeojson/ramblas.json', 'w') as f:
    json.dump(map,f)

print(datetime.datetime.now(), 'Done...')




