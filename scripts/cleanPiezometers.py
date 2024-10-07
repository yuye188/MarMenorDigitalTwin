import pandas as pd
import os

piezoDir = '/home/thinking/raw/SAIHdatasActualizados/piezometros/'
dirs = os.listdir(piezoDir)
for dir in dirs:
    if 'txt' in dir:
        continue

    ficheros = os.listdir(piezoDir+dir)
    for f in ficheros:
        if 'from_parquet' in f:
            print(piezoDir+dir+'/'+f)
            os.remove(piezoDir+dir+'/'+f)