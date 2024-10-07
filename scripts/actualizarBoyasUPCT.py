import os
import xarray as xr
import pandas as pd
import geopy.distance

def actualizarLocData(df_param, coord_original, name_param, num_ctd):

    num_ctd = num_ctd + 1
    if num_ctd > 4: # Se salta el CTD5 
        num_ctd = num_ctd + 1 
    newpath = '/home/thinking/raw/boyaUPCT/extractedData/CTD' + str(num_ctd)

    df_param['time'] = pd.to_datetime(df_param['time'])

    df_pasado = pd.read_csv(newpath+'/'+name_param+'.csv')
    last_date = df_pasado['Date'].iloc[-1]

    df_param = df_param[df_param['time'] > last_date]
    if len(df_param) == 0:
        print('No new data for CTD',num_ctd,' -> ', name_param)
        return
    #print('longitud', len(df_param))

    closest_coord = None
    loc = []
    for f in df_param['time'].unique():
        #print(closest_coord)
        #print('closest_coord', closest_coord)
        
        single_day = df_param[(df_param['time'] == f)]

        single_day_closestPoint = pd.DataFrame()

        # Si ya se ha encontrado el punto más cercano, extracter los datos de ese punto
        if closest_coord != None:
            single_day_closestPoint = single_day[(single_day['latitude'] == closest_coord[0]) & (single_day['longitude'] == closest_coord[1])]
            print(f, 'same coords found:', len(single_day_closestPoint))
            #print('closest coord:', closest_coord)
            #print(single_day_closestPoint)

        # Si no se ha encontrado el punto más cercano o el punto encontrado anteriormente no existe para esta fecha, 
        # volver a buscar el punto más cercano y extraer los datos de ese punto
        if (closest_coord == None) | (len(single_day_closestPoint) == 0):
            print(f, 'closest_coord none or different')
            if 'Transparency' not in name_param:
                single_day_copy = single_day[single_day['level'] == 0.5].copy()
            else:
                single_day_copy = single_day.copy()
            
            single_day_copy['dis'] = single_day_copy.apply(lambda x: geopy.distance.geodesic(coord_original, (x.latitude, x.longitude)).km, axis=1)
            single_day_copy = single_day_copy.sort_values(by='dis').iloc[0:3,:] #[['longitude', 'latitude', 'dis', 'level', name_param]]
            closest_coord = (single_day_copy['latitude'].iloc[0], single_day_copy['longitude'].iloc[0])
            #print('closest coord:', closest_coord)
            single_day_closestPoint = single_day[(single_day['latitude'] == closest_coord[0]) & (single_day['longitude'] == closest_coord[1])]
            #print(single_day_closestPoint)
        
        if 'Transparency' in name_param:
            single_day_closestPoint = single_day_closestPoint[['time', name_param]].rename(columns={'time':'Date'})
        else:
            single_day_closestPoint = single_day_closestPoint[['level', name_param]].sort_values(by='level', ascending=True).drop_duplicates('level').set_index('level').transpose()
            single_day_closestPoint['Date'] = f
        
        #single_day_closestPoint['Date'] = pd.to_datetime(single_day_closestPoint['Date'])
        
        #print(single_day_closestPoint)
        loc.append(single_day_closestPoint)
    
    #print(pd.concat(loc))
    
    
    final_df = pd.concat(loc).sort_values(by='Date')

    # Poner la columna Date en la primera columna
    if 'Transparency' not in name_param:
        columns = final_df.columns.to_list()
        columns.remove('Date')
        cols = ['Date']
        cols.extend(columns)
        final_df = final_df[cols]

    filepath = newpath+'/'+name_param+'.csv'
    final_df.to_csv(filepath, index=False, mode='a', header=False,)
    
    #return closest_coord

def actualizarUPCTBoyaData(df_param, name_param):
    coords = [ (37.811800, -0.784483),
           (37.760617, -0.807800),
           (37.761783, -0.783550),
           (37.748233, -0.749617),
           #(37.740450, -0.727117), CTD5, fuera/salida del mar menor
           (37.710417, -0.778383),
           (37.718000, -0.839783),
           (37.694517, -0.810400),
           (37.666817, -0.809683),
           (37.659833, -0.781967),
           (37.651800, -0.728883),
           (37.687350, -0.783783)]

    for i in range(len(coords)):
        print(i, coords[i])
        actualizarLocData(df_param, coords[i], name_param, i)
  



dir = '/home/thinking/raw/boyaUPCT/ncFiles/'
files = os.listdir(dir)
for f in files:
    print(f)
    da = xr.open_dataset(dir+f) 
    name_param = f.split('.')[0]

    if 'Transparency' in name_param:
        tranparecny = da.to_dataframe()
        tranparecny = tranparecny['temp'].dropna().reset_index()
        tranparecny['time'] = pd.to_datetime(tranparecny['time'])
        tranparecny = tranparecny.sort_values(by='time').reset_index().drop(columns='index')
        tranparecny.columns = ['latitude', 'longitude', 'time', 'Transparency']
        actualizarUPCTBoyaData(tranparecny, name_param)
    else:
        df_param = da.to_dataframe()
        df_param = df_param[name_param].dropna().reset_index()
        actualizarUPCTBoyaData(df_param, name_param)