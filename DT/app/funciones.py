import requests
import pandas as pd
from folium import plugins, FeatureGroup, LayerControl
import folium
from datetime import datetime, timedelta
from geopy import distance
import numpy as np
import os
from apscheduler.schedulers.background import BackgroundScheduler
import plotly.express as px
import plotly.graph_objects as go
import plotly
import json
import glob
import utilities as util
from skforecast.utils import save_forecaster, load_forecaster
from tokens import cesiumToken, AQIToken



#### Variables ####
df_aqi = None
df_contaminantes = None
m = None

# Piezo
dirPiezometros = './piezometros/'
ficherosPiezometros = os.listdir(dirPiezometros)
ficherosPiezometros.remove('ultimaFecha.txt')
locPiezometros = pd.read_csv('./locSondeos.csv')
# Ramblas
dirRamblas = './Ramblas/'
ficherosRamblas = os.listdir(dirRamblas)
ficherosRamblas.remove('ultimaFecha.txt')
locRamblas = pd.read_csv('./locRamblas.csv')
# Sinqlair
dirSinqlair = './contaminantesData/'
locSinqlair = pd.read_csv('./locSinqlair.csv')
# BoyaUPCT
dirBoyaUPCT = './extractedData'
locBoyaUPCT = pd.read_csv('./locBoyasUPCT.csv')
# PredicLSTM
dirPredictions = './salidaPredicciones'
# AEMET
dirAEMET = './aemet/'
locAEMET = pd.read_csv('./locAEMET.csv')



urlAQI = "https://api.waqi.info/v2/map/bounds?latlng=43.973967,-10.341545,36.580506,5.130601&networks=all&token="+AQIToken

propiedadesSMLG = ['Temperatura Aire', 'Humedad Relativa', 'Presion Vapor', 'Velocidad Viento', 'Temperatura Agua', 'Concentracion O2',
                  'Saturacion O2', 'Conductividad', 'Clorofila', 'Turbidez', 'Temperatura Agua SB', 'Conductividad SB', 'Presion SB',
                  'Temperatura Agua EC1550']

propiedadesUPCT = ['CDOM', 'Clorofila', 'Oxigeno', 'PE', 'Salinidad', 'Temperatura', 'Transparency', 'Turbidez']

def getJsonFor3DMap():
    f = open('./3DGeojson/ramblas.json','r')
    geoson = json.load(f)
    f.close()

    f = open('./3DGeojson/caudalLine.json','r')
    caudalLine = json.load(f)
    f.close()

    return [geoson, caudalLine]

def getCesiumToken():
    return cesiumToken

# Obatain the AQI values of all peninsula using two points (43.973967,-10.341545),(36.580506,5.130601) to from the area.
# Return a df with following attributes: lat, lon, name, lastUpdate
def getAQI():
    
    r = requests.get(urlAQI, headers={"Content-Type": "application/json"})

    global df_aqi
    df_aqi = pd.DataFrame.from_dict(pd.json_normalize(r.json()['data']))
    df_aqi.rename(columns={'station.name': 'name',
                  'station.time': 'lastUpdate'}, inplace=True)
    df_aqi['name'] = df_aqi['name'].apply(lambda x: x.replace(", Spain", ""))
    df_aqi['lastUpdate'] = pd.to_datetime(
        df_aqi['lastUpdate']).apply(lambda x: x.to_datetime64())
    #print('Nº of location for AQI: ', len(df_aqi))
    return df_aqi

# get the map (if it is created, return it, if not, create it by calling renderMap())
def getMap():

    global m
    if m != None:
        print('m created')
        return m.get_root().render()
    else:
        print('m not created')
        return renderMap()
    
# Create the map with different component
def renderMap():
    
    global m
    m = folium.Map(location=[37.732304, -0.798740], zoom_start=9, tiles='https://{s}.basemaps.cartocdn.com/rastertiles/voyager_labels_under/{z}/{x}/{y}{r}.png', 
               attr='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>')
    folium.TileLayer(tiles='https://server.arcgisonline.com/ArcGIS/rest/services/NatGeo_World_Map/MapServer/tile/{z}/{y}/{x}', attr='Tiles &copy; Esri &mdash; National Geographic, Esri, DeLorme, NAVTEQ, UNEP-WCMC, USGS, NASA, ESA, METI, NRCAN, GEBCO, NOAA, iPC').add_to(m)
    folium.TileLayer('openstreetmap').add_to(m)

    ###### Boya SMLG #######
    fgBuoy = FeatureGroup(name='Buoy SMLG')
    folium.Marker(location=[37.70940, -0.78552],
                  icon=folium.Icon(color='cadetblue', icon='water', prefix='fa'),
                  popup=folium.Popup('<b> SmartLagoon Buoy </b><button class="btn btn-primary mt-4" type="button" onclick="getLatestData(0, 0 )" data-bs-toggle="offcanvas" data-bs-target="#offcanvasBottom" aria-controls="offcanvasBottom">View the latest data</button>')).add_to(fgBuoy)
    fgBuoy.add_to(m)


    ###### Boyas UPCT #######
    fgBuoyUPCT = FeatureGroup(name='Buoys UPCT')
    for i in range(len(locBoyaUPCT)):
            folium.Marker(location=[locBoyaUPCT['Latitud'][i], locBoyaUPCT['Longitud'][i]], 
                      icon=folium.Icon(color='lightblue', icon='water', prefix='fa'),
                      popup=folium.Popup('<b>'+ locBoyaUPCT['Nombre'][i] +' </b><button class="btn btn-primary mt-4" type="button" onclick="getLatestData(4, '+str(i)+' )" data-bs-toggle="offcanvas" data-bs-target="#offcanvasBottom" aria-controls="offcanvasBottom">View data</button>')
                      ).add_to(fgBuoyUPCT)
    fgBuoyUPCT.add_to(m)
    

    ###### Contaminantes de sinqlair #######
    fgSinqlair = FeatureGroup(name='Sinqlair stations')
    for i in range(len(locSinqlair)):
            loc = locSinqlair['Cod'][i]
            icon,popup = util.createSinqlairMarker(loc, i)
            folium.Marker(location=[locSinqlair['Latitud'][i], locSinqlair['Longitud'][i]], 
                      icon=folium.DivIcon(html=(icon)),
                      popup=folium.Popup(popup)
                      ).add_to(fgSinqlair)
    fgSinqlair.add_to(m)


    ###### AEMET #######
    fgAEMET = FeatureGroup(name='AEMET')
    for i in range(len(locAEMET)):
            folium.Marker(location=[locAEMET['Latitud'][i], locAEMET['Longitud'][i]], 
                      icon=folium.Icon(color='green', icon='fa-tint', prefix='fa'),
                      popup=folium.Popup('<b>'+ locAEMET['Nombre'][i] +' </b><button class="btn btn-primary mt-4" type="button" onclick="getLatestData(5, '+str(i)+' )" data-bs-toggle="offcanvas" data-bs-target="#offcanvasBottom" aria-controls="offcanvasBottom">View data</button>')
                      ).add_to(fgAEMET)
    fgAEMET.add_to(m)


    ###### Ramblas #######
    fgRamblas = FeatureGroup(name='Catchments')
    for i in range(len(locRamblas)):
            loc = locRamblas['CodPuntoControl'][i] +'-' + locRamblas['Nombre'][i]
            icon,popup = util.createRamblaMarker(locRamblas['CodPuntoControl'][i], loc, i)
            folium.Marker(location=[locRamblas['Latitud'][i], locRamblas['Longitud'][i]], 
                      icon=folium.DivIcon(html=(icon)),
                      popup=folium.Popup(popup) 
                      ).add_to(fgRamblas)
    fgRamblas.add_to(m)


    ###### Piezometros #######
    fgPiezometros = FeatureGroup(name='Piezometers')
    for i in range(len(locPiezometros)):
            loc = locPiezometros['CodPuntoControl'][i] +'-' + locPiezometros['Nombre'][i] 
            folium.Marker(location=[locPiezometros['Latitud'][i], locPiezometros['Longitud'][i]], 
                      icon=folium.DivIcon(html=('<svg height="100" width="100" ">'
                                '<rect  x="1" y="1" width="55" height="22" stroke="#ff6d00" stroke-width="1" fill="#f8e16c" />'
                                '<text x="7" y="17" fill="black" font-size="15" stroke="black" stroke-width="0.5">'+locPiezometros['CodPuntoControl'][i]+'</text>'
                                '</svg>')), 
                      popup=folium.Popup('<b>'+ loc +' </b><br /> <button class="btn btn-primary mt-4" type="button" onclick="getLatestData(2, '+str(i)+' )" data-bs-toggle="offcanvas" data-bs-target="#offcanvasBottom" aria-controls="offcanvasBottom">View the latest data</button>')
                      ).add_to(fgPiezometros)
    fgPiezometros.add_to(m)


    ###### Valores de AQI ##############
    tooltips = []
    icons = []

    for i in range(0, len(df_aqi)):
        e = df_aqi.iloc[i,]

        tooltips.append(folium.Popup("<b>"+e['name']+"</b>"+"<br>" +
                                     str(e['lastUpdate'] + timedelta(hours=2)), max_width=600
                                     ))

        icons.append(plugins.BeautifyIcon(icon="arrow-down",
                                          icon_shape="marker",
                                          number=e['aqi'],
                                          inner_icon_style="font-size: 10pt; font-family: Arial; padding-left: 2px",
                                          border_color="#0077b6",
                                          background_color="#caf0f8"))

    fgAQI = FeatureGroup(name="AQI")

    plugins.MarkerCluster(
        df_aqi[['lat', 'lon']], icons=icons, popups=tooltips).add_to(fgAQI)

    fgAQI.add_to(m)

    m.add_child(folium.ClickForMarker(
        popup='''<b>Lat:</b> ${lat}<br /><b>Lon:</b> ${lng}<br /> <br /> <input value="Estimate AQI" type="button" class="btn btn-primary btn-sm" onclick="calculateAQI(${lat}, ${lng})"> <br /> <br /><b id=aqivalue></b>'''
    ))

    LayerControl().add_to(m)

    m.get_root().height = "600px"
    return m.get_root().render()


# Estimate the value of AQI with giving latitude and longitude.
def calculateAQI(lat, lon):

    df_aqi_local = df_aqi.copy()
    dis = []
    weights = []
    power = 1.5
    for i in range(len(df_aqi_local)):
        e = df_aqi_local.iloc[i,]
        d = distance.distance((lat, lon), (e['lat'], e['lon'])).km
        dis.append(d)
        weights.append(1/d**power)
    df_aqi_local['dis'] = dis
    df_aqi_local['weight'] = weights
    df_aqi_local = df_aqi_local.sort_values(
        by='dis', ascending=True).reset_index().drop(columns=["index"])
    
    if df_aqi_local['dis'][0] > 100:
        return 'The nearest station is more than 100km away'

    values = pd.to_numeric(df_aqi_local.iloc[:6,]['aqi'].apply(
        lambda x: x.replace('-', '0')))
    ws = df_aqi_local.iloc[:6,]['weight']

    return str(round(np.dot(values, ws)/np.sum(ws), 3))

# Obatain the last data (csv files)
def getLatestData(type, index):

    if type == '0':
        return getSMLGPlot()
    elif type == '1':
         return getDataPlot(type=type, loc=locSinqlair['Nombre'][index], csv=True)
    elif type == '2':
        return getDataPlot(type=type, loc=locPiezometros['CodPuntoControl'][index] +'-' + locPiezometros['Nombre'][index], csv=True)
    elif type == '3':
        return getDataPlot(type=type, loc=locRamblas['CodPuntoControl'][index] +'-' + locRamblas['Nombre'][index], csv=True)
    elif type == '4':
        return getUPCTPlot(locBoyaUPCT['Nombre'][index])
    elif type == '5':
        return getDataPlot(type=type, loc=locAEMET['Nombre'][index], csv=True)
    else:
        return json.dumps(px.line(pd.DataFrame(), title='No data available yet. Please check later.'), cls=plotly.utils.PlotlyJSONEncoder)


# Create UPCT buoy plot, which have multiple variables and different depth
def getUPCTPlot(boya):
    fig = go.Figure()
    list_buttons = [] # to save each option of the list of variables
    indice = 0

    # Iterate each variable
    for p in propiedadesUPCT:
        df = pd.read_csv('./extractedData/' + boya.replace('-','') + '/' + p +'.csv')
        df['Date'] = pd.to_datetime(df['Date'])
        df.sort_values(by='Date', inplace=True)

        ## Add traces (depths)
        for c in df.columns[1:]:
            fig.add_trace(go.Scatter(
                x=df['Date'],
                y=df[c],
                name=c+'m',
                legendgroup=p,
                legendgrouptitle=dict(text=p),
                visible=True if indice == 0 else False,
                
            ))

        # Set visibility of each trace (depth) at each parameter
        visibles = [False] * indice
        indice = indice + len(df.columns[1:])
        visibles.extend([True] * len(df.columns[1:]))
        visibles.extend([False] * (100-len(visibles))) # 33 is the total number of all variables+depths
        
        list_buttons.append(dict(label=p,
                        method="update",
                        args=[{"visible": visibles}]))
    
    fig.update_layout(
        # set the dropdown list
        updatemenus=[
            dict(
                buttons=list_buttons
            )
        ],
        legend=dict(groupclick="toggleitem"),
        title={
            'text': "<b> UPCT buoy "+boya+" data </b>",
            'y':0.9,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'}
    )

    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

    return graphJSON

# Create SMLG buoy plot, which have multiple variables and different depth
def getSMLGPlot():
    fig = go.Figure()
    list_buttons = [] # to save each option of the list of variables
    indice = 0

    # Iterate each variable
    for p in propiedadesSMLG:
        df = pd.read_csv('./boyaSMLG/'+p+'.csv')
        df['Date'] = pd.to_datetime(df['Date'])
        df.sort_values(by='Date', inplace=True)

        ## Add traces (depths)
        for c in df.columns[1:]:
            fig.add_trace(go.Scatter(
                x=df['Date'],
                y=df[c],
                name=c,
                legendgroup=p,
                legendgrouptitle=dict(text=p),
                visible=True if indice == 0 else False,
                
            ))

        # Set visibility of each trace (depth) at each parameter
        visibles = [False] * indice
        indice = indice + len(df.columns[1:])
        visibles.extend([True] * len(df.columns[1:]))
        visibles.extend([False] * (33-len(visibles))) # 33 is the total number of all variables+depths
        
        list_buttons.append(dict(label=p,
                        method="update",
                        args=[{"visible": visibles}]))
    
    fig.update_layout(
        # set the dropdown list
        updatemenus=[
            dict(
                buttons=list_buttons
            )
        ],
        legend=dict(groupclick="toggleitem"),
        title={
            'text': "<b> SmartLagoon buoy data </b>",
            'y':0.9,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'}
    )

    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

    return graphJSON

# Plot the historic data giving the startDate and endDate, if csav==True, it will plot only the csv files
def getDataPlot(type, loc, startDate=None, endDate=None, csv=False):

    if csv == False:
        startDate = startDate.replace('T', ' ')
        endDate = endDate.replace('T', ' ')

    fig = None

    # Si es Sinqlair
    if type == "1":

        df = None

        if csv == True: # Leer solo el ficher csv correspondiente
            df = pd.read_csv(dirSinqlair+loc+'.csv', na_values='---')
            df['Date'] = pd.to_datetime(df['Date'])
            df.drop(columns='Comunidad', inplace=True)
        else:   # Leer el fichero parquet y extraer las fechas indicadas
            df = pd.read_parquet(dirSinqlair+loc+'.parquet')
            df['Date'] = pd.to_datetime(df['Date'])
            df = df[(df['Date'] >= startDate) & (df['Date'] <= endDate)].drop(columns='Comunidad')
            for c in df.columns[1:]:
                df[c] = pd.to_numeric(df[c].apply(lambda x: convertirNum(x)))

        if len(df) == 0:
            return json.dumps(px.line(pd.DataFrame(), title='No data available yet. Please check later.'), cls=plotly.utils.PlotlyJSONEncoder)
        
        df.set_index('Date', inplace=True)

        # Resample with different date lengths requested
        diffDays = abs((df.index[-1] - df.index[0]).days)
        if  diffDays > 30:
            df = df.resample('H').mean()
        if  diffDays > 365:
            df = df.resample('D').mean()

        df = df.reset_index().melt('Date',var_name='Data', value_name='Value')
        df['Value'] = pd.to_numeric(df['Value'], errors='coerce')
        fig = px.line(df, x="Date", y="Value", color='Data')
        fig.update_layout(
        title={
            'text': "<b>" + loc + " data (" + str(df['Date'].iloc[0]) + ' - ' + str(df['Date'].iloc[-1]) + ')</b>',
            'y':0.9,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'})
    
    # Si es piezometros (2) o ramblas (3)
    elif type == "2" or type=="3":

        if type == "2":
            path = dirPiezometros+loc
        else:
            path = dirRamblas +loc

        # List all csv or parquet files depending the request
        params = glob.glob(os.path.join(path+'/', "*"+ '.csv' if csv else "*" +'.parquet'))
        if len(params) == 0:
            return json.dumps(px.line(pd.DataFrame(), title='No location found, please check the location\'s name.'), cls=plotly.utils.PlotlyJSONEncoder)
        df = pd.DataFrame()
        # Merge all variables 
        for p in params:
            if csv:
                df_param = pd.read_csv(p)
            else:
                df_param = pd.read_parquet(p)
            df_param['Date'] = pd.to_datetime(df_param['Date'])

            # If is parquet (historic) request, the range of date should be specified
            if csv == False:
                df_param = df_param[(df_param['Date'] >= startDate) & (df_param['Date'] <= endDate)]

            if len(df) == 0:
                df = df_param
            else:
                df = pd.merge(df, df_param, on="Date", how="outer")

        # Sort values and convert all variables to number values
        df = df.sort_values(by='Date').set_index('Date')
        for c in df.columns:
            df[c] = pd.to_numeric(df[c].apply(lambda x : convertirNum(x)))

        # Resample with different date lengths requested
        diffDays = abs((df.index[-1] - df.index[0]).days)
        if  diffDays > 30:
            df = df.resample('H').mean()
        if  diffDays > 365:
            df = df.resample('D').mean()

        df = df.reset_index().melt('Date',var_name='Data', value_name='Value')
        fig = px.line(df, x="Date", y="Value", color='Data')
        fig.update_layout(
        title={
            'text': "<b>" + loc + " data (" + str(df['Date'].iloc[0]) + ' - ' + str(df['Date'].iloc[-1]) + ')</b>',
            'y':0.9,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'})
    
    # Si es AEMET
    elif type == "5":

        # Read prediction data
        pred = pd.read_csv('./aemet/prediction/' + loc + '/' +loc+'manana.csv')
        for c in pred.columns[1:]:
            pred[c] = pd.to_numeric(pred[c].apply(lambda x : convertirNum(x)))
        pred.columns = ['Date', 'Precip-pred', 'Temp-pred', 'HR-pred']
        pred['Date'] = pd.to_datetime(pred['Date'])
        # Get last 7 days data
        pred = pred[pred['Date'] >= str(datetime.today().date() - timedelta(days=7))]

        # Try to read real data which doesn't exist for some locations, in which case only prediction data will be showed
        try:
            real = pd.read_csv('./aemet/real/'+loc+'.csv')
            real.drop(columns=['tamin', 'tamax'], inplace=True)
            for c in real.columns[1:]:
                real[c] = pd.to_numeric(real[c].apply(lambda x : convertirNum(x)))
            real.columns = ['Date', 'Precip-real', 'HR-real', 'Temp-real']
            real['Date'] = pd.to_datetime(real['Date'].apply(lambda x: x.split('+')[0]))
            real = real[real['Date'] >= str(datetime.today().date() - timedelta(days=7))]

            # Merge the prediction/real data
            df_merged = pd.merge(pred, real, how='outer', on='Date').sort_values(by='Date')

        except FileNotFoundError:
            print(loc,' real data not found.')
            df_merged = pred
        except Exception as e:
            print("The Error is: ", e)

        fig = go.Figure()
        for c in df_merged.columns[1:]:
            fig.add_trace(go.Scatter(x=df_merged['Date'], y=df_merged[c], name=c))
        fig.update_layout(
        title={
            'text': "<b>" + loc + " data (" + str(df_merged['Date'].iloc[0]) + ' - ' + str(df_merged['Date'].iloc[-1]) + ')</b>',
            'y':0.9,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'})

    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

    return graphJSON

# Convert the giving string to a float number, if not possible, return NA
def convertirNum(x):
    try:
        return float(x)
    except:
        return pd.NA

# Plot the prediction data of different ML models giving the type (location type), index (location), predictionType (model type), and number of hours predicted (horizon)
def getPredictionData(type, index, predictionType, horas):
    
    print(type, index, predictionType, horas)

    if (type != "1") & (type != '3'): # if not sinqlair or ramblas, return no data
            print('no type')
            return json.dumps(px.line(pd.DataFrame(), title='No prediction done for this location, please check later.'), cls=plotly.utils.PlotlyJSONEncoder)

    if (predictionType < 1) & (predictionType > 2): # if not LSTM (type 1) or global models (type 2) return no data
            print('no predictiontype')
            return json.dumps(px.line(pd.DataFrame(), title='No prediction done for this location, please check later.'), cls=plotly.utils.PlotlyJSONEncoder)

    '''
        # Para mostrar los ultimos datos y una zona sombreada verde mostrando la prediccion de todas las variables
        loc=locRamblas['CodPuntoControl'][index] +'-' + locRamblas['Nombre'][index]
        path = dirRamblas + loc
        df_lastWeek = pd.DataFrame()
        params = os.listdir(path)
        for p in params:
            if 'csv' not in p:
                continue
            df = pd.read_csv(path+'/'+p)
            if len(df_lastWeek) == 0:
                df_lastWeek = df
            else:
                df_lastWeek = pd.merge(df_lastWeek, df, on="Date", how="outer")

        df_lastWeek['Date'] = pd.to_datetime(df_lastWeek['Date'])
        df_lastWeek = df_lastWeek.sort_values(by='Date').set_index('Date')
        for c in df_lastWeek.columns:
            df_lastWeek[c] = pd.to_numeric(df_lastWeek[c].apply(lambda x : convertirNum(x)))

        df_lastWeek = df_lastWeek.resample('60min').mean().interpolate('linear')

        forecaster = load_forecaster(path+'/forecaster.py',verbose=False)
        df_predicted = forecaster.predict(hours, last_window=df_lastWeek)
        df_concated = pd.concat([df_lastWeek, df_predicted])

        fig = go.Figure()
        for c in df_concated.columns:
            fig.add_trace(go.Scatter(x=df_concated.index, y=df_concated[c], name=c))
        fig.add_vline(x=df_predicted.index[0], line_width=3, line_dash="dash", line_color="green", )
        fig.add_vrect(x0=df_predicted.index[0], x1=df_predicted.index[-1], 
                        annotation_text="Prediction", annotation_position="top left",
                    fillcolor="green", opacity=0.15, line_width=1)
        
        graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

        return graphJSON
    '''
    if predictionType == 1: # Si es para modelos LSTM
        loc=locRamblas['CodPuntoControl'][index] +'-' + locRamblas['Nombre'][index]
        path = dirRamblas + loc
        # Obtener primero los datos reales
        df = pd.read_csv(path +'/' + locRamblas['CodPuntoControl'][index] +'Q01-Caudal.csv')
        df['Date'] = pd.to_datetime(df['Date'])

        # Resampling de datos reales de 5min a H
        df = df.resample('60T', on='Date').mean()

        # Leer los datos de prediccion de 1H
        locSalidasPred = loc.replace(' ', '')
        prediction1H = pd.read_csv(dirPredictions+'/predicciones_' + locSalidasPred + '.csv')
        prediction1H['Date'] = pd.to_datetime(prediction1H['Date'])
        prediction1H['Date'] = prediction1H['Date'].apply(lambda x : x +timedelta(hours=1))
        # Recortar el histórico de predicciones (se eliminan aquellas que sean antes de los ultimos 7 dias)
        prediction1H = prediction1H[prediction1H['Date'] >= df.index.tolist()[0]]

        # Leer los datos de prediccion de 1H (AEMET)
        prediction1H_aemet = pd.read_csv(dirPredictions+'/predicciones_aemet_' + locSalidasPred + '.csv')
        prediction1H_aemet['Date'] = pd.to_datetime(prediction1H_aemet['Date'])
        prediction1H_aemet['Date'] = prediction1H_aemet['Date'].apply(lambda x : x +timedelta(hours=1))
        # Recortar el histórico de predicciones (se eliminan aquellas que sean antes de los ultimos 7 dias)
        prediction1H_aemet = prediction1H_aemet[prediction1H_aemet['Date'] >= df.index.tolist()[0]]

        prediction6H_aemet = util.readPredictionFile(dirPredictions+'/predicciones_aemet_' + locSalidasPred, 6)
        prediction12H_aemet = util.readPredictionFile(dirPredictions+'/predicciones_aemet_' + locSalidasPred, 12)
        prediction24H_aemet = util.readPredictionFile(dirPredictions+'/predicciones_aemet_' + locSalidasPred, 24)
        # Modificar la prediccion de 24 poniendo la cabezera de la serie con las predicciones de 1, 6 y 12h
        prediction24H_aemet.iloc[0:12] = prediction12H_aemet
        prediction24H_aemet.iloc[0:6] = prediction6H_aemet
        prediction24H_aemet.iloc[0] = prediction1H_aemet.iloc[-1]

        # Leer predicciones de 6, 12 y 24H
        if '06A01' in locSalidasPred:
            prediction6H = util.readPredictionFile(dirPredictions+'/predicciones_' + locSalidasPred, 6)
            prediction12H = util.readPredictionFile(dirPredictions+'/predicciones_' + locSalidasPred, 12)
            prediction24H = util.readPredictionFile(dirPredictions+'/predicciones_' + locSalidasPred, 24)
            # Modificar la prediccion de 24 poniendo la cabezera de la serie con las predicciones de 1, 6 y 12h
            prediction24H.iloc[0:12] = prediction12H
            prediction24H.iloc[0:6] = prediction6H
            prediction24H.iloc[0] = prediction1H.iloc[-1]
        
        
        
            

        # Merge de datos reales y predcciones
        df_merged = pd.merge(df, prediction1H, on='Date', how='inner')
        df_merged_aemet = pd.merge(df, prediction1H_aemet, on='Date', how='inner')

        # Crear figura y añadir trazas
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df.index, y=df['Caudal'], name='Real', mode='lines'))
        fig.add_trace(go.Scatter(x=prediction1H['Date'], y=prediction1H['Predicción_1H'], name='Prediction_1h', mode='lines'))
        fig.add_trace(go.Scatter(x=prediction1H_aemet['Date'], y=prediction1H_aemet['Predicción_1H'], name='Prediction_1h_AEMET', mode='lines'))
        fig.add_trace(go.Scatter(x=prediction24H_aemet['Date'], y=prediction24H_aemet['Predicción_24h'], name='Prediction_24h_AEMET', mode='lines'))
        if '06A01' in locSalidasPred:
            fig.add_trace(go.Scatter(x=prediction24H['Date'], y=prediction24H['Predicción_24H'], name='Prediction_24h', mode='lines'))

        fig.update_layout(
        title={
            'text': "<b>" + loc + " streamflow prediction (" + str(df.index[0]) + ' - ' + str(df.index[-1]) + ')</b>',
            'y':0.9,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'})
        
        # add annotation
        fig.add_annotation(dict(font=dict(color='black',size=18),
                                        x=0,
                                        y=-0.3,
                                        showarrow=False,
                                        text= util.getMetricsInText('', df_merged['Caudal'], df_merged['Predicción_1H']),
                                        textangle=0,
                                        xanchor='left',
                                        xref="paper",
                                        yref="paper"))
        # add annotation
        fig.add_annotation(dict(font=dict(color='black',size=18),
                                        x=0,
                                        y=-0.2,
                                        showarrow=False,
                                        text= util.getMetricsInText('(AEMET)', df_merged_aemet['Caudal'], df_merged_aemet['Predicción_1H']),
                                        textangle=0,
                                        xanchor='left',
                                        xref="paper",
                                        yref="paper"))
    
        graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

        return graphJSON
    elif predictionType == 2:

        if type == "1":
            df_lastWeek = pd.read_csv(dirSinqlair+locSinqlair['Nombre'][index]+'.csv')
            for c in df_lastWeek.columns[2:]:
                df_lastWeek[c] = pd.to_numeric(df_lastWeek[c].apply(lambda x: pd.NA if x=='---' else float(x)))
            df_lastWeek['Date'] = pd.to_datetime(df_lastWeek['Date'])
            df_lastWeek = df_lastWeek.set_index('Date').drop(columns='Comunidad')
            df_lastWeek = df_lastWeek.resample('H').mean().interpolate('linear')

            loaded_forecaster = load_forecaster(dirSinqlair+'Forecaster-' +locSinqlair['Nombre'][index]+'.joblib', verbose=False )
        elif type == '3':
            df_lastWeek = pd.DataFrame()
            path  = dirRamblas + locRamblas['CodPuntoControl'][index] +'-' + locRamblas['Nombre'][index] + '/'
            params = os.listdir(path)
            for p in params:
                if 'csv' not in p:
                    continue
                df = pd.read_csv(path+'/'+p)
                if len(df_lastWeek) == 0:
                    df_lastWeek = df
                else:
                    df_lastWeek = pd.merge(df_lastWeek, df, on="Date", how="outer")

            df_lastWeek['Date'] = pd.to_datetime(df_lastWeek['Date'])
            df_lastWeek = df_lastWeek.sort_values(by='Date').set_index('Date')
            for c in df_lastWeek.columns:
                df_lastWeek[c] = pd.to_numeric(df_lastWeek[c].apply(lambda x : convertirNum(x)))
            df_lastWeek = df_lastWeek.resample('H').mean().interpolate('linear')

            loaded_forecaster = load_forecaster(path + 'forecaster.joblib', verbose=False )
            
        #window_size = loaded_forecaster.window_size
        df_predicted = pd.DataFrame()
        steps = horas

        # Obtain predictions from the begining + window_size (neccesary lagas to make the first prediction) until the last trace which is less than 24h
        for i in range(len(loaded_forecaster.lags), len(df_lastWeek), steps):
            if i+steps < len(df_lastWeek):
                df_predicted = pd.concat([df_predicted, loaded_forecaster.predict(steps=steps, last_window=df_lastWeek.iloc[:i])])

        # Obtain predictions of last trace (less than 24)
        df_predicted = pd.concat([df_predicted, loaded_forecaster.predict(steps=len(df_lastWeek)-i, last_window=df_lastWeek.iloc[:i])])

        # Obtain prediction of next 24h from now
        df_predicted = pd.concat([df_predicted, loaded_forecaster.predict(steps=steps, last_window=df_lastWeek)])

        df_predicted = df_predicted.clip(lower=0)

        # Plotting predictions vs real values 
        df_merged = pd.concat([
                            df_predicted.melt(ignore_index=False).assign(group="Predictions"),
                            df_lastWeek[df_predicted.columns].melt(ignore_index=False).assign(group="Real")
                        ]).reset_index().rename(columns={"index": "date_time"})
        fig = px.line(
            data_frame = df_merged,
            x="date_time",
            y="value",
            facet_row="variable",
            color="group"
        )

        fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))

        variables = []
        for ann in fig['layout']['annotations']:
            variables.append(ann['text'])
        np.seterr(divide='ignore', invalid='ignore')
        # Add annotations to each facet
        annotations = [
            dict(font=dict(color='black',size=15),
                                                x=df_merged['date_time'][0],
                                                y=0,
                                                showarrow=False,
                                                text= variable +': '+ util.getMetricsInTextForGlobalModels(pd.merge(df_predicted[variable], df_lastWeek[variable], left_index=True, right_index=True)),
                                                textangle=0,
                                                xanchor='left',
                                                yref=f"y{1 + i}")

            for i, variable in enumerate(variables)
        ]

        for annotation in annotations:
            fig.add_annotation(annotation)

        fig.update_layout(
            title="Prediction vs real values",
            #width=1000,
            height=650,
            margin=dict(l=20, r=20, t=35, b=20),
            legend=dict(
                orientation="h",
                yanchor="top",
                y=1,
                xanchor="left",
                x=0
            )
        )

        fig.update_yaxes(matches=None, automargin=True)
        graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

        return graphJSON

# update in-memory (temporal) data stored in application/map
def updateDatas():
    print(str(datetime.now()), ' Updating datas...')
    getAQI()
    print("AQI values updated.")
    renderMap()
    print('Map updated.')
    print(str(datetime.now()), ' Datas updated...')
    # redirect? Or use Javascript setInterval?

# set the background job (run every hour)
def startBackgourndScheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(updateDatas, 'interval', minutes=60)
    sched.start()
    print(str(datetime.now()), ' Background demon started...')

# Return the name of file of different type of datas
def getRutaFicheros():

    return [locSinqlair['Nombre'],
            [f.split(".")[0] for f in ficherosPiezometros],
            [f.split(".")[0] for f in ficherosRamblas]]

# Return the last sunday's date
def getUltimaFechaUnion(anterior=False):
    today = datetime.now()
    last_sun = today - timedelta(days=today.weekday()+1)
    if today.weekday() == 0:
        last_sun = last_sun -timedelta(days=7)
    
    if anterior:
        last_sun = last_sun -timedelta(days=1) 
    print(last_sun)
    
    return last_sun.date().strftime('%Y-%m-%d') + 'T23:55'