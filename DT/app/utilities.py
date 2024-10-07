from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, mean_squared_error
import numpy as np
import pandas as pd
from datetime import timedelta



# Return the rambla marker in text format
def createRamblaMarker(codPuntoControl, loc, i):

    icon = f'''<svg height="100" width="100">
        <rect  x="1" y="1" width="55" height="22" stroke="#38a3a5" stroke-width="1" fill="#ffc2d1" />
        <text x="7" y="17" fill="black" font-size="15" stroke="black" stroke-width="0.5"> {codPuntoControl} </text>
        </svg>'''
    
    # Si lleva modelos LSTM añadir opciones correspondientes
    if '06A01' in codPuntoControl or '06A18' in codPuntoControl:
        popup = f'''<b> {loc} </b><button class="btn btn-primary mt-4" type="button" onclick="getLatestData(3, {i} )" data-bs-toggle="offcanvas" data-bs-target="#offcanvasBottom" aria-controls="offcanvasBottom">View the latest data</button> 
                    <br>
                    <br>
                    <div class="btn-group dropend">
                    <button type="button" class="btn btn-primary dropdown-toggle" data-bs-toggle="dropdown" aria-expanded="false">
                        Prediction
                    </button>
                    <ul class="dropdown-menu ">
                        <li><a class="dropdown-item" type="button" onclick="getPredictionData(3, {i}, 1, 0 )" data-bs-toggle="offcanvas" data-bs-target="#offcanvasBottom" aria-controls="offcanvasBottom">Streamflow LSTM model</a></li>
                        <li><hr class="dropdown-divider"></li>
                        <li><a class="dropdown-item" type="button" onclick="getPredictionData(3, {i}, 2, 1 )" data-bs-toggle="offcanvas" data-bs-target="#offcanvasBottom" aria-controls="offcanvasBottom">Global autoregressive model (1H) </a></li>                    
                        <li><a class="dropdown-item" type="button" onclick="getPredictionData(3, {i}, 2, 6 )" data-bs-toggle="offcanvas" data-bs-target="#offcanvasBottom" aria-controls="offcanvasBottom">Global autoregressive model (6H) </a></li>                    
                        <li><a class="dropdown-item" type="button" onclick="getPredictionData(3, {i}, 2, 12 )" data-bs-toggle="offcanvas" data-bs-target="#offcanvasBottom" aria-controls="offcanvasBottom">Global autoregressive model (12H) </a></li>
                        <li><a class="dropdown-item" type="button" onclick="getPredictionData(3, {i}, 2, 24 )" data-bs-toggle="offcanvas" data-bs-target="#offcanvasBottom" aria-controls="offcanvasBottom">Global autoregressive model (24H) </a></li>                                        
                    </ul>
                    </div>'''
    else:
        popup = f'''<b> {loc} </b><button class="btn btn-primary mt-4" type="button" onclick="getLatestData(3, {i} )" data-bs-toggle="offcanvas" data-bs-target="#offcanvasBottom" aria-controls="offcanvasBottom">View the latest data</button> 
                    <br>
                    <br>
                    <div class="btn-group dropend">
                    <button type="button" class="btn btn-primary dropdown-toggle" data-bs-toggle="dropdown" aria-expanded="false">
                        Prediction
                    </button>
                    <ul class="dropdown-menu ">
                        <li><a class="dropdown-item" type="button" onclick="getPredictionData(3, {i}, 2, 1 )" data-bs-toggle="offcanvas" data-bs-target="#offcanvasBottom" aria-controls="offcanvasBottom">Global autoregressive model (1H) </a></li>                    
                        <li><a class="dropdown-item" type="button" onclick="getPredictionData(3, {i}, 2, 6 )" data-bs-toggle="offcanvas" data-bs-target="#offcanvasBottom" aria-controls="offcanvasBottom">Global autoregressive model (6H) </a></li>                    
                        <li><a class="dropdown-item" type="button" onclick="getPredictionData(3, {i}, 2, 12 )" data-bs-toggle="offcanvas" data-bs-target="#offcanvasBottom" aria-controls="offcanvasBottom">Global autoregressive model (12H) </a></li>
                        <li><a class="dropdown-item" type="button" onclick="getPredictionData(3, {i}, 2, 24 )" data-bs-toggle="offcanvas" data-bs-target="#offcanvasBottom" aria-controls="offcanvasBottom">Global autoregressive model (24H) </a></li>                                        
                    </ul>
                    </div>'''

    return icon, popup

# Return the sinqlair marker in text format
def createSinqlairMarker(loc, i):
    icon=f'''<svg height="100" width="100" ">
             <rect  x="1" y="1" width="55" height="22" stroke="#38a3a5" stroke-width="1" fill="#c7f9cc" />
             <text x="7" y="17" fill="black" font-size="15" stroke="black" stroke-width="0.5"> {loc} </text>
             </svg>'''
    popup = f'''<b> {loc} </b><button class="btn btn-primary mt-4" type="button" onclick="getLatestData(1, {i} )" data-bs-toggle="offcanvas" data-bs-target="#offcanvasBottom" aria-controls="offcanvasBottom">View the latest data</button> 
                    <br>
                    <br>
                    <div class="btn-group dropend">
                    <button type="button" class="btn btn-primary dropdown-toggle" data-bs-toggle="dropdown" aria-expanded="false">
                        Prediction
                    </button>
                    <ul class="dropdown-menu ">
                        <li><a class="dropdown-item" type="button" onclick="getPredictionData(1, {i}, 2, 1 )" data-bs-toggle="offcanvas" data-bs-target="#offcanvasBottom" aria-controls="offcanvasBottom">Global autoregressive model (1H) </a></li>                    
                        <li><a class="dropdown-item" type="button" onclick="getPredictionData(1, {i}, 2, 6 )" data-bs-toggle="offcanvas" data-bs-target="#offcanvasBottom" aria-controls="offcanvasBottom">Global autoregressive model (6H) </a></li>                    
                        <li><a class="dropdown-item" type="button" onclick="getPredictionData(1, {i}, 2, 12 )" data-bs-toggle="offcanvas" data-bs-target="#offcanvasBottom" aria-controls="offcanvasBottom">Global autoregressive model (12H) </a></li>
                        <li><a class="dropdown-item" type="button" onclick="getPredictionData(1, {i}, 2, 24 )" data-bs-toggle="offcanvas" data-bs-target="#offcanvasBottom" aria-controls="offcanvasBottom">Global autoregressive model (24H) </a></li>                                        
                    </ul>
                    </div>'''
    #popup=folium.Popup('<b>'+ locSinqlair['Nombre'][i] +' </b><button class="btn btn-primary mt-4" type="button" onclick="getLatestData(1, '+str(i)+' )" data-bs-toggle="offcanvas" data-bs-target="#offcanvasBottom" aria-controls="offcanvasBottom">View the latest data</button>')
    return icon, popup

def nash_sutcliffe_efficiency(observed, simulated):
    """
    Calcula el coeficiente de eficiencia de Nash-Sutcliffe (NSE).
    
    Args:
    observed: Array de numpy con los valores observados.
    simulated: Array de numpy con los valores simulados o predichos.
    
    Returns:
    nse: Coeficiente de eficiencia de Nash-Sutcliffe.
    """
    # Calcular el promedio de los valores observados
    mean_observed = np.mean(observed)
    
    # Calcular el error cuadrático
    numerator = np.sum((observed - simulated) ** 2)
    
    # Calcular la varianza de los valores observados
    denominator = np.sum((observed - mean_observed) ** 2)
    
    # Calcular el NSE
    nse = 1 - (numerator / denominator)
    
    return nse


def willmott_index(observed, simulated):
    """
    Calcula el índice de Willmott entre dos conjuntos de datos.
    
    Args:
    observed: Array de numpy con los valores observados.
    simulated: Array de numpy con los valores simulados o predichos.
    
    Returns:
    willmott: Índice de Willmott.
    """
    # Calcular la diferencia entre valores observados y simulados
    numerator = np.sum(np.abs(simulated - observed))
    
    # Calcular la diferencia entre los valores simulados y la media de los valores observados
    denominator = np.sum(np.abs(np.abs(simulated - np.mean(observed)) + np.abs(observed - np.mean(observed))))
    
    # Calcular el índice de Willmott
    willmott = 1 - (numerator / denominator)
    
    return willmott

# Return the metrics in text format giving the real and prediction values, the version is the text to show in the legend (LSTM models)
def getMetricsInText(version, real, prediction):
    MAE = str(round(mean_absolute_error(real, prediction) ,3))
    RMSE = str(round(mean_squared_error(real, prediction, squared=False), 3))
    CVRMSE = str(round(mean_squared_error(real, prediction, squared=False)/np.mean(real)*100,3))
    nash = str(round(nash_sutcliffe_efficiency(real, prediction),3))
    will = str(round(willmott_index(real, prediction),3))
    
    return "Prediction metrics "+version+": MAE=" + MAE + ',   RMSE=' + RMSE + ',   CVRMSE='+CVRMSE+',   nash_sutcliffe_efficiency='+nash+',   willmott_index='+will

# Return the metrics in text giving the df which contains the real and prediction data
def getMetricsInTextForGlobalModels(df):

    prediction = df.iloc[:, 0]
    real = df.iloc[:, 1]
    MAE = str(round(mean_absolute_error(real, prediction) ,3))
    RMSE = str(round(mean_squared_error(real, prediction, squared=False), 3))
    CVRMSE = str(round(mean_squared_error(real, prediction, squared=False)/np.mean(real)*100,3))

    
    return "MAE=" + MAE + ',   RMSE=' + RMSE + ',   CVRMSE='+CVRMSE

# Read the prediction files with indicated horizon
def readPredictionFile(dir,horas):
    salida = pd.read_csv(dir+'_'+ str(horas) + 'h.csv').iloc[-horas:,]
    salida['Date'] = pd.to_datetime(salida['Date'])
    
    fixedDates = []
    for idx, t in enumerate(salida['Date']):
        fixedDates.append(t + timedelta(hours=idx+1))
    salida['Date'] = fixedDates
    return salida