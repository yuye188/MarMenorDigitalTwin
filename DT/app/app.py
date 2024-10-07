from flask import Flask, render_template, request
import funciones as fun
from datetime import datetime, timedelta
import __main__

def custom_weights(index):
    return 0

__main__.custom_weights = custom_weights

# Create app and set path of templates and static files
app = Flask(__name__, template_folder='templates', static_folder='static')


# Initialize the variables/dataframes
fun.getAQI()



# Define each path
@app.get('/')
def home():

    return render_template('index.html', aqimap = fun.getMap(),
                                         ficherosSinqlair=fun.getRutaFicheros()[0],
                                         ficherosPiezometros=fun.getRutaFicheros()[1],
                                         ficherosRamblas=fun.getRutaFicheros()[2],
                                         fechaMaxDiaAnterior = fun.getUltimaFechaUnion(anterior=True) ,
                                         fechaMax = fun.getUltimaFechaUnion(),
                                         jsonObjs = fun.getJsonFor3DMap(),
                                         cesiumToken = fun.getCesiumToken())

# Path to estimate the AQI value giving latitud and longitud 
@app.route('/calculateAQI' , methods=['GET'])
def calculateAQI():
    lat = request.args.get('lat')
    lon = request.args.get('lon')
    return fun.calculateAQI(lat, lon)


# Path to get the data of today giving the type and index of corresponding df
@app.route('/getLatestData' , methods=['GET'])
def getLatestData():
    type = request.args.get('type')
    index = int(request.args.get('index'))
    return fun.getLatestData(type, index)

# Path to get the data of prediction giving the type and index and prediction type
@app.route('/getPredictionData' , methods=['GET'])
def getPredictionData():
    type = request.args.get('type')
    index = int(request.args.get('index'))
    predictionType = int(request.args.get('predictionType'))
    horas = int(request.args.get('horas'))
    return fun.getPredictionData(type, index, predictionType, horas)

# Path to get selected data
@app.route('/selectData')
def getSelectedData():
    type = request.args.get('type')
    loc = request.args.get('loc')
    startDate = request.args.get('start')
    endDate = request.args.get('end')
    return fun.getDataPlot(type= type, loc=loc, startDate=startDate, endDate=endDate)

fun.startBackgourndScheduler()

# Launch th app
if __name__ == '__main__':
    app.run(debug=True)
    #app.run(host="0.0.0.0")