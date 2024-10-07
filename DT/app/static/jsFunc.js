function calculateAQI(lat, lon) {
    $.ajax({
      type: 'GET',
      url: '/calculateAQI',
      data: { 'lat': lat, 'lon': lon },
      success: function (data) {
        document.getElementById("aqivalue").textContent = data;
      }
    });
  }


  function selectData(type) {

    let loc = null
    if (type == 1)   
      loc = document.getElementById("ficherosSinqlair-select").value
    else if (type == 2) 
      loc = document.getElementById("ficherosPiezometros-select").value
    else loc = document.getElementById("ficherosRamblas-select").value

    start = document.getElementById("start").value
    end = document.getElementById("end").value

    $.getJSON({
      url: "/selectData",
      data: {
        'type': type,
        'loc': loc,
        'start': start,
        'end': end
      },
      success: function (result) {
        Plotly.newPlot('airStationsChart', result, { staticPlot: true })
        document.getElementById("airStationsChart").hidden =false
        //document.getElementById("downloadAirStations").textContent = "Download";
      }
    });
  }

  function getLatestData(type, indexDF){
    const collection = document.getElementsByClassName("ChartCanvas");
    for (let i = 0; i < collection.length; i++) {
      //console.log(collection[i].id)
      Plotly.purge(collection[i]);
    }
    document.getElementById("loading").hidden =false
    console.log(type, indexDF)
    $.getJSON({
      url: "/getLatestData",
      data: {
        'type': type,
        'index': indexDF
      },
      success: function (result) {
        document.getElementById("loading").hidden =true
        //document.getElementsByClassName("leaflet-popup-content")[0].style.width = "600px";
        Plotly.newPlot('latestData', result, { staticPlot: true })
        //document.getElementById("todayData").hidden =false
      }
    });
  }

  function getPredictionData(type, indexDF, predictionType, horas){
    const collection = document.getElementsByClassName("ChartCanvas");
    for (let i = 0; i < collection.length; i++) {
      //console.log(collection[i].id)
      Plotly.purge(collection[i]);
    }
    document.getElementById("loading").hidden =false
    console.log(type, indexDF, predictionType, horas)
    
    $.getJSON({
      url: "/getPredictionData",
      data: {
        'type': type,
        'index': indexDF,
        'predictionType': predictionType,
        'horas': horas
      },
      success: function (result) {
        document.getElementById("loading").hidden =true
        Plotly.newPlot('predictionData', result, { staticPlot: true })
      }
    });
    
  }