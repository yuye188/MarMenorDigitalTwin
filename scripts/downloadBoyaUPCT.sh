#!/bin/bash

echo 'starting download...'

wget -q https://marmenor.upct.es/thredds/fileServer/L2/CDOM.nc -O /home/thinking/raw/boyaUPCT/ncFiles/CDOM.nc 
wget -q https://marmenor.upct.es/thredds/fileServer/L2/Clorofila.nc -O /home/thinking/raw/boyaUPCT/ncFiles/Clorofila.nc
wget -q https://marmenor.upct.es/thredds/fileServer/L2/Oxigeno.nc -O /home/thinking/raw/boyaUPCT/ncFiles/Oxigeno.nc
wget -q https://marmenor.upct.es/thredds/fileServer/L2/PE.nc -O /home/thinking/raw/boyaUPCT/ncFiles/PE.nc
wget -q https://marmenor.upct.es/thredds/fileServer/L2/Salinidad.nc -O /home/thinking/raw/boyaUPCT/ncFiles/Salinidad.nc
wget -q https://marmenor.upct.es/thredds/fileServer/L2/Temperatura.nc -O /home/thinking/raw/boyaUPCT/ncFiles/Temperatura.nc
wget -q https://marmenor.upct.es/thredds/fileServer/L2/Transparency.nc -O /home/thinking/raw/boyaUPCT/ncFiles/Transparency.nc
wget -q https://marmenor.upct.es/thredds/fileServer/L2/Turbidez.nc -O /home/thinking/raw/boyaUPCT/ncFiles/Turbidez.nc

echo 'finished download'
date '+%Y-%m-%d %H:%M'
