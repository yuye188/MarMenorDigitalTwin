#!/bin/bash

./services delete

# Eliminar el directorio node-app
rm -rf node-app

# Eliminar el archivo DockerFile
rm Dockerfile

# Eliminar el archivo docker-compose.yml
rm docker-compose.yml

# Copiar los contenidos desde /mnt/c/Users/marie/OneDrive/Escritorio/Antiguo\ ordenador/Universidad/Master\ 1º/TFM/NGSI-LD/nodeServer/
cp -R /mnt/c/Users/marie/OneDrive/Escritorio/Antiguo\ ordenador/Universidad/Master\ 1º/TFM/NGSI-LD/marMenorDockerCompose/* .

./services orion

echo "Script ejecutado con éxito"