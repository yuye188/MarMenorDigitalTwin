#!/bin/bash

echo 'starting to excute scripts...'

cd /home/thinking/src/

/usr/local/pyenv/.pyenv/versions/think/bin/python acg_lstm_prod_puebla_aemet_yu.py

/usr/local/pyenv/.pyenv/versions/think/bin/python acg_lstm_prod_puebla_6h_aemet_yu.py

/usr/local/pyenv/.pyenv/versions/think/bin/python acg_lstm_prod_puebla_12h_aemet_yu.py

/usr/local/pyenv/.pyenv/versions/think/bin/python acg_lstm_prod_puebla_24h_aemet_yu.py


/usr/local/pyenv/.pyenv/versions/think/bin/python acg_lstm_prod_desem_aemet_yu.py

/usr/local/pyenv/.pyenv/versions/think/bin/python acg_lstm_prod_desem_6h_aemet_yu.py

/usr/local/pyenv/.pyenv/versions/think/bin/python acg_lstm_prod_desem_12h_aemet_yu.py

/usr/local/pyenv/.pyenv/versions/think/bin/python acg_lstm_prod_desem_24h_aemet_yu.py

echo 'finished excute scripts'
date '+%Y-%m-%d %H:%M'
