#!/bin/bash
# source ./daily_data_gather.sh

alias server_path='cd api'
alias dal_path='cd ../securities_db_py_dal/securities_db_py_dal'
alias generate_signals_path='cd ../../../systems/system_development/live_systems'

server_path
node server.js &

dal_path
python dal.py

generate_signals_path
python generate_signals.py
