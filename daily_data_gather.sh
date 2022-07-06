#!/bin/bash
# source ./daily_data_gather.sh

alias server_path='cd api'
alias dal_path='cd ../securities_db_py_dal/securities_db_py_dal'
#alias generate_signals_path='cd ../../../systems/system_development/live_systems'
alias system_handler_path='cd ../../../systems/system_development/live_systems'

server_path
node server.js &

dal_path
python dal.py

#generate_signals_path
#python generate_signals.py
system_handler_path
python system_handler.py

#cd ../../../securities_pg_db/api
#exit node program