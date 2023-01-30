DA02_HOME_PATH=/your_path/data_acquisition_local
DA02_PYTHON_PATH=/your_path/miniconda3/bin/python

$DA02_PYTHON_PATH "$DA02_HOME_PATH/SwitchDA.py" >> "$DA02_HOME_PATH/crontab_out.log" 2>&1