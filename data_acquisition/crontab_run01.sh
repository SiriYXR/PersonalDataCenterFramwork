# 0 0 * * *

DA01_HOME_PATH=/your_path/data_acquisition
DA01_PYTHON_PATH=/your_path/miniconda3/bin/python

$DA01_PYTHON_PATH "$DA01_HOME_PATH/NotionDBDA_BookList.py" >> "$DA01_HOME_PATH/crontab_out.log" 2>&1