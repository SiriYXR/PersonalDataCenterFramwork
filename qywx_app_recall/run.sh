ps -ef | grep "flask run --host 0.0.0.0 --port 8001" | grep -v grep | awk '{print $2}' | xargs kill -9 # 关闭已在运行的进程

flask run --host 0.0.0.0 --port 8001  >> nohup.out 2>&1 & disown # 使进程从git hook中脱离