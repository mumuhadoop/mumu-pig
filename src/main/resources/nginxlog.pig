nginxlog = load 'hdfs://192.168.11.25:9000/mapreduce/nginxlog/access/input/20171015' using PigStorage(' ');

group_nginxlog = group nginxlog by $0;

ipcounter_nginxlog = foreach group_nginxlog generate group,COUNT(nginxlog);

order_nginxlog = order ipcounter_nginxlog by $1 desc;

limit_nginxlog = limit order_nginxlog 10;

dump limit_nginxlog;