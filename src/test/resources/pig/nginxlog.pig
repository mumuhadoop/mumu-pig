nginxlog = load '/mapreduce/nginxlog/access/input' using PigStorage(' ');

group_nginxlog = group nginxlog by $0;

ipcounter_nginxlog = foreach group_nginxlog generate group,COUNT(nginxlog);

order_nginxlog = order ipcounter_nginxlog by $1 desc;

limit_nginxlog = limit order_nginxlog 10;

dump limit_nginxlog;