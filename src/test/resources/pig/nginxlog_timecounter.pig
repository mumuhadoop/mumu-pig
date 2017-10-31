register mumu-pig.jar

nginxlog = load 'hdfs://192.168.11.25:9000/mapreduce/nginxlog/access/input' using com.lovecws.mumu.pig.loader.NginxLogLoader();

describe nginxlog;

filter_nginxlog = foreach nginxlog generate com.lovecws.mumu.pig.eval.HourDateFormatEval(accessTime) as accessTime ,remoteAddr;

distinct_nginxlog = distinct filter_nginxlog;

group_nginxlog = group distinct_nginxlog by accessTime;

counter_nginxlog = foreach group_nginxlog generate group,COUNT(distinct_nginxlog.remoteAddr);

dump counter_nginxlog;