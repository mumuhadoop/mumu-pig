##从文件中获取数据
records = load 'hdfs://192.168.11.25:9000/pig/input' as (year:int,temperature:int,quarity:int);

##过滤掉不符合的数据
filter_records = filter records by temperature>=0 and quarity in(0,1,5,9);

##按照年进行分组
temperature_group_records = group filter_records by year;

##计算最大温度
max_temperature_records = foreach temperature_group_records generate group,MAX(filter_records.temperature);

##打印数据
dump max_temperature_records;
