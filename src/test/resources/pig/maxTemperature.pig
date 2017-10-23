records = load '/pig/input' as (year:int,temperature:int,quarity:int);

filter_records = filter records by temperature>=0 and quarity in(0,1,5,9);

temperature_group_records = group filter_records by year;

max_temperature_records = foreach temperature_group_records generate group,MAX(filter_records.temperature);

dump max_temperature_records;
