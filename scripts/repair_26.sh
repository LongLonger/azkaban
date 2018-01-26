#!/bin/sh
NUMS="00 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19"

data_date="2018/01/26"
for NUM in $NUMS
do

    echo $data_date/$NUM" hour start!"
    time_hour=$data_date/$NUM
    #sh /home/flume/secondary_log_job/cidMappingMerge/sh/hour.sh $time_hour

    echo
    echo "${time_hour} hour done!"
    echo "==========="
    echo
done
