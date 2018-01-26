#!/bin/sh
NUMS="00 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23"

for k in $(seq 1 4)
do
        for NUM in $NUMS
        do
            data_date=`date -d "-$k day" +%Y/%m/%d`
            echo $data_date/$NUM" hour start!"
            time_hour=$data_date/$NUM
            #sh /home/flume/secondary_log_job/cidMappingMerge/sh/hour.sh $time_hour

            echo
            echo "${time_hour} hour done!"
            echo "==========="
            echo
        done

        #拷贝完24小时的数据之后，就对这一天的数据进行合并
        data_date2=`date -d "-$k day" +%Y-%m-%d`
        echo $data_date2" day start!"
        #sh /home/flume/secondary_log_job/cidMappingMerge/sh/daily.sh $data_date2

        echo
        echo "${data_date2} day done!"
        echo "************************************************************************"
        echo
done