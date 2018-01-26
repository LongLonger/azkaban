data_date=`date -d "-1 day" +%Y/%m/%d`
data_date2=`date -d "-1 day" +%Y-%m-%d`

if [ x$1 != x ]; then
    data_date2=$1
fi
base_home="/user/flume/binlog_new_cids"


hadoop fs -text ${base_home}/cache/partition*.lzo > ${data_date2}
lzop  ${data_date2}
hadoop fs -put ${data_date2}.lzo ${base_home}/cache
hadoop fs -rm ${base_home}/cache/partition*.lzo
rm ${data_date2}
rm ${data_date2}.lzo