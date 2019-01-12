set -x
#!/bin/sh

export app_dir=$1
. ${app_dir}/scripts/ingest_app_env.txt

Script_Nm=`basename $0 | cut -f1 -d'.'`

Prcs_Strtd_TS=$(date '+%Y%m%d%H%M%S')
Prcs_Strtd_TS_Fmt=`date +%Y-%m-%d_%H-%M-%S_%N`
#Temp_Path="/tmp/"

Log_File=${log_dir}/log/${Script_Nm}_$Prcs_Strtd_TS_Fmt.log
Err_Log_File=${log_dir}/log/${Script_Nm}_Error_$Prcs_Strtd_TS_Fmt.log

exec 1> $Temp_Path/$Log_File 2> $Temp_Path/$Err_Log_File

#export log_file="${log_dir}/log/sample_$(date "+%Y%m%d%H%M%s.%N").log"
#time=`date +%m/%d/%Y" "%T`
#print "INFO: start_time:$time " >>$log_file

echo -e BDA Ingestion process Script $0 started at $Prcs_Strtd_TS_Fmt"\n"

#export mdb_actimize_staging=$2
export pool=ingestion
config_file="${app_dir}/scripts/config/testabc.config"
#Read the config file
while read line
do
num_executors="`echo $line|cut -f1 -d'|'`"
executor_cores="`echo $line|cut -f2 -d'|'`"
executor_memory="`echo $line|cut -f3 -d'|'`"
driver_memory="`echo $line|cut -f4 -d'|'`"
property_file="`echo  $line|cut -f5 -d'|'`"

echo "INFO Executing: spark-submit --queue ${pool} --class com.vantiv.spark.actimize_tables_load.Actimize_Tables_Load --deploy-mode client --master yarn --conf spark.hadoop.parquet.enable.summary-metadata=false --num-executors ${num_executors} --executor-cores ${executor_cores} --executor-memory ${executor_memory} --driver-memory ${driver_memory}  --jars ${app_dir}/scripts/jar/abc.jar, ${app_dir}/scripts/jar/Util.jar ${app_dir}/scripts/config/${property_file} "

spark2-submit --queue ${pool} --class com.vantiv.spark.BDASparkModule1.CtrlTbl --deploy-mode client --master yarn --conf spark.hadoop.parquet.enable.summary-metadata=false --num-executors ${num_executors} --executor-cores ${executor_cores} --executor-memory ${executor_memory} --driver-memory ${driver_memory}  --jars /tmp/spark_BDA/abc.jar, ${app_dir}/scripts/jar/Util.jar ${app_dir}/scripts/config/${property_file} 

	if [ $? -eq 0 ]; then

		echo -e "\n"BDA Ingestion process Script $0 completed at $Prcs_Strtd_TS_Fmt"\n"

	else

		echo -e "\n"BDA Ingestion process Script $0 failed at $Prcs_Strtd_TS_Fmt"\n"
		exit 20
	fi
done <"$config_file"
