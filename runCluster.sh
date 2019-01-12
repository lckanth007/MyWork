Base_DNA_Path="/user/n46995a"
Temp_Path=$Base_DNA_Path/TempFiles
Script_Path=$Base_DNA_Path/ScriptFiles/
Property_Path=$Base_DNA_Path/PropertyFiles

Local_Base_DNA_Path="/home/n46995a"
Local_Temp_Path=$Local_Base_DNA_Path/TempFiles
Local_Script_Path=$Local_Base_DNA_Path/ScriptFiles/
Local_Property_Path=$Local_Base_DNA_Path/PropertyFiles

DateTs_Strt=`date +%Y-%m-%d_%H-%M-%S_%N`

Script_Nm=`basename $0 | cut -f1 -d'.'`

Log_File=${Script_Nm}_$DateTs_Strt.log
Err_Log_File=${Script_Nm}_$DateTs_Strt.err

echo -e "\n"Ecomm source file search process Script $Script_Nm started at $DateTs_Strt"\n"
 
#it_cnt=$3
Ecomm_Src_Path="/home/n46995a/source"
Ecomm_Tar_Path="/user/n46995a/output/extract_date="
#Ecomm_Src_Sys_Nm=$2
#Ecomm_Full_Path=$Ecomm_Src_Path/$Ecomm_Src_Sys_Nm/
Ecomm_Full_Path=$Ecomm_Src_Path/
Pfx_Schema='PROD4.'
Schema_Extn='.schema'
Stats_Extn='.stats'
echo -e "\n"enter 1"\n"
find $Ecomm_Full_Path -name '*.schema' -size 0c -o -name '*data.dsv' -size 0c -o -name '*.stats' -size 0c | sort > $Local_Temp_Path/Src_0B_FL_Lst.txt

find $Ecomm_Full_Path -name '*.schema' -o -name '*data.dsv' -o -name '*.stats' | sort >  $Local_Temp_Path/Src_FL_All_Lst.txt

find $Ecomm_Full_Path -name '*.stats' | sort >  $Local_Temp_Path/Src_FL_All_Stats_Lst.txt

find $Ecomm_Full_Path -name '*.schema' | sort >  $Local_Temp_Path/Src_FL_All_Schema_Lst.txt

find $Ecomm_Full_Path -name '*data.dsv' | sort >  $Local_Temp_Path/Src_FL_All_Data_Lst.txt

hadoop fs -copyFromLocal  $Local_Temp_Path/Src_FL_All_Data_Lst.txt $Temp_Path

echo -e "\n"enter 2"\n"
Iteration=1


echo -e "\n"enter $Iteration"\n"

while read line;
do

	echo -e "\n"Iteration $Iteration started"\n"
	
  Src_FL=$line
  
	echo Processing iteration for $Src_FL
	echo -e "\n"enter $Iteration"\n"
	Src_FL_Nm=`basename $Src_FL`
	echo Source file name is $Src_FL_Nm
        Src_FL_Path=`dirname $Src_FL`
	echo Source file name $Src_FL_Nm exists in path $Src_FL_Path/
	
	Dlmtr_Cnt=`echo "$Src_FL_Nm" | awk -F'_' '{ print NF }' | tr -d '\n'`
	echo Dlmtr_Cnt is $Dlmtr_Cnt

	Dlmtr_Cnt1=`expr $Dlmtr_Cnt - 4`
	echo Dlmtr_Cnt1 is $Dlmtr_Cnt1

	Src_Nm=`echo $Src_FL_Nm | cut -f3-$Dlmtr_Cnt1 -d'_'`
	echo Source name from source file $Src_FL_Nm is $Src_Nm

	Schema_FL_Nm=`echo $Pfx_Schema$Src_Nm$Schema_Extn`
	echo Schema file name for source name $Src_FL_Nm is $Schema_FL_Nm

	Dlmtr_Cnt2=`expr $Dlmtr_Cnt - 1`
	echo Dlmtr_Cnt2 is $Dlmtr_Cnt2

	Stats_FL_Nm_Sfx=`echo $Src_FL_Nm | cut -f1-$Dlmtr_Cnt2 -d'_'`
	echo Stats_FL_Nm_Sfx is $Stats_FL_Nm_Sfx

	Stats_FL_Nm=`echo $Stats_FL_Nm_Sfx$Stats_Extn`
	echo stats file name from source file name $Src_FL_Nm is $Stats_FL_Nm
	
	Local_Prop_file_path=$Local_Property_Path/job_$Src_FL_Nm.properties

        echo -e "\n"Property file name: $Local_Prop_file_path"\n"
        
  	hadoop fs -copyFromLocal $Local_Prop_file_path $Property_Path
   	
	Prop_file_path =$Property_Path/job_$Src_FL_Nm.properties

	echo -e tbl_id=$Iteration"00""\n"SRC_FILE_NM=$Src_FL_Nm"\n"SRC_FILE_SCHEMA_NM=$Schema_FL_Nm"\n"SRC_FILE_STATS_NM=$Stats_FL_Nm"\n"TAR_FILE_PATH=hdfs://bda6clu-ns/user/n46995a/output"\n"SRC_FILE_PATH=hdfs://bda6clu-ns/user/n46995a/source/localCopy"\n"TBL_NAME=$Src_Nm>$Prop_file_path
	#echo -e tbl_id=$Iteration"00""\n"SRC_FILE_NM=$Src_FL_Nm"\n"SRC_FILE_SCHEMA_NM=$Schema_FL_Nm"\n"SRC_FILE_STATS_NM=$Stats_FL_Nm"\n"SRC_FILE_PATH=hdfs:$Src_FL_Path"\n"TBL_NAME=$Src_Nm>$Prop_file_path
	
	echo $Prop_file_path
	datafilepath=`echo file://$Src_FL_Path/$Src_FL_Nm`
	echo $datafilepath
	schemafilepath=`echo file://$Src_FL_Path/$Schema_FL_Nm`
	echo $datafilepath
	#spark-submit --class "SparkDataApp" /home/azimukangda5500/Project-2/spark/SupportingFiles/PropertyFiles/sparkdataframeapp_2.11-0.1.jar $Prop_file_path
hadoop fs -copyFromLocal $Src_FL_Path/$Src_FL_Nm /user/n46995a/source/localCopy
#hadoop fs -put $schemafilepath hdfs://bda6clu-ns/user/n46995a/source/data	
echo spark2-submit --queue ingestion --class "com.spark.ingest.SparkIngestApp" --master yarn --deploy-mode cluster hdfs://bda6clu-ns/user/n46995a/jarFiles/sparksample_2.11-0.1_CSV.jar $Prop_file_path "&">>spark_submit_command.sh
echo jps >> spark_submit_command.sh	
#spark-submit --class "SparkDataApp" --master yarn-client /home/azimukangda5500/Project-2/spark/SupportingFiles/PropertyFiles/sparkdataframeapp_2.11-0.1.jar $Prop_file_path

	#sh spark_submit.sh $Prop_file_path
	Iteration=`expr $Iteration + 1`


done < $Local_Temp_Path/Src_FL_All_Data_Lst.txt
sh spark_submit_command.sh
jps
rm -f /home/n46995a/spark_submit_command.sh






