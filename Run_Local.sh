Base_DNA_Path="/home/azimukangda5500/Project-2/spark/SupportingFiles"
#Base_DNA_Path="D:/Workspace/Chandra/Shell_script/fwdupdates/test"
Temp_Path=$Base_DNA_Path/TempFiles/
Script_Path=$Base_DNA_Path/ScriptFiles/
Property_Path=$Base_DNA_Path/PropertyFiles
extract_date=$(date +'%m-%d-%Y')
hive_Path="/user/azimukangda5500/Rajesh/Hive"

DateTs_Strt=`date +%Y-%m-%d_%H-%M-%S_%N`

Script_Nm=`basename $0 | cut -f1 -d'.'`

Log_File=${Script_Nm}_$DateTs_Strt.log
Err_Log_File=${Script_Nm}_$DateTs_Strt.err

echo -e "\n"Ecomm source file search process Script $Script_Nm started at $DateTs_Strt"\n"

#it_cnt=$3
Ecomm_Src_Path="/home/azimukangda5500/Project-2/spark/SupportingFiles/Source"
#Ecomm_Src_Path="/user/azimukangda5500/Rajesh/Source"
#Ecomm_Src_Sys_Nm=$2
#Ecomm_Full_Path=$Ecomm_Src_Path/$Ecomm_Src_Sys_Nm/
Ecomm_Full_Path=$Ecomm_Src_Path/
Pfx_Schema='PROD4.'
Schema_Extn='.schema'
Stats_Extn='.stats'

find $Ecomm_Full_Path -name '*.schema' -size 0c -o -name '*data.dsv' -size 0c -o -name '*.stats' -size 0c | sort > $Temp_Path/Src_0B_FL_Lst.txt

find $Ecomm_Full_Path -name '*.schema' -o -name '*data.dsv' -o -name '*.stats' | sort >  $Temp_Path/Src_FL_All_Lst.txt

find $Ecomm_Full_Path -name '*.stats' | sort >  $Temp_Path/Src_FL_All_Stats_Lst.txt

find $Ecomm_Full_Path -name '*.schema' | sort >  $Temp_Path/Src_FL_All_Schema_Lst.txt

find $Ecomm_Full_Path -name '*data.dsv' | sort >  $Temp_Path/Src_FL_All_Data_Lst.txt

Iteration=1

while read line;
do

	echo -e "\n"Iteration $Iteration started"\n"
	
  Src_FL=$line
  
	echo Processing iteration for $Src_FL
	
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
	
	Prop_file_path=$Property_Path/job_$Src_Nm.properties
	#hive_Path=`expr $hive_dir/$Src_Nm/extract_date=$extract_date`
	#echo $hive_Path

	echo -e tbl_id=$Iteration"00""\n"SRC_FILE_NM=$Src_FL_Nm"\n"SRC_FILE_SCHEMA_NM=$Schema_FL_Nm"\n"SRC_FILE_STATS_NM=$Stats_FL_Nm"\n"SRC_FILE_PATH=file://$Src_FL_Path"\n"TBL_NAME=$Src_Nm"\n"HIVE_PATH=$hive_Path>$Prop_file_path
	#echo -e tbl_id=$Iteration"00""\n"SRC_FILE_NM=$Src_FL_Nm"\n"SRC_FILE_SCHEMA_NM=$Schema_FL_Nm"\n"SRC_FILE_STATS_NM=$Stats_FL_Nm"\n"SRC_FILE_PATH=hdfs:$Src_FL_Path"\n"TBL_NAME=$Src_Nm>$Prop_file_path
	echo $Prop_file_path
	datafilepath=`echo file://$Src_FL_Path/$Src_FL_Nm`
	echo $datafilepath
	schemafilepath=`echo file://$Src_FL_Path/$Schema_FL_Nm`
	echo $datafilepath
	#spark-submit --class "SparkDataApp" /home/azimukangda5500/Project-2/spark/SupportingFiles/PropertyFiles/sparkdataframeapp_2.11-0.1.jar $Prop_file_path
	echo spark-submit --class "SparkDataApp" --master local[*] /home/azimukangda5500/Project-2/spark/SupportingFiles/PropertyFiles/sparkdataframeapp_2.11-0.1.jar file://$Prop_file_path "&">>spark_submit_command.sh
	#echo spark-submit --class "SparkDataApp" --master local[*] /home/azimukangda5500/Project-2/spark/SupportingFiles/PropertyFiles/sparkdataframeapp_2.11-0.1.jar $Prop_file_path>>spark_submit_command.sh

		if ((`expr $Iteration % 2` == 0))
		then
			echo "in if"
			#sh spark_submit_command.sh
			#echo "sh spark_submit_command.sh"
			cat /home/azimukangda5500/Project-2/spark/SupportingFiles/spark_submit_command.sh |sed '$ s/&$//' >> spark_submit_command1.sh
			sh spark_submit_command1.sh
			#cat /home/azimukangda5500/Project-2/spark/SupportingFiles/spark_submit_command.sh |sed '$ s/&$//' > spark_submit_command.sh
			#sh spark_submit_command.sh
			wait
			rm -f /home/azimukangda5500/Project-2/spark/SupportingFiles/spark_submit_command.sh
			rm -f /home/azimukangda5500/Project-2/spark/SupportingFiles/spark_submit_command1.sh

		fi
		
	#spark-submit --class "SparkDataApp" --master yarn-client /home/azimukangda5500/Project-2/spark/SupportingFiles/PropertyFiles/sparkdataframeapp_2.11-0.1.jar $Prop_file_path

	#sh spark_submit.sh $Prop_file_path
	Iteration=`expr $Iteration + 1`


done < $Temp_Path/Src_FL_All_Data_Lst.txt







