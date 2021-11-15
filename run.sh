#!/bin/bash
GCP_STORAGE_ADDRESS="big-data-lab-141317"

echo ">>>> cleaning files after previous runs"
if $(hadoop fs -test -d /user/motor_vehicle_collisions) ; then hadoop fs -rm -f -r /user/motor_vehicle_collisions; fi
if $(test -d ./output) ; then rm -rf ./output; fi
if $(test -d ./input) ; then rm -rf ./input; fi
if $(test -d ./input.zip) ; then rm -f ./input.zip; fi

echo " "
echo ">>>> Copying zip file with input data from Google Storage"
hadoop fs -mkdir -p /user/motor_vehicle_collisions
hadoop fs -copyToLocal gs://"${GCP_STORAGE_ADDRESS}"/input.zip .
unzip input.zip
hadoop fs -mkdir -p /user/motor_vehicle_collisions/input
hadoop fs -copyFromLocal ./input /user/motor_vehicle_collisions

echo " "
echo ">>>> Running MapReduce job"
hadoop jar Motor-Vehicle-Collision-1.0.jar pl.michalsz.mapreduce.JobRunner /user/motor_vehicle_collisions/input/datasource_collisions /user/motor_vehicle_collisions/output_mr

echo " "
echo ">>>> Running Hive script"
hive -f hive_transform.hql

echo " "
echo ">>>> Downloading result files from HDFS to local environment"
mkdir -p ./output6
hadoop fs -copyToLocal /user/motor_vehicle_collisions/output/* ./output

echo " "
echo ">>>> Final results:"
cat ./output/*
