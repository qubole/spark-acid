#!/bin/bash

eg='\033[0;32m'
enc='\033[0m'
echoe () {
	OIFS=${IFS}
	IFS='%'
	echo -e $@
	IFS=${OIFS}
}

gprn() {
	echoe "${eg} >> ${1}${enc}"
}


## Setup ENV variables

export JAVA_HOME="/usr/lib/jvm/java-openjdk"

export HDFS_NAMENODE_USER="root"
export HDFS_SECONDARYNAMENODE_USER="root"
export HDFS_DATANODE_USER="root"
export YARN_RESOURCEMANAGER_USER="root"
export YARN_NODEMANAGER_USER="root"

export HADOOP_HOME="/hadoop"
export HADOOP_ROOT_LOGGER=DEBUG
export HADOOP_COMMON_LIB_NATIVE_DIR="/hadoop/lib/native"

export TEZ_CONF_DIR="/tez/conf"
export TEZ_JARS="/tez"
export HADOOP_CLASSPATH=${TEZ_CONF_DIR}:${TEZ_JARS}/*:${HADOOP_CLASSPATH}:${JAVA_JDBC_LIBS}:${MAPREDUCE_LIBS}
export CLASSPATH=$CLASSPATH:${TEZ_CONF_DIR}:${TEZ_JARS}/*
export TEZ_CLASSPATH=${TEZ_CONF_DIR}:${TEZ_JARS}/*

## Add it to bashrc for starting hadoop
echo 'export JAVA_HOME="/usr/lib/jvm/java-openjdk"' >> ~/.bashrc
echo 'export HADOOP_HOME="/hadoop"' >> ~/.bashrc

echo 'export TEZ_CONF_DIR="/tez/conf"' >> ~/.bashrc
echo 'export TEZ_JARS="/tez"' >> ~/.bashrc
echo 'export HADOOP_CLASSPATH=${TEZ_CONF_DIR}:${TEZ_JARS}/*' >> ~/.bashrc
echo 'export CLASSPATH=$CLASSPATH:${TEZ_CONF_DIR}:${TEZ_JARS}/*' >> ~/.bashrc
echo 'export TEZ_CLASSPATH=${TEZ_CONF_DIR}:${TEZ_JARS}/*' >> ~/.bashrc


gprn "set up mysql"
service mysqld start

# Set root password 
mysql -uroot -e "set password = PASSWORD('root');"
mysql -uroot -e "grant all privileges on *.* to 'root'@'%' identified by 'root';"
service sshd start

gprn "start yarn"
hadoop/sbin/start-yarn.sh

gprn "Formatting name node"
hadoop/bin/hdfs namenode -format

gprn "Start hdfs"
hadoop/sbin/start-dfs.sh

jps

mkdir -p /hive/warehouse -dbType mysql  -initSchemaTo 3.1.0


gprn "Set up metastore DB"
hive/bin/schematool -dbType mysql  -initSchemaTo 3.1.0

gprn "Start HMS server"
hive/bin/hive --service metastore -p  10000 &

gprn "Sleep and wait for HMS to be up and running"
sleep 20

gprn "Start HiveServer2"
hive/bin/hive --service hiveserver2 --hiveconf hive.server2.thrift.port=10001 --hiveconf hive.execution.engine=tez
