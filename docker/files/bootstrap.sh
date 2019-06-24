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

export HADOOP_HOME="/hadoop-3.1.1"

## Add it to bashrc for starting hadoop
echo 'export JAVA_HOME="/usr/lib/jvm/java-openjdk"' >> ~/.bashrc
echo 'export HADOOP_HOME="/hadoop-3.1.1"' >> ~/.bashrc


gprn "set up mysql"
service mysqld start
mysql -uroot -e "set password = PASSWORD('root');"
mysql -uroot -e "grant all privileges on *.* to 'root'@'%';"
service sshd start

gprn "start yarn"
hadoop-3.1.1/sbin/start-yarn.sh

gprn "Formatting name node"
hadoop-3.1.1/bin/hdfs namenode -format

gprn "Start hdfs"
hadoop-3.1.1/sbin/start-dfs.sh

jps

gprn "Set up metastore DB"
apache-hive-3.1.1-bin/bin/schematool -dbType mysql  -initSchemaTo 3.1.0

gprn "Start HMS server"
apache-hive-3.1.1-bin/bin/hive --service metastore -p  10000
