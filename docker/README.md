A pseudo-distributed Hadoop image for testing Spark ACID datasource, based on 
1. CentOS 6
2. [Hadoop3.1.1] (https://archive.apache.org/dist/hadoop/common/hadoop-3.1.1/hadoop-3.1.1.tar.gz)
3. [Hive3.1.1] (http://mirrors.estointernet.in/apache/hive/hive-3.1.1/apache-hive-3.1.1-bin.tar.gz)
4. [MySQL 5.6.44] (http://repo.mysql.com/mysql-community-release-el6-5.noarch.rpm)

# Setup

Refer for [Install Docker] (https://docs.docker.com/v17.12/install/) to install docker.

# Build

To build docker image
```bash
./build
```

# Start

_NB Configure docker to run with the atleast 4GB of memory. For mac it can be configured in Docker Desktop_

To start docker image
```bash
./start
```

# Stop

To stop docker
```bash
./stop
```

