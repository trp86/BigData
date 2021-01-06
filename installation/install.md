# Up & Run Real-time Data Processing dev environment #

Below are the components which are the part of installation on **Ubuntu 20.04.1 LTS**
- Full Update & Upgrade
- openjdk-8-jre
- scala-2.11.11
- intellij-idea-community
- Apache kafka_2.11-2.4.0
- Apache spark-2.4.7-bin-hadoop2.7
- git version 2.25.1

## Step 1 - Full update and upgrade
```    
  $ sudo apt-get update
  $ sudo apt-get upgrade
```


## Step 2 - Install JDK 8
**Reference URL** : https://tecadmin.net/install-oracle-java-8-ubuntu-via-ppa/

- Check JAVA version
```    
  $ java -version
```

- Install JAVA 8 with root user and check JAVA version after installation
```    
  $ sudo apt install openjdk-8-jdk openjdk-8-jre
  $ java -version
```

- Set JAVA HOME
```    
  $ cat >> /etc/environment <<EOL
  	JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
  	JRE_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre
  	EOL
  $	source /etc/environment
```


## Step 3 - Install Scala 2.11.11

- Create an installation directory
```    
  $ mkdir /usr/local/src/scala
```

- Download the scala installable and unzip it
```    
  $ wget http://www.scala-lang.org/files/archive/scala-2.11.11.tgz
  $ tar xvf scala-2.11.11.tgz -C /usr/local/src/scala/ 
```

- Set SCALA HOME
```    
  $ echo 'export SCALA_HOME=/usr/local/src/scala/scala-2.11.11' >> ~/.bashrc
  $ echo 'export PATH=$SCALA_HOME/bin:$PATH' >> ~/.bashrc 
  $ source ~/.bashrc
```


## Step 4 - Install Intellij
**Reference URL** : https://linuxconfig.org/how-to-install-intellij-idea-on-ubuntu-20-04-linux-desktop

- Install the classic version of intellij
```    
  $ sudo snap install intellij-idea-community --classic
```

- To start the IntelliJ IDEA
```    
  $ intellij-idea-community
```


## Step 5 - Install Apache kafka_2.11-2.4.0 
**Reference URL** : https://hevodata.com/blog/how-to-install-kafka-on-ubuntu/

- Download kafka installable
```    
  $ wget "http://www-eu.apache.org/dist/kafka/2.4.0/kafka_2.11-2.4.0.tgz
```

- Create a directory for extracting Kafka
```    
  $ sudo mkdir /opt/kafka
  $ sudo tar -xvzf kafka_2.11-2.4.0.tgz --directory /opt/kafka
```

- Create a directory where kafka persists data
```    
  $ sudo mkdir /var/lib/kafka
  $ sudo mkdir /var/lib/kafka/data
```

- Allow kafka topics deletion and set the log dir

   NOTE: By default, Kafka doesn’t allow us to delete topics. To be able to delete topics, find the line (delete.topic.enable) and change it to true.
   
   Open the server.properties and perform the below changes 
```    
  $ sudo vi /opt/kafka/config/server.properties
  delete.topic.enable = true
  log.dirs=/var/lib/kafka/data
  
```

- Ensure Permission of Directories
```    
  $ sudo chown -R kafka:nogroup /opt/kafka
  $ sudo chown -R kafka:nogroup /var/lib/kafka
``` 

- Start Zookeeper and Kafka
```    
  $ sh /opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties
  $ sudo /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
``` 

**URL helped to resolve kafka installation issues** :
https://stackoverflow.com/questions/43293870/i-cant-run-zookeeper/43300155
https://stackoverflow.com/questions/59481878/unable-to-start-kafka-with-zookeeper-kafka-common-inconsistentclusteridexceptio

- Create 3 node kafka cluster (OPTIONAL)

    - Make 3 copies of server.properties and name them as server0.properties,server1.properties and server2.properties
    - Change the below parameters in server0.properties
    ```    
  broker.id=0
  listeners=PLAINTEXT://:9092
  log.dirs=/var/lib/kafka/data-0
  ``` 
    - Change the below parameters in server1.properties
    ```    
  broker.id=1
  listeners=PLAINTEXT://:9093
  log.dirs=/var/lib/kafka/data-1
  ``` 
    - Change the below parameters in server2.properties
    ```    
  broker.id=2
  listeners=PLAINTEXT://:9094
  log.dirs=/var/lib/kafka/data-2
  ``` 
    - Start zookeeper
    ```    
  $ sh /opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties
  $ sudo /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server0.properties
  $ sudo /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server1.properties
  $ sudo /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server2.properties

  ```   




