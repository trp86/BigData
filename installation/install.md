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

- Allow kafka topics deletion

NOTE: By default, Kafka doesn’t allow us to delete topics. To be able to delete topics, find the line (delete.topic.enable)and change it to true.
```    
  $ sudo vi /opt/kafka/config/server.properties
```
   