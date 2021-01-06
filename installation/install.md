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

- Set JAVA home
```    
  $ cat >> /etc/environment <<EOL
  	JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
  	JRE_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre
  	EOL
  $	source /etc/environment
```
