

# install zookeeper

```
    brew install zookeeper
```
# start zookeeper server

```
    zkServer start
```     


#### Error in starting zookeeper server

    ```
    zkServer start
    ZooKeeper JMX enabled by default
    Using config: /usr/local/etc/zookeeper/zoo.cfg
    mkdir: /usr/local/var/run/zookeeper: Permission denied
    Starting zookeeper ... /usr/local/Cellar/zookeeper/3.9.4/libexec/bin/zkServer.sh: line 178: /usr/local/var/run/zookeeper/data/zookeeper_server.pid: No such file or directory
    FAILED TO WRITE PID
    ```

    # Solution

    ```
    sudo mkdir -p /usr/local/var/run/zookeeper
    sudo chown -R sanjivsingh:admin /usr/local/var/run/zookeeper
    zkServer start
    ``` 

# stop zookeeper server

```
    zkServer stop
```
# check zookeeper server status

```
    zkServer status
``` 

# zookeeper cli

```
    zkCli
```

