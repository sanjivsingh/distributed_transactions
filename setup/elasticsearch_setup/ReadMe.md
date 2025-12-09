# Install Manually 

## download from https://www.elastic.co/downloads/elasticsearch

elasticsearch-9.2.1-darwin-x86_64.tar.gz

## prepare 

unzip elasticsearch-9.2.1-darwin-x86_64.tar.gz
cd elasticsearch-9.2.1

## start elasticsearch
cd /Users/sanjivsingh/softwares/elasticsearch-9.2.1
./bin/elasticsearch

or 

nohup ./bin/elasticsearch > es.out &

#
export DYLD_LIBRARY_PATH=/usr/local/Cellar/zstd/1.5.7/lib
export ES_HOME=~/dev/elasticsearch/elasticsearch-1.1.0
export JAVA_HOME=/Library/Java/JavaVirtualMachines/microsoft-21.jdk/Contents/Home
export PATH=$ES_HOME/bin:$JAVA_HOME/bin:$PATH


# Troubleshoot

## Error :
````
[2025-11-22T02:50:43,877][WARN ][o.e.n.NativeAccess       ] [IMUL-ML0406] Unable to load native provider. Native methods will be disabled.java.lang.UnsatisfiedLinkError: Native library [/Users/sanjivsingh/softwares/elasticsearch-9.2.1/lib/platform/darwin-x64/libzstd.dylib] does not exist
	at org.elasticsearch.nativeaccess@9.2.1/org.elasticsearch.nativeaccess.lib.LoaderHelper.loadLibrary(LoaderHelper.java:58)
	at org.elasticsearch.nativeaccess@9.2.1/org.elasticsearch.nativeaccess.jdk.JdkZstdLibrary.<clinit>(JdkZstdLibrary.java:29)
	at org.elasticsearch.nativeaccess@9.2.1/org.elasticsearch.nativeaccess.lib.NativeLibraryProvider.getLibrary(NativeLibraryProvider.java:56)
	at org.elasticsearch.nativeaccess@9.2.1/org.elasticsearch.nativeaccess.AbstractNativeAccess.<init>(AbstractNativeAccess.java:31)
	at org.elasticsearch.nativeaccess@9.2.1/org.elasticsearch.nativeaccess.PosixNativeAccess.<init>(PosixNativeAccess.java:35)

See logs for more details.
````

### Solution 



## Error :

````
[2025-11-22T03:16:42,438][INFO ][o.e.c.m.DataStreamFailureStoreSettings] [IMUL-ML0406] Updated data stream name patterns for enabling failure store to [[]]
[2025-11-22T03:16:42,610][ERROR][o.e.b.Elasticsearch      ] [IMUL-ML0406] fatal exception while booting Elasticsearchorg.elasticsearch.ElasticsearchSecurityException: invalid configuration for xpack.security.transport.ssl - [xpack.security.transport.ssl.enabled] is not set, but the following settings have been configured in elasticsearch.yml : [xpack.security.transport.ssl.keystore.secure_password,xpack.security.transport.ssl.truststore.secure_password]
	at org.elasticsearch.xcore@9.2.1/org.elasticsearch.xpack.core.ssl.SSLService.validateServerConfiguration(SSLService.java:626)
	at org.elasticsearch.xcore@9.2.1/org.elasticsearch.xpack.core.ssl.SSLService.loadSslConfigurations(SSLService.java:600)
	at org.elasticsearch.xcore@9.2.1/org.elasticsearch.xpack.core.ssl.SSLService.<init>(SSLService.java:207)
	at org.elasticsearch.xcore@9.2.1/org.elasticsearch.xpack.core.XPackPlugin.createSSLService(XPackPlugin.java:506)
	at org.elasticsearch.xcore@9.2.1/org.elasticsearch.xpack.core.XPackPlugin.createComponents(XPackPlugin.java:332)
```
### Solution 

```
./elasticsearch-keystore  list


	xpack.security.http.ssl.keystore.secure_password
	xpack.security.transport.ssl.keystore.secure_password
	xpack.security.transport.ssl.truststore.secure_password

./elasticsearch-keystore remove xpack.security.http.ssl.keystore.secure_password
./elasticsearch-keystore remove xpack.security.transport.ssl.keystore.secure_password
./elasticsearch-keystore remove xpack.security.transport.ssl.truststore.secure_password
```

## Error :

```
[2025-12-04T05:19:26,675][ERROR][o.e.b.Elasticsearch      ] [impetus-nsrv07.impetus.com] node validation exception
[1] bootstrap checks failed. You must address the points described in the following [1] lines before starting Elasticsearch. For more information see [https://www.elastic.co/docs/deploy-manage/deploy/self-managed/bootstrap-checks?version=9.2]
bootstrap check failure [1] of [1]: max virtual memory areas vm.max_map_count [65530] is too low, increase to at least [262144]; for more information see [https://www.elastic.co/docs/deploy-manage/deploy/self-managed/bootstrap-checks?version=9.2#bootstrap-checks-max-map-count]
ERROR: Elasticsearch did not exit normally - check the logs at /home/impadmin/software/elasticsearch-9.2.1/logs/elasticsearch.log
```
### Solution 
```
sudo sysctl -w vm.max_map_count=262144
```


