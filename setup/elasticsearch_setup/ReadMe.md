brew install zstd


# Install Manually 

## download from https://www.elastic.co/downloads/elasticsearch

elasticsearch-9.2.1-darwin-x86_64.tar.gz

## prepare 

unzip elasticsearch-9.2.1-darwin-x86_64.tar.gz
cd elasticsearch-9.2.1

## start elasticsearch
cd /Users/sanjivsingh/softwares/elasticsearch-9.2.1
bin/elasticsearch

#
export DYLD_LIBRARY_PATH=/usr/local/Cellar/zstd/1.5.7/lib
export ES_HOME=~/dev/elasticsearch/elasticsearch-1.1.0
export JAVA_HOME=/Library/Java/JavaVirtualMachines/microsoft-21.jdk/Contents/Home
export PATH=$ES_HOME/bin:$JAVA_HOME/bin:$PATH



# Troubleshoot

````
[2025-11-22T02:50:43,877][WARN ][o.e.n.NativeAccess       ] [IMUL-ML0406] Unable to load native provider. Native methods will be disabled.java.lang.UnsatisfiedLinkError: Native library [/Users/sanjivsingh/softwares/elasticsearch-9.2.1/lib/platform/darwin-x64/libzstd.dylib] does not exist
	at org.elasticsearch.nativeaccess@9.2.1/org.elasticsearch.nativeaccess.lib.LoaderHelper.loadLibrary(LoaderHelper.java:58)
	at org.elasticsearch.nativeaccess@9.2.1/org.elasticsearch.nativeaccess.jdk.JdkZstdLibrary.<clinit>(JdkZstdLibrary.java:29)
	at org.elasticsearch.nativeaccess@9.2.1/org.elasticsearch.nativeaccess.lib.NativeLibraryProvider.getLibrary(NativeLibraryProvider.java:56)
	at org.elasticsearch.nativeaccess@9.2.1/org.elasticsearch.nativeaccess.AbstractNativeAccess.<init>(AbstractNativeAccess.java:31)
	at org.elasticsearch.nativeaccess@9.2.1/org.elasticsearch.nativeaccess.PosixNativeAccess.<init>(PosixNativeAccess.java:35)

See logs for more details.
````



````
[2025-11-22T03:16:42,438][INFO ][o.e.c.m.DataStreamFailureStoreSettings] [IMUL-ML0406] Updated data stream name patterns for enabling failure store to [[]]
[2025-11-22T03:16:42,610][ERROR][o.e.b.Elasticsearch      ] [IMUL-ML0406] fatal exception while booting Elasticsearchorg.elasticsearch.ElasticsearchSecurityException: invalid configuration for xpack.security.transport.ssl - [xpack.security.transport.ssl.enabled] is not set, but the following settings have been configured in elasticsearch.yml : [xpack.security.transport.ssl.keystore.secure_password,xpack.security.transport.ssl.truststore.secure_password]
	at org.elasticsearch.xcore@9.2.1/org.elasticsearch.xpack.core.ssl.SSLService.validateServerConfiguration(SSLService.java:626)
	at org.elasticsearch.xcore@9.2.1/org.elasticsearch.xpack.core.ssl.SSLService.loadSslConfigurations(SSLService.java:600)
	at org.elasticsearch.xcore@9.2.1/org.elasticsearch.xpack.core.ssl.SSLService.<init>(SSLService.java:207)
	at org.elasticsearch.xcore@9.2.1/org.elasticsearch.xpack.core.XPackPlugin.createSSLService(XPackPlugin.java:506)
	at org.elasticsearch.xcore@9.2.1/org.elasticsearch.xpack.core.XPackPlugin.createComponents(XPackPlugin.java:332)
```

