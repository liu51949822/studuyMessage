ElasticSearch 是一个基于 Lucene 的搜索服务器。它是一个分布式多用户能力的全文搜索引擎，基于 RESTful web 接口。Elasticsearch 是用 Java 开发的。

目前 Elasticsearch 与阿里云携手合作在阿里云上提供的 Elasticsearch 和 Kibana 托管云服务，包含 X-Pack 全部功能。目前应用 elasticsearch 构建搜索系统的公司有：Github 使用 Elasticsearch 搜索 TB 级别的数据、Sony（Sony 公司使用 elasticsearch 作为信息搜索引擎）国内有赶集网、京东等。

搜索流程如图：用户打开浏览器输入关键字如“羽绒服”后浏览器将会请求我们的应用服务器，应用服务器通过调用 elasticsearch 的 api 后请求 es 的搜索服务器，搜索服务器得到结果后返回给应用服务器最终返回到浏览器。

![搜索流程](https://doc.shiyanlou.com/document-uid618173labid4433timestamp1515551859245.png)



##### cluster（集群）

代表一个集群，集群中有多个节点，其中有一个为主节点，默认这个主节点是可以通过选举产生的，配置方式可指定哪些节点主节点，哪些节点数据节点，主从节点是对于集群内部来说的。es 的一个概念就是去中心化，字面上理解就是无中心节点，这是对于集群外部来说的，因为从外部来看 es 集群，在逻辑上是个整体，你与任何一个节点的通信和与整个 es 集群通信是等价的。

##### shards（分片）

代表索引分片，es 可以把一个完整的索引分成多个分片，这样的好处是可以把一个大的索引拆分成多个，分布到不同的节点上。构成分布式搜索，可提高搜索性能，当然也不是越大越好，还是要看你要存储的数据量，如果数据量大，可把分片设置大点，数据量小，可把分片设置小点，默认是 5 个分片，分片的数量只能在索引创建前指定，并且索引创建后不能更改。

##### replicas(副本)

代表索引副本，es 可以设置多个索引的副本，副本的作用一是提高系统的容错性，当某个节点某个分片损坏或丢失时可以从副本中恢复。二是提高 es 的查询效率，es 会自动对搜索请求进行负载均衡。默认是 1 个副本。

##### gateway

代表 es 索引快照的存储方式，es 默认是先把索引存放到内存中，当内存满了时再持久化到本地硬盘，默认是 1 g,官方建议一个节点最大不超过 32G，超过 32G 性能反而会下降。gateway 对索引快照进行存储，当这个 es 集群关闭再重新启动时就会从 gateway 中读取索引备份数据。

##### discovery.zen

代表 es 的自动发现节点机制，es 是一个基于 p2p 的系统，它先通过广播寻找存在的节点，再通过多播协议来进行节点之间的通信，同时也支持点对点的交互。早期的版本不需要任何配置，只要在一个网段，启动后就会组成集群，非常方便，但是同时也带来了问题，某台机器的测试数据和另一台机器的数据会互相同步。

##### Transport

代表 es 内部节点或集群与客户端的交互方式，默认内部是使用 tcp 协议进行交互，同时它支持 http 协议（json 格式）、thrift、servlet、memcached、zeroMQ 等的传输协议（通过插件方式集成）java 客户端的方式是以 tcp 协议在 9300 端口上进行通信 http 客户端的方式是以 http 协议在 9200 端口上进行通信。



在实验楼环境中，打开 xfce 终端，依次执行如下命令： 1.切换到 Code 目录

```bash
cd /home/shiyanlou/Code/
```

2.下载 elasticsearch 软件包到当前目录中

```bash
sudo wget https://labfile.oss.aliyuncs.com/courses/1014/elasticsearch-2.3.4.zip
```

3.解压 elasticsearch 文件包

```bash
unzip elasticsearch-2.3.4.zip
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid18510labid4433timestamp1516863727199.png) 4.切换到 elasticsearch-2.3.4

```bash
cd elasticsearch-2.3.4
```

5.创建日志目录和数据目录

```bash
mkdir logs
mkdir data
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4433timestamp1515563817147.png)

============下面为安装 service 的启动步骤============ 6.切换到 elasticsearch-2.3.4 的 bin 目录下

```bash
cd bin
```

7.安装 service 用于启动 es

```bash
 sudo wget https://labfile.oss.aliyuncs.com/courses/1014/elasticsearch-servicewrapper-master.zip
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid18510labid4433timestamp1516863883271.png)

8.解压 elasticsearch-servicewrapper-master.zip 文件包

```bash
unzip elasticsearch-servicewrapper-master.zip
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid18510labid4433timestamp1516863935798.png)

9.执行移动命令把 bin/elasticsearch-servicewrapper-master 下的 service 移动到 bin 目录下

```bash
mv elasticsearch-servicewrapper-master/service   /home/shiyanlou/Code/elasticsearch-2.3.4/bin
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4433timestamp1515565744270.png)

10.修改配置

修改配置 `elasticsearch-2.3.4/bin/service/elasticsearch.conf` 和 `elasticsearch-2.3.4/config/elasticsearch.yml`

在 `elasticsearch.conf` 第 30 行左右修改配置

```
wrapper.java.classpath.1=%ES_HOME%/bin/service/lib/wrapper.jar
# 注释掉 classpath.2 这句，把以前的 classpath.3 和 4 改为 2 和 3
# wrapper.java.classpath.2=%ES_HOME%/lib/elasticsearch*.jar
wrapper.java.classpath.2=%ES_HOME%/lib/*.jar
wrapper.java.classpath.3=%ES_HOME%/lib/sigar/*.jar
```

![图片描述](https://doc.shiyanlou.com/courses/uid871732-20191015-1571119463866)

在第 52 行后加入

```
wrapper.java.additional.10=-Des.insecure.allow.root=true
```

启动类的修改 在 58 行左右

```
# 把这句注释掉，然后在下面添加下面这两句
# wrapper.app.parameter.1=org.elasticsearch.bootstrap.ElasticsearchF
wrapper.app.parameter.1=org.elasticsearch.bootstrap.Elasticsearch
wrapper.app.parameter.2=start
```

![图片描述](https://doc.shiyanlou.com/courses/uid871732-20191015-1571119526194)

11.修改 config/elasticsearch.yml 文件

新加入下面这两行

```yml
network.host: 127.0.0.1
security.manager.enabled: false
```

12.执行关键的一步启动 elasticsearch

在 bin/service/ 下执行

```bash
./elasticsearch start # 启动后检查日志没有任何错误
./elasticsearch stop
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4433timestamp1515574548127.png)

13.检查 elasticsearch 端口 `9300` 用于程序开发 `9200` 用于 rest 访问



1.创建 elasticsearch-analysis-ik-1.8.1 目录

```bash
cd /home/shiyanlou/Code/elasticsearch-2.3.4/plugins
mkdir  elasticsearch-analysis-ik-1.8.1
cd  elasticsearch-analysis-ik-1.8.1
```

2.下载 elasticsearch-ik 分词软件包到`elasticsearch-analysis-ik-1.8.1`目录中

```bash
sudo wget https://labfile.oss.aliyuncs.com/courses/1014/elasticsearch-analysis-ik-1.8.1.zip
```

3.解压 `elasticsearch-analysis-ik-1.8.1.zip`

```bash
unzip elasticsearch-analysis-ik-1.8.1.zip
```

4.删除 `elasticsearch-analysis-ik-1.8.1.zip`

```bash
rm -rf elasticsearch-analysis-ik-1.8.1.zip
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4522timestamp1516975762993.png)

5.进入 elasticsearch-analysis-ik-1.8.1 目录并修改 plugin-descriptor.properties （大概在 71 行）

```bash
elasticsearch.version=2.3.4
```

6.重新启动 elasticsearch

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4522timestamp1515768405379.png)

7.进入 logs 目录，查看 elasticsearch.log 日志出现加载 ik 的字样代表成功

```bash
cat elasticsearch.log | grep "ik-analyzer"
```

![图片描述](https://doc.shiyanlou.com/courses/uid871732-20191015-1571120406425)

下节实验中我们将会结合本节实验内容进行代码方面的实战。



#### 2.2.1 创建 Maven 项目 syl_es

因为升级后的环境自带的 Eclipse 没有 Maven，所以我们使用 ScalaIDE。

```bash
$ cd ~
$ wget https://labfile.oss.aliyuncs.com/courses/1014/scala-SDK-4.7.0-vfinal-2.12-linux.gtk.x86_64.tar.gz
$ tar -zxvf scala-SDK-4.7.0-vfinal-2.12-linux.gtk.x86_64.tar.gz
$ cd eclipse
$ ./eclipse
```

等待启动完毕后全程需要保持终端窗口运行，然后点击 Launch。

![图片描述](https://doc.shiyanlou.com/courses/uid871732-20191015-1571115324009)

等待主页面加载完毕后，选择 File -> New -> Other，新建一个 Maven 项目 syl_es ，包名为 com.syl.es，选择 quickstart 生成项目。

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid18510labid4524timestamp1517132039486.png)

#### 2.2.2 在 pom 中加入 elasticsearch 的 jar 配置

```xml
<dependency>
    <groupId>org.elasticsearch</groupId>
    <artifactId>elasticsearch</artifactId>
    <version>2.3.4</version>
</dependency>
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid18510labid4524timestamp1517132151474.png)

#### 2.2.3 导入 maven 的 jar 包（由于环境原因，在显示 build success 后可能会等待一些时间才能导入 jar 包）

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid18510labid4524timestamp1517132195698.png)

#### 2.2.4 创建类编写创建索引的代码

在 `com.syl.es`下新建 CreateIndex 类，写入代码：

```java
package com.syl.es;
import java.net.InetAddress;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class CreateIndex {

    /**
     * @param args
     */
    public static void main(String[] args) {
        createCluterName("user");    //调用创建索引函数
    }
    /**
     * 创建索引
     * @param indices 索引名称
     */
     public static void createCluterName(String indices){
         //索引服务的地址
         String elasticServer= "127.0.0.1";
         //索引服务的端口
         Integer elasticServerPort = 9300;
         Client client=null;
         try{
             //初始化连接
             Settings settings = Settings.settingsBuilder()
                     .build();
             client = TransportClient.builder().settings(settings).build()
                      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(elasticServer),
                              elasticServerPort));
             //创建索引
             client.admin().indices().prepareCreate(indices).execute().actionGet();
             //关闭连接
             client.close();
         }catch (Exception e) {
             e.printStackTrace();
         }
     }
}
```

以 Java Application 的方式，运行`CreateInde.java`文件。

#### 2.2.5 查看索引

打开浏览器访问 head 地址`http://127.0.0.1:9200/_plugin/head`查看索引创建情况

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4524timestamp1515855545527.png)

此时我们看到已经成功创建索引 user，并且默认 5 个分片，1 个副本。接下来，我们继续讲解如何删除索引。



#### 2.3.1 修改`CreatIndex`类，添加删除索引函数

```java
    /**
     * 删除索引
     * @param indices 索引名称
     */
      public static void deleteCluterName(String indices){
          //索引服务的地址
          String elasticServer= "127.0.0.1";
          //索引服务的端口
          Integer elasticServerPort = 9300;
          Client client=null;
          try{
              //初始化连接
              Settings settings = Settings.settingsBuilder()
                      .build();
              client = TransportClient.builder().settings(settings).build()
                       .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(elasticServer),
                               elasticServerPort));
              //删除索引
              client.admin().indices().prepareDelete(indices).execute().actionGet();
              //关闭连接
              client.close();
          }catch (Exception e) {
              e.printStackTrace();
          }
      }
```

#### 2.3.2 修改主函数，调用删除索引函数

注释掉 createCluterName 的调用，增加调用删除索引函数。

```java
    public static void main(String[] args) {
          //调用创建索引方法
        //createCluterName("user");
          //调用删除索引方法
          deleteCluterName("user");
    }
```

#### 2.3.3 运行代码

以 Java Application 的方式，运行`CreateInde.java`文件。

#### 2.3.4 查看结果

打开浏览器访问 head 地址`http://127.0.0.1:9200/_plugin/head`可以查看到之前创建的索引消失了。



#### 3.1.1 在`syl_es`项目的`com.syl.es`下新建 CreateMapping 类

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4545timestamp1516196274561.png)

#### 3.1.2

编写 mapping 方法，具体代码如下：

```java
package com.syl.es;

import java.net.InetAddress;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class CreateMapping {

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) {
        //调用创建索引方法
        createMapping("user","userInfo");
    }
    /**
     * 创建索引mapping
     * @param indices 索引名称
     * @param mappingType 索引type名称
     */
     public static void createMapping(String indices,String mappingType){
            //索引服务的地址
            String elasticServer= "127.0.0.1";
            //索引服务的端口
            Integer elasticServerPort = 9300;
            Client client=null;
            try{
                //初始化连接
                Settings settings = Settings.settingsBuilder()
                         .build();
                client = TransportClient.builder().settings(settings).build()
                         .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(elasticServer), elasticServerPort));
                new XContentFactory();
                XContentBuilder builder=XContentFactory.jsonBuilder()
                .startObject()
                .startObject(mappingType)
                .startObject("properties")
                .startObject("name").field("type", "string").field("analyzer","ik").field("searchAnalyzer", "ik").endObject()//设置name为ik分词
                .startObject("nickname").field("type", "string").field("index", "not_analyzed").endObject()//设置nicknamenot_analyzed不分词
                .startObject("nativeplace").field("type", "string").field("index", "no").endObject()//设置nativeplace不索引
                .startObject("address").field("type", "string").field("index", "no").endObject()//设置nativeplace不索引
                .startObject("birthdate").field("type", "date").field("format", "yyyy-MM-dd").field("index", "not_analyzed").endObject()//设置birthdate为日期型格式
                .endObject()
                .endObject()
                .endObject();
                System.out.println(builder.toString());
                PutMappingRequest mapping = Requests.putMappingRequest(indices).type(mappingType).source(builder);
                //创建索引的mapping
                client.admin().indices().putMapping(mapping).actionGet();
                //关闭连接
                client.close();
            }catch (Exception e) {
                e.printStackTrace();
            }

        }
}
```

以 Java Application 的方式，运行`CreateMapping.java`文件 (在运行 CreateMapping.java 之前，需要创建好索引)。

#### 3.1.3 访问地址栏查看 mapping 创建情况

通过浏览器访问`http://127.0.0.1:9200/user/userInfo/_mapping`

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4545timestamp1516197208837.png)

#### 3.1.4 查看 mapping 创建情况

打开浏览器访问 head 地址 `http://127.0.0.1:9200/_plugin/head`打开信息->索引信息

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4545timestamp1516196505132.png)

通过查看 head 发现 mapping 已经创建成功。



#### 3.2.1 创建 batchAdd 方法

在`AddData.java`中添加函数，请根据编辑报错，解决 Import 报错。

```java
    /**
     * 批量添加索引数据
     */
   public static void batchAdd (){
        //初始化数据
        List dataList = new ArrayList();
        Map dataMap1=new HashMap();
        dataMap1.put("name","张中国1");
        dataMap1.put("nickname", "张中1");
        dataMap1.put("nativeplace", "上海静安寺1");
        dataMap1.put("address", "上海静安寺1街坊10栋1");
        dataMap1.put("birthdate", "1980-02-15");

        Map dataMap2=new HashMap();
        dataMap2.put("name","张中国2");
        dataMap2.put("nickname", "张中2");
        dataMap2.put("nativeplace", "上海静安寺2");
        dataMap2.put("address", "上海静安寺1街坊10栋2");
        dataMap2.put("birthdate", "1980-02-16");
        dataList.add(dataMap1);
        dataList.add(dataMap2);
        //索引服务的地址
        String elasticServer= "127.0.0.1";
        //索引服务的端口
        Integer elasticServerPort = 9300;
        Client client=null;
        try{
            //初始化连接
            Settings settings = Settings.settingsBuilder().build();
            client = TransportClient.builder().settings(settings).build()
                     .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(elasticServer), elasticServerPort));
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            int y=dataList.size();
            //添加数据
            for(int i=0;i<y;i++){
                Map<String, Object> m = (Map)dataList.get(i);
                bulkRequest.add(client.prepareIndex("user", "userInfo", i+2+"")
                     .setSource(m));
                if (i % 10000 == 0) {
                    bulkRequest.execute().actionGet();
                }
            }
            bulkRequest.execute().actionGet();
            //关闭连接
            client.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("do end!!");
    }
```

代码中模拟实际项目构造多条 list 数据，list 中放 map 类型的数据，并通过循环批量添加并且每 1 万条数据添加一次。

#### 2.3.2 修改主函数，调用删除索引函数

注释掉创建单条 elasticsearch 数据函数的调用，增加调用批量创建 elasticsearch 数据。

```java
 public static void main(String[] args) {
        //添加数据
        // add();
        //批量添加数据
        batchAdd();
    }
```

#### 2.3.3 运行代码

以 Java Application 的方式，运行`AddData.java`文件。

#### 2.3.4 查看数据添加情况

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4560timestamp1516283370280.png)

点击数据浏览在点击类型 userInfo 我们可以看到已经有 3 条数据添加进去了。

> 拓展：大家可以试试如果 id 相同添加后是什么效果?

#### 3.2.1 创建 batchAdd 方法

在`AddData.java`中添加函数，请根据编辑报错，解决 Import 报错。

```java
    /**
     * 批量添加索引数据
     */
   public static void batchAdd (){
        //初始化数据
        List dataList = new ArrayList();
        Map dataMap1=new HashMap();
        dataMap1.put("name","张中国1");
        dataMap1.put("nickname", "张中1");
        dataMap1.put("nativeplace", "上海静安寺1");
        dataMap1.put("address", "上海静安寺1街坊10栋1");
        dataMap1.put("birthdate", "1980-02-15");

        Map dataMap2=new HashMap();
        dataMap2.put("name","张中国2");
        dataMap2.put("nickname", "张中2");
        dataMap2.put("nativeplace", "上海静安寺2");
        dataMap2.put("address", "上海静安寺1街坊10栋2");
        dataMap2.put("birthdate", "1980-02-16");
        dataList.add(dataMap1);
        dataList.add(dataMap2);
        //索引服务的地址
        String elasticServer= "127.0.0.1";
        //索引服务的端口
        Integer elasticServerPort = 9300;
        Client client=null;
        try{
            //初始化连接
            Settings settings = Settings.settingsBuilder().build();
            client = TransportClient.builder().settings(settings).build()
                     .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(elasticServer), elasticServerPort));
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            int y=dataList.size();
            //添加数据
            for(int i=0;i<y;i++){
                Map<String, Object> m = (Map)dataList.get(i);
                bulkRequest.add(client.prepareIndex("user", "userInfo", i+2+"")
                     .setSource(m));
                if (i % 10000 == 0) {
                    bulkRequest.execute().actionGet();
                }
            }
            bulkRequest.execute().actionGet();
            //关闭连接
            client.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("do end!!");
    }
```

代码中模拟实际项目构造多条 list 数据，list 中放 map 类型的数据，并通过循环批量添加并且每 1 万条数据添加一次。

#### 2.3.2 修改主函数，调用删除索引函数

注释掉创建单条 elasticsearch 数据函数的调用，增加调用批量创建 elasticsearch 数据。

```java
 public static void main(String[] args) {
        //添加数据
        // add();
        //批量添加数据
        batchAdd();
    }
```

#### 2.3.3 运行代码

以 Java Application 的方式，运行`AddData.java`文件。

#### 2.3.4 查看数据添加情况

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4560timestamp1516283370280.png)

点击数据浏览在点击类型 userInfo 我们可以看到已经有 3 条数据添加进去了。

> 拓展：大家可以试试如果 id 相同添加后是什么效果?

#### 2.2.1 查看添加数据前的数据

1.确认 Es 服务开启。

2.打开浏览器访问 http://127.0.0.1:9200/_plugin/head

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4560timestamp1516281614777.png)

查看我们上节课创建的 userInfo，此时为空。

#### 2.2.2 在`com.syl.es` 下，新建 AddData 类

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4560timestamp1516281621112.png)

#### 2.2.3 编写添加数据的方法

```java
package com.syl.es;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class AddData {

    /**
     * @param args
     */
    public static void main(String[] args) {
        //调用添加数据方法
        add();
    }

    /**
     * 添加索引数据
     */
    public static void add(){
        //索引服务的地址
        String elasticServer= "127.0.0.1";
        //索引服务的端口
        Integer elasticServerPort = 9300;
        Client client=null;
        try{
            //初始化连接
            Settings settings = Settings.settingsBuilder()
                         .build();
            client = TransportClient.builder().settings(settings).build()
                        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(elasticServer), elasticServerPort));
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            //设置字段的值
             XContentBuilder docBuilder = XContentFactory.jsonBuilder().startObject();
             docBuilder.field("name", "张中国");
            docBuilder.field("nickname", "张中");
            docBuilder.field("nativeplace", "上海静安寺");
            docBuilder.field("address", "上海静安寺1街坊10栋");
            docBuilder.field("birthdate", "1980-02-14");
            //添加索引数据并且设置id为1
             bulkRequest.add(client.prepareIndex("user", "userInfo", "1")
                      .setSource(docBuilder));
            BulkResponse bulkResponse = bulkRequest.execute().actionGet();
            System.out.println(bulkResponse.hasFailures());
            //判断添加是否成功
            if (bulkResponse.hasFailures()) {
               System.out.println("error!!!");
            }
            //关闭连接
            client.close();
         }catch (Exception e) {
                e.printStackTrace();
         }
         System.out.println("do end!!");
    }
}
```

以 Java Application 的方式，运行`AddData.java`文件。

> 注：实验楼在线环境暂时不支持中文复制粘贴，请下载前面给出的项目文件中复制粘贴中文。或者使用英文，我们的目的是学会如何使用。而不是纠结中文英文。

#### 2.3.3 访问 head 查看创建的数据

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4560timestamp1516281719676.png)

通过点击数据浏览，点击左侧 userInfo 可以看到数据已经创建成功。

####  java 查询 elasticsearch 全部数据

1.确保启动 es 服务 2.打开浏览器访问`http://127.0.0.1:9200/_plugin/head` 查看我们上节课创建的 userInfo。

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4583timestamp1516369443761.png)

#### 2.2.2 在`com.syl.es`下新建 QueryData1 类

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4583timestamp1516369559837.png)

#### 2.2.3 编写搜索全部数据的函数

```java
package com.syl.es;

import java.net.InetAddress;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;

public class QueryData1 {

    /**
     * @param args
     */
    public static void main(String[] args) {
        //调用搜索全部数据方法
        searchAll();
    }
    /**
     * 搜索全部数据
     */
    public static void searchAll(){
        //索引服务的地址
        String elasticServer= "127.0.0.1";
        //索引服务的端口
        Integer elasticServerPort = 9300;
        Client client=null;
        try{
            //初始化连接
            Settings settings = Settings.settingsBuilder().build();
            client = TransportClient.builder().settings(settings).build()
                     .addTransportAddress(new InetSocketTransportAddress(
                             InetAddress.getByName(elasticServer), elasticServerPort));
            //搜索全部数据
            QueryBuilder  bqb=QueryBuilders.matchAllQuery();
            System.out.println(bqb.toString()+"====================");
            SearchResponse response = client.prepareSearch("user").setTypes("userInfo") //set index set type
                    .setQuery(bqb.toString())
                    .execute()
                    .actionGet();
            SearchHits hits = response.getHits();
            System.out.println(hits.getTotalHits() +" "+ hits.getHits().length);
            for (int i = 0; i < hits.getHits().length; i++) {
                System.out.println("===searchAll()====="+hits.getAt(i).getId()+"-------"
                        + "------"+hits.getAt(i).getSource());
            }
            //关闭连接
            client.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

- QueryBuilders.matchAllQuery() 代表查询全部数据。
- client.prepareSearch("user").setTypes("userInfo")代表查询 user 索引的 userInfo
- type（一个 index 索引下可以有多个 type，所以要指定某个 type）。
- setQuery(bqb.toString()) 代表设置查询的条件

以 Java Application 的方式，运行`QueryData1.java`文件。

#### 2.2.4 查看执行结果

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4583timestamp1516369616429.png)

#### 编写 searchSize 方法

在`QueryData1.java`,添加函数 searchSize

```java
    /**
     * 搜索指定数量数据
     */
    public static void searchSize(){
        //索引服务的地址
        String elasticServer= "127.0.0.1";
        //索引服务的端口
        Integer elasticServerPort = 9300;
        Client client=null;
        try{
            //初始化连接
            Settings settings = Settings.settingsBuilder().build();
            client = TransportClient.builder().settings(settings).build()
                     .addTransportAddress(new InetSocketTransportAddress(
                             InetAddress.getByName(elasticServer), elasticServerPort));
            //搜索数据
            QueryBuilder  bqb=QueryBuilders.matchAllQuery();
            SearchResponse response = client.prepareSearch("user").setTypes("userInfo")
                    .setQuery(bqb.toString())
                    .setFrom(0).setSize(2)//设置条数
                    .execute()
                    .actionGet();
            SearchHits hits = response.getHits();
            System.out.println(hits.getTotalHits() +" "+ hits.getHits().length);
            //打印搜索结果
            for (int i = 0; i < hits.getHits().length; i++) {
                System.out.println("====searchSize()==="+hits.getAt(i).getId()+"------"
                        + "-------"+hits.getAt(i).getSource());
            }
            //关闭连接
            client.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
```

setFrom(0).setSize(2) 代表从开始查询返回 2 条数据。

#### 2.3.2 修改主函数，调用删除索引函数

注释掉 searchAll() 的调用，增加调用 searchSize()。

```java
 public static void main(String[] args) {
        //searchAll();
        searchSize();
    }
```

#### 2.3.3 运行代码

以 Java Application 的方式，运行`QueryData1.java`文件。

#### 2.3.4 查看运行结果

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4583timestamp1516370292457.png)



#### 编写 searchMatchQuery 方法

在`QueryData1.java`,添加函数 searchMatchQuery

```java
    /**
     * 模糊搜索索引数据
     */
    public static void searchMatchQuery(){
        //索引服务的地址
        String elasticServer= "127.0.0.1";
        //索引服务的端口
        Integer elasticServerPort = 9300;
        Client client=null;
        try{
            //初始化连接
            Settings settings = Settings.settingsBuilder().build();
            client = TransportClient.builder().settings(settings).build()
                     .addTransportAddress(new InetSocketTransportAddress(
                             InetAddress.getByName(elasticServer), elasticServerPort));
            //设置查询条件
            BoolQueryBuilder bqb=QueryBuilders.boolQuery();
            float BOOST = (float) 1.2;
            MatchQueryBuilder titleSearchBuilder = QueryBuilders.matchQuery("name", "张中国");
            titleSearchBuilder.boost(BOOST);
            titleSearchBuilder.operator(Operator.AND);
            bqb.must(titleSearchBuilder);
            //模糊搜索数据
            SearchResponse response = client.prepareSearch("user").setTypes("userInfo")
                    .setQuery(bqb.toString())
                    .setFrom(0).setSize(60).setExplain(true) //setExplain 按查询匹配度排序
                    .execute()
                    .actionGet();
            SearchHits hits = response.getHits();
            System.out.println(hits.getTotalHits() +" "+ hits.getHits().length);
            //打印搜索结果
            for (int i = 0; i < hits.getHits().length; i++) {
                System.out.println("===searchMatchQuery()==="+hits.getAt(i).getId()+"--" +
                        "-----------"+hits.getAt(i).getSource());
            }
            //关闭连接
            client.close();
        }catch (Exception e) {
            e.printStackTrace();
        }

    }
}
```

#### 2.4.2 修改主函数，调用删除索引函数

注释掉 searchSize()的调用，增加调用 searchMatchQuery。

```java
 public static void main(String[] args) {
        //搜索全部数据
        //searchAll();
        //搜索指定数量数据
        //searchSize();
        //模糊搜索
          searchMatchQuery();
    }
```

#### 2.4.3 运行代码

以 Java Application 的方式，运行`QueryData1.java`文件。

#### 2.4.4 查看运行结果

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4583timestamp1516372484062.png)

运行发现 3 条数据都查询出来了。

> 拓展：大家可以把搜索关键词换成中国试试能否搜索出来，在换成中看能否搜索出来。

#### 查看修改前的数据

1.确定 es 服务启动。

2.打开浏览器访问`http://127.0.0.1:9200/_plugin/head`，查看我们前面创建的 userInfo。

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4584timestamp1516685236770.png)

选中数据，查看 version 信息。

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4584timestamp1516685245075.png)

#### 2.2.2 在`com.syl.es`下新建 UpdateData 类

编写更新单条数据的函数

```java
package com.syl.es;

import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class UpdateData {

    /**
     * @param args
     */
    public static void main(String[] args) {
         updateEs();
    }

    /**
     * 更新索引
     */
    public static void updateEs(){
        //索引服务的地址
        String elasticServer= "127.0.0.1";
        //索引服务的端口
        Integer elasticServerPort = 9300;
        Client client=null;
        try{
            //初始化连接
            Settings settings = Settings.settingsBuilder().build();
            client = TransportClient.builder().settings(settings).build()
                     .addTransportAddress(new InetSocketTransportAddress(
                             InetAddress.getByName(elasticServer), elasticServerPort));
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            //设置更新的字段
            XContentBuilder jsonBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("name","王美丽")
                    .field("nickname","美丽")
                    .field("birthdate",df.format(new Date()))
                    .endObject();
            //更新为1的字段数据
            UpdateResponse response=client.prepareUpdate("user","userInfo","1")
                    .setDoc(jsonBuilder)
                    .get();
            String _index = response.getIndex();
            String _type = response.getType();
            String _id = response.getId();
            long _version = response.getVersion();
            boolean created = response.isCreated();
            System.out.println(_index+" "+_type+" "+_id+" "+_version+" "+created);
            //关闭连接
            client.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

以 Java Application 的方式，运行`UpdateData.java`文件。

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4584timestamp1516688269957.png)

#### 2.2.3 查看 version 变化

打开 head 查看 version 变化 点击 id 为 1 的数据查看 version

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4584timestamp1516689455898.png)

根据结果我们可以看见 version 已经为 2，并且数据已经按照我们编写的修改了。



####  编写批量修改多条数据的方法 batchUpdateEs

在`UpdateData.java`,添加 batchUpdateEs 函数。

```java
    /**
     * 批量更新索引数据
     */
    public static void batchUpdateEs(){
        String elasticServer= "127.0.0.1";
        Integer elasticServerPort = 9300;
        Client client=null;
        try{
            Settings settings = Settings.settingsBuilder().build();
            client = TransportClient.builder().settings(settings).build()
                     .addTransportAddress(new InetSocketTransportAddress(
                             InetAddress.getByName(elasticServer), elasticServerPort));
            //初始化数据
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            List dataList = new ArrayList();
            Map dataMap1=new HashMap();
            dataMap1.put("name","张中国");
            dataMap1.put("nickname", "张中");
            dataMap1.put("nativeplace", "上海静安寺");
            dataMap1.put("address", "上海静安寺1街坊10栋");
            dataMap1.put("birthdate", "1980-02-15");
            dataMap1.put("id", "2");
            Map dataMap2=new HashMap();
            dataMap2.put("name","张三丰");
            dataMap2.put("nickname", "张三");
            dataMap2.put("nativeplace", "上海静安寺");
            dataMap2.put("address", "上海静安寺1街坊10栋");
            dataMap2.put("birthdate", "2018-01-23");
            dataMap2.put("id", "3");
            dataList.add(dataMap1);
            dataList.add(dataMap2);
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            int y=dataList.size();
            //执行批量更新
            for(int i=0;i<y;i++){
                Map<String, Object> m =(Map<String, Object>) dataList.get(i);
                XContentBuilder jsonBuilder = XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name",m.get("name").toString())
                        .field("nickname",m.get("nickname").toString())
                        .field("nativeplace",m.get("nativeplace").toString())
                        .field("address",m.get("address").toString())
                        .field("birthdate",m.get("birthdate").toString())
                        .endObject();
                bulkRequest.add(client.prepareUpdate("user", "userInfo",m.get("id").toString())
                     .setDoc(jsonBuilder));
                if (i % 10000 == 0) {
                    bulkRequest.execute().actionGet();
                }
            }
            bulkRequest.execute().actionGet();
            //关闭连接
            client.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
```

#### 2.3.2 修改主函数，调用删除索引函数

注释掉 updateEs 的调用，增加调用 batchUpdateEs。

```java
    public static void main(String[] args) {
         //更新索引数据
         //updateEs();
         //批量更新数据
           batchUpdateEs();
    }
```

#### 2.3.3 运行代码

以 Java Application 的方式，运行`UpdateData.java`文件。

#### 2.3.4 查看执行结果

打开 head 查看 version 及数据变化（实际项目我们一般不会关注 version，这里只是让大家知道它底层每更新一次 version 都在变化）

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4584timestamp1516692390675.png)

大家可以看到数据跟 version 都已经变了。

####  查看删除前的数据

1.确保 es 服务已经启动。 2.打开浏览器访问`http://127.0.0.1:9200/_plugin/head` 查看我们之前创建的 userInfo。

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4585timestamp1516695163167.png)

这里一共有 3 条数据内容。

#### 2.2.2 创建 DeleteData 类

在`com.syl.es`下创建 DeleteData 类，并编写单条数据删除函数。

```java
package com.syl.es;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class DeleteData {

    public static void main(String args[]){
        deleteEs();
    }
    /**
     * 删除索引数据
     */
    public static void deleteEs(){
        //索引服务的地址
        String elasticServer= "127.0.0.1";
        //索引服务的端口
        Integer elasticServerPort = 9300;
        Client client=null;
        try{
            //初始化连接
            Settings settings = Settings.settingsBuilder().build();
            client = TransportClient.builder().settings(settings).build()
                     .addTransportAddress(new InetSocketTransportAddress(
                             InetAddress.getByName(elasticServer), elasticServerPort));
            //删除为1的索引数据
            client.prepareDelete("user","userInfo","1").execute().actionGet();
            //关闭连接
            client.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    }
```

以 Java Application 的方式，运行`DeleteData.java`文件。

#### 2.2.3 查看执行结果

`注意`:刷新下浏览器或点击下 head 中的重新连接

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4585timestamp1516695237536.png)

我们看到已经删除 id 为 1 的数据了。

####  批量删除方法 1：deleteEsBulk

在`DeleteData.java`中编写 deleteEsBulk 方法

```java
    /**
     * 批量删除索引数据
     */
    public static void deleteEsBulk(){
        //索引服务的地址
        String elasticServer= "127.0.0.1";
        //索引服务的端口
        Integer elasticServerPort = 9300;
        Client client=null;
        try{
            //初始化连接
            Settings settings = Settings.settingsBuilder().build();
            client = TransportClient.builder().settings(settings).build()
                     .addTransportAddress(new InetSocketTransportAddress(
                             InetAddress.getByName(elasticServer), elasticServerPort));
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            //设置删除的索引的数据id
            bulkRequest.add(client.prepareDelete("user", "userInfo", "2")  );
            bulkRequest.add(client.prepareDelete("user", "userInfo", "3")  );
            BulkResponse bulkResponse = bulkRequest.get();
            //判断执行是否成功
            if(bulkResponse.hasFailures()){
                System.out.println("bulk error:"+bulkResponse.buildFailureMessage());
            }
            bulkRequest.get();
            //关闭连接
            client.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
```

#### 2.3.2 修改主函数，调用删除索引函数

注释掉 deleteEs 的调用，增加调用 deleteEsBulk。

```java
public class DeleteData {
    public static void main(String args[]){
        //deleteEs();
        deleteEsBulk();
    }
```

以 Java Application 的方式，运行`DeleteData.java`文件。

#### 2.3.3 查看运行结果

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4585timestamp1516696061221.png)

我们可以看到 id 为 2,3 的数据已经被删除掉了。

#### 2.3.4 批量删除方法 2：deleteEsBulkList `

> 补充说明：`因为上面执行成功后将会删除数据，所以需要重新执行 AddData 类，请大家自行执行以下。`

在`DeleteData.java`中编写 deleteEsBulkList 方法

```java
    /**
     * 批量删除索引数据
     */
    public static void deleteEsBulkList(){
        //索引服务的地址
        String elasticServer= "127.0.0.1";
        //索引服务的端口
        Integer elasticServerPort = 9300;
        Client client=null;
        try{
            //初始化连接
            Settings settings = Settings.settingsBuilder().build();
            client = TransportClient.builder().settings(settings).build()
                     .addTransportAddress(new InetSocketTransportAddress(
                             InetAddress.getByName(elasticServer), elasticServerPort));
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            //删除数据的id
            List dataList= new ArrayList();
            dataList.add("2");
            dataList.add("3");
            //执行删除
            for(int i=0;i<dataList.size();i++){
                bulkRequest.add(client.prepareDelete("user", "userInfo",dataList.get(i).toString()));
                if (i % 200 == 0) {
                    bulkRequest.get();
                }
            }
            bulkRequest.get();
            //关闭连接
            client.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
```

#### 2.3.5 修改主函数，调用删除索引函数

注释掉 deleteEsBulk 的调用，增加调用 deleteEsBulkList。

```java
public class DeleteData {
    public static void main(String args[]){
        //删除索引
        //deleteEs();
        //批量删除索引1
        //deleteEsBulk();
        //批量删除索引2
          deleteEsBulkList();
    }
```

以 Java Application 的方式，运行`DeleteData.java`文件。

#### 2.3.6 查看运行结果

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid618173labid4585timestamp1516696202870.png)

可以看到 id 为 2,3 的数据已经被删除掉了。

对比总结：可以看到第二种方式比较灵活，实际项目我们一般都是第二种删除方法，删除数据由 list 指定比较随意。



springboot整合es

pom添加
<dependency>

​    <groupId>org.springframework.boot</groupId>

​    <artifactId>spring-boot-starter-data-elasticsearch</artifactId>

</dependency>

配置application

data:
  elasticsearch:
    cluster-name: elasticsearch #默认为elasticsearch
    cluster-nodes: 127.0.0.1:9300 #配置es节点信息，逗号分隔，如果没有指定，则启动ClientNode
  properties:
    path:
      logs: ./elasticsearch/log #elasticsearch日志存储目录
      data: ./elasticsearch/data #elasticsearch数据存储目录



esconfig类

```kotlin
@Configuration
public class ElasticSearchConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchConfig.class);

    /**
     * elk集群地址
     */
    @Value("${elasticsearch.ip}")
    private String hostName;

    /**
     * 端口
     */
    @Value("${elasticsearch.port}")
    private String port;

    /**
     * 集群名称
     */
    @Value("${elasticsearch.cluster.name}")
    private String clusterName;

    /**
     * 连接池
     */
    @Value("${elasticsearch.pool}")
    private String poolSize;

    /**
     * Bean name default  函数名字
     *
     * @return
     */
    @Bean(name = "transportClient")
    public TransportClient transportClient() {
        LOGGER.info("Elasticsearch初始化开始。。。。。");
        TransportClient transportClient = null;
        try {
            // 配置信息
            Settings esSetting = Settings.builder()
                    .put("cluster.name", clusterName) //集群名字
                    .put("client.transport.sniff", true)//增加嗅探机制，找到ES集群
                    .put("thread_pool.search.size", Integer.parseInt(poolSize))//增加线程池个数，暂时设为5
                    .build();
            //配置信息Settings自定义
            transportClient = new PreBuiltTransportClient(esSetting);
            TransportAddress transportAddress = new TransportAddress(InetAddress.getByName(hostName), Integer.valueOf(port));
            transportClient.addTransportAddresses(transportAddress);
        } catch (Exception e) {
            LOGGER.error("elasticsearch TransportClient create error!!", e);
        }
        return transportClient;
    }
}
```



操作

1、TransportClient
2、JestClient
3、RestClient

三种客户端，以第一种为例(操作方式差不多)

```dart
@Component
public class ElasticsearchUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchUtil.class);

    @Autowired
    private TransportClient transportClient;

    private static TransportClient client;

    /**
     * @PostContruct是spring框架的注解 spring容器初始化的时候执行该方法
     */
    @PostConstruct
    public void init() {
        client = this.transportClient;
    }

    /**
     * 创建索引
     *
     * @param index
     * @return
     */
    public static boolean createIndex(String index) {
        if (!isIndexExist(index)) {
            LOGGER.info("Index is not exits!");
        }
        CreateIndexResponse indexresponse = client.admin().indices().prepareCreate(index).execute().actionGet();
        LOGGER.info("执行建立成功？" + indexresponse.isAcknowledged());
        return indexresponse.isAcknowledged();
    }

    /**
     * 删除索引
     *
     * @param index
     * @return
     */
    public static boolean deleteIndex(String index) {
        if (!isIndexExist(index)) {
            LOGGER.info("Index is not exits!");
        }
        DeleteIndexResponse dResponse = client.admin().indices().prepareDelete(index).execute().actionGet();
        if (dResponse.isAcknowledged()) {
            LOGGER.info("delete index " + index + "  successfully!");
        } else {
            LOGGER.info("Fail to delete index " + index);
        }
        return dResponse.isAcknowledged();
    }

    /**
     * 判断索引是否存在
     *
     * @param index
     * @return
     */
    public static boolean isIndexExist(String index) {
        IndicesExistsResponse inExistsResponse = client.admin().indices().exists(new IndicesExistsRequest(index)).actionGet();
        if (inExistsResponse.isExists()) {
            LOGGER.info("Index [" + index + "] is exist!");
        } else {
            LOGGER.info("Index [" + index + "] is not exist!");
        }
        return inExistsResponse.isExists();
    }

    /**
     * @Description: 判断inde下指定type是否存在
     */
    public boolean isTypeExist(String index, String type) {
        return isIndexExist(index)
                ? client.admin().indices().prepareTypesExists(index).setTypes(type).execute().actionGet().isExists()
                : false;
    }

    /**
     * 数据添加，正定ID
     *
     * @param jsonObject 要增加的数据
     * @param index      索引，类似数据库
     * @param type       类型，类似表
     * @param id         数据ID
     * @return
     */
    public static String addData(JSONObject jsonObject, String index, String type, String id) {
        IndexResponse response = client.prepareIndex(index, type, id).setSource(jsonObject).get();
        LOGGER.info("addData response status:{},id:{}", response.status().getStatus(), response.getId());
        return response.getId();
    }

    /**
     * 数据添加
     *
     * @param jsonObject 要增加的数据
     * @param index      索引，类似数据库
     * @param type       类型，类似表
     * @return
     */
    public static String addData(JSONObject jsonObject, String index, String type) {
        return addData(jsonObject, index, type, UUID.randomUUID().toString().replaceAll("-", "").toUpperCase());
    }

    /**
     * 通过ID删除数据
     *
     * @param index 索引，类似数据库
     * @param type  类型，类似表
     * @param id    数据ID
     */
    public static void deleteDataById(String index, String type, String id) {

        DeleteResponse response = client.prepareDelete(index, type, id).execute().actionGet();

        LOGGER.info("deleteDataById response status:{},id:{}", response.status().getStatus(), response.getId());
    }

    /**
     * 通过ID 更新数据
     *
     * @param jsonObject 要增加的数据
     * @param index      索引，类似数据库
     * @param type       类型，类似表
     * @param id         数据ID
     * @return
     */
    public static void updateDataById(JSONObject jsonObject, String index, String type, String id) {

        UpdateRequest updateRequest = new UpdateRequest();

        updateRequest.index(index).type(type).id(id).doc(jsonObject);

        client.update(updateRequest);

    }

    /**
     * 通过ID获取数据
     *
     * @param index  索引，类似数据库
     * @param type   类型，类似表
     * @param id     数据ID
     * @param fields 需要显示的字段，逗号分隔（缺省为全部字段）
     * @return
     */
    public static Map<String, Object> searchDataById(String index, String type, String id, String fields) {

        GetRequestBuilder getRequestBuilder = client.prepareGet(index, type, id);

        if (StringUtils.isNotEmpty(fields)) {
            getRequestBuilder.setFetchSource(fields.split(","), null);
        }

        GetResponse getResponse = getRequestBuilder.execute().actionGet();

        return getResponse.getSource();
    }


    /**
     * 使用分词查询,并分页
     *
     * @param index          索引名称
     * @param type           类型名称,可传入多个type逗号分隔
     * @param startPage      当前页
     * @param pageSize       每页显示条数
     * @param query          查询条件
     * @param fields         需要显示的字段，逗号分隔（缺省为全部字段）
     * @param sortField      排序字段
     * @param highlightField 高亮字段
     * @return
     */
    public static EsPage searchDataPage(String index, String type, int startPage, int pageSize, QueryBuilder query, String fields, String sortField, String highlightField) {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index);
        if (StringUtils.isNotEmpty(type)) {
            searchRequestBuilder.setTypes(type.split(","));
        }
        searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH);

        // 需要显示的字段，逗号分隔（缺省为全部字段）
        if (StringUtils.isNotEmpty(fields)) {
            searchRequestBuilder.setFetchSource(fields.split(","), null);
        }

        //排序字段
        if (StringUtils.isNotEmpty(sortField)) {
            searchRequestBuilder.addSort(sortField, SortOrder.DESC);
        }

        // 高亮（xxx=111,aaa=222）
        if (StringUtils.isNotEmpty(highlightField)) {
            HighlightBuilder highlightBuilder = new HighlightBuilder();

            //highlightBuilder.preTags("<span style='color:red' >");//设置前缀
            //highlightBuilder.postTags("</span>");//设置后缀

            // 设置高亮字段
            highlightBuilder.field(highlightField);
            searchRequestBuilder.highlighter(highlightBuilder);
        }

        //searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        searchRequestBuilder.setQuery(query);

        // 分页应用
        searchRequestBuilder.setFrom(startPage).setSize(pageSize);

        // 设置是否按查询匹配度排序
        searchRequestBuilder.setExplain(true);

        //打印的内容 可以在 Elasticsearch head 和 Kibana  上执行查询
        LOGGER.info("\n{}", searchRequestBuilder);

        // 执行搜索,返回搜索响应信息
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        long totalHits = searchResponse.getHits().totalHits;
        long length = searchResponse.getHits().getHits().length;

        LOGGER.debug("共查询到[{}]条数据,处理数据条数[{}]", totalHits, length);

        if (searchResponse.status().getStatus() == 200) {
            // 解析对象
            List<Map<String, Object>> sourceList = setSearchResponse(searchResponse, highlightField);

            return new EsPage(startPage, pageSize, (int) totalHits, sourceList);
        }

        return null;

    }


    /**
     * 使用分词查询
     *
     * @param index          索引名称
     * @param type           类型名称,可传入多个type逗号分隔
     * @param query          查询条件
     * @param size           文档大小限制
     * @param fields         需要显示的字段，逗号分隔（缺省为全部字段）
     * @param sortField      排序字段
     * @param highlightField 高亮字段
     * @return
     */
    public static List<Map<String, Object>> searchListData(
            String index, String type, QueryBuilder query, Integer size,
            String fields, String sortField, String highlightField) {

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index);
        if (StringUtils.isNotEmpty(type)) {
            searchRequestBuilder.setTypes(type.split(","));
        }

        if (StringUtils.isNotEmpty(highlightField)) {
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            // 设置高亮字段
            highlightBuilder.field(highlightField);
            searchRequestBuilder.highlighter(highlightBuilder);
        }

        searchRequestBuilder.setQuery(query);

        if (StringUtils.isNotEmpty(fields)) {
            searchRequestBuilder.setFetchSource(fields.split(","), null);
        }
        searchRequestBuilder.setFetchSource(true);

        if (StringUtils.isNotEmpty(sortField)) {
            searchRequestBuilder.addSort(sortField, SortOrder.DESC);
        }

        if (size != null && size > 0) {
            searchRequestBuilder.setSize(size);
        }

        //打印的内容 可以在 Elasticsearch head 和 Kibana  上执行查询
        LOGGER.info("\n{}", searchRequestBuilder);

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        long totalHits = searchResponse.getHits().totalHits;
        long length = searchResponse.getHits().getHits().length;

        LOGGER.info("共查询到[{}]条数据,处理数据条数[{}]", totalHits, length);

        if (searchResponse.status().getStatus() == 200) {
            // 解析对象
            return setSearchResponse(searchResponse, highlightField);
        }
        return null;

    }


    /**
     * 高亮结果集 特殊处理
     *
     * @param searchResponse
     * @param highlightField
     */
    private static List<Map<String, Object>> setSearchResponse(SearchResponse searchResponse, String highlightField) {
        List<Map<String, Object>> sourceList = new ArrayList<Map<String, Object>>();
        StringBuffer stringBuffer = new StringBuffer();

        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            searchHit.getSourceAsMap().put("id", searchHit.getId());

            if (StringUtils.isNotEmpty(highlightField)) {

                System.out.println("遍历 高亮结果集，覆盖 正常结果集" + searchHit.getSourceAsMap());
                Text[] text = searchHit.getHighlightFields().get(highlightField).getFragments();

                if (text != null) {
                    for (Text str : text) {
                        stringBuffer.append(str.string());
                    }
                    //遍历 高亮结果集，覆盖 正常结果集
                    searchHit.getSourceAsMap().put(highlightField, stringBuffer.toString());
                }
            }
            sourceList.add(searchHit.getSourceAsMap());
        }
        return sourceList;
    }
}
```

