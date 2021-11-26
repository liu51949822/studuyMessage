经过多年的发展形成了 Hadoop1.X 生态系统，其结构如下图所示：

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1427382197256)

- `HDFS`：Hadoop 生态圈的基本组成部分是 Hadoop 分布式文件系统（HDFS）。HDFS 是一种分布式文件系统，数据被保存在计算机集群上，HDFS 为 HBase 等工具提供了基础。
- `MapReduce`：Hadoop 的主要执行框架是 MapReduce，它是一个分布式、并行处理的编程模型，MapReduce 把任务分为 map（映射）阶段和 reduce（化简）阶段。由于 MapReduce 工作原理的特性，Hadoop 能以并行的方式访问数据，从而实现快速访问数据。
- `Hbase`：HBase 是一个建立在 HDFS 之上，面向列的 NoSQL 数据库，用于快速读 / 写大量数据，HBase 使用 Zookeeper 进行管理。
- `Zookeeper`：用于 Hadoop 的分布式协调服务。Hadoop 的许多组件依赖于 Zookeeper，它运行在计算机集群中，用于管理 Hadoop 集群。
- `Pig`：它是 MapReduce 编程的复杂性的抽象。Pig 平台包括运行环境和用于分析 Hadoop 数据集的脚本语言 (Pig Latin)，其编译器将 Pig Latin 翻译成 MapReduce 程序序列。
- `Hive`：类似于 SQL 高级语言，用于运行存储在 Hadoop 上的查询语句，Hive 让不熟悉 MapReduce 的开发人员也能编写数据查询语句，然后这些语句被翻译为 Hadoop 上面的 MapReduce 任务。像 Pig 一样，Hive 作为一个抽象层工具，吸引了很多熟悉 SQL 而不是 Java 编程的数据分析师。
- `Sqoop`：一个连接工具，用于在关系数据库、数据仓库和 Hadoop 之间转移数据。Sqoop 利用数据库技术描述架构，进行数据的导入 / 导出；利用 MapReduce 实现并行化运行和容错技术。
- `Flume`：提供了分布式、可靠、高效的服务，用于收集、汇总大数据，并将单台计算机的大量数据转移到 HDFS。它基于一个简单而灵活的架构，利用简单的可扩展的数据模型，将企业中多台计算机上的数据转移到 Hadoop 中。

Apache Hadoop 版本分为两代，我们将第一代 Hadoop 称为 Hadoop 1.0，第二代 Hadoop 称为 Hadoop 2.0。

第一代 Hadoop 包含三个大版本，分别是 0.20.x，0.21.x 和 0.22.x。其中，0.20.x 最后演化成 1.0.x，变成了稳定版，而 0.21.x 和 0.22.x 则包括 NameNode HA 等新的重大特性。

第二代 Hadoop 包含两个版本，分别是 0.23.x 和 2.x，它们完全不同于 Hadoop 1.0，是一套全新的架构，均包含 HDFS Federation 和 YARN 两个系统，相比于 0.23.x，2.x 增加了 NameNode HA 和 Wire-compatibility 两个重大特性。

![img](https://doc.shiyanlou.com/userid29778labid764time1427637657385)

Hadoop 安装有如下三种方式：

- `单机模式`：安装简单，几乎不用做任何配置，但仅限于调试用途。
- `伪分布模式`：在单节点上同时启动 NameNode、DataNode、JobTracker、TaskTracker、Secondary Namenode 等 5 个进程，模拟分布式运行的各个节点。
- `完全分布式模式`：正常的 Hadoop 集群，由多个各司其职的节点构成。

由于实验环境的限制，本节课程将讲解伪分布模式安装，并在随后的课程中以该环境为基础进行其他组件部署实验。以下为伪分布式环境下在 CentOS6 中配置 Hadoop-1.1.2，该配置可以作为其他 Linux 系统和其他版本的 Hadoop 部署参考

节点使用 CentOS 系统，防火墙和 SElinux 需要禁用，创建了一个 shiyanlou 用户，并在系统根目录下创建 `/app` 目录，用于存放 Hadoop 等组件运行包。由于该目录用于安装 hadoop 等组件程序，用户对 shiyanlou 必须赋予 rwx 权限（一般做法是 root 用户在根目录下创建 `/app` 目录，并修改该目录拥有者为 shiyanlou(`chown –R shiyanlou:shiyanlou /app`)。

**Hadoop 搭建环境：**

- 虚拟机操作系统：CentOS6.6 64 位，单核，1G 内存
- JDK：1.7.0_55 64 位
- Hadoop：1.1.2



#### 配置本地环境

**设置机器名（实验楼环境已配置，无需操作）：**

使用 `sudo vi /etc/sysconfig/network`。

打开配置文件，根据实际情况设置该服务器的机器名，新机器名在重启后生效。

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid214292labid764timestamp1488190165875.png)

**设置 Host 映射文件（需要您在实验楼环境操作）：**

1. 设置 IP 地址与机器名的映射，设置信息如下：

```bash
# 配置主机名对应的IP地址
$ sudo vi /etc/hosts
```

设置：`<IP 地址> <主机名>`

例如：`192.168.42.3 55a95997af1c hadoop`

注意：就是在打开的 `/etc/hosts` 文件的最后一行加上 hadoop，记得使用的是 `tab` 键而不是空格。

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid214292labid764timestamp1488269552713.png)

1. 使用 `ping` 命令验证设置是否成功。

```bash
ping hadoop
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid214292labid764timestamp1488190512632.png)

#### 设置操作系统环境（以下内容实验楼环境已配置，无需操作）

**关闭防火墙：**

在 Hadoop 安装过程中需要关闭防火墙和 SElinux，否则会出现异常。

1. 使用 `sudo service iptables status`。

查看防火墙状态，如下所示表示 iptables 已经开启。

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433141096136)

> 注意：若弹出权限不足，可能防火墙已经关闭，请输入命令：`chkconfig iptables --list` 查看防火墙的状态。

1. 使用如下命令关闭 iptables。

```bash
sudo chkconfig iptables off
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433141135347)

**关闭 SElinux：**

1. 使用 `getenforce` 命令查看是否关闭。
2. 修改 `/etc/selinux/config` 文件。

将 `SELINUX=enforcing` 改为 `SELINUX=disabled`，执行该命令后重启机器生效。

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433141171432)

**JDK 安装及配置：**

1. 下载 JDK 64bit 安装包。

打开 JDK 64bit 安装包下载链接，注意需要登录 Oracle 账户才能下载：

https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html

打开界面之后，选择下载对应平台版本的 .tar.gz 包：

![图片描述](https://doc.shiyanlou.com/courses/uid871732-20201021-1603245067947)

1. 创建 `/app` 目录，把该目录的所有者修改为 shiyanlou。

```bash
sudo mkdir /app
sudo chown -R shiyanlou:shiyanlou /app
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433141273685)

1. 创建 `/app/lib` 目录，使用命令如下：

```bash
mkdir /app/lib
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433141283780)

1. 把下载的安装包解压并迁移到 `/app/lib` 目录下。

```bash
cd /home/shiyanlou/install-pack
tar -zxf jdk-7u55-linux-x64.tar.gz
mv jdk1.7.0_55/ /app/lib
ll /app/lib
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433141297095)

1. 使用 `sudo vi /etc/profile` 命令打开配置文件，设置 JDK 路径。

```bash
export JAVA_HOME=/app/lib/jdk1.7.0_55
export PATH=$JAVA_HOME/bin:$PATH
export CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433141308466)

1. 编译并验证。

```bash
source /etc/profile
java -version
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433141316786)

> 需要注意的是：由于实验楼虚拟机原因使用 `java -version` 显示 JDK 版本为 1.5，该版本并不影响后续实验。

**更新 OpenSSL（实验楼环境已配置，无需操作）：**

CentOS 自带的 OpenSSL 存在 bug，使用如下命令进行更新：

```bash
yum update openssl
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433141340991)

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433141352659)

**SSH 无密码验证配置（实验楼环境已配置，无需操作）：**

1. 使用 `sudo vi /etc/ssh/sshd_config`，打开 sshd_config 配置文件，开放三个配置，如下图所示：

```text
RSAAuthentication yes
PubkeyAuthentication yes
AuthorizedKeysFile .ssh/authorized_keys
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433141462396)

1. 配置后重启服务。

```bash
sudo service sshd restart
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433141475180)

1. 使用 shiyanlou 用户登录使用如下命令生成私钥和公钥。

```bash
ssh-keygen -t rsa
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433141507684)

1. 进入 `/home/shiyanlou/.ssh` 目录把公钥命名为 `authorized_keys`，使用命令如下：

```bash
cp id_rsa.pub authorized_keys
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433141519844)

1. 使用如下设置 authorized_keys 读写权限。

```bash
sudo chmod 400 authorized_keys
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433141529173)

1. 测试 ssh 免密码登录是否生效。



#### 下载并解压 hadoop 安装包（无需操作）

在 Apache 的归档目录中下载 `hadoop-1.1.2-bin.tar.gz` 安装包，也可以在 `/home/shiyanlou/install-pack` 目录中找到该安装包，解压该安装包并把该安装包复制到 `/app` 目录中。

```bash
cd /home/shiyanlou/install-pack
tar -xzf hadoop-1.1.2-bin.tar.gz
rm -rf /app/hadoop-1.1.2
mv hadoop-1.1.2 /app
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433147689247)

#### 在 Hadoop-1.1.2 目录下创建子目录（需要操作）

```bash
cd /app/hadoop-1.1.2
mkdir -p tmp hdfs hdfs/name hdfs/data
ls
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433147658777)

进入 hdfs 目录中，使用 `chmod -R 755 data` 命令把 hdfs/data 设置为 755，否则 DataNode 会启动失败。

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433147759048)

**后续配置均已配置完毕，无需再次操作，可直接操作格式化 namenode 和启动 hadoop 的步骤。**

#### 配置 hadoop-env.sh

1. 进入 `hadoop-1.1.2/conf` 目录，打开配置文件 `hadoop-env.sh`。

```bash
cd /app/hadoop-1.1.2/conf
vi hadoop-env.sh
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433147774618)

1. 加入配置内容，设置 Hadoop 中 jdk 和 `hadoop/bin` 路径。

```bash
export JAVA_HOME=/app/lib/jdk1.7.0_55
export PATH=$PATH:/app/hadoop-1.1.2/bin
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433147786848)

1. 编译配置文件 `hadoop-env.sh`，并确认生效。

```bash
source hadoop-env.sh
hadoop version
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433147797748)

#### 配置 core-site.xml

1. 使用如下命令打开 `core-site.xml` 配置文件。

```bash
sudo vi core-site.xml   # 如果 sudo 需要密码，可以点击桌面右侧工具栏的ssh直连，其中的密码就是这里需要输入的密码
```

1. 在配置文件中，按照如下内容进行配置。

```xml
<configuration>
  <property>
    <name>fs.default.name</name>
    <value>hdfs://hadoop:9000</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/app/hadoop-1.1.2/tmp</value>
  </property>
</configuration>
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433147866409)

#### 配置 hdfs-site.xml

1. 使用如下命令打开 `hdfs-site.xml` 配置文件。

```bash
sudo vi hdfs-site.xml
```

1. 在配置文件中，按照如下内容进行配置。

```xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.name.dir</name>
    <value>/app/hadoop-1.1.2/hdfs/name</value>
  </property>
  <property>
    <name>dfs.data.dir</name>
    <value>/app/hadoop-1.1.2/hdfs/data</value>
  </property>
</configuration>
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433147890390)

#### 配置 mapred-site.xml

1. 使用如下命令打开 `mapred-site.xml` 配置文件。

```bash
sudo vi mapred-site.xml
```

1. 在配置文件中，按照如下内容进行配置。

```xml
<configuration>
  <property>
    <name>mapred.job.tracker</name>
    <value>hadoop:9001</value>
  </property>
</configuration>
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433148000192)

#### 配置 masters 和 slaves 文件

1. 设子主节点。

```bash
vi masters
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433148013243)

1. 设置从节点。

```bash
vi slaves
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433148127917)

#### 格式化 namenode

在 hadoop 机器上使用如下命令进行格式化 namenode。

```bash
cd /app/hadoop-1.1.2/bin
./hadoop namenode -format
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433148120053)

#### 启动 hadoop

```bash
./start-all.sh
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid764time1433148138993)

#### 用 jps 检验各后台进程是否成功启动

使用 `jps` 命令查看 hadoop 相关进程是否启动。

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid600404labid764timestamp1552011393120.png)

> 注意：如果是会员，推荐把实验环境保存下来，后续的实验都会依赖于现在的环境；如果是非会员进行后续的实验，则每次开始实验都需要对环境进行配置。

HDFS 原理



HDFS（Hadoop Distributed File System）是一个分布式文件系统。它具有高容错性并提供了高吞吐量的数据访问，非常适合大规模数据集上的应用，它提供了一个高度容错性和高吞吐量的海量数据存储解决方案。

- `高吞吐量访问`：HDFS 的每个 Block 分布在不同的 Rack 上，在用户访问时，HDFS 会计算使用最近和访问量最小的服务器给用户提供。由于 Block 在不同的 Rack 上都有备份，所以不再是单数据访问，速度和效率是非常快的。另外 HDFS 可以并行从服务器集群中读写，增加了文件读写的访问带宽。
- `高容错性`：系统故障不可避免，如何做到故障之后的数据恢复和容错处理是至关重要的。HDFS 通过多方面保证数据的可靠性，多份复制并且分布到物理位置的不同服务器上，数据校验功能、后台的连续自检数据一致性功能都为高容错提供了可能。
- `线性扩展`：因为 HDFS 的 Block 信息存放到 NameNode 上，文件的 Block 分布到 DataNode 上，当扩充的时候仅仅添加 DataNode 数量，系统可以在不停止服务的情况下做扩充，不需要人工干预。



HDFS 架构



![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433385988267)

如上图所示 HDFS 是 Master 和 Slave 的结构，分为 NameNode、Secondary NameNode 和 DataNode 三种角色。

- `NameNode`：在 Hadoop1.X 中只有一个 Master 节点，管理 HDFS 的名称空间和数据块映射信息、配置副本策略和处理客户端请求。
- `Secondary NameNode`：辅助 NameNode，分担 NameNode 工作，定期合并 fsimage 和 fsedits 并推送给 NameNode，紧急情况下可辅助恢复 NameNode。
- `DataNode`：Slave 节点，实际存储数据、执行数据块的读写并汇报存储信息给 NameNode。

HDFS 读操作



![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386065314)

1. 客户端通过调用 FileSystem 对象的 `open()` 方法来打开希望读取的文件，对于 HDFS 来说，这个对象是分布式文件系统的一个实例；
2. DistributedFileSystem 通过使用 RPC 来调用 NameNode 以确定文件起始块的位置，同一 Block 按照重复数会返回多个位置，这些位置按照 Hadoop 集群拓扑结构排序，距离客户端近的排在前面；
3. 前两步会返回一个 FSDataInputStream 对象，该对象会被封装成 DFSInputStream 对象，DFSInputStream 可以方便的管理 datanode 和 namenode 数据流，客户端对这个输入流调用 `read()` 方法；
4. 存储着文件起始块的 DataNode 地址的 DFSInputStream 随即连接距离最近的 DataNode，通过对数据流反复调用 `read()` 方法，可以将数据从 DataNode 传输到客户端；
5. 到达块的末端时，DFSInputStream 会关闭与该 DataNode 的连接，然后寻找下一个块的最佳 DataNode，这些操作对客户端来说是透明的，从客户端的角度来看只是读一个持续不断的流；
6. 一旦客户端完成读取，就对 FSDataInputStream 调用 `close()` 方法关闭文件读取。

HDFS 写操作



![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386130571)

1. 客户端通过调用 DistributedFileSystem 的 `create()` 方法创建新文件；
2. DistributedFileSystem 通过 RPC 调用 NameNode 去创建一个没有 Blocks 关联的新文件，创建前 NameNode 会做各种校验，比如文件是否存在、客户端有无权限去创建等。如果校验通过，NameNode 会为创建新文件记录一条记录，否则就会抛出 IO 异常；
3. 前两步结束后会返回 FSDataOutputStream 的对象，和读文件的时候相似，FSDataOutputStream 被封装成 DFSOutputStream，DFSOutputStream 可以协调 NameNode 和 Datanode。客户端开始写数据到 DFSOutputStream，DFSOutputStream 会把数据切成一个个小的数据包，并写入内部队列称为“数据队列”(Data Queue)；
4. DataStreamer 会去处理接受 Data Queue，它先问询 NameNode 这个新的 Block 最适合存储在哪几个 DataNode 里，比如重复数是 3，那么就找到 3 个最适合的 DataNode，把他们排成一个 pipeline。DataStreamer 把 Packet 按队列输出到管道的第一个 Datanode 中，第一个 DataNode 又把 Packet 输出到第二个 DataNode 中，以此类推；
5. DFSOutputStream 还有一个队列叫 Ack Quene，也是由 Packet 组成，等待 DataNode 的收到响应，当 Pipeline 中的所有 DataNode 都表示已经收到的时候，这时 Akc Quene 才会把对应的 Packet 包移除掉；
6. 客户端完成写数据后调用 `close()` 方法关闭写入流；
7. DataStreamer 把剩余的包都刷到 Pipeline 里然后等待 Ack 信息，收到最后一个 Ack 后，通知 NameNode 把文件标示为已完成。



HDFS 中的常用命令



**hadoop fs**

```bash
hadoop fs -ls /
hadoop fs -lsr
hadoop fs -mkdir /user/hadoop
hadoop fs -put a.txt /user/hadoop/
hadoop fs -get /user/hadoop/a.txt /
hadoop fs -cp src dst
hadoop fs -mv src dst
hadoop fs -cat /user/hadoop/a.txt
hadoop fs -rm /user/hadoop/a.txt
hadoop fs -rmr /user/hadoop/a.txt
hadoop fs -text /user/hadoop/a.txt
hadoop fs -copyFromLocal localsrc dst  # 与hadoop fs -put 功能类似
hadoop fs -moveFromLocal localsrc dst  # 将本地文件上传到 hdfs，同时删除本地文件
```

**hadoop dfsadmin**

运行一个 HDFS 的 dfsadmin 客户端。

```bash
# 报告文件系统的基本信息和统计信息
hadoop dfsadmin -report

hadoop dfsadmin -safemode enter | leave | get | wait
# 安全模式维护命令。安全模式是 Namenode 的一个状态，这种状态下，Namenode
# 1. 不接受对名字空间的更改(只读)
# 2. 不复制或删除块
# Namenode 会在启动时自动进入安全模式，当配置的块最小百分比数满足最小的副本数条件时，会自动离开安全模式。安全模式可以手动进入，但是这样的话也必须手动关闭安全模式。
```

**hadoop fsck**

运行 HDFS 文件系统检查工具。

用法：

```text
hadoop fsck [GENERIC_OPTIONS] <path> [-move | -delete | -openforwrite] [-files [-blocks [-locations | -racks]]]
```

**start-balancer.sh**

相关 HDFS API 可以到 Apache 官网进行查看：

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386076582)

运行代码



```java
import java.io.InputStream;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

public class FileSystemCat {
    public static void main(String[] args) throws Exception {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem. get(URI.create (uri), conf);
        InputStream in = null;
        try {
            in = fs.open( new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
```

**创建代码目录**

配置本机主机名为 hadoop，sudo 时需要输入 shiyanlou 用户的密码。将 hadoop 添加到最后一行的末尾。

```bash
sudo vim /etc/hosts
# 将hadoop添加到最后一行的末尾，修改后类似：（使用 tab 键添加空格）
# 172.17.2.98 f738b9456777 hadoop
ping hadoop
```

使用如下命令启动 Hadoop：

```bash
cd /app/hadoop-1.1.2/bin
./start-all.sh
jps # 查看启动的进程，确保 NameNode 和 DataNode 都有启动
```

在 `/app/hadoop-1.1.2` 目录下使用如下命令建立 myclass 和 input 目录：

```bash
cd /app/hadoop-1.1.2
rm -rf myclass input
mkdir -p myclass input
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386280625)

**建立例子文件上传到 HDFS 中**

进入 `/app/hadoop-1.1.2/input` 目录，在该目录中创建 `quangle.txt` 文件。

```bash
cd /app/hadoop-1.1.2/input
touch quangle.txt
vi quangle.txt
```

内容为：

```text
On the top of the Crumpetty Tree
The Quangle Wangle sat,
But his face you could not see,
On account of his Beaver Hat.
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386298726)

使用如下命令在 hdfs 中建立目录 `/class4`。

```bash
hadoop fs -mkdir /class4
hadoop fs -ls /
```

`说明`：如遇到报错没有 hadoop 命令，请重新执行 `source hadoop-env.sh`。后续的实验中同理。

> 如果需要直接使用 hadoop 命令，需要把 /app/hadoop-1.1.2 加入到 Path 路径中。

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386320432)

把例子文件上传到 hdfs 的 `/class4` 文件夹中。

```bash
cd /app/hadoop-1.1.2/input
hadoop fs -copyFromLocal quangle.txt /class4/quangle.txt
hadoop fs -ls /class4
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386332967)

**配置本地环境**

对 `/app/hadoop-1.1.2/conf` 目录中的 `hadoop-env.sh` 进行配置，如下所示：

```bash
cd /app/hadoop-1.1.2/conf
sudo vi hadoop-env.sh
```

加入 HADOOP_CLASSPATH 变量，值为 `/app/hadoop-1.1.2/myclass`，设置完毕后编译该配置文件，使配置生效。

```bash
export HADOOP_CLASSPATH=/app/hadoop-1.1.2/myclass
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386689762)

**编写代码**

进入 `/app/hadoop-1.1.2/myclass` 目录，在该目录中建立 `FileSystemCat.java` 代码文件，命令如下：

```bash
cd /app/hadoop-1.1.2/myclass/
vi FileSystemCat.java
```

输入代码内容：

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386708661)

**编译代码**

在 `/app/hadoop-1.1.2/myclass` 目录中，使用如下命令编译代码：

```bash
javac -classpath ../hadoop-core-1.1.2.jar FileSystemCat.java
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386723631)

**使用编译代码读取 HDFS 文件**

使用如下命令读取 HDFS 中 `/class4/quangle.txt` 内容：

```bash
hadoop FileSystemCat /class4/quangle.txt
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386733691)

运行代码



```java
//注意：在编译前请先删除中文注释！
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class LocalFile2Hdfs {
    public static void main(String[] args) throws Exception {

        // 获取读取源文件和目标文件位置参数
        String local = args[0];
        String uri = args[1];

        FileInputStream in = null;
        OutputStream out = null;
        Configuration conf = new Configuration();
        try {
            // 获取读入文件数据
            in = new FileInputStream(new File(local));

            // 获取目标文件信息
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            out = fs.create(new Path(uri), new Progressable() {
                @Override
                public void progress() {
                    System.out.println("*");
                }
            });

            // 跳过前100个字符
            in.skip(100);
            byte[] buffer = new byte[20];

            // 从101的位置读取20个字符到buffer中
            int bytesRead = in.read(buffer);
            if (bytesRead >= 0) {
                out.write(buffer, 0, bytesRead);
            }
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
    }
}
```

实现过程



**编写代码**

进入 `/app/hadoop-1.1.2/myclass` 目录，在该目录中建立 `LocalFile2Hdfs.java` 代码文件，命令如下：

```bash
cd /app/hadoop-1.1.2/myclass/
vi LocalFile2Hdfs.java
```

输入代码内容：

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386750442)

**编译代码**

在 `/app/hadoop-1.1.2/myclass` 目录中，使用如下命令编译代码：

```bash
javac -classpath ../hadoop-core-1.1.2.jar LocalFile2Hdfs.java
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386769269)

**建立测试文件**

进入 `/app/hadoop-1.1.2/input` 目录，在该目录中建立 `local2hdfs.txt` 文件。

```bash
cd /app/hadoop-1.1.2/input/
vi local2hdfs.txt
```

内容为：

```text
Washington (CNN) -- Twitter is suing the U.S. government in an effort to loosen restrictions on what the social media giant can say publicly about the national security-related requests it receives for user data.
The company filed a lawsuit against the Justice Department on Monday in a federal court in northern California, arguing that its First Amendment rights are being violated by restrictions that forbid the disclosure of how many national security letters and Foreign Intelligence Surveillance Act court orders it receives -- even if that number is zero.
Twitter vice president Ben Lee wrote in a blog post that it's suing in an effort to publish the full version of a "transparency report" prepared this year that includes those details.
The San Francisco-based firm was unsatisfied with the Justice Department's move in January to allow technological firms to disclose the number of national security-related requests they receive in broad ranges.
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386789932)

**使用编译代码上传文件内容到 HDFS**

使用如下命令读取 local2hdfs 第 101-120 字节的内容写入 HDFS 成为一个新文件：

```bash
cd /app/hadoop-1.1.2/input
hadoop LocalFile2Hdfs local2hdfs.txt /class4/local2hdfs_part.txt
hadoop fs -ls /class4
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386807502)

**验证是否成功**

使用如下命令读取 `local2hdfs_part.txt` 内容：

```bash
hadoop fs -cat /class4/local2hdfs_part.txt
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386824253)

```java
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class Hdfs2LocalFile {
    public static void main(String[] args) throws Exception {

        String uri = args[0];
        String local = args[1];

        FSDataInputStream in = null;
        OutputStream out = null;
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            in = fs.open(new Path(uri));
            out = new FileOutputStream(local);

            byte[] buffer = new byte[20];
            in.skip(100);
            int bytesRead = in.read(buffer);
            if (bytesRead >= 0) {
                out.write(buffer, 0, bytesRead);
            }
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
    }
}
```

**编写代码**

进入 `/app/hadoop-1.1.2/myclass` 目录，在该目录中建立 `Hdfs2LocalFile.java` 代码文件，命令如下：

```bash
cd /app/hadoop-1.1.2/myclass/
vi Hdfs2LocalFile.java
```

输入代码内容：

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386849203)

**编译代码**

在 `/app/hadoop-1.1.2/myclass` 目录中，使用如下命令编译代码：

```bash
javac -classpath ../hadoop-core-1.1.2.jar Hdfs2LocalFile.java
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386868743)

**建立测试文件**

进入 `/app/hadoop-1.1.2/input` 目录，在该目录中建立 `hdfs2local.txt` 文件。

```bash
cd /app/hadoop-1.1.2/input/
vi hdfs2local.txt
```

内容为：

```text
The San Francisco-based firm was unsatisfied with the Justice Department's move in January to allow technological firms to disclose the number of national security-related requests they receive in broad ranges.
"It's our belief that we are entitled under the First Amendment to respond to our users' concerns and to the statements of U.S. government officials by providing information about the scope of U.S. government surveillance -- including what types of legal process have not been received," Lee wrote. "We should be free to do this in a meaningful way, rather than in broad, inexact ranges."
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386882259)

在 `/app/hadoop-1.1.2/input` 目录下把该文件上传到 hdfs 的 `/class4/` 文件夹中：

```bash
hadoop fs -copyFromLocal hdfs2local.txt /class4/hdfs2local.txt
hadoop fs -ls /class4/
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386893704)

**使用编译代码把文件内容从 HDFS 输出到文件系统中**

使用如下命令读取 `hdfs2local.txt` 第 101-120 字节的内容写入本地文件系统成为一个新文件：

```bash
hadoop Hdfs2LocalFile /class4/hdfs2local.txt hdfs2local_part.txt
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386910674)

**验证是否成功**

使用如下命令读取 `hdfs2local_part.txt` 内容：

```bash
cat hdfs2local_part.txt
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386918844)



在本地文件系统生成一个大约 100 字节的文本文件，写一段程序读入这个文件并将其第 101-120 字节的内容写入 HDFS 成为一个新文件。



//注意：在编译前请先删除中文注释！ import java.io.File; import java.io.FileInputStream; import java.io.FileOutputStream; import java.io.OutputStream; import java.net.URI; import org.apache.hadoop.conf.Configuration; import org.apache.hadoop.fs.FSDataInputStream; import org.apache.hadoop.fs.FileSystem; import org.apache.hadoop.fs.Path; import org.apache.hadoop.io.IOUtils; import org.apache.hadoop.util.Progressable; public class LocalFile2Hdfs {    public static void main(String[] args) throws Exception {         // 获取读取源文件和目标文件位置参数        String local = args[0];        String uri = args[1];         FileInputStream in = null;        OutputStream out = null;        Configuration conf = new Configuration();        try {            // 获取读入文件数据            in = new FileInputStream(new File(local));             // 获取目标文件信息            FileSystem fs = FileSystem.get(URI.create(uri), conf);            out = fs.create(new Path(uri), new Progressable() {                @Override                public void progress() {                    System.out.println("*");                }            });             // 跳过前100个字符            in.skip(100);            byte[] buffer = new byte[20];             // 从101的位置读取20个字符到buffer中            int bytesRead = in.read(buffer);            if (bytesRead >= 0) {                out.write(buffer, 0, bytesRead);            }        } finally {            IOUtils.closeStream(in);            IOUtils.closeStream(out);        }    } }



**编写代码**

进入 `/app/hadoop-1.1.2/myclass` 目录，在该目录中建立 `LocalFile2Hdfs.java` 代码文件，命令如下：

```bash
cd /app/hadoop-1.1.2/myclass/
vi LocalFile2Hdfs.java
```

输入代码内容：

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386750442)

**编译代码**

在 `/app/hadoop-1.1.2/myclass` 目录中，使用如下命令编译代码：

```bash
javac -classpath ../hadoop-core-1.1.2.jar LocalFile2Hdfs.java
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386769269)

**建立测试文件**

进入 `/app/hadoop-1.1.2/input` 目录，在该目录中建立 `local2hdfs.txt` 文件。

```bash
cd /app/hadoop-1.1.2/input/
vi local2hdfs.txt
```

内容为：

```text
Washington (CNN) -- Twitter is suing the U.S. government in an effort to loosen restrictions on what the social media giant can say publicly about the national security-related requests it receives for user data.
The company filed a lawsuit against the Justice Department on Monday in a federal court in northern California, arguing that its First Amendment rights are being violated by restrictions that forbid the disclosure of how many national security letters and Foreign Intelligence Surveillance Act court orders it receives -- even if that number is zero.
Twitter vice president Ben Lee wrote in a blog post that it's suing in an effort to publish the full version of a "transparency report" prepared this year that includes those details.
The San Francisco-based firm was unsatisfied with the Justice Department's move in January to allow technological firms to disclose the number of national security-related requests they receive in broad ranges.
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386789932)

**使用编译代码上传文件内容到 HDFS**

使用如下命令读取 local2hdfs 第 101-120 字节的内容写入 HDFS 成为一个新文件：

```bash
cd /app/hadoop-1.1.2/input
hadoop LocalFile2Hdfs local2hdfs.txt /class4/local2hdfs_part.txt
hadoop fs -ls /class4
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386807502)

**验证是否成功**

使用如下命令读取 `local2hdfs_part.txt` 内容：

```bash
hadoop fs -cat /class4/local2hdfs_part.txt
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386824253)



实验案例 3 的反向操作，在 HDFS 中生成一个大约 100 字节的文本文件，写一段程序读入这个文件，并将其第 101-120 字节的内容写入本地文件系统成为一个新文件。

import java.io.File; import java.io.FileInputStream; import java.io.FileOutputStream; import java.io.OutputStream; import java.net.URI; import org.apache.hadoop.conf.Configuration; import org.apache.hadoop.fs.FSDataInputStream; import org.apache.hadoop.fs.FileSystem; import org.apache.hadoop.fs.Path; import org.apache.hadoop.io.IOUtils; public class Hdfs2LocalFile {    public static void main(String[] args) throws Exception {         String uri = args[0];        String local = args[1];         FSDataInputStream in = null;        OutputStream out = null;        Configuration conf = new Configuration();        try {            FileSystem fs = FileSystem.get(URI.create(uri), conf);            in = fs.open(new Path(uri));            out = new FileOutputStream(local);             byte[] buffer = new byte[20];            in.skip(100);            int bytesRead = in.read(buffer);            if (bytesRead >= 0) {                out.write(buffer, 0, bytesRead);            }        } finally {            IOUtils.closeStream(in);            IOUtils.closeStream(out);        }    } }



**编写代码**

进入 `/app/hadoop-1.1.2/myclass` 目录，在该目录中建立 `Hdfs2LocalFile.java` 代码文件，命令如下：

```bash
cd /app/hadoop-1.1.2/myclass/
vi Hdfs2LocalFile.java
```

输入代码内容：

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386849203)

**编译代码**

在 `/app/hadoop-1.1.2/myclass` 目录中，使用如下命令编译代码：

```bash
javac -classpath ../hadoop-core-1.1.2.jar Hdfs2LocalFile.java
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386868743)

**建立测试文件**

进入 `/app/hadoop-1.1.2/input` 目录，在该目录中建立 `hdfs2local.txt` 文件。

```bash
cd /app/hadoop-1.1.2/input/
vi hdfs2local.txt
```

内容为：

```text
The San Francisco-based firm was unsatisfied with the Justice Department's move in January to allow technological firms to disclose the number of national security-related requests they receive in broad ranges.
"It's our belief that we are entitled under the First Amendment to respond to our users' concerns and to the statements of U.S. government officials by providing information about the scope of U.S. government surveillance -- including what types of legal process have not been received," Lee wrote. "We should be free to do this in a meaningful way, rather than in broad, inexact ranges."
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386882259)

在 `/app/hadoop-1.1.2/input` 目录下把该文件上传到 hdfs 的 `/class4/` 文件夹中：

```bash
hadoop fs -copyFromLocal hdfs2local.txt /class4/hdfs2local.txt
hadoop fs -ls /class4/
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386893704)

**使用编译代码把文件内容从 HDFS 输出到文件系统中**

使用如下命令读取 `hdfs2local.txt` 第 101-120 字节的内容写入本地文件系统成为一个新文件：

```bash
hadoop Hdfs2LocalFile /class4/hdfs2local.txt hdfs2local_part.txt
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386910674)

**验证是否成功**

使用如下命令读取 `hdfs2local_part.txt` 内容：

```bash
cat hdfs2local_part.txt
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1032time1433386918844)



本节我们将正式开始讲解 MapReduce 原理，分步骤进行。

MapReduce 是现今一个非常流行的分布式计算框架，它被设计用于并行计算海量数据。第一个提出该技术框架的是 Google 公司，而 Google 的灵感则来自于函数式编程语言，如 LISP、Scheme、ML 等。

MapReduce 框架的核心步骤主要分两部分：Map 和 Reduce。当你向 MapReduce 框架提交一个计算作业时，它会首先把计算作业拆分成若干个 Map 任务，然后分配到不同的节点上去执行，每一个 Map 任务处理输入数据中的一部分，当 Map 任务完成后，它会生成一些中间文件，这些中间文件将会作为 Reduce 任务的输入数据。Reduce 任务的主要目标就是把前面若干个 Map 的输出汇总到一起并输出。

从高层抽象来看，MapReduce 的数据流图如下图所示：

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404183969)



![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404196962)

**Map 过程**

1. 每个输入分片会让一个 map 任务来处理，默认情况下，以 HDFS 的一个块的大小（默认为 64M）为一个分片，当然我们也可以设置块的大小。map 输出的结果会暂时放在一个环形内存缓冲区中（该缓冲区的大小默认为 100M，由 `io.sort.mb` 属性控制），当该缓冲区快要溢出时（默认为缓冲区大小的 80%，由 `io.sort.spill.percent` 属性控制），会在本地文件系统中创建一个溢出文件，将该缓冲区中的数据写入这个文件。
2. 在写入磁盘之前，线程首先根据 reduce 任务的数目将数据划分为相同数目的分区，也就是一个 reduce 任务对应一个分区的数据。这样做是为了避免有些 reduce 任务分配到大量数据，而有些 reduce 任务却分到很少数据，甚至没有分到数据的尴尬局面。其实分区就是对数据进行 hash 的过程。然后对每个分区中的数据进行排序，如果此时设置了 Combiner，将排序后的结果进行 Combia 操作，这样做的目的是让尽可能少的数据写入到磁盘。
3. 当 map 任务输出最后一个记录时，可能会有很多的溢出文件，这时需要将这些文件合并。合并的过程中会不断地进行排序和 combia 操作，目的有两个：
   - 尽量减少每次写入磁盘的数据量
   - 尽量减少下一次复制阶段网络传输的数据量。最后合并成了一个已分区且已排序的文件。为了减少网络传输的数据量，这里可以将数据压缩，只要将 `mapred.compress.map.out` 设置为 `true` 就可以了。
4. 将分区中的数据拷贝给相对应的 reduce 任务。有人可能会问：分区中的数据怎么知道它对应的 reduce 是哪个呢？其实 map 任务一直和其父 TaskTracker 保持联系，而 TaskTracker 又一直和 JobTracker 保持心跳。所以 JobTracker 中保存了整个集群中的宏观信息。只要 reduce 任务向 JobTracker 获取对应的 map 输出位置就可以了。

**Reduce 过程**

1. Reduce 会接收到不同 map 任务传来的数据，并且每个 map 传来的数据都是有序的。如果 reduce 端接受的数据量相当小，则直接存储在内存中（缓冲区大小由 `mapred.job.shuffle.input.buffer.percent` 属性控制，表示用作此用途的堆空间的百分比），如果数据量超过了该缓冲区大小的一定比例（由 `mapred.job.shuffle.merge.percent` 决定），则对数据合并后溢写到磁盘中。
2. 随着溢写文件的增多，后台线程会将它们合并成一个更大的有序的文件，这样做是为了给后面的合并节省时间。其实不管在 map 端还是 reduce 端，MapReduce 都是反复地执行排序、合并操作。
3. 合并的过程中会产生许多的中间文件（写入磁盘了），但 MapReduce 会让写入磁盘的数据尽可能地少，并且最后一次合并的结果并没有写入磁盘，而是直接输入到 reduce 函数。

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404314211)

1. 在集群中的任意一个节点提交 MapReduce 程序。
2. JobClient 收到作业后，JobClient 向 JobTracker 请求获取一个 Job ID。
3. 将运行作业所需要的资源文件复制到 HDFS 上（包括 MapReduce 程序打包的 JAR 文件、配置文件和客户端计算所得的输入划分信息），这些文件都存放在 JobTracker 专门为该作业创建的文件夹中，文件夹名为该作业的 Job ID。
4. 获得作业 ID 后，提交作业。
5. JobTracker 接收到作业后，将其放在一个作业队列里，等待作业调度器对其进行调度，当作业调度器根据自己的调度算法调度到该作业时，会根据输入划分信息为每个划分创建一个 map 任务，并将 map 任务分配给 TaskTracker 执行。
6. 对于 map 和 reduce 任务，TaskTracker 根据主机核的数量和内存的大小有固定数量的 map 槽和 reduce 槽。这里需要强调的是：map 任务不是随随便便地分配给某个 TaskTracker 的，这里有个概念叫：数据本地化（Data-Local）。意思是：将 map 任务分配给含有该 map 处理的数据块的 TaskTracker 上，同时将程序 JAR 包复制到该 TaskTracker 上来运行，这叫“运算移动，数据不移动”。
7. TaskTracker 每隔一段时间会给 JobTracker 发送一个心跳，告诉 JobTracker 它依然在运行，同时心跳中还携带着很多的信息，比如当前 map 任务完成的进度等信息。当 JobTracker 收到作业的最后一个任务完成信息时，便把该作业设置成“成功”。当 JobClient 查询状态时，它将得知任务已完成，便显示一条消息给用户。
8. 运行的 TaskTracker 从 HDFS 中获取运行所需要的资源，这些资源包括 MapReduce 程序打包的 JAR 文件、配置文件和客户端计算所得的输入划分等信息。
9. TaskTracker 获取资源后启动新的 JVM 虚拟机。
10. 运行每一个任务。

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404314211)

1. 在集群中的任意一个节点提交 MapReduce 程序。
2. JobClient 收到作业后，JobClient 向 JobTracker 请求获取一个 Job ID。
3. 将运行作业所需要的资源文件复制到 HDFS 上（包括 MapReduce 程序打包的 JAR 文件、配置文件和客户端计算所得的输入划分信息），这些文件都存放在 JobTracker 专门为该作业创建的文件夹中，文件夹名为该作业的 Job ID。
4. 获得作业 ID 后，提交作业。
5. JobTracker 接收到作业后，将其放在一个作业队列里，等待作业调度器对其进行调度，当作业调度器根据自己的调度算法调度到该作业时，会根据输入划分信息为每个划分创建一个 map 任务，并将 map 任务分配给 TaskTracker 执行。
6. 对于 map 和 reduce 任务，TaskTracker 根据主机核的数量和内存的大小有固定数量的 map 槽和 reduce 槽。这里需要强调的是：map 任务不是随随便便地分配给某个 TaskTracker 的，这里有个概念叫：数据本地化（Data-Local）。意思是：将 map 任务分配给含有该 map 处理的数据块的 TaskTracker 上，同时将程序 JAR 包复制到该 TaskTracker 上来运行，这叫“运算移动，数据不移动”。
7. TaskTracker 每隔一段时间会给 JobTracker 发送一个心跳，告诉 JobTracker 它依然在运行，同时心跳中还携带着很多的信息，比如当前 map 任务完成的进度等信息。当 JobTracker 收到作业的最后一个任务完成信息时，便把该作业设置成“成功”。当 JobClient 查询状态时，它将得知任务已完成，便显示一条消息给用户。
8. 运行的 TaskTracker 从 HDFS 中获取运行所需要的资源，这些资源包括 MapReduce 程序打包的 JAR 文件、配置文件和客户端计算所得的输入划分等信息。
9. TaskTracker 获取资源后启动新的 JVM 虚拟机。
10. 运行每一个任务。

下载气象数据集部分数据，写一个 Map-Reduce 作业，求每年的最低温度。



**MinTemperature.java**

```java
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MinTemperature {

    public static void main(String[] args) throws Exception {
        if(args.length != 2) {
            System.err.println("Usage: MinTemperature<input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(MinTemperature.class);
        job.setJobName("Min temperature");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(MinTemperatureMapper.class);
        job.setReducerClass(MinTemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

**MinTemperatureMapper.java**

```java
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String year = line.substring(15, 19);

        int airTemperature;
        if(line.charAt(87) == '+') {
            airTemperature = Integer.parseInt(line.substring(88, 92));
        } else {
            airTemperature = Integer.parseInt(line.substring(87, 92));
        }

        String quality = line.substring(92, 93);
        if(airTemperature != MISSING && quality.matches("[01459]")) {
            context.write(new Text(year), new IntWritable(airTemperature));
        }
    }
}
```

**MinTemperatureReducer.java**

```java
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MinTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int minValue = Integer.MAX_VALUE;
        for(IntWritable value : values) {
            minValue = Math.min(minValue, value.get());
        }
        context.write(key, new IntWritable(minValue));
    }
}
```



配置本机主机名为 hadoop，`sudo` 时需要输入 shiyanlou 用户的密码。将 hadoop 添加到最后一行的末尾。

```bash
sudo vim /etc/hosts
# 将hadoop添加到最后一行的末尾，修改后类似：（使用 tab 键添加空格）
# 172.17.2.98 f738b9456777 hadoop
ping hadoop
```

使用如下命令启动 Hadoop

```bash
cd /app/hadoop-1.1.2/bin
./start-all.sh
jps # 查看启动的进程，确保 NameNode 和 DataNode 都有启动
```

**编写代码**

进入 `/app/hadoop-1.1.2/myclass` 目录，在该目录中建立 `MinTemperature.java`、`MinTemperatureMapper.java` 和 `MinTemperatureReducer.java` 代码文件，执行命令如下：

```bash
cd /app/hadoop-1.1.2/myclass/
vi MinTemperature.java
vi MinTemperatureMapper.java
vi MinTemperatureReducer.java
```

`MinTemperature.java`：

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404351252)

`MinTemperatureMapper.java`：

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404364542)

`MinTemperatureReducer.java`:

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404376152)

**编译代码**

在 `/app/hadoop-1.1.2/myclass` 目录中，使用如下命令对 java 代码进行编译，为保证编译成功，加入 `classpath` 变量，引入 `hadoop-core-1.1.2.jar` 包：

```bash
javac -classpath ../hadoop-core-1.1.2.jar *.java
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404474910)

**打包编译文件**

把编译好的 class 文件打包，否则在执行过程会发生错误。把打好的包移动到上级目录并删除编译好的 class 文件：

```bash
jar cvf ./MinTemperature.jar ./Min*.class
mv *.jar ..
rm Min*.class
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404501775)

**解压气象数据并上传到 HDFS 中**

气象数据在 `/home/shiyanlou/install-pack/class5/temperature` 子目录下。

把 NCDC 气象数据解压，并使用 zcat 命令把这些数据文件解压并合并到一个 `temperature.txt` 文件中。

```bash
cd /home/shiyanlou/install-pack/class5/temperature
zcat *.gz > temperature.txt
tail temperature.txt
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404586595)

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404595281)

气象数据具体的下载地址为 `ftp://ftp3.ncdc.noaa.gov/pub/data/noaa/`，该数据包括 1900 年到现在所有年份的气象数据，大小大概有 70 多个 G。为了测试简单，我们这里选取一部分的数据进行测试。合并后把这个文件上传到 HDFS 文件系统的 `/class5/in` 目录中：

```bash
hadoop fs -mkdir -p /class5/in
hadoop fs -copyFromLocal temperature.txt /class5/in
hadoop fs -ls /class5/in
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404623756)

**运行程序**

以 jar 的方式启动 MapReduce 任务，执行输出目录为 `/class5/out`：

```bash
cd /app/hadoop-1.1.2
hadoop jar MinTemperature.jar MinTemperature /class5/in/temperature.txt  /class5/out
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404641286)

**查看结果**

执行成功后，查看 `/class5/out` 目录中是否存在运行结果，使用 cat 查看结果（温度需要除以 10）：

```bash
hadoop fs -ls /class5/out
hadoop fs -cat /class5/out/part-r-00000
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404655717)

**查看页面结果（由于实验楼环境是命令行界面，以下仅为说明运行过程和结果可以通过界面进行查看）**

1. 查看 `jobtracker.jsp`。

```bash
http://XX.XXX.XX.XXX:50030/jobtracker.jsp
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404670007)

查看已经完成的作业任务：

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404681837)

任务的详细信息：

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404693660)

1. 查看 `dfshealth.jsp`。

```bash
http://XX.XXX.XX.XXX:50070/dfshealth.jsp
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404825788)

分别查看 HDFS 文件系统和日志：

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404836858)

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404856318)

如果求温度的平均值

**AvgTemperature.java**

```java
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgTemperature {

    public static void main(String[] args) throws Exception {

        if(args.length != 2) {
            System.out.println("Usage: AvgTemperatrue <input path><output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(AvgTemperature.class);
        job.setJobName("Avg Temperature");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(AvgTemperatureMapper.class);
        job.setCombinerClass(AvgTemperatureCombiner.class);
        job.setReducerClass(AvgTemperatureReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

**AvgTemperatureMapper.java**

```java
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AvgTemperatureMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String line = value.toString();
        String year = line.substring(15, 19);

        int airTemperature;
        if(line.charAt(87) == '+') {
            airTemperature = Integer.parseInt(line.substring(88, 92));
        } else {
            airTemperature =  Integer.parseInt(line.substring(87, 92));
        }

        String quality = line.substring(92, 93);
        if(airTemperature != MISSING && !quality.matches("[01459]")) {
            context.write(new Text(year), new Text(String.valueOf(airTemperature)));
        }
    }
}
```

**AvgTemperatureCombiner.java**

```java
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgTemperatureCombiner extends Reducer<Text, Text, Text, Text>{

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        double sumValue = 0;
        long numValue = 0;

        for(Text value : values) {
            sumValue += Double.parseDouble(value.toString());
            numValue ++;
        }

        context.write(key, new Text(String.valueOf(sumValue) + ',' + String.valueOf(numValue)));
    }
}
```

**AvgTemperatureReducer.java**

```java
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgTemperatureReducer extends Reducer<Text, Text, Text, IntWritable>{

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        double sumValue = 0;
        long numValue = 0;
        int avgValue = 0;

        for(Text value : values) {
            String[] valueAll = value.toString().split(",");
            sumValue += Double.parseDouble(valueAll[0]);
            numValue += Integer.parseInt(valueAll[1]);
        }

        avgValue  = (int)(sumValue/numValue);
        context.write(key, new IntWritable(avgValue));
    }
}
```

**编写代码**

进入 `/app/hadoop-1.1.2/myclass` 目录，在该目录中建立 `AvgTemperature.java`、`AvgTemperatureMapper.java`、`AvgTemperatureCombiner.java` 和`AvgTemperatureReducer.java` 代码文件，代码内容已在前面列出，执行命令如下：

```bash
cd /app/hadoop-1.1.2/myclass/
vi AvgTemperature.java
vi AvgTemperatureMapper.java
vi AvgTemperatureCombiner.java
vi AvgTemperatureReducer.java
```

`AvgTemperature.java`：

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404897200)

`AvgTemperatureMapper.java`：

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404910560)

`AvgTemperatureCombiner.java`：

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404921341)

`AvgTemperatureReducer.java`:

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404933651)

**编译代码**

在 `/app/hadoop-1.1.2/myclass` 目录中，使用如下命令对 java 代码进行编译，为保证编译成功，加入 `classpath` 变量，引入 `hadoop-core-1.1.2.jar` 包：

```bash
javac -classpath ../hadoop-core-1.1.2.jar Avg*.java
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404966539)

**打包编译文件**

把编译好的 class 文件打包，否则在执行过程中会发生错误。把打好的包移动到上级目录并删除编译好的 class 文件：

```bash
jar cvf ./AvgTemperature.jar ./Avg*.class
ls
mv *.jar ..
rm Avg*.class
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404980052)

**运行程序**

数据使用作业 2 求每年最低温度的气象数据，数据在 HDFS 位置为 `/class5/in/temperature.txt`，以 jar 的方式启动 MapReduce 任务，执行输出目录为 `/class5/out2`：

```bash
cd /app/hadoop-1.1.2
hadoop jar AvgTemperature.jar AvgTemperature /class5/in/temperature.txt /class5/out2
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404990642)

**查看结果**

执行成功后，查看 `/class5/out2` 目录中是否存在运行结果，使用 `cat` 查看结果（温度需要除以 10）：

```bash
hadoop fs -ls /class5/out2
hadoop fs -cat /class5/out2/part-r-00000
```

![图片描述信息](https://doc.shiyanlou.com/userid29778labid1033time1433404999021)

配置本机主机名为 hadoop，sudo 时需要输入 shiyanlou 用户的密码。将 hadoop 添加到最后一行的末尾。

```bash
sudo vim /etc/hosts
# 将hadoop添加到最后一行的末尾，修改后类似：（使用 tab 键添加空格）
# 172.17.2.98 f738b9456777 hadoop
ping hadoop
```

使用如下命令启动 Hadoop：

```bash
cd /app/hadoop-1.1.2/bin
./start-all.sh
jps # 查看启动的进程，确保 NameNode 和 DataNode 都有启动
```

测试数据包括两个文件 dept（部门）和 emp（员工），其中各字段用逗号分隔：

**dept 文件内容：**

```text
10,ACCOUNTING,NEW YORK
20,RESEARCH,DALLAS
30,SALES,CHICAGO
40,OPERATIONS,BOSTON
```

**emp 文件内容：**

```text
7369,SMITH,CLERK,7902,17-12月-80,800,,20
7499,ALLEN,SALESMAN,7698,20-2月-81,1600,300,30
7521,WARD,SALESMAN,7698,22-2月-81,1250,500,30
7566,JONES,MANAGER,7839,02-4月-81,2975,,20
7654,MARTIN,SALESMAN,7698,28-9月-81,1250,1400,30
7698,BLAKE,MANAGER,7839,01-5月-81,2850,,30
7782,CLARK,MANAGER,7839,09-6月-81,2450,,10
7839,KING,PRESIDENT,,17-11月-81,5000,,10
7844,TURNER,SALESMAN,7698,08-9月-81,1500,0,30
7900,JAMES,CLERK,7698,03-12月-81,950,,30
7902,FORD,ANALYST,7566,03-12月-81,3000,,20
7934,MILLER,CLERK,7782,23-1月-82,1300,,10
```

在 `/home/shiyanlou/install-pack/class6` 目录可以找到这两个文件，把这两个文件上传到 HDFS 中 `/class6/input` 目录中，执行如下命令：

```bash
cd /home/shiyanlou/install-pack/class6
hadoop fs -mkdir -p /class6/input
hadoop fs -copyFromLocal dept /class6/input
hadoop fs -copyFromLocal emp /class6/input
hadoop fs -ls /class6/input
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433602760844.png)

**问题分析**

MapReduce 中的 join 分为好几种，比如有最常见的 reduce side join、map side join 和 semi join 等。reduce join 在 shuffle 阶段要进行大量的数据传输，会造成大量的网络 IO 效率低下，而 map side join 在处理多个小表关联大表时非常有用 。

Map side join 是针对以下场景进行的优化：两个待连接表中，有一个表非常大，而另一个表非常小，以至于小表可以直接存放到内存中。这样我们可以将小表复制多份，让每个 map task 内存中存在一份（比如存放到 hash table 中），然后只扫描大表，对于大表中的每一条记录 key/value，在 hash table 中查找是否有相同的 key 的记录，如果有，则连接后输出即可。为了支持文件的复制，Hadoop 提供了一个类 DistributedCache，使用该类的方法如下：

1. 用户使用静态方法 `DistributedCache.addCacheFile()` 指定要复制的文件，它的参数是文件的 URI（如果是 HDFS 上的文件，可以这样：`hdfs://jobtracker:50030/home/XXX/file`）。JobTracker 在作业启动之前会获取这个 URI 列表，并将相应的文件拷贝到各个 TaskTracker 的本地磁盘上。
2. 用户使用 `DistributedCache.getLocalCacheFiles()` 方法获取文件目录，并使用标准的文件读写 API 读取相应的文件。

在下面代码中，将会把数据量小的表（部门 dept) 缓存在内存中，在 Mapper 阶段对员工部门编号映射成部门名称，该名称作为 key 输出到 Reduce 中，在 Reduce 中按照部门计算各个部门的总工资。

**处理流程图**

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433602822185.png)

**测试代码**

**Q1SumDeptSalary.java 代码（vi 编辑代码是不能存在中文）：**

```java
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Q1SumDeptSalary extends Configured implements Tool {

    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

        // 用于缓存 dept文件中的数据
        private Map<String, String> deptMap = new HashMap<String, String>();
        private String[] kv;

        // 此方法会在Map方法执行之前执行且执行一次
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader in = null;
            try {

                // 从当前作业中获取要缓存的文件
                Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                String deptIdName = null;
                for (Path path : paths) {

                    // 对部门文件字段进行拆分并缓存到deptMap中
                    if (path.toString().contains("dept")) {
                        in = new BufferedReader(new FileReader(path.toString()));
                        while (null != (deptIdName = in.readLine())) {

                            // 对部门文件字段进行拆分并缓存到deptMap中
                            // 其中Map中key为部门编号，value为所在部门名称
                            deptMap.put(deptIdName.split(",")[0], deptIdName.split(",")[1]);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // 对员工文件字段进行拆分
            kv = value.toString().split(",");

            // map join: 在map阶段过滤掉不需要的数据，输出key为部门名称和value为员工工资
            if (deptMap.containsKey(kv[7])) {
                if (null != kv[5] && !"".equals(kv[5].toString())) {
                    context.write(new Text(deptMap.get(kv[7].trim())), new Text(kv[5].trim()));
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, LongWritable> {

public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // 对同一部门的员工工资进行求和
            long sumSalary = 0;
            for (Text val : values) {
                sumSalary += Long.parseLong(val.toString());
            }

            // 输出key为部门名称和value为该部门员工工资总和
            context.write(key, new LongWritable(sumSalary));
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        // 实例化作业对象，设置作业名称、Mapper和Reduce类
        Job job = new Job(getConf(), "Q1SumDeptSalary");
        job.setJobName("Q1SumDeptSalary");
        job.setJarByClass(Q1SumDeptSalary.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        // 设置输入格式类
        job.setInputFormatClass(TextInputFormat.class);

        // 设置输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 第1个参数为缓存的部门数据路径、第2个参数为员工数据路径和第3个参数为输出路径
    String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
    DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), job.getConfiguration());
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : 1;
    }

    /**
     * 主方法，执行入口
     * @param args 输入参数
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Q1SumDeptSalary(), args);
        System.exit(res);
    }
}
```

**编译并打包代码**

进入 `/app/hadoop-1.1.2/myclass/class6` 目录中新建 `Q1SumDeptSalary.java` 程序代码（代码也可以使用 `/home/shiyanlou/install-pack/class6/Q1SumDeptSalary.java 文件`）。

```bash
cd /app/hadoop-1.1.2/myclass
mkdir class6 && cd class6
vi Q1SumDeptSalary.java
```

编译代码：

```bash
javac -classpath ../../hadoop-core-1.1.2.jar:../../lib/commons-cli-1.2.jar Q1SumDeptSalary.java
```

把编译好的代码打成 jar 包（如果不打成 jar 形式运行会提示 class 无法找到的错误）。

```bash
jar cvf ./Q1SumDeptSalary.jar ./Q1SumDept*.class
mv *.jar ../..
rm Q1SumDept*.class
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433602842300.png)

**运行并查看结果**

运行 Q1SumDeptSalary 时需要输入部门数据路径、员工数据路径和输出路径三个参数，需要注意的是 hdfs 的路径参数需要全路径，否则运行会报错：

- 部门数据路径：`hdfs://hadoop:9000/class6/input/dept`，部门数据将缓存在各运行任务的节点内容中，可以提高处理的效率。
- 员工数据路径：`hdfs://hadoop:9000/class6/input/emp`。
- 输出路径：`hdfs://hadoop:9000/class6/out1`。

运行如下命令：

```bash
cd /app/hadoop-1.1.2
hadoop jar Q1SumDeptSalary.jar Q1SumDeptSalary hdfs://hadoop:9000/class6/input/dept hdfs://hadoop:9000/class6/input/emp hdfs://hadoop:9000/class6/out1
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433602855465.png)

运行成功后，刷新 CentOS HDFS 中的输出路径 `/class6/out1` 目录，打开 `part-r-00000` 文件。

```bash
hadoop fs -ls /class6/out1
hadoop fs -cat /class6/out1/part-r-00000
```

可以看到运行结果：

```text
ACCOUNTING 8750
RESEARCH 6775
SALES 9400
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433602863529.png)

**问题分析**

求各个部门的人数和平均工资，需要得到各部门工资总数和部门人数，通过两者相除获取各部门平均工资。首先和问题 1 类似在 Mapper 的 Setup 阶段缓存部门数据，然后在 Mapper 阶段抽取出部门编号和员工工资，利用缓存部门数据把部门编号对应为部门名称，接着在 Shuffle 阶段把传过来的数据处理为部门名称对应该部门所有员工工资的列表，最后在 Reduce 中按照部门归组，遍历部门所有员工，求出总数和员工数，输出部门名称和平均工资。

**处理流程图**

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433602921145.png)

**编写代码**

**Q2DeptNumberAveSalary.java 代码：**

```java
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Q2DeptNumberAveSalary extends Configured implements Tool {

    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

        // 用于缓存 dept文件中的数据
        private Map<String, String> deptMap = new HashMap<String, String>();
        private String[] kv;

        // 此方法会在Map方法执行之前执行且执行一次
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader in = null;
            try {
                // 从当前作业中获取要缓存的文件
                Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                String deptIdName = null;
                for (Path path : paths) {

                    // 对部门文件字段进行拆分并缓存到deptMap中
                    if (path.toString().contains("dept")) {
                        in = new BufferedReader(new FileReader(path.toString()));
                        while (null != (deptIdName = in.readLine())) {

                            // 对部门文件字段进行拆分并缓存到deptMap中
                            // 其中Map中key为部门编号，value为所在部门名称
                            deptMap.put(deptIdName.split(",")[0], deptIdName.split(",")[1]);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // 对员工文件字段进行拆分
            kv = value.toString().split(",");

            // map join: 在map阶段过滤掉不需要的数据，输出key为部门名称和value为员工工资
            if (deptMap.containsKey(kv[7])) {
                if (null != kv[5] && !"".equals(kv[5].toString())) {
                    context.write(new Text(deptMap.get(kv[7].trim())), new Text(kv[5].trim()));
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            long sumSalary = 0;
            int deptNumber = 0;

            // 对同一部门的员工工资进行求和
            for (Text val : values) {
                sumSalary += Long.parseLong(val.toString());
                deptNumber++;
            }

            // 输出key为部门名称和value为该部门员工工资平均值
    context.write(key, new Text("Dept Number:" + deptNumber + ", Ave Salary:" + sumSalary / deptNumber));
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        // 实例化作业对象，设置作业名称、Mapper和Reduce类
        Job job = new Job(getConf(), "Q2DeptNumberAveSalary");
        job.setJobName("Q2DeptNumberAveSalary");
        job.setJarByClass(Q2DeptNumberAveSalary.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        // 设置输入格式类
        job.setInputFormatClass(TextInputFormat.class);

        // 设置输出格式类
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 第1个参数为缓存的部门数据路径、第2个参数为员工数据路径和第3个参数为输出路径
    String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
        DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), job.getConfiguration());
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : 1;
    }

    /**
     * 主方法，执行入口
     * @param args 输入参数
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Q2DeptNumberAveSalary(), args);
        System.exit(res);
    }
}
```

**3.2.4 编译并打包代码**

进入 `/app/hadoop-1.1.2/myclass/class6` 目录中新建 `Q2DeptNumberAveSalary.java` 程序代码（代码也可以使用 `/home/shiyanlou/install-pack/class6/Q2DeptNumberAveSalary.java` 文件）。

```bash
cd /app/hadoop-1.1.2/myclass/class6
vi Q2DeptNumberAveSalary.java
```

编译代码：

```bash
javac -classpath ../../hadoop-core-1.1.2.jar:../../lib/commons-cli-1.2.jar Q2DeptNumberAveSalary.java
```

把编译好的代码打成 jar 包，如果不打成 jar 形式运行会提示 class 无法找到的错误。

```bash
jar cvf ./Q2DeptNumberAveSalary.jar ./Q2DeptNum*.class
mv *.jar ../..
rm Q2DeptNum*.class
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433602934155.png)

**运行并查看结果**

运行 Q2DeptNumberAveSalary 时需要输入部门数据路径、员工数据路径和输出路径三个参数，需要注意的是 hdfs 的路径参数路径需要全路径，否则运行会报错：

- 部门数据路径：`hdfs://hadoop:9000/class6/input/dept`，部门数据将缓存在各运行任务的节点内容中，可以提高处理的效率
- 员工数据路径：`hdfs://hadoop:9000/class6/input/emp`
- 输出路径：`hdfs://hadoop:9000/class6/out2`

运行如下命令：

```bash
cd /app/hadoop-1.1.2
hadoop jar Q2DeptNumberAveSalary.jar Q2DeptNumberAveSalary hdfs://hadoop:9000/class6/input/dept hdfs://hadoop:9000/class6/input/emp hdfs://hadoop:9000/class6/out2
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433602944499.png)

运行成功后，刷新 CentOS HDFS 中的输出路径 `/class6/out2` 目录。

```bash
hadoop fs -ls /class6/out2
hadoop fs -cat /class6/out2/part-r-00000
```

打开 `part-r-00000` 文件，可以看到运行结果：

```bash
ACCOUNTING    Dept Number:3,Ave Salary:2916
RESEARCH    Dept Number:3,Ave Salary:2258
SALES    Dept Number:6,Ave Salary:1566
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433602955055.png)

**问题分析**

求每个部门最早进入公司员工姓名，需要得到各部门所有员工的进入公司日期，通过比较获取最早进入公司员工姓名。首先和问题 1 类似在 Mapper 的 Setup 阶段缓存部门数据，然后 Mapper 阶段抽取出 key 为部门名称（利用缓存部门数据把部门编号对应为部门名称），value 为员工姓名和进入公司日期，接着在 Shuffle 阶段把传过来的数据处理为部门名称对应该部门所有员工 + 进入公司日期的列表，最后在 Reduce 中按照部门归组，遍历部门所有员工，找出最早进入公司的员工并输出。

**处理流程图**

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433602977537.png)

**编写代码**

```java
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Q3DeptEarliestEmp extends Configured implements Tool {

    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

        // 用于缓存 dept文件中的数据
        private Map<String, String> deptMap = new HashMap<String, String>();
        private String[] kv;

        // 此方法会在Map方法执行之前执行且执行一次
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader in = null;
            try {
                // 从当前作业中获取要缓存的文件
                Path[] paths =     DistributedCache.getLocalCacheFiles(context.getConfiguration());
                String deptIdName = null;
                for (Path path : paths) {
                    if (path.toString().contains("dept")) {
                        in = new BufferedReader(new FileReader(path.toString()));
                        while (null != (deptIdName = in.readLine())) {

                            // 对部门文件字段进行拆分并缓存到deptMap中
                            // 其中Map中key为部门编号，value为所在部门名称
                            deptMap.put(deptIdName.split(",")[0], deptIdName.split(",")[1]);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException,         InterruptedException {

            // 对员工文件字段进行拆分
            kv = value.toString().split(",");

            // map join: 在map阶段过滤掉不需要的数据
            // 输出key为部门名称和value为员工姓名+","+员工进入公司日期
            if (deptMap.containsKey(kv[7])) {
                if (null != kv[4] && !"".equals(kv[4].toString())) {
                    context.write(new Text(deptMap.get(kv[7].trim())), new Text(kv[1].trim()                     + "," + kv[4].trim()));
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,         InterruptedException {

            // 员工姓名和进入公司日期
            String empName = null;
            String empEnterDate = null;

            // 设置日期转换格式和最早进入公司的员工、日期
            DateFormat df = new SimpleDateFormat("dd/MM/yy");

            Date earliestDate = new Date();
            String earliestEmp = null;

            // 遍历该部门下所有员工，得到最早进入公司的员工信息
            for (Text val : values) {
                empName = val.toString().split(",")[0];
                empEnterDate = val.toString().split(",")[1].toString().trim();
                try {
                    System.out.println(df.parse(empEnterDate));
                    if (df.parse(empEnterDate).compareTo(earliestDate) < 0) {
                        earliestDate = df.parse(empEnterDate);
                        earliestEmp = empName;
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }

            // 输出key为部门名称和value为该部门最早进入公司员工
            context.write(key, new Text("The earliest emp of dept:" + earliestEmp + ", Enter             date:" + new SimpleDateFormat("yyyy-MM-dd").format(earliestDate)));
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        // 实例化作业对象，设置作业名称
        Job job = new Job(getConf(), "Q3DeptEarliestEmp");
        job.setJobName("Q3DeptEarliestEmp");

        // 设置Mapper和Reduce类
        job.setJarByClass(Q3DeptEarliestEmp.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        // 设置输入格式类
        job.setInputFormatClass(TextInputFormat.class);

        // 设置输出格式类
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 第1个参数为缓存的部门数据路径、第2个参数为员工数据路径和第三个参数为输出路径
    String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
    DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), job.getConfiguration());
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : 1;
    }

    /**
     * 主方法，执行入口
     * @param args 输入参数
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Q3DeptEarliestEmp(), args);
        System.exit(res);
    }
}
```

**编译并打包代码**

进入 `/app/hadoop-1.1.2/myclass/class6` 目录中新建 `Q3DeptEarliestEmp.java` 程序代码（代码也可以使用 `/home/shiyanlou/install-pack/class6/Q3DeptEarliestEmp.java` 文件）。

```bash
cd /app/hadoop-1.1.2/myclass/class6
vi Q3DeptEarliestEmp.java
```

编译代码：

```bash
javac -classpath ../../hadoop-core-1.1.2.jar:../../lib/commons-cli-1.2.jar Q3DeptEarliestEmp.java
```

把编译好的代码打成 jar 包，如果不打成 jar 形式运行会提示 class 无法找到的错误。

```bash
jar cvf ./Q3DeptEarliestEmp.jar ./Q3DeptEar*.class
mv *.jar ../..
rm Q3DeptEar*.class
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433602994523.png)

**运行并查看结果**

运行 Q3DeptEarliestEmp 时需要输入部门数据路径、员工数据路径和输出路径三个参数，需要注意的是 hdfs 的路径参数路径需要全路径，否则运行会报错：

- 部门数据路径：`hdfs://hadoop:9000/class6/input/dept`，部门数据将缓存在各运行任务的节点内容中，可以提高处理的效率
- 员工数据路径：`hdfs://hadoop:9000/class6/input/emp`
- 输出路径：`hdfs://hadoop:9000/class6/out3`

运行如下命令：

```bash
cd /app/hadoop-1.1.2
hadoop jar Q3DeptEarliestEmp.jar Q3DeptEarliestEmp  hdfs://hadoop:9000/class6/input/dept hdfs://hadoop:9000/class6/input/emp hdfs://hadoop:9000/class6/out3
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603004904.png)

运行成功后，刷新 CentOS HDFS 中的输出路径 `/class6/out3` 目录。

```bash
hadoop fs -ls /class6/out3
hadoop fs -cat /class6/out3/part-r-00000
```

打开 `part-r-00000` 文件，可以看到运行结果：

```bash
ACCOUNTING    The earliest emp of dept:CLARK, Enter date:1981-06-09
RESEARCH    The earliest emp of dept:SMITH, Enter date:1980-12-17
SALES    The earliest emp of dept:ALLEN, Enter date:1981-02-20
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603015927.png)

**问题分析**

求各个城市员工的总工资，需要得到各个城市所有员工的工资，通过对各个城市所有员工工资求和得到总工资。首先和测试例子 1 类似在 Mapper 的 Setup 阶段缓存部门对应所在城市数据，然后在 Mapper 阶段抽取出 key 为城市名称（利用缓存数据把部门编号对应为所在城市名称），value 为员工工资，接着在 Shuffle 阶段把传过来的数据处理为城市名称对应该城市所有员工工资，最后在 Reduce 中按照城市归组，遍历城市所有员工，求出工资总数并输出。

**处理流程图**

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603152878.png)

**编写代码**

```java
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Q4SumCitySalary extends Configured implements Tool {

    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

        // 用于缓存 dept文件中的数据
        private Map<String, String> deptMap = new HashMap<String, String>();
        private String[] kv;

        // 此方法会在Map方法执行之前执行且执行一次
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader in = null;
            try {
                // 从当前作业中获取要缓存的文件
                Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                String deptIdName = null;
                for (Path path : paths) {
                    if (path.toString().contains("dept")) {
                        in = new BufferedReader(new FileReader(path.toString()));
                        while (null != (deptIdName = in.readLine())) {

                            // 对部门文件字段进行拆分并缓存到deptMap中
                            // 其中Map中key为部门编号，value为所在城市名称
                            deptMap.put(deptIdName.split(",")[0], deptIdName.split(",")[2]);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException,         InterruptedException {

            // 对员工文件字段进行拆分
            kv = value.toString().split(",");

            // map join: 在map阶段过滤掉不需要的数据，输出key为城市名称和value为员工工资
            if (deptMap.containsKey(kv[7])) {
                if (null != kv[5] && !"".equals(kv[5].toString())) {
                    context.write(new Text(deptMap.get(kv[7].trim())), new Text(kv[5].trim()));
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, LongWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,         InterruptedException {

            // 对同一城市的员工工资进行求和
            long sumSalary = 0;
            for (Text val : values) {
                sumSalary += Long.parseLong(val.toString());
            }

            // 输出key为城市名称和value为该城市工资总和
            context.write(key, new LongWritable(sumSalary));
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        // 实例化作业对象，设置作业名称
        Job job = new Job(getConf(), "Q4SumCitySalary");
        job.setJobName("Q4SumCitySalary");

        // 设置Mapper和Reduce类
        job.setJarByClass(Q4SumCitySalary.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        // 设置输入格式类
        job.setInputFormatClass(TextInputFormat.class);

        // 设置输出格式类
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 第1个参数为缓存的部门数据路径、第2个参数为员工数据路径和第3个参数为输出路径
    String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
    DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), job.getConfiguration());
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : 1;
    }

    /**
     * 主方法，执行入口
     * @param args 输入参数
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Q4SumCitySalary(), args);
        System.exit(res);
    }
}
```

**编译并打包代码**

进入 `/app/hadoop-1.1.2/myclass/class6` 目录中新建 `Q4SumCitySalary.java` 程序代码（代码也可以使用 `/home/shiyanlou/install-pack/class6/Q4SumCitySalary.java` 文件）。

```bash
cd /app/hadoop-1.1.2/myclass/class6
vi Q4SumCitySalary.java
```

编译代码：

```bash
javac -classpath ../../hadoop-core-1.1.2.jar:../../lib/commons-cli-1.2.jar Q4SumCitySalary.java
```

把编译好的代码打成 jar 包，如果不打成 jar 形式运行会提示 class 无法找到的错误。

```bash
jar cvf ./Q4SumCitySalary.jar ./Q4SumCity*.class
mv *.jar ../..
rm Q4SumCity*.class
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603164935.png)

**运行并查看结果**

运行 Q4SumCitySalary 时需要输入部门数据路径、员工数据路径和输出路径三个参数，需要注意的是 hdfs 的路径参数路径需要全路径，否则运行会报错：

- 部门数据路径：`hdfs://hadoop:9000/class6/input/dept`，部门数据将缓存在各运行任务的节点内容中，可以提高处理的效率。
- 员工数据路径：`hdfs://hadoop:9000/class6/input/emp`。
- 输出路径：`hdfs://hadoop:9000/class6/out4`。

运行如下命令：

```bash
cd /app/hadoop-1.1.2
hadoop jar Q4SumCitySalary.jar Q4SumCitySalary hdfs://hadoop:9000/class6/input/dept hdfs://hadoop:9000/class6/input/emp hdfs://hadoop:9000/class6/out4
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603237983.png)

运行成功后，刷新 CentOS HDFS 中的输出路径 `/class6/out4` 目录。

```bash
hadoop fs -ls /class6/out4
hadoop fs -cat /class6/out4/part-r-00000
```

打开 `part-r-00000` 文件，可以看到运行结果：

```text
CHICAGO    9400
DALLAS    6775
NEW YORK    8750
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603245515.png)

**问题分析**

求工资比上司高的员工姓名及工资，需要得到上司工资及上司所有下属员工，通过比较他们工资高低得到比上司工资高的员工。在 Mapper 阶段输出经理数据和员工对应经理表数据，其中经理数据 key 为员工编号、value 为"M，该员工工资"，员工对应经理表数据 key 为经理编号、value 为"E，该员工姓名，该员工工资"。然后在 Shuffle 阶段把传过来的经理数据和员工对应经理表数据进行归组，如编号为 7698 员工，value 中标志 M 为自己工资，value 中标志 E 为其下属姓名及工资。最后在 Reduce 中遍历比较员工与经理工资高低，输出工资高于经理的员工。

**处理流程图**

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603264082.png)

**编写代码**

```java
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Q5EarnMoreThanManager extends Configured implements Tool {

    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException,         InterruptedException {

            // 对员工文件字段进行拆分
            String[] kv = value.toString().split(",");

            // 输出经理表数据，其中key为员工编号和value为M+该员工工资
            context.write(new Text(kv[0].toString()), new Text("M," + kv[5]));

            // 输出员工对应经理表数据，其中key为经理编号和value为(E，该员工姓名，该员工工资)
            if (null != kv[3] && !"".equals(kv[3].toString())) {
                context.write(new Text(kv[3].toString()), new Text("E," + kv[1] + "," + kv[5]));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,         InterruptedException {

            // 定义员工姓名、工资和存放部门员工Map
            String empName;
            long empSalary = 0;
            HashMap<String, Long> empMap = new HashMap<String, Long>();

            // 定义经理工资变量
            long mgrSalary = 0;

            for (Text val : values) {
                if (val.toString().startsWith("E")) {
                    // 当是员工标示时，获取该员工对应的姓名和工资并放入Map中
                    empName = val.toString().split(",")[1];
                    empSalary = Long.parseLong(val.toString().split(",")[2]);
                    empMap.put(empName, empSalary);
                } else {
                    // 当时经理标志时，获取该经理工资
                    mgrSalary = Long.parseLong(val.toString().split(",")[1]);
                }
            }

            // 遍历该经理下属，比较员工与经理工资高低，输出工资高于经理的员工
            for (java.util.Map.Entry<String, Long> entry : empMap.entrySet()) {
                if (entry.getValue() > mgrSalary) {
                    context.write(new Text(entry.getKey()), new Text("" + entry.getValue()));
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        // 实例化作业对象，设置作业名称
        Job job = new Job(getConf(), "Q5EarnMoreThanManager");
        job.setJobName("Q5EarnMoreThanManager");

        // 设置Mapper和Reduce类
        job.setJarByClass(Q5EarnMoreThanManager.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        // 设置输入格式类
        job.setInputFormatClass(TextInputFormat.class);

        // 设置输出格式类
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 第1个参数为员工数据路径和第2个参数为输出路径
String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : 1;
    }

    /**
     * 主方法，执行入口
     * @param args 输入参数
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Q5EarnMoreThanManager(), args);
        System.exit(res);
    }
}
```

**编译并打包代码**

进入 `/app/hadoop-1.1.2/myclass/class6` 目录中新建 `Q5EarnMoreThanManager.java` 程序代码（代码也可以使用 `/home/shiyanlou/install-pack/class6/Q5EarnMoreThanManager.java` 文件）。

```bash
cd /app/hadoop-1.1.2/myclass/class6
vi Q5EarnMoreThanManager.java
```

编译代码：

```bash
javac -classpath ../../hadoop-core-1.1.2.jar:../../lib/commons-cli-1.2.jar Q5EarnMoreThanManager.java
```

把编译好的代码打成 jar 包，如果不打成 jar 形式运行会提示 class 无法找到的错误。

```bash
jar cvf ./Q5EarnMoreThanManager.jar ./Q5EarnMore*.class
mv *.jar ../..
rm Q5EarnMore*.class
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603277405.png)

**运行并查看结果**

运行 Q5EarnMoreThanManager 运行的员工数据路径和输出路径两个参数，需要注意的是 hdfs 的路径参数路径需要全路径，否则运行会报错：

- 员工数据路径：`hdfs://hadoop:9000/class6/input/emp`。
- 输出路径：`hdfs://hadoop:9000/class6/out5`。

运行如下命令：

```bash
cd /app/hadoop-1.1.2
hadoop jar Q5EarnMoreThanManager.jar Q5EarnMoreThanManager hdfs://hadoop:9000/class6/input/emp hdfs://hadoop:9000/class6/out5
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603287190.png)

运行成功后，刷新 CentOS HDFS 中的输出路径 `/class6/out5` 目录。

```bash
hadoop fs -ls /class6/out5
hadoop fs -cat /class6/out5/part-r-00000
```

打开 `part-r-00000` 文件，可以看到运行结果：

```text
FORD    3000
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603300924.png)

**问题分析**

求工资比公司平均工资要高的员工姓名及工资，需要得到公司的平均工资和所有员工工资，通过比较得出工资比平均工资高的员工姓名及工资。这个问题可以分两个作业进行解决，先求出公司的平均工资，然后与所有员工进行比较得到结果。也可以在一个作业进行解决，这里就得使用作业 setNumReduceTasks 方法，设置 Reduce 任务数为 1，保证每次运行一个 reduce 任务，从而能先求出平均工资，然后进行比较得出结果。

在 Mapper 阶段输出两份所有员工数据，其中一份 key 为 0、value 为该员工工资，另外一份 key 为 0、value 为"该员工姓名，员工工资"。然后在 Shuffle 阶段把传过来数据按照 key 进行归组，在该任务中有 key 值为 0 和 1 两组数据。最后在 Reduce 中对 key 值 0 的所有员工求工资总数和员工数，获得平均工资。对 key 值 1，比较员工与平均工资的大小，输出比平均工资高的员工和对应的工资。

**处理流程图**

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603330240.png)

**编写代码**

```java
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Q6HigherThanAveSalary extends Configured implements Tool {

    public static class MapClass extends Mapper<LongWritable, Text, IntWritable, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException,         InterruptedException {

            // 对员工文件字段进行拆分
            String[] kv = value.toString().split(",");

            // 获取所有员工数据，其中key为0和value为该员工工资
            context.write(new IntWritable(0), new Text(kv[5]));

            // 获取所有员工数据，其中key为0和value为(该员工姓名 ,员工工资)
            context.write(new IntWritable(1), new Text(kv[1] + "," + kv[5]));
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, Text, Text> {

        // 定义员工工资、员工数和平均工资
        private long allSalary = 0;
        private int allEmpCount = 0;
        private long aveSalary = 0;

        // 定义员工工资变量
        private long empSalary = 0;

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws         IOException, InterruptedException {

            for (Text val : values) {
                if (0 == key.get()) {
                    // 获取所有员工工资和员工数
                    allSalary += Long.parseLong(val.toString());
                    allEmpCount++;
                    System.out.println("allEmpCount = " + allEmpCount);
                } else if (1 == key.get()) {
                    if (aveSalary == 0) {
                        aveSalary = allSalary / allEmpCount;
                        context.write(new Text("Average Salary = "), new Text("" + aveSalary));
                        context.write(new Text("Following employees have salarys higher than                         Average:"), new Text(""));
                    }

                    // 获取员工的平均工资
                    System.out.println("Employee salary = " + val.toString());
                    aveSalary = allSalary / allEmpCount;

                    // 比较员工与平均工资的大小，输出比平均工资高的员工和对应的工资
                    empSalary = Long.parseLong(val.toString().split(",")[1]);
                    if (empSalary > aveSalary) {
                        context.write(new Text(val.toString().split(",")[0]), new Text("" +                         empSalary));
                    }
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        // 实例化作业对象，设置作业名称
        Job job = new Job(getConf(), "Q6HigherThanAveSalary");
        job.setJobName("Q6HigherThanAveSalary");

        // 设置Mapper和Reduce类
        job.setJarByClass(Q6HigherThanAveSalary.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        // 必须设置Reduce任务数为1 # -D mapred.reduce.tasks = 1
        // 这是该作业设置的核心，这样才能够保证各reduce是串行的
        job.setNumReduceTasks(1);

        // 设置输出格式类
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 设置输出键和值类型
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 第1个参数为员工数据路径和第2个参数为输出路径
String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : 1;
    }

    /**
     * 主方法，执行入口
     * @param args 输入参数
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Q6HigherThanAveSalary(), args);
        System.exit(res);
    }
}
```

**编译并打包代码**

进入 `/app/hadoop-1.1.2/myclass/class6` 目录中新建 `Q5EarnMoreThanManager.java` 程序代码（代码也可以使用 `/home/shiyanlou/install-pack/class6/Q6HigherThanAveSalary.java` 文件）。

```bash
cd /app/hadoop-1.1.2/myclass/class6
vi Q6HigherThanAveSalary.java
```

编译代码：

```bash
javac -classpath ../../hadoop-core-1.1.2.jar:../../lib/commons-cli-1.2.jar Q6HigherThanAveSalary.java
```

把编译好的代码打成 jar 包，如果不打成 jar 形式运行会提示 class 无法找到的错误。

```bash
jar cvf ./Q6HigherThanAveSalary.jar ./Q6HigherThan*.class
mv *.jar ../..
rm Q6HigherThan*.class
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603343320.png)

**运行并查看结果**

运行 Q6HigherThanAveSalary 运行的员工数据路径和输出路径两个参数，需要注意的是 hdfs 的路径参数路径需要全路径，否则运行会报错：

- 员工数据路径：`hdfs://hadoop:9000/class6/input/emp`
- 输出路径：`hdfs://hadoop:9000/class6/out6`

运行如下命令：

```bash
cd /app/hadoop-1.1.2
hadoop jar Q6HigherThanAveSalary.jar Q6HigherThanAveSalary  hdfs://hadoop:9000/class6/input/emp hdfs://hadoop:9000/class6/out6
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603352738.png)

运行成功后，刷新 CentOS HDFS 中的输出路径 `/class6/out6` 目录。

```bash
hadoop fs -ls /class6/out6
hadoop fs -cat /class6/out6/part-r-00000
```

打开 `part-r-00000` 文件，可以看到运行结果：

```text
Average Salary =     2077
Following employees have salarys higher than Average:
FORD    3000
CLARK    2450
KING    5000
JONES    2975
BLAKE    2850
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603362431.png)

**问题分析**

求名字以 J 开头的员工姓名机器所属部门名称，只需判断员工姓名是否以 J 开头。首先和问题 1 类似在 Mapper 的 Setup 阶段缓存部门数据，然后在 Mapper 阶段判断员工姓名是否以 J 开头，如果是抽取出员工姓名和员工所在部门编号，利用缓存部门数据把部门编号对应为部门名称，转换后输出结果。

**处理流程图**

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603380732.png)

**编写代码**

```java
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Q7NameDeptOfStartJ extends Configured implements Tool {

    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

        // 用于缓存 dept文件中的数据
        private Map<String, String> deptMap = new HashMap<String, String>();
        private String[] kv;

        // 此方法会在Map方法执行之前执行且执行一次
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader in = null;
            try {

                // 从当前作业中获取要缓存的文件
                Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                String deptIdName = null;
                for (Path path : paths) {

                    // 对部门文件字段进行拆分并缓存到deptMap中
                    if (path.toString().contains("dept")) {
                        in = new BufferedReader(new FileReader(path.toString()));
                        while (null != (deptIdName = in.readLine())) {

                            // 对部门文件字段进行拆分并缓存到deptMap中
                            // 其中Map中key为部门编号，value为所在部门名称
                            deptMap.put(deptIdName.split(",")[0], deptIdName.split(",")[1]);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException,         InterruptedException {

            // 对员工文件字段进行拆分
            kv = value.toString().split(",");

            // 输出员工姓名为J开头的员工信息，key为员工姓名和value为员工所在部门名称
            if (kv[1].toString().trim().startsWith("J")) {
                context.write(new Text(kv[1].trim()), new Text(deptMap.get(kv[7].trim())));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        // 实例化作业对象，设置作业名称
        Job job = new Job(getConf(), "Q7NameDeptOfStartJ");
        job.setJobName("Q7NameDeptOfStartJ");

        // 设置Mapper和Reduce类
        job.setJarByClass(Q7NameDeptOfStartJ.class);
        job.setMapperClass(MapClass.class);

        // 设置输入格式类
        job.setInputFormatClass(TextInputFormat.class);

        // 设置输出格式类
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 第1个参数为缓存的部门数据路径、第2个参数为员工数据路径和第3个参数为输出路径
String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
    DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), job.getConfiguration());
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : 1;
    }

    /**
     * 主方法，执行入口
     * @param args 输入参数
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Q7NameDeptOfStartJ(), args);
        System.exit(res);
    }
}
```

**编译并打包代码**

进入 `/app/hadoop-1.1.2/myclass/class6` 目录中新建 `Q7NameDeptOfStartJ.java` 程序代码（代码也可以使用 `/home/shiyanlou/install-pack/class6/Q7NameDeptOfStartJ.java` 文件）。

```bash
cd /app/hadoop-1.1.2/myclass/class6
vi Q7NameDeptOfStartJ.java
```

编译代码：

```bash
javac -classpath ../../hadoop-core-1.1.2.jar:../../lib/commons-cli-1.2.jar Q7NameDeptOfStartJ.java
```

把编译好的代码打成 jar 包，如果不打成 jar 形式运行会提示 class 无法找到的错误。

```bash
jar cvf ./Q7NameDeptOfStartJ.jar ./Q7NameDept*.class
mv *.jar ../..
rm Q7NameDept*.class
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603392562.png)

**运行并查看结果**

运行 Q7NameDeptOfStartJ 时需要输入部门数据路径、员工数据路径和输出路径三个参数，需要注意的是 hdfs 的路径参数路径需要全路径，否则运行会报错：

- 部门数据路径：`hdfs://hadoop:9000/class6/input/dept`，部门数据将缓存在各运行任务的节点内容中，可以提高处理的效率。
- 员工数据路径：`hdfs://hadoop:9000/class6/input/emp`。
- 输出路径：`hdfs://hadoop:9000/class6/out7`。

运行如下命令：

```bash
cd /app/hadoop-1.1.2
hadoop jar Q7NameDeptOfStartJ.jar Q7NameDeptOfStartJ hdfs://hadoop:9000/class6/input/dept hdfs://hadoop:9000/class6/input/emp hdfs://hadoop:9000/class6/out7
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603494769.png)

运行成功后，刷新 CentOS HDFS 中的输出路径 `/class6/out7` 目录。

```bash
hadoop fs -ls /class6/out7
hadoop fs -cat /class6/out7/part-r-00000
```

打开 `part-r-00000` 文件，可以看到运行结果：

```text
JAMES    SALES
JONES    RESEARCH
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603507727.png)

**问题分析**

求工资最高的头三名员工姓名及工资，可以通过冒泡法得到。在 Mapper 阶段输出经理数据和员工对应经理表数据，其中经理数据 key 为 0 值、value 为"员工姓名，员工工资"。最后在 Reduce 中通过冒泡法遍历所有员工，比较员工工资多少，求出前三名。

**处理流程图**

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603530065.png)

**编写代码**

```java
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Q8SalaryTop3Salary extends Configured implements Tool {

    public static class MapClass extends Mapper<LongWritable, Text, IntWritable, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException,         InterruptedException {

            // 对员工文件字段进行拆分
            String[] kv = value.toString().split(",");

            // 输出key为0和value为员工姓名+","+员工工资
            context.write(new IntWritable(0), new Text(kv[1].trim() + "," + kv[5].trim()));
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, Text, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws         IOException, InterruptedException {

            // 定义工资前三员工姓名
            String empName;
            String firstEmpName = "";
            String secondEmpName = "";
            String thirdEmpName = "";

            // 定义工资前三工资
            long empSalary = 0;
            long firstEmpSalary = 0;
            long secondEmpSalary = 0;
            long thirdEmpSalary = 0;

            // 通过冒泡法遍历所有员工，比较员工工资多少，求出前三名
            for (Text val : values) {
                empName = val.toString().split(",")[0];
                empSalary = Long.parseLong(val.toString().split(",")[1]);

                if(empSalary > firstEmpSalary) {
                    thirdEmpName = secondEmpName;
                    thirdEmpSalary = secondEmpSalary;
                    secondEmpName = firstEmpName;
                    secondEmpSalary = firstEmpSalary;
                    firstEmpName = empName;
                    firstEmpSalary = empSalary;
                } else if (empSalary > secondEmpSalary) {
                    thirdEmpName = secondEmpName;
                    thirdEmpSalary = secondEmpSalary;
                    secondEmpName = empName;
                    secondEmpSalary = empSalary;
                } else if (empSalary > thirdEmpSalary) {
                    thirdEmpName = empName;
                    thirdEmpSalary = empSalary;
                }
            }

            // 输出工资前三名信息
            context.write(new Text( "First employee name:" + firstEmpName), new Text("Salary:"             + firstEmpSalary));
            context.write(new Text( "Second employee name:" + secondEmpName), new                     Text("Salary:" + secondEmpSalary));
            context.write(new Text( "Third employee name:" + thirdEmpName), new Text("Salary:"             + thirdEmpSalary));
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        // 实例化作业对象，设置作业名称
        Job job = new Job(getConf(), "Q8SalaryTop3Salary");
        job.setJobName("Q8SalaryTop3Salary");

        // 设置Mapper和Reduce类
        job.setJarByClass(Q8SalaryTop3Salary.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 设置输入格式类
        job.setInputFormatClass(TextInputFormat.class);

        // 设置输出格式类
        job.setOutputKeyClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputValueClass(Text.class);

        // 第1个参数为员工数据路径和第2个参数为输出路径
        String[] otherArgs = new GenericOptionsParser(job.getConfiguration(),                     args).getRemainingArgs();
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : 1;
    }

    /**
     * 主方法，执行入口
     * @param args 输入参数
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Q8SalaryTop3Salary(), args);
        System.exit(res);
    }
}
```

**编译并打包代码**

进入 `/app/hadoop-1.1.2/myclass/class6` 目录中新建 `Q8SalaryTop3Salary.java` 程序代码（代码也可以使用 `/home/shiyanlou/install-pack/class6/Q8SalaryTop3Salary.java` 文件）。

```bash
cd /app/hadoop-1.1.2/myclass/class6
vi Q8SalaryTop3Salary.java
```

编译代码：

```bash
javac -classpath ../../hadoop-core-1.1.2.jar:../../lib/commons-cli-1.2.jar Q8SalaryTop3Salary.java
```

把编译好的代码打成 jar 包，如果不打成 jar 形式运行会提示 class 无法找到的错误。

```bash
jar cvf ./Q8SalaryTop3Salary.jar ./Q8SalaryTop3*.class
mv *.jar ../..
rm Q8SalaryTop3*.class
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603542945.png)

**运行并查看结果**

运行 Q8SalaryTop3Salary 运行的员工数据路径和输出路径两个参数，需要注意的是 hdfs 的路径参数路径需要全路径，否则运行会报错：

- 员工数据路径：`hdfs://hadoop:9000/class6/input/emp`。
- 输出路径：`hdfs://hadoop:9000/class6/out8`。

运行如下命令：

```bash
cd /app/hadoop-1.1.2
hadoop jar Q8SalaryTop3Salary.jar Q8SalaryTop3Salary hdfs://hadoop:9000/class6/input/emp hdfs://hadoop:9000/class6/out8
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603552439.png)

运行成功后，刷新 CentOS HDFS 中的输出路径 `/class6/out8` 目录。

```bash
hadoop fs -ls /class6/out8
hadoop fs -cat /class6/out8/part-r-00000
```

打开 `part-r-00000` 文件，可以看到运行结果：

```text
First employee name:KING    Salary:5000
Second employee name:FORD    Salary:3000
Third employee name:JONES    Salary:2975
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603567071.png)

**问题分析**

求全体员工总收入降序排列，获得所有员工总收入并降序排列即可。在 Mapper 阶段输出所有员工总工资数据，其中 key 为员工总工资、value 为员工姓名，在 Mapper 阶段的最后会先调用 `job.setPartitionerClass` 对数据进行分区，每个分区映射到一个 reducer，每个分区内又调用 `job.setSortComparatorClass` 设置的 key 比较函数类排序。由于在本作业中 Map 的 key 只有 0 值，故能实现对所有数据进行排序。

**处理流程图**

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603621983.png)

**编写代码**

```java
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Q9EmpSalarySort extends Configured implements Tool {

    public static class MapClass extends Mapper<LongWritable, Text, IntWritable, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException,         InterruptedException {

            // 对员工文件字段进行拆分
            String[] kv = value.toString().split(",");

            // 输出key为员工所有工资和value为员工姓名
            int empAllSalary = "".equals(kv[6]) ? Integer.parseInt(kv[5]) :                             Integer.parseInt(kv[5]) + Integer.parseInt(kv[6]);
            context.write(new IntWritable(empAllSalary), new Text(kv[1]));
        }
    }

    /**
     * 递减排序算法
     */
    public static class DecreaseComparator extends IntWritable.Comparator {
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        // 实例化作业对象，设置作业名称
        Job job = new Job(getConf(), "Q9EmpSalarySort");
        job.setJobName("Q9EmpSalarySort");

        // 设置Mapper和Reduce类
        job.setJarByClass(Q9EmpSalarySort.class);
        job.setMapperClass(MapClass.class);

        // 设置输出格式类
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setSortComparatorClass(DecreaseComparator.class);

        // 第1个参数为员工数据路径和第2个参数为输出路径
        String[] otherArgs = new GenericOptionsParser(job.getConfiguration(),                     args).getRemainingArgs();
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : 1;
    }

    /**
     * 主方法，执行入口
     * @param args 输入参数
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Q9EmpSalarySort(), args);
        System.exit(res);
    }
}
```

**编译并打包代码**

进入 `/app/hadoop-1.1.2/myclass/class6` 目录中新建 `Q9EmpSalarySort.java` 程序代码（代码也可以使用 `/home/shiyanlou/install-pack/class6/Q9EmpSalarySort.java` 文件）。

```bash
cd /app/hadoop-1.1.2/myclass/class6
vi Q9EmpSalarySort.java
```

编译代码：

```bash
javac -classpath ../../hadoop-core-1.1.2.jar:../../lib/commons-cli-1.2.jar Q9EmpSalarySort.java
```

把编译好的代码打成 jar 包，如果不打成 jar 形式运行会提示 class 无法找到的错误。

```bash
jar cvf ./Q9EmpSalarySort.jar ./Q9EmpSalary*.class
mv *.jar ../..
rm Q9EmpSalary*.class
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603638840.png)

**运行并查看结果**

运行 Q9EmpSalarySort 运行的员工数据路径和输出路径两个参数，需要注意的是 hdfs 的路径参数路径需要全路径，否则运行会报错：

- 员工数据路径：`hdfs://hadoop:9000/class6/input/emp`
- 输出路径：`hdfs://hadoop:9000/class6/out9`

运行如下命令：

```bash
cd /app/hadoop-1.1.2
hadoop jar Q9EmpSalarySort.jar Q9EmpSalarySort hdfs://hadoop:9000/class6/input/emp hdfs://hadoop:9000/class6/out9
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603650353.png)

运行成功后，刷新 CentOS HDFS 中的输出路径 `/class6/out9` 目录。

```bash
hadoop fs -ls /class6/out9
hadoop fs -cat /class6/out9/part-r-00000
```

打开 `part-r-00000` 文件，可以看到运行结果：

```text
5000    KING
3000    FORD
2975    JONES
2850    BLAKE
......
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603659520.png)

该公司所有员工可以形成入下图的树形结构，求两个员工的沟通的中间节点数，可转换在员工树中求两个节点连通所经过的节点数，即从其中一节点到汇合节点经过节点数加上另一节点到汇合节点经过节点数。例如求 M 到 Q 所需节点数，可以先找出 M 到 A 经过的节点数，然后找出 Q 到 A 经过的节点数，两者相加得到 M 到 Q 所需节点数。

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603682756.png)

在作业中首先在 Mapper 阶段所有员工数据，其中经理数据 key 为 0 值、value 为"员工编号，员工经理编号"，然后在 Reduce 阶段把所有员工放到员工列表和员工对应经理链表 Map 中，最后在 Reduce 的 Cleanup 中按照上面说所算法对任意两个员工计算出沟通的路径长度并输出。

**处理流程图**

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603691352.png)

**编写代码**

```java
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Q10MiddlePersonsCountForComm extends Configured implements Tool {

    public static class MapClass extends Mapper<LongWritable, Text, IntWritable, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException,         InterruptedException {

            // 对员工文件字段进行拆分
            String[] kv = value.toString().split(",");

            // 输出key为0和value为员工编号+","+员工经理编号
            context.write(new IntWritable(0), new Text(kv[0] + "," + ("".equals(kv[3]) ? " " : kv[3])));
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, NullWritable, Text> {

        // 定义员工列表和员工对应经理Map
        List<String> employeeList = new ArrayList<String>();
        Map<String, String> employeeToManagerMap = new HashMap<String, String>();

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws         IOException, InterruptedException {

            // 在reduce阶段把所有员工放到员工列表和员工对应经理Map中
            for (Text value : values) {
                employeeList.add(value.toString().split(",")[0].trim());
                employeeToManagerMap.put(value.toString().split(",")[0].trim(),                             value.toString().split(",")[1].trim());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int totalEmployee = employeeList.size();
            int i, j;
            int distance;
            System.out.println(employeeList);
            System.out.println(employeeToManagerMap);

            // 对任意两个员工计算出沟通的路径长度并输出
            for (i = 0; i < (totalEmployee - 1); i++) {
                for (j = (i + 1); j < totalEmployee; j++) {
                    distance = calculateDistance(i, j);
                    String value = employeeList.get(i) + " and " + employeeList.get(j) + " =                     " + distance;
                    context.write(NullWritable.get(), new Text(value));
                }
            }
        }

        /**
         * 该公司可以由所有员工形成树形结构，求两个员工的沟通的中间节点数，可以转换在员工树中两员工之间的距离
         * 由于在树中任意两点都会在某上级节点汇合，根据该情况设计了如下算法
         */
        private int calculateDistance(int i, int j) {
            String employeeA = employeeList.get(i);
            String employeeB = employeeList.get(j);
            int distance = 0;

            // 如果A是B的经理，反之亦然
            if (employeeToManagerMap.get(employeeA).equals(employeeB) ||                                     employeeToManagerMap.get(employeeB).equals(employeeA)) {
                distance = 0;
            }
            // A和B在同一经理下
            else if  (employeeToManagerMap.get(employeeA).equals(
                    employeeToManagerMap.get(employeeB))) {
                distance = 0;
            } else {
                // 定义A和B对应经理链表
                List<String> employeeA_ManagerList = new ArrayList<String>();
                List<String> employeeB_ManagerList = new ArrayList<String>();

                // 获取从A开始经理链表
                employeeA_ManagerList.add(employeeA);
                String current = employeeA;
                while (false == employeeToManagerMap.get(current).isEmpty()) {
                    current = employeeToManagerMap.get(current);
                    employeeA_ManagerList.add(current);
                }

                // 获取从B开始经理链表
                employeeB_ManagerList.add(employeeB);
                current = employeeB;
                while (false == employeeToManagerMap.get(current).isEmpty()) {
                    current = employeeToManagerMap.get(current);
                    employeeB_ManagerList.add(current);
                }

                int ii = 0, jj = 0;
                String currentA_manager, currentB_manager;
                boolean found = false;

                // 遍历A与B开始经理链表，找出汇合点计算
                for (ii = 0; ii < employeeA_ManagerList.size(); ii++) {
                    currentA_manager = employeeA_ManagerList.get(ii);
                    for (jj = 0; jj < employeeB_ManagerList.size(); jj++) {
                        currentB_manager = employeeB_ManagerList.get(jj);
                        if (currentA_manager.equals(currentB_manager)) {
                            found = true;
                            break;
                        }
                    }

                    if (found) {
                        break;
                    }
                }

                // 最后获取两只之前的路径
                distance = ii + jj - 1;
            }

            return distance;
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        // 实例化作业对象，设置作业名称
        Job job = new Job(getConf(), "Q10MiddlePersonsCountForComm");
        job.setJobName("Q10MiddlePersonsCountForComm");

        // 设置Mapper和Reduce类
        job.setJarByClass(Q10MiddlePersonsCountForComm.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        // 设置Mapper输出格式类
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 设置Reduce输出键和值类型
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // 第1个参数为员工数据路径和第2个参数为输出路径
        String[] otherArgs = new GenericOptionsParser(job.getConfiguration(),                     args).getRemainingArgs();
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : 1;
    }

    /**
     * 主方法，执行入口
     * @param args 输入参数
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Q10MiddlePersonsCountForComm(), args);
        System.exit(res);
    }
}
```

**编译并打包代码**

进入 `/app/hadoop-1.1.2/myclass/class6` 目录中新建 `Q10MiddlePersonsCountForComm.java` 程序代码（代码也可以使用 `/home/shiyanlou/install-pack/class6/Q10MiddlePersonsCountForComm.java` 文件）。

```bash
cd /app/hadoop-1.1.2/myclass/class6
vi Q10MiddlePersonsCountForComm.java
```

编译代码

```bash
javac -classpath ../../hadoop-core-1.1.2.jar:../../lib/commons-cli-1.2.jar Q10MiddlePersonsCountForComm.java
```

把编译好的代码打成 jar 包，如果不打成 jar 形式运行会提示 class 无法找到的错误。

```bash
jar cvf ./Q10MiddlePersonsCountForComm.jar ./Q10MiddlePersons*.class
mv *.jar ../..
rm Q10MiddlePersons*.class
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603716501.png)

**运行并查看结果**

运行 Q10MiddlePersonsCountForComm 运行的员工数据路径和输出路径两个参数，需要注意的是 hdfs 的路径参数路径需要全路径，否则运行会报错：

- 员工数据路径：`hdfs://hadoop:9000/class6/input/emp`。
- 输出路径：`hdfs://hadoop:9000/class6/out10`。

运行如下命令：

```bash
cd /app/hadoop-1.1.2
hadoop jar Q10MiddlePersonsCountForComm.jar Q10MiddlePersonsCountForComm hdfs://hadoop:9000/class6/input/emp hdfs://hadoop:9000/class6/out10
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603729512.png)

运行成功后，刷新 CentOS HDFS 中的输出路径 `/class6/out10` 目录。

```bash
hadoop fs -ls /class6/out10
hadoop fs -cat /class6/out10/part-r-00000
```

打开 part-r-00000 文件，可以看到运行结果：

```text
7369 and 7499 = 4
7369 and 7521 = 4
7369 and 7566 = 1
7369 and 7654 = 4
7369 and 7698 = 3
......
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1034timestamp1433603739017.png)

Pig 是 yahoo 捐献给 apache 的一个项目，使用 SQL-like 语言，是在 MapReduce 上构建的一种高级查询语言，把一些运算编译进 MapReduce 模型的 Map 和 Reduce 中。

Pig 有两种运行模式： Local 模式和 MapReduce 模式

- `Local 模式`：Pig 运行于 Local 模式，只涉及到单独的一台计算机
- `MapReduce 模式`：Pig 运行于 MapReduce 模式，需要能访问一个 Hadoop 集群，并且需要装上 HDFS

Pig 的调用方式：

- `Grunt shell 方式`：通过交互的方式，输入命令执行任务。
- `Pig script 方式`：通过 script 脚本的方式来运行任务。
- `嵌入式方式`：嵌入 java 源代码中，通过 java 调用来运行任务。

搭建 Pig 环境



配置本机主机名为 hadoop，sudo 时需要输入 shiyanlou 用户的密码。将 hadoop 添加到最后一行的末尾。

```bash
sudo vim /etc/hosts
# 将hadoop添加到最后一行的末尾，修改后类似：（使用 tab 键添加空格）
# 172.17.2.98 f738b9456777 hadoop
ping hadoop
```

使用如下命令启动 Hadoop：

```bash
cd /app/hadoop-1.1.2/bin
./start-all.sh
jps # 查看启动的进程，确保 NameNode 和 DataNode 都有启动
```

#### 下载并解压安装包

在 Apache 下载最新的 Pig 软件包，点击下载会推荐最快的镜像站点，以下为下载地址：

```text
http://mirrors.aliyun.com/apache/pig/
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1035timestamp1433725154113.png)

也可以在 `/home/shiyanlou/install-pack` 目录中找到该安装包，解压该安装包并把该安装包复制到 `/app` 目录中。

```bash
cd /home/shiyanlou/install-pack
tar -xzf pig-0.13.0.tar.gz
mv pig-0.13.0 /app
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1035timestamp1433725167480.png)

#### 设置环境变量

使用如下命令编辑 `/etc/profile` 文件：

```bash
sudo vi /etc/profile
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1035timestamp1433725184291.png)

设置 pig 的 class 路径和在 path 加入 pig 的路径，其中 `PIG_CLASSPATH` 参数是设置 pig 在 MapReduce 工作模式：

```text
export PIG_HOME=/app/pig-0.13.0
export PIG_CLASSPATH=/app/hadoop-1.1.2/conf
export PATH=$PATH:$PIG_HOME/bin
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1035timestamp1433725195860.png)

编译配置文件 `/etc/profile`，并确认生效。

```bash
source /etc/profile
echo $PATH
```

#### 验证安装完成

重新登录终端，确保 hadoop 集群启动，键入 `pig` 命令，应该能看到 pig 连接到 hadoop 集群的信息并且进入了 grunt shell 命令行模式：

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1035timestamp1433725209548.png)





//加载HDFS中访问日志，使用空格进行分割，只加载ip列 records = LOAD 'hdfs://hadoop:9000/class7/input/website_log.txt' USING PigStorage(' ') AS (ip:chararray); // 按照ip进行分组，统计每个ip点击数 records_b = GROUP records BY ip; records_c = FOREACH records_b GENERATE group,COUNT(records) AS click; // 按照点击数排序，保留点击数前10个的ip数据 records_d = ORDER records_c by click DESC; top10 = LIMIT records_d 10; // 把生成的数据保存到HDFS的class7目录中 STORE top10 INTO 'hdfs://hadoop:9000/class7/out';



可以在 `/home/shiyanlou/install-pack/class7` 中找到本节使用的测试数据 `website_log.zip` 文件，使用 `unzip` 文件解压缩，然后调用 hadoop 上传本地文件命令把该文件传到 HDFS 中的 `/class7` 目录，如下图所示：

```bash
cd /home/shiyanlou/install-pack/class7
unzip website_log.zip
ll
hadoop fs -mkdir /class7/input
hadoop fs -copyFromLocal website_log.txt /class7/input
hadoop fs -cat /class7/input/website_log.txt | less
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1035timestamp1433725223134.png)

**输入代码**

进入 pig shell 命令行模式：

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1035timestamp1433725247049.png)

输入代码：（运行需要等待较长时间）

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1035timestamp1433725269288.png)

**运行过程**

注：实验楼为命令行界面，无法观测到该步骤界面，以下描述仅做参考。

在执行过程中在 JobTracker 页面观察运行情况，链接地址为：`http://**.***.**.***:50030/jobtracker.jsp`。

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1035timestamp1433725291992.png)

点击查看具体作业信息。

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1035timestamp1433725301943.png)

可以观察到本次任务分为 4 个作业，每个作业都是在上一次作业的结果上进行计算。

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1035timestamp1433725307993.png)

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1035timestamp1433725320891.png)

**运行结果**

通过以下命令查看最后的结果：

```bash
hadoop fs -ls /class7/out
hadoop fs -cat /class7/out/part-r-00000
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1035timestamp1433725331541.png)



Hive 是 Facebook 开发的构建于 Hadoop 集群之上的数据仓库应用，它提供了类似于 SQL 语法的 HQL 语句作为数据访问接口，这使得普通分析人员应用 Hadoop 的学习曲线变小，Hive 有如下特性：

- Hive 是基于 Hadoop 的一个数据仓库工具，可以将结构化的数据文件映射为一张数据库表，并使用 sql 语句转换为 MapReduce 任务进行运行。其优点是学习成本低，可以通过类 SQL 语句快速实现简单的 MapReduce 统计，不必开发专门的 MapReduce 应用，十分适合数据仓库的统计分析。
- Hive 是建立在 Hadoop 上的数据仓库基础构架。它提供了一系列的工具，可以用来进行数据提取转化加载（ETL），这是一种可以存储、查询和分析存储在 Hadoop 中的大规模数据的机制。Hive 定义了简单的类 SQL 查询语言，称为 HQL，它允许熟悉 SQL 的用户查询数据。同时，这个语言也允许熟悉 MapReduce 的开发者来开发自定义的 Mapper 和 Reducer 处理内建的 Mapper 和 Reducer 无法完成的复杂分析工作。

使用 Hive 的命令行接口很像操作关系数据库，但是 Hive 和关系数据库还是有很大的不同， Hive 与关系数据库的区别具体如下：

1. Hive 和关系数据库存储文件的系统不同，Hive 使用的是 Hadoop 的 HDFS（Hadoop 的分布式文件系统），关系数据库则是服务器本地的文件系统。
2. Hive 使用的计算模型是 MapReduce，而关系数据库则是自身的计算模型。
3. 关系数据库都是为实时查询的业务进行设计的，而 Hive 则是为海量数据做数据挖掘设计的，实时性很差；实时性的区别导致 Hive 的应用场景和关系数据库有很大的不同。
4. Hive 很容易扩展自己的存储能力和计算能力，这个是继承 Hadoop 的，而关系数据库在这个方面相比起来要差很多。

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1042timestamp1433943328371.png)

由上图可知，Hadoop 的 MapReduce 是 Hive 架构的根基。Hive 架构包括如下组件：CLI（command line interface）、JDBC/ODBC、Thrift Server、WEB GUI、Metastore 和 Driver(Complier、Optimizer 和 Executor)，这些组件分为两大类：服务端组件和客户端组件。

**服务端组件：**

- `Driver 组件`：该组件包括 Complier、Optimizer 和 Executor，它的作用是将 HiveQL（类 SQL）语句进行解析、编译优化，生成执行计划，然后调用底层的 MapReduce 计算框架。
- `Metastore 组件`：元数据服务组件，这个组件存储 Hive 的元数据，Hive 的元数据存储在关系数据库里，Hive 支持的关系数据库有 derby 和 mysql。元数据对于 Hive 十分重要，因此 Hive 支持把 metastore 服务独立出来，安装到远程的服务器集群里，从而解耦 Hive 服务和 metastore 服务，保证 Hive 运行的健壮性。
- `Thrift 服务`：Thrift 是 facebook 开发的一个软件框架，它用来进行可扩展且跨语言的服务的开发，Hive 集成了该服务，能让不同的编程语言调用 hive 的接口。

**客户端组件：**

- `CLI`：command line interface，命令行接口。
- `Thrift 客户端`：上面的架构图里没有写上 Thrift 客户端，但是 Hive 架构的许多客户端接口是建立在 Thrift 客户端之上，包括 JDBC 和 ODBC 接口。
- `WEBGUI`：Hive 客户端提供了一种通过网页的方式访问 hive 所提供的服务。这个接口对应 Hive 的 hwi 组件（hive web interface），使用前要启动 hwi 服务。

mysql 设置 hive账户

**设置 Hive 用户**

进入 mysql 命令行，创建 hive 用户并赋予所有权限：（创建前请确认是否已有 hive 用户，若有则不需要创建）

```sql
mysql -uroot -proot
mysql> set password=password('root');

# 创建 hive 用户，若已经存在则无需再创建
mysql> create user 'hive' identified by 'hive';

# 赋予权限
mysql> grant all on *.* TO 'hive'@'%' identified by 'hive' with grant option;
mysql> grant all on *.* TO 'hive'@'localhost' identified by 'hive' with grant option;

# 刷新 MySQL 的系统权限相关表，否则会出现拒绝访问，还有一种方法是重新启动 mysql 服务器，来使新设置生效。
mysql> flush privileges;
```

（注意：如果是 root 第一次登录数据库，需要重新设置一下密码，所报异常信息如下：ERROR 1820 (HY000): You must SET PASSWORD before executing this statement）

**创建 hive 数据库**

使用 hive 用户登录，创建 hive 数据库：

```sql
mysql -uhive -phive -h hadoop
mysql> create database hive; # 若存在则无需再创建
mysql> show databases;
```

配置本机主机名为 hadoop，sudo 时需要输入 shiyanlou 用户的密码。将 hadoop 添加到最后一行的末尾。

```bash
sudo vim /etc/hosts
# 将hadoop添加到最后一行的末尾，修改后类似：（使用 tab 键添加空格）
# 172.17.2.98 f738b9456777 hadoop
ping hadoop
```

使用如下命令启动 Hadoop

```bash
cd /app/hadoop-1.1.2/bin
./start-all.sh
jps # 查看启动的进程，确保 NameNode 和 DataNode 都有启动
```

**解压并移动 Hive 安装包**

可以到 Apache 基金 Hive 官网 `http://hive.apache.org/downloads.html`，选择镜像下载地址：`http://mirrors.cnnic.cn/apache/hive/` 下载一个稳定版本，如下图所示：

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid276733labid1042timestamp1519716491988.png)

也可以在 `/home/shiyanlou/install-pack` 目录中找到该安装包，解压该安装包并把该安装包复制到 `/app` 目录中（注：实验环境中的 hive 版本没有官网更新的高）。

```bash
cd /home/shiyanlou/install-pack
tar -xzf hive-0.12.0-bin.tar.gz
mv hive-0.12.0-bin /app/hive-0.12.0
```

**解压并移动 MySql 驱动包**

到 mysql 官网进入下载页面：http://dev.mysql.com/downloads/connector/j/，选择所需要的版本进行下载，这里下载的 zip 格式的文件。

也可以在 `/home/shiyanlou/install-pack` 目录中找到该安装包，解压该安装包并把该安装包复制到 `/app/lib` 目录中

```bash
cd /home/shiyanlou/install-pack
cp mysql-connector-java-5.1.22-bin.jar /app/hive-0.12.0/lib
```

**配置 /etc/profile 环境变量**

使用如下命令打开 `/etc/profile` 文件：

```bash
sudo vi /etc/profile
```

设置如下参数：

```text
export HIVE_HOME=/app/hive-0.12.0
export PATH=$PATH:$HIVE_HOME/bin
export CLASSPATH=$CLASSPATH:$HIVE_HOME/bin
```

使配置文件生效：

```bash
source /etc/profile
echo $PATH
```

**设置 hive-env.sh 配置文件**

进入 `hive-0.12.0/conf` 目录，复制 `hive-env.sh.templaete` 为 `hive-env.sh`：

```bash
cd /app/hive-0.12.0/conf
cp hive-env.sh.template hive-env.sh
sudo vi hive-env.sh
```

分别设置 `HADOOP_HOME` 和 `HIVE_CONF_DIR` 两个值：

```text
export HADOOP_HOME=/app/hadoop-1.1.2
export HIVE_CONF_DIR=/app/hive-0.12.0/conf
```

**设置 hive-site.xml 配置文件**

复制 `hive-default.xml.templaete` 为 `hive-site.xml`：

```bash
cd /app/hive-0.12.0/conf
cp hive-default.xml.template hive-site.xml
sudo vi hive-site.xml
```

**（1）加入配置项**

默认 metastore 在本地，添加配置改为非本地。

```text
<property>
  <name>hive.metastore.local</name>
  <value>false</value>
</property>
```

**（2）修改配置项**

hive 默认为 derby 数据库，需要把相关信息调整为 mysql 数据库。

```text
 <property>
   <name>hive.metastore.uris</name>
   <value>thrift://hadoop:9083</value>
   <description>Thrift URI for the remote metastore. ...</description>
 </property>
 <property>
   <name>javax.jdo.option.ConnectionURL</name>
   <value>jdbc:mysql://hadoop:3306/hive?=createDatabaseIfNotExist=true</value>
   <description>JDBC connect string for a JDBC metastore</description>
 </property>
 <property>
   <name>javax.jdo.option.ConnectionDriverName</name>
   <value>com.mysql.jdbc.Driver</value>
   <description>Driver class name for a JDBC metastore</description>
 </property>
 <property>
   <name>javax.jdo.option.ConnectionUserName</name>
   <value>hive</value>
   <description>username to use against metastore database</description>
 </property>
 <property>
   <name>javax.jdo.option.ConnectionPassword</name>
   <value>hive</value>
   <description>password to use against metastore database</description>
 </property>
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1042timestamp1433944084172.png)

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1042timestamp1433944092960.png)

**（3）订正错误项**

把 `hive.metastore.schema.verification` 配置项值修改为 false。

```text
 <property>
   <name>hive.metastore.schema.verification</name>
   <value>false</value>
    <desc....>
 </property>
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1042timestamp1433944116338.png)

把 `/app/hive-0.12.0/conf/hive-site.xml` 文件中的大概在 2000 行位置左右，把原来的 `<value>auth</auth>` 修改为 `<value>auth</value>`，如下所示：

```text
 <property>
    <name>hive.server2.thrift.sasl.qop</name>
    <value>auth</value>
    ......
  </property>
```

还需要在环境中使用命令 `sudo service mysql start` 启动 mysql。

**启动 metastore 和 hiveserver**

启动 MySQL 服务：

```bash
sudo service mysql start
```

在使用 hive 之前需要启动 metastore 和 hiveserver 服务，通过如下命令启用：

```bash
hive --service metastore &
hive --service hiveserver &
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1042timestamp1433944130232.png)

通过 `jps` 命令可以看到两个进程运行在后台。

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1042timestamp1433944136265.png)

**在 hive 中操作**

登录 hive，在 hive 创建表并查看该表，命令如下：

```sql
hive
hive> create table test(a string, b int);
hive> show tables;
hive> desc test;
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1042timestamp1433944147986.png)

登录 mysql，在 TBLS 表中查看新增 test 表：

```sql
mysql -uhive -phive
mysql> use hive;
mysql> select TBL_ID, CREATE_TIME, DB_ID, OWNER, TBL_NAME,TBL_TYPE from TBLS;
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1042timestamp1433944157594.png)

**（1） 查找以前是否安装有 mysql**

使用命令查看是否已经安装过 mysql：

```bash
sudo rpm -qa | grep -i mysql
```

可以看到如下图的所示：

说明之前安装了：

```text
MySQL-client-5.6.21-1.el6.x86_64
MySQL-server-5.6.21-1.el6.x86_64
MySQL-devel-5.6.21-1.el6.x86_64
```

如果没有结果，可以进行跳到之前的 mysql 数据库安装步骤进行安装。

**（2） 停止 mysql 服务、删除之前安装的 mysql**

停止 mysql 服务、删除之前安装的 mysql 删除命令：rpm -ev –nodeps 包名

```bash
sudo rpm -ev MySQL-server-5.6.21-1.el6.x86_64
sudo rpm -ev MySQL-devel-5.6.21-1.el6.x86_64
sudo rpm -ev MySQL-client-5.6.21-1.el6.x86_64
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1042timestamp1433944181399.png)

如果存在 CentOS 自带 `mysql-libs-5.6.21-1.el6.x86_64` 使用下面的命令卸载即可。

```bash
sudo rpm -ev --nodeps mysql-libs-5.6.21-1.el6.x86_64
```

**（3） 查找之前老版本 mysql 的目录并且删除老版本 mysql 的文件和库**

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1042timestamp1433944197444.png)

```bash
sudo find / -name mysql
```

删除对应的 mysql 目录。

```bash
sudo rm -rf /usr/lib64/mysql
sudo rm -rf /var/lib/mysql
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1042timestamp1433944218976.png)

**（4） 再次查找机器是否安装 mysql**

```bash
sudo rpm -qa | grep -i mysql
```

无结果，说明已经卸载彻底、接下来直接安装 mysql 即可

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1042timestamp1433944236587.png)

启动 hive 时，出现 `CommandNeedRetryException` 异常，具体信息如下：

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1042timestamp1433944253543.png)

```text
Exception in thread "main" java.lang.NoClassDefFoundError:org/apache/hadoop/hive/ql/CommandNeedRetryException
        at java.lang.Class.forName0(Native Method)
        at java.lang.Class.forName(Class.java:270)
        at org.apache.hadoop.util.RunJar.main(RunJar.java:149)
Caused by: java.lang.ClassNotFoundException: org.apache.hadoop.hive.ql.CommandNeedRetryException
        at java.net.URLClassLoader$1.run(URLClassLoader.java:366)
        at java.net.URLClassLoader$1.run(URLClassLoader.java:355)
        at java.security.AccessController.doPrivileged(Native Method)
        at java.net.URLClassLoader.findClass(URLClassLoader.java:354)
        at java.lang.ClassLoader.loadClass(ClassLoader.java:425)
        at java.lang.ClassLoader.loadClass(ClassLoader.java:358)
```

由于以前使用 hadoop 时，修改 hadoop-env.sh 的 HADOOP_CLASSPATH 配置项，由以前的：

```bash
export HADOOP_CLASSPATH=/app/hadoop-1.1.2/myclass
```

修改为：

```bash
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/app/hadoop-1.1.2/myclass
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1042timestamp1433944272197.png)

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1042timestamp1433944279419.png)

启动 hive 后，使用 Hql 出现异常，需要启动 metastore 和 hiveserver。

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1042timestamp1433944379068.png)

```text
FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. java.lang.RuntimeException: Unable to instantiate org.apache.hadoop.hive.metastore.HiveMetaStoreClient
```

在使用 hive 之前需要启动 metastore 和 hiveserver 服务，通过如下命令启用：

```bash
hive --service metastore &
hive --service hiveserver &
```

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1042timestamp1433944407865.png)

启动用通过 jps 命令可以看到两个进程运行在后台：

![此处输入图片的描述](https://doc.shiyanlou.com/document-uid29778labid1042timestamp1433944504114.png)