流处理并不是一个新概念，但是要做好并不是一件容易的事情。提到流处理，我们最先想到的可能是金融交易、信号检测以及地图导航等领域的应用。但是近年来随着信息技术的发展，除了前面提到的三个领域，其它方向对数据时效性的要求也越来越高。随着 Hadoop 生态的崛起，Storm、Spark Streaming、Samza、MillWheel 等一众流处理技术开始走入大众视野，但是我们最熟悉的应该还是 Storm 和 Spark Steaming。

我们知道，“高吞吐”、“低延迟”和”exactly-once“是衡量一个流处理框架的重要指标。 Storm 虽然提供了低延迟的流处理，但是在高吞吐方面的表现并不算佳，可以说基本满足不了日益暴涨的数据量，而且也没办法保证精准一次消费。Spark Streaming 中通过微批次的批处理来模拟流处理，只要将批处理的批次分的足够小，那么从宏观上来看就是流处理，这也是 Spark Steaming 的核心思想。通过微观批处理的方式，Spark Streaming 也实现了高吞吐和 exactly-once 语义，时效性也有了大幅提升，在很长一段时间里占据流处理榜首。但是受限于其实现方式，依然存在几秒的延迟，对于那些实时性要求较高的领域来说依然不够完美。 在这样的背景下，Flink 应运而生，接下来我们正式进入 Flink 的学习。



Apache Flink 是为分布式、高性能、随时可用以及准确 的流处理应用程序打造的开源流处理框架，用于对无界和有界数据流进行有状态计算。Flink 最早起源于在 2010 ~ 2014 年，由 3 所地处柏林的大学和欧洲的一些其它大学共同进行研究的名为 Stratosphere 的项目。2014 年 4 月 Stratosphere 将其捐赠给 Apache 软件基 金会， 初始成员是 Stratosphere 系统的核心开发人员，2014 年 12 月，Flink 一跃成为 Apache 软件基金会的顶级项目。在 2015 年，阿里也加入到了 Flink 的开发工作中，并贡献了至少 150 万行代码。

Flink 一词在德语中有着“灵巧”、“快速”的意思，它的 logo 原型也是柏林常见的一种松鼠，以身材娇小、灵活著称，为该项目取这样的名字和选定这样的 logo 也正好符合 Flink 的特点和愿景。

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/392e36ba98e8e3cf157bfb3dfce1d503-0)

注意，虽然我们说 Flink 是一个流处理框架，但是它同样可以进行批处理。因为在 Flink 的世界观里，批处理是流处理的一种特殊形式，这和 Spark 不同，在 Spark 中，流处理是通过大批量的微批处理实现的。



接下来我们运行第一个 Flink 程序，感受一下它的魅力，从而对它有一个初步的印象。

#### 搭建开发环境

**因为课程使用的是云主机环境，无法保存，所以请确保有充足的时间实验，或者自行保存代码和重建环境。后续实验不再提示。**

首先我们需要在环境中搭建 Flink 运行环境，总共可以分为下面这几步：

- 安装 jdk 并配置环境变量
- 安装 scala 并配置环境变量
- 安装 maven 并修改中心仓库为阿里云地址
- 安装 IDEA 开发工具

我们的实验环境已经为大家安装了 jdk、scala、maven 和 IDEA，只需要在 IDEA 里配置使用即可。

双击桌面的 IDEA 程序，启动之后，点击 File -> New -> Project 创建一个新的 Maven 工程 `FlinkLearning`：

![图片描述](https://doc.shiyanlou.com/courses/uid871732-20210324-1616571242275)

![图片描述](https://doc.shiyanlou.com/courses/uid871732-20210324-1616571265983)

创建好之后点击左上角 `File > Settings` 中，将 Maven 的配置文件修改为 `/usr/share/maven/conf/settings.xml`，配置之后的 Maven 中心仓库为阿里云，加载依赖会快很多：

![图片描述](https://doc.shiyanlou.com/courses/uid871732-20210324-1616571324271)

接下来在工程的总目录 `FlinkLearning` 右键点击 `Add Framework Support`，为工程勾选添加 Scala 支持：

![图片描述](https://doc.shiyanlou.com/courses/uid871732-20210324-1616571556473)

在工程 `src/main` 目录中创建 scala 文件夹，然后右键，选择 Mark Directory as，并将其标记为 Sources Root。

![图片描述](https://doc.shiyanlou.com/courses/uid871732-20210323-1616465508790)

在 scala 目录里创建 `com.shiyanlou.wc` 包，并分别创建 `BatchWordCount` 和 `StreamWordCount` 两个 Scala Object，分别代表 Flink 批处理和 Flink 流处理。

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/4fad871ff9292c40d1acfae6309058c5-0)

至此，我们的准备工作已经完成，接下来正式进入编码阶段。

#### Flink 批处理 WordCount

修改 pom.xml 文件中的代码为如下：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.shiyanlou</groupId>
    <artifactId>FlinkLearning</artifactId>
    <version>1.0-SNAPSHOT</version>

    <dependencies>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-scala_2.11</artifactId>
            <version>1.7.2</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-scala_2.11</artifactId>
            <version>1.7.2</version>
        </dependency>
    </dependencies>
    <build>
        <plugins>
          <!-- 该插件用于将 Scala 代码编译成 class 文件 -->
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <version>3.4.6</version>
                <executions>
                    <execution>
                        <goals>
                            <goal>testCompile</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-assembly-plugin</artifactId>
                <version>3.0.0</version>
                <configuration>
                    <archive>
                        <manifest>
                            <mainClass></mainClass>
                        </manifest>
                    </archive>
                    <descriptorRefs>
                        <descriptorRef>jar-with-dependencies</descriptorRef>
                    </descriptorRefs>
                </configuration>
                <executions>
                    <execution>
                        <id>make-assembly</id>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

修改完成后注意点击加载图标重新 Load。

![图片描述](https://doc.shiyanlou.com/courses/uid871732-20210323-1616466923004)

`BatchWordCount.scala` 中的代码如下：

```scala
package com.shiyanlou.wc

import org.apache.flink.api.scala._

object BatchWordCount {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 从文本读取数据
    val inputPath = "/home/shiyanlou/words.txt"
    val inputDS: DataSet[String] = env.readTextFile(inputPath)
    // 计算逻辑
    val wordCountDS: AggregateDataSet[(String, Int)] = inputDS
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    // 打印输出
    wordCountDS.print()
  }
}
```

在 `/home/shiyanlou/` 路径下创建 `words.txt` 文件并在其中加入如下内容（注意每个单词之间使用空格分隔）：

```txt
hello world
hello flink
hello spark
hello java
```

右键选中 `BatchWordCount.scala`，点击 Run 运行，将会看到如下输出：

> 如果出现一些其他的报错和警告可以忽略。

```txt
(java,1)
(world,1)
(flink,1)
(hello,4)
(spark,1)
```

#### Flink 流处理 WordCount

在 `StreamWordCount.scala` 中加入如下代码：

```scala
package com.shiyanlou.wc

import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //  监控Socket数据
    val textDstream: DataStream[String] = env.socketTextStream("localhost", 9999)
    // 导入隐式转换
    import org.apache.flink.api.scala._
    // 计算逻辑
    val dataStream: DataStream[(String, Int)] = textDstream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    // 设置并行度
    dataStream.print().setParallelism(1)

    // 执行
    env.execute("Socket stream word count")
  }
}
```

打开终端，并输入 `nc -l -p 9999`，然后输入以下内容：

```txt
hello world
hello flink
hello spark
hello java
```

运行 `StreamWordCount.scala` 将会看到如下输出：

```txt
(hello,1)
(flink,1)
(hello,2)
(spark,1)
(java,1)
(hello,3)
...
```

至此，我们的第一个 Flink 实验就已经完成了。



下一步

首先打开终端 ，在`/home/shiyanlou/` 下执行命令获取安装包：

```bash
wget https://labfile.oss.aliyuncs.com/courses/3423/flink-1.10.0-bin-scala_2.11.tar
tar -xvf flink-1.10.0-bin-scala_2.11.tar
```

将其解压到该目录下。

#### Standalone

这里的 Standalone 和 Spark 中的 Standalone 的含义是一样的，就是 Flink 自己来负责资源调度，不依赖于其它工具。

搭建步骤如下：

1. 将解压好的安装包复制到 `/opt/`目录下

   ```bash
   sudo cp -r ~/flink-1.10.0 /opt/
   ```

2. 进入到 `/opt/flink-1.10.0`目录下，启动集群

   ```bash
   sudo cd /opt/flink-1.10.0
   sudo bin/start-cluster.sh
   ```

至此，我们已经搭建了一个最简单的 Standalone 模式的集群，也就是伪分布式或者说是单节点的集群。双击桌面的 Firefox 浏览器，打开 `localhost:8081` 就会看到下面的界面：

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/616f880ec30595700e99bc554c73ed3f-0)

受限于我们的云主机环境，在实验课程中只能搭建伪分布式的集群。如果需要搭建多台机器组成的 Standalone 模式集群也并不难，只需要在以上步骤的基础上做简单配置即可。

假设要有三台机器，主机名分别为 bigdata1、bigdata2、bigdata3，接下来只需要：

1. 在 bigdata1 的 Flink 安装包中的 `conf/flink-conf.yaml` 文件中添加以下内容：

   ```bash
   jobmanager.rpc.address:bigdata1
   ```

2. 在 bigdata1 中的 Flink 安装包中的 `conf/slaves` 文件中添加以下内容：

   ```bash
   bigdata2
   bigdata3
   ```

3. 将 bigdata1 中的 Flink 安装包分别拷贝到 bigdata2 和 bigdata3 的相同目录下，然后在 bigdata1 中执行 `bin/start-cluster.sh` 启动集群。

注意：以上三个步骤的前提是三台机器上都安装了相同的版本的 jdk 和 scala，并且配置了 ssh 免密登录。

#### Yarn 模式

和 Spark 类似，Flink 同样支持以 Yarn 模式部署 Flink 任务，但是 Hadoop 的版本必须在 2.2 以上。

Yarn 模式的部署分为两种，分别是 Session-Cluster 和 Pre-Job-Cluster。

1. Session-Cluster

   Session-Cluster 的特点是在提交任务之前，需要先去 Yarn 启动一个 Flink 集群，启动成功之后通过 Flink run 命令往 Flink 集群中提交任务。当 Job 执行完之后，集群并不会关闭，等待下个 Job 提交。这样做的好处是 Job 启动的时间变短，适合规模小执行时间短的作业。

2. Pre-Job-Cluster

   Pre-Job-Cluster 的特点是一个 Job 对应一个集群，每提交一个 Job，会根据其自身情况单独向 Yarn 申请资源。这样做的好处是各个 Job 使用的资源之间独立，一个作业失败并不会影响到其它作业的正常运行，适合规模大运行时间长的任务。

#### Kubernetes 模式

随着信息化技术的发展，容器化技术也越来越流行。你可能没有听过 Kubernetes，但是你一定听过 Docker。是的，Kubernetes 和 Docker 类似，也是容器化技术的一种。虽然 Flink 支持基于 Kubernetes 部署，但是在业界好像并没有听到过有哪家企业在使用这种部署模式，所以大家只需要了解即可，不用做过多研究。



接下来我们将基于在第一节实验中的批计算代码来学习在 Flink 中的两种任务提交方式。

在 IDEA 中点击 Maven 中的 `package` 按钮打包，然后将会得到两个 jar 包，一个是没有依赖的，一个是带依赖的。

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/89b11e3a36f7086a48ecb0813acf4a8f-0)

#### 界面化提交

在打开的 Flink Web 界面中依次点击 `Submit New Job` -> `Add New`，在弹出的窗口中找到项目路径的 target 目录里刚才打好的 jar 包，选择带依赖的点击上传。

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/84e47c9004e9564bce9f890263ff355b-0)

点击上传的 jar 包名称，在弹出的几个输入框中分别输入全类名 `com.shiyanlou.wc.BatchWordCount` 和并行度 `2`，点击 `Submit` 提交。

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/7a426e5b2cf4aff4a03fb17320b9dab4-0)

#### 命令提交

在 Flink 安装目录下执行以下命令：

```bash
cd /opt/flink-1.10.0
# 注意替换成你实际的 Jar 包的地址
bin/flink run -m localhost:8081 -p 2 -c com.shiyanlou.wc.BatchWordCount /home/shiyanlou/IdeaProjects/FlinkLearning/target/FlinkLearning-1.0-SNAPSHOT-jar-with-dependicies.jar
```

其中 `-m` 指定提交的主节点地址，`-p` 指定任务并行度，`-c`指定要执行任务入口程序的全类名，最后是 jar 包的路径。

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/05ee9ab5be35398305c2076dded72599-0)

不仅仅是 Flink，其实不论是流处理还是批处理框架，总的来说都有以下这么⼏个流程：获取数据、对数据进行 ETL 转换，输出转换之后的结果。如果再细分的话，结合我们在第一个实验中的 WordCount 代码，可以将 Flink 流处理分为以下几个过程：

- 设置执行环境（Environment）
- 获取输入流（Source）
- 转换操作（Transform）
- 输出结果（Sink）
- 执行（Execute）

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/f7bb2e7811b7edf757ac1b74d6aebb20-0)

设置输入环境和执行是比较简单的，重点是 Source、Transform 和 Sink。在 Source 阶段，我们可以从内存对象中创建，也可以从文件以及 Kafka 等外部数据源中获取，还可以根据 Flink 提供的 API 自定义 Source。在 Sink 阶段，我们可以将计算的结果输出到控制台、文件、数据库或者下游的 Kafka，也可以根据 Flink 提供的 API 自定义 Sink。我们本节以及接下来的两个实验介绍的内容都是属于 Transform 阶段的，关于 Source 和 Sink 我们会在后面专门的实验中进行介绍。



Flink 中的三个基础算子 map、filter、flatMap 是必须要掌握的，在企业开发中的使用频率相当高。当然如果之前有学习过 Scala 或者 Spark 的话，你会发现⾮常简单。

在我们 FlinkLearning 工程里创建 `com.shiyanlou.operator` 包，然后创建一个名为 `BaseOperator` 的 Scala object，本节实验的示例代码都在此文件中执行。

![图片描述](https://doc.shiyanlou.com/courses/uid871732-20210323-1616481383676)

#### map

map 算子的作用是将原数据集中的每个元素通过传入的转换函数 F 映射为一个新元素，返回由所有新元素组成的集合。

示例如下：

```scala
package com.shiyanlou.operator

import org.apache.flink.api.scala._

object BaseOperator {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 从集合中创建数据集
    val data = env.fromCollection(List(1, 2, 3, 4, 5))
    // 对 data 中的每个元素乘以 10
    val data2 = data.map(x => x * 10)

    // 打印
    data2.print()
  }
}
```

上面的代码中，我们创建了一个包含 1、2、3、4、5 的一个数据集 data，然后通过 map 算子对 data 中的每个元素乘以 10，最后打印输出的结果为：

```bash
10
20
30
40
50
```

#### filter

filter 算子顾名思义就是过滤，将原数据集中的每个元素，经过传入的函数 F 计算之后返回一个布尔值，过滤出计算结果全部为 True 的元素。

示例如下：

```scala
package com.shiyanlou.operator

import org.apache.flink.api.scala._

object BaseOperator {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val data = env.fromCollection(List(1, 2, 3, 4, 5))
    // 过滤出偶数元素
    val data2 = data.filter(x => x % 2 == 0)

    data2.print()
  }
}
```

在上面的代码中，我们创建了一个包含 1、2、3、4、5 的一个数据集 data，然后过滤出 data 中的偶数元素，最后打印的结果为：

```bash
2
4
```

#### flatMap

flatMap 算子和 Scala 语言中的 flatMap 函数类似，可以理解为拉平操作。将深度大于 1 的嵌套序列压缩到深度为 1。

示例如下：

```scala
package com.shiyanlou.operator

import org.apache.flink.api.scala._

object BaseOperator {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val data = env.fromCollection(List("a b", "c d", "e f"))
    val data2 = data.flatMap(x => x.split(" "))

    data2.print()
  }
}
```

在上面的代码中，我们创建了一个 data 对象，其中的每个元素都是一个字符串，在 flatMap 算子中，我们对每个元素使用空格进行了分隔，最后打印输出。data 本身就是一个 List，使用空格针对里面的每个元素分隔后，每个元素会返回一个数组，也就是说最后返回的结构应该是 `List(Array('a', 'b'), Array('c', 'd'), Array('e', 'f'))`，很明显是一个深度为 2 的序列，但是经过 flatMap 之后，输出的是一个如下所示的深度为 1 的序列：

```bash
a
b
c
d
e
f
```

最后，附上本次实验代码的工程结构：

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/e77f231bdf5f0b66dfa99ba873bb9e68-0)

在我们 FlinkLearning 工程的 `com.shiyanlou.operator` 包下创建一个名为 `KeyByOperator` 的 Scala object，本节实验的示例代码都在此文件中执行。

紧接着我们拟造一下本节实验需要使用的数据。在 `/home/shiyanlou/` 路径下执行 `touch userlog.txt` 命令创建一个名为 `userlog.txt` 的文件，使用 vim 编辑该文件，添加以下内容：

```bash
Jack Beijing 100
Bob Chengdu 300
William Chengdu 600
Lily Shanghai 200
Loius Beijing 400
Joker Shanghai 200
```

我们用以上文本中的每一行表示一条用户访问日志，每行中的三个字段分别表示访问用户名、访问者城市以及访问时长。例如第一行所表示的是 Jack 在北京访问某服务器 100 秒。如果你不嫌麻烦可以多添加一些数据。

#### KeyBy

KeyBy 算子的作用是将一个 DataStream 转换成 KeyedStream，输⼊必须是 Tuple 类型，逻辑地将⼀个流拆分成不相交的分区， 每个分区包含具有相同 key 的元素，在其内部以 hash 的形式实现的。

但是 `keyBy` 算子一般不会单独使用，会和我们后面介绍的 `Rolling Aggregation` 算子和 `reduce` 算子集合使用。

#### Rolling Aggregation

Rolling Aggregation 算子又被称作滚动聚合算子，是在经过 keyBy 之后的 KeyedStream 做聚合操作。sum、min、max 分别表示求和、求最小值和最大值。minBy 和 maxBy 稍微有点特殊，我们接下来用代码来解释。

```scala
package com.shiyanlou.operator

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object KeyByOperator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data: DataStream[String] = env.readTextFile("/home/shiyanlou/userlog.txt")
    val userLogStream = data.map(log => {
      val arr = log.split(" ")

      // 将读取的数据集转为 UserLog
      UserLog(arr(0), arr(1), arr(2).toInt)
    })

    userLogStream
      .keyBy("city")
      .sum("duration")
      .print()

    env.execute("KeyByOperator")
  }

  case class UserLog(name: String, city: String, duration: Int)
}
```

在上面的代码中，我们创建了一个 `UserLog` 样例类，然后读取我们前面准备的数据集，使用 map 函数将读取的日志转为 UserLog 对象，并使用 `keyBy` 算子按照 `city` 分组，最后对 `duration` 求和并打印结果。输出如下：

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/3b494774ee84253a3925fbea0243b7ab-0)

你可能会疑惑，输出的不应该是有两列，city 和 sum(duration)吗？请注意，我们这里的计算是流处理，而不是离线的批处理，我们创建的环境是 `StreamExecutionEnvironment`。程序从上往下一行一行读取文本，然后按照 `city` 字段分组，当执行到第一行的时候，只有它自己，所以输出自己本身。当执行到第二行的时候，`city`为 `Chengdu` 的也只有一行，所以也输出了它自己。当程序执行到第三行的时候，第二个 `Chengdu` 出现了，所以 sum 的结果是 900（300 + 600）。当程序执行到第四行的时候，`Shanghai` 第一次出现，所以也只有它自己。当程序执行到第 5 行的时候，第二个 `Beijing` 出现了，所以输出的是 500（100 + 400）。当程序执行到第 6 行，第二个 `Shanghai` 出现了，所以输出的是 400（200 + 200）。

min 和 max 的执行原理和 sum 类似，同样是按照从上往下的原则依次在同一个分组寻 找截止目前的最小值或者最大值，如果当前数据是该分组的第一条，则输出自己本身。接下来我们看一下 max 和 maxBy 的区别，其中 min 和 minBy 的区别也是类似。请看代码：

```scala
package com.shiyanlou.operator

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object KeyByOperator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data: DataStream[String] = env.readTextFile("/home/shiyanlou/userlog.txt")
    val userLogStream = data.map(log => {
      val arr = log.split(" ")

      UserLog(arr(0), arr(1), arr(2).toInt)
    })

    userLogStream
      .keyBy("city")
      .max("duration")
      .print()

    userLogStream
      .keyBy("city")
      .maxBy("duration")
      .print()

    env.execute("KeyByOperator")
  }

  case class UserLog(name: String, city: String, duration: Int)
}
```

`max` 输出的结果应该是：

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/885895c4c12ec16b0e732dd36b7ea7fa-0)

`maxBy` 输出的结果应该是：

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/7309b516487f1481faaf06af3a8c9296-0)

乍眼一看好像并没有什么区别，但是请注意，这里要和原始数据进行对比，从第三行开始就不一致了。当程序执行到第三行的时候 `Chengdu` 分组中，`duration` 最大是 600，最大值对应的 `name` 是 William。maxBy 所对应的 `name` 正好是 William，而 max 对应的 `name` 依然是 Bob。同样在第五行第六行也有类似的情况出现。也就是说，max 和 maxBy 算子都会返回被聚合的最大值，但是 max 只会返回最大值，不会更新最大值所对应的其它字段，而 maxBy 会更新最大值对应的其它字段。

#### reduce

reduce 算子和 Spark 中的 reduceBy 的逻辑是一模一样的。它将 KeyedStream 转换成 DataStream，即⼀个有初始值的分组数据流的滚动折叠操作，合并当前元素和前⼀次折叠操作的结果，并产⽣⼀个新的值，返回的流中包含每⼀次折叠的结果，⽽不是只返回最后⼀次折叠的最终结果。

修改代码为如下：

```scala
package com.shiyanlou.operator

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object KeyByOperator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data: DataStream[String] = env.readTextFile("/home/shiyanlou/userlog.txt")
    val userLogStream = data.map(log => {
      val arr = log.split(" ")

      UserLog(arr(0), arr(1), arr(2).toInt)
    })


    userLogStream
      .keyBy("city")
      .reduce((x, y) => UserLog(y.name, y.city, x.duration + y.duration))
      .print()

    env.execute("KeyByOperator")
  }

  case class UserLog(name: String, city: String, duration: Int)
}
```

在上面的代码中，我们使用 keyBy 算子对 UserLog 按照 city 做了分组，然后使用 reduce 算子对每个分组内的 UserLog 做了聚合。返回的 `name` 和 `city` 从后面一条日志中取，`duration` 是两条日志中的 `duration` 相加的结果。最终输出如下：

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/bd4d0c3d4cd354e1b7ddc739fac00230-0)

#### Union

union 顾名思义就是连接的意思，所以 union 算子的作用就是合并两条或者多条相同类型的 DataStream，生成一个新的类型相同的 DataStream。如图所示：

(![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/f2546aec2cb33903238873d09a78ff4d-0)

需要注意的是，事件合流的方式为 FIFO 方式。操作符并不会产生一个特定顺序的事件流。union 操作符也不会进行去重。每一个输入事件都被发送到了下一个操作符。

假设某公司分别在淘宝和天猫都开设了自己的直营店，公司高层需要实时监控到两个店铺的交易数据，并希望通过大屏展示的方式实时滚动。我们可以通过两条 Socket 输入流来模拟这样的场景。

首先在我们 FlinkLearning 工程的 `com.shiyanlou.operator` 包下创建一个 `UnionOperator` 的 Scala object，输入如下代码：

```scala
package com.shiyanlou.operator


import org.apache.flink.streaming.api.scala._

object UnionOperator {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 接收淘宝的订单信息
    val taobaoOrder: DataStream[String] = env.socketTextStream("localhost", 9998)
    // 接收天猫的订单信息
    val tianmaoOrder: DataStream[String] = env.socketTextStream("localhost", 9999)

    // 将两条输入流合并为一条输入流
    val dataStream = taobaoOrder.union(tianmaoOrder)

    // 设置并行度
    dataStream.print().setParallelism(1)

    // 执行
    env.execute("Union Stream...")
  }
}
```

我们使用 netcat 监控两个端口来模拟发送淘宝和天猫的订单信息，然后使用 Flink 接收。打开终端窗口，执行 `nc -l -p 9998` 命令，紧接着打开另一个终端窗口，执行 `nc -l -p 9999` 命令。这样的话我们监控了 9998 和 9999 两个端口，接下来在 Flink 中进行接收。

运行刚刚的代码，然后在前面打开的两个终端中交替发送订单数据，观察 idea 控制台输出。

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/d2501f4a3c4dbdffc44bbad247778725-0)

#### Split 和 Select

Split 操作是 Union 操作的逆操作。它将根据一定的条件，将一条输入流分隔为两条甚至多条输入流。基于分隔之后的各个子流，我们可以应用不同的业务逻辑。结合 API 来解释就是，对于一个 DataStream 根据一些特征将其拆分为多个 SplitStream，然后从一个 SplitStream 中获取一个或者多个 DataStream。

疫情期间，全国各地的超市、医院、机场等公共场所入口都有温度监控设备，当该设备检测到某个人体温异常之后就会报警。假设鉴别正常体温和发烧体温的阈值为 36.0 摄氏度，也就是说，只要体温大于等于 36.0 摄氏度我们就认为其为发烧状态。我们使用 Split 和 Select 算子来模拟这样的场景。

在我们 FlinkLearning 工程的 `com.shiyanlou.operator` 包下创建一个名为 `SelectOperator` 的 Scala object，代码如下：

```scala
package com.shiyanlou.operator

import org.apache.flink.streaming.api.scala._

object SelectOperator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStream[String] = env.socketTextStream("localhost", 9999)

    val splitStream = dataStream
      .map(line => {
        val arr = line.split(" ")
        People(arr(0), arr(1).toFloat)
      })
        .split(people =>{
          if (people.temperature > 36.0) Seq("fever") else Seq("normal")
        })

    val normal = splitStream.select("normal")
    val fever = splitStream.select("fever")

    fever.print()

    env.execute("Select Operator...")
  }

  case class People(name: String, temperature: Float)
}
```

上面的代码中，我们创建了一个 Socket 输入流监控`localhost`下的 9999 端口，然后将输入的文本使用空格分隔之后转换为`People`类。紧接着使用 Split 算子将体温大于 36.0 的人群定义为`fever`，将体温小于等于 36.0 的人群定义为`normal`，最后使用`select`算子选择了`fever`（发烧）状态的人群并输出到控制台。

打开终端，执行`nc -l -p 9999`，在 idea 运行以上代码，并在终端中依次发送下面的信息：

```txt
张小明 35.6
李鹏程 36.3
赵露 36.7
李阳 35.5
刘明 37.0
```

在 idea 的控制台会看到将体温高于 36.0 的做了打印（李鹏程、赵露、刘明）。

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/10edfa732bb2636d4f7869c20a6b20bf-0)

Source 中，从文件、Socket 和集合读取数据分别对应几个方法：

- env.readTextFile(）
- env.socketTextStream()
- env.fromCollection()
- env.fromElement()
- env.generateSequence()

`readTextFile`、`socketTextStream`和`fromCollection`这三个方法在前面的实验中已经频繁使用了，所以这里不再举例。需要额外说明的是，`fromCollection`方法我们在前面使用的时候传入的参数是`List`对象，实际上所有序列以及迭代器对象都可以。如：

```scala
val iterator = Iterator(1,2,3,4)
val inputStream = env.fromCollection(iterator)
```

下面介绍新认识的两个方法以及自定义 Source：

#### `fromElements(elements:_*)`

从⼀个给定的对象序列中创建⼀个数据流，所有的对象必须是相同类型的。如：

```scala
val lst1 = List(1,2,3,4,5)
val lst2 = List(6,7,8,9,10)
val inputStream = env.fromElement(lst1, lst2)
```

#### `generateSequence(from, to)`

从给定的间隔中并⾏地产⽣⼀个数字序列。

```scala
val inputStream = env.generateSequence(1,10)
```

#### 自定义 Source

一般来说，Flink 官方提供的 Source 和第三方依赖提供的 Source 已经完全可以满足我们日常的开发需求了，但是如果存在不能满足的情况，那么就需要我们自己去实现一个 Source 了。虽然这种情况少之又少，但其依然是一个很重要的知识点。

细心的同学可能已经发现了，我们在前面的实验中通过，`env` 对象是有一个 `addSource` 方法的，这个方法就是我们自定义 Source 用的。自定义 Source 总得来说可以分为两步：

- 自定义一个类 `MySource`，继承 `SourceFunction` 并重写其方法
- 将 `MySource` 的实例对象作为参数传入 `addSource`

在我们 FlinkLearning 工程的 `com.shiyanlou.source` 包下创建一个名为 `Source` 的 Scala object，代码如下：

```scala
package com.shiyanlou.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

object Source {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data = env.addSource(new MySource())

    data.setParallelism(1).print()


    env.execute()
  }

  class MySource extends SourceFunction[Integer]{
    var flag = true

    override def run(ctx: SourceFunction.SourceContext[Integer]): Unit = {

      var count = 0
      val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      while(flag){
        for (elem <- list) {
          ctx.collect(elem)
          count += 1

          if (count == 20)
            cancel()
        }
      }
    }

    override def cancel(): Unit = {
      flag = false
    }
  }
}
```

在上面的代码中，我们定义了一个名为 `MySource` 的类，该类继承了 `SourceFunction` 类，在 `SourceFunction` 类后面我们加了一个 `Integer` 的泛型，该泛型表示我们自定义 Source 中要生成的对象类型为 Integer。如果你要生成其它类型的对象（如：People），则替换成相应的类名就好。在 `MySource` 类当中有两个方法需要实现：`run()` 和 `cancal()`。cancel 方法是在取消数据生成的时候需要调用的，run 方法则是在生产数据的时候调用的。需要注意的是：run 方法有一个 `ctx`，该参数是整个自定义 Source 的上下文环境，我们在 run 方法中生成的数据，需要通过 ctx 对象，才能发送到 Flink 环境中被使用，或者说 ctx 对象是我们自定义 Source 和 Flink 环境之间的一个桥梁，注意代码中的 `ctx.collect(elem)`。

最后我们将 `MySource()` 类的实例对象作为参数传给 `addSource()` 方法，运行程序则看到类似如下输出（输出顺序可能不一致）：

```txt
1
2
3
4
5
6
7
8
9
10
1
2
3
4
5
6
7
8
9
10
```

#### Kafka Source

Flink 作为一个流处理框架，肯定少不了和 Kafka 交互。如果要以 Kafka 作为 Source，需要在 pom.xml 文件中添加如下依赖：

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-kafka_2.11</artifactId>
  <version>1.9.1</version>
</dependency>
```

打开终端，执行 `cd /opt/kafka-1.1.0` 进入到 Kafka 安装目录，然后执行以下命令启动 Kafka 自带的 Zookeeper：

```bash
sudo nohup bin/zookeeper-server-start.sh config/zookeeper.properties &
```

执行下面的命令启动 Kafka：

```bash
sudo bin/kafka-server-start.sh -daemon config/server.properties
```

执行下面的命令创建一个名为 shiyanlou 的 Topic：

```bash
sudo bin/kafka-topics.sh --zookeeper localhost:2181 --create --replication-factor 1 --partitions 1 --topic shiyanlou
```

执行下面的命令启动一个 Console Producer：

```bash
sudo bin/kafka-console-producer.sh --broker-list localhost:9092 --topic shiyanlou
```

至此，我们已经在 Kafka 中创建了一个名为 shiyanlou 的 Topic，并且启动了一个 Console Producer 负责生产数据，接下来在 `com.shiyanlou.source` 包下创建一个 `KafkaSource` object，完整代码如下：

```scala
package com.shiyanlou.source

import java.util.Properties

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.kafka.common.serialization.StringDeserializer

object KafkaSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.streaming.api.scala._

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "localhost:9092")
    prop.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    prop.setProperty("value.deserializer", classOf[StringDeserializer].getName)
    prop.setProperty("auto.offset.reset", "latest")

    // 添加 Kafka Source
    val data = env.addSource(new
        FlinkKafkaConsumer011[String]("shiyanlou", new SimpleStringSchema(), prop))

    data.print()
    env.execute()
  }
}
```

在上面的代码中，我们在 `prop` 对象中添加了 Kafka 的相关属性，然后通过 `addSource` 方法将 Kafka 和 Flink 做了绑定，监听了 `shiyanlou` 这个 Topic，最后将接收到的数据做了打印。注意使用的 `FlinkKafkaConsumer011` 类最后有个 `011` 标识，这个是和 Kafka 版本相关的，由于环境里安装的是 Kafka 1.1.0 的版本，所以在这里使用的是 `011`，在导包的时候注意。

最后在终端中发送数据，就可以在控制台看到相应输出了。

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/b6f90388036b16287b97359372f9c832-0)

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/ac436cb8593bab51f7009b4b054ab9ee-0)

Flink 官方提供的 Sink，主要包含写入到文件和 Socket 中。

- writeAsText
- writeAsCsv
- writeToSocket

其中，`writeAsText` 将元素以字符串形式逐⾏写⼊（TextOutputFormat），这些字符串通过调⽤每个元素的 toString()⽅法来获取。`writeAsCsv` 将元组以逗号分隔写⼊⽂件中（CsvOutputFormat），⾏及字段之间的分隔符可以通过参数配置。每个字段的值来⾃对象的 toString()⽅法。

我们通过代码来实验一下，在我们 FlinkLearning 工程的 `com.shiyanlou.sink` 包下创建一个名为 `FlinkSink` 的 Scala object。代码如下：

```scala
package com.shiyanlou.sink

import org.apache.flink.streaming.api.scala._

object FlinkSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.readTextFile("/home/shiyanlou/score.txt")

    data.print()

    data.writeAsText("/home/shiyanlou/out/score/txt")
    data
      .map(line =>{
        val arr = line.split(" ")
        (arr(0), arr(1), arr(2))
      })
      .writeAsCsv("/home/shiyanlou/out/score/csv")

    env.execute()
  }
}
```

然后在 `/home/shiyanlou/` 路径下创建一个名为 `score.txt` 的文件，文件内容如下：

```txt
赵露 语文 60
李阳 语文 70
刘明 语文 88
朱晓明 语文 90
李艳 语文 89
```

然后运行代码，在终端中分别执行 `ls /home/shiyanlou/out/score/txt` 和 `ls /home/shiyanlou/out/score/csv`，会分别看到有 4 个分区文件，这是因为当前程序的并行度为 4，可以在代码中通过 `println(env.getParallelism)` 进行验证。如果要输出到一个文件中，可以通过 `writeAsText()` 和 `writeAsCsv()` 之前通过 `env.setParallelism(1)` 设置并行度为 1 来实现。如果分区数大于 1，可以通过 `cat /home/shiyanlou/out/score/txt/*` 和 `cat /home/shiyanlou/out/score/csv/*` 查看该目录下的所有输出结果。

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/877511b5aa82a9a3d83e5ca912e7ba82-0)

#### 自定义 Sink

与自定义 Source 类似，自定义 Sink 也需要两步：

- 自定义一个类 `MySink`，继承 `SinkFunction` 并重写其方法
- 将 `MySink` 的实例对象作为参数传入 `addSink`

```scala
package com.shiyanlou.sink

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._

object FlinkSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.readTextFile("/home/shiyanlou/score.txt")

    data.addSink(new MySink())

    env.execute()
  }

  class MySink extends SinkFunction[String] {
    override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
      val time = context.currentProcessingTime()
      val waterMark = context.currentWatermark()

      println(s"$value : $time : $waterMark")
    }
  }
}
```

在上面的代码中，我们定义了 `MySink` 类，该类继承了 `SinkFunction` 类，并重写了该类的 `invoke` 方法。在该方法中，我们通过 `context` 对象获取了 `currentProcessingTime`（处理时间戳）和 `currentWatermark` （水印）信息，然后和 `data` 中的元素一并打印输出。关于 `currentProcessingTime` 和 `currentWatermark` 大家暂时不用纠结，在后面的实验中我们会重点介绍。最后输出的结果如下：

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/aba9ae4803d128e74e7aabd4b5cf1949-0)

在 Flink 中，State 总的来说可以分为两种类型，Keyed State（键控状态）和 Operator State（算子状态）。

- Operator State：Operator State 可以作用在所有算子上，每个算子中并行的 Task 都可以共享一个状态，或者说同⼀个算⼦中的多个 Task 的状态是相同的。但是请注意，算子状态不能由相同或不同算子的另一个实例访问。Operator State 支持三种基本数据结构，分别是：
  - ListState：存储列表类型的状态。
  - UnionListState：存储列表类型的状态。和 ListState 的区别是，如果发生故障，ListState 会将该算子的所有并发的状态实例进行汇总，然后均分给新的 Task；而 UnionListState 只是将所有并发的状态实例汇总起来，具体的划分行为则由用户进行定义。
  - BroadcastState：用于广播的算子状态。如果一个算子有多项任务，并且它的每项任务状态又都相同，这种情况就可以使用广播状态。
- Keyed State：Keyed State 是作用在 KeyedStream 上的。从名称中就可以看出来，它的特点是和 Key 强相关的。在任务处理中，Flink 为每个 Key 维护一个状态实例，而且相同 Key 所对应的数据都会被分配到同一个任务中执行。Keyed State 支持五种基本数据结构，分别是：
  - ValueState：保存单个 Value，可以针对该 Value 进行 get/set 操作。
  - ListState：保存一个列表，列表中可以存储多个 Value。可以针对列表进行 add、get、update 操作。
  - MapState：保存 Key-Value 类型的值。可以针对其进行 get、put、remove 操作，还可以使用 contains 判断某个 key 是否存在。
  - ReducingState：保存一个单一值，该值是添加到状态的所有值聚合的结果。
  - AggregatingState：保存一个单一值，该值是添加到状态的所有值聚合的结果。与 ReducingState 有些不同，聚合类型可能不同于添加到状态的元素的类型。接口和 ListState 相同，但是使用 add(IN)添加的元素本质是通过使用指定的 AggregateFunction 进行聚合。

接下来我们以 KeyedState 为例进行实验。假设某网站的用户访问日志样例如下：

```txt
20210301121533,login,北京,118.128.11.31,0001
20210301121536,login,上海,10.90.113.150,0002
20210301121544,login,成都,112.112.31.33,0003
20210301121559,login,成都,101.132.93.24,0004
20210301121612,login,上海,189.112.89.78,0005
20210301121638,login,北京,113.52.101.50,0006
```

从左往右依次表示用户访问时间、用户触发的 action（登录、登出等）、城市、IP 地址、用户 ID。在实验代码中用 `UserLog` 样例类来表示该日志：

```scala
case class UserLog(time: Long, action: String, city: String, ip: String, user_id: String)
```

现在的业务需求是：现在的业务需求是：统计同城市两个⽤户登录的时间差。特殊的是第⼀个⽤户登录的时 候，它前⾯没有⽤户登录，我们需要在代码⾥特殊处理。在 `com.shiyanlou.state` 包下创建 `KeyedState` 的 object。完整代码如下：

```scala
package com.shiyanlou.state

object KeyedState {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.streaming.api.scala._
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.socketTextStream("localhost", 9999)
      .map(line => {
        val arr = line.split(",")

        UserLog(arr(0).toLong, arr(1), arr(2), arr(3), arr(4))
      })

    data
      .keyBy(_.city)
      .mapWithState[(String, Long), UserLog]{
        case (curr:UserLog, None) =>((curr.city, 0), Some(curr))
        case (curr:UserLog, last:Some[UserLog]) => {
          val diff = curr.time - last.get.time
          ((curr.city, Math.abs(diff)), Some(curr))
        }
      }
      .print()

    env.execute("KeyedState")
  }

  case class UserLog(time: Long, action: String, city: String, ip: String, user_id: String)
}
```

上面代码的 `main` 函数中，我们监听 `localhost` 的 9999 端口并将接收到的日志通过 `map` 算子转换为 `UserLog` 类型。重点是接下来的这段代码：

```scala
    data
      .keyBy(_.city)
      .mapWithState[(String, Long), UserLog]{
        case (curr:UserLog, None) => ((curr.city, 0), Some(curr))
        case (curr:UserLog, last:Some[UserLog]) => {
          val diff = curr.time - last.get.time
          ((curr.city, Math.abs(diff)), Some(curr))
        }
      }
      .print()
```

在根据城市 `keyBy` 之后，使用了 `mapWithState` 方法，`mapWithState` 算子是 `map` 算子对应的带状态的方法。Flink 中基本上所有的基础算子都有一个对应的带状态的算子，且以 `WithState` 结尾。在 `mapWithState` 中，`curr` 变量和 `last` 变量分别表示当前登录用户和前一个登录用户，但是由于第一个用户登录的时候，它前面没有用户登录，所以用 `case (curr:UserLog, None) => ((curr.city, 0), Some(curr))` 来处理，注意第二个输入参数为 `None`。由于数据在传输过程中可能存在乱序的情况（输出的顺序和输入的顺序不一致），为了避免时间差为负值的情况，所以用了 `Math.abs()` 取绝对值。

在终端中执行 `nc -l -p 9999`，运行以上代码，并在终端中输入以下日志：

```txt
20210301121533,login,北京,118.128.11.31,0001
20210301121536,login,上海,10.90.113.150,0002
20210301121544,login,成都,112.112.31.33,0003
20210301121559,login,成都,101.132.93.24,0004
20210301121612,login,上海,189.112.89.78,0005
20210301121638,login,北京,113.52.101.50,0006
```

输出如下：

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/5c85519272d4a168e640eb710c1488a2-0)

在开始认识 Checkpoint 之前，我们先认识三个分布式中表示状态一致性的术语，分别是 at-most-once、at-least-once、exactly-once。

- At-most-once：最多一次。数据最多被处理一次，这意味着当程序发生故障的时候，有可能会造成数据丢失。
- At-least-once：最少一次。数据至少会被处理一次，这意味着当程序发生故障的时候，可能会存在同一条数据被重复多次计算的情况。
- Exactly-once：精准一次。数据不多不少刚好被使用了一次，即使程序发生故障，但是任务恢复之后会和发生故障之前的状态保持一致，已经处理过的数据不会被重复处理，没有被处理的数据也不会丢失。

这三个术语并不是 Flink 中特有的，在其它分布式框架中也会涉及到，特别是在 Kafka 中尤为频繁。

既然 Flink 提供了 State 的支持，那么在程序出现问题或者机器宕机的时候，如何保证整个程序都能够顺利恢复到正确的 State？或者说如何保证 Exactly-Once 语义？为了解决这个问题，Flink 引入了 Checkpoint（检查点）机制。Flink Checkpoint 是一种容错恢复机制，这种机制保证了实时程序运行时，即使突然遇到异常也可以自我进行恢复。Checkpoint 示意图如下所示，其中一个非常重要的角色就是 barrier（栅栏，可以理解为一个个标记）。在数据流入 Flink 的过程中，会从源头开始就往数据流中注入 barrier。作为数据流数据的一部分，barrier 不会干扰正常的数据流，一个 barrier 会把数据分割成两个部分，一部分进入当前快照，另一部分进入下一个快照。每个 barrier 都带有快照的 id，并且 barrier 之前的数据都进入了此快照。多个 barrier 会出现在数据流中，也就是会产生多个快照。当 barrier 在 source 源头被注入时，系统就会记录当前快照的位置。

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/77a1ad2822522fc574e5d28afb25f8d8-0)

Flink 中默认没有开启 Checkpoint，需要我们通过 `env.enableCheckpointing()` 方法去指定。`env.enableCheckpointing()` 方法接受一个 `Long` 类型的参数，表示设定 Checkpoint 的时间间隔，单位为毫秒。如设置时间间隔为 10 秒：

```scala
env.enableCheckpointing(10 * 1000)
```



当 Checkpoint 机制启动时，State 将在检查点中持久化来应对数据丢失以及恢复。而状态在内部是如何表示的、状态是如何持久化到检查点中以及持久化到哪里都取决于选定的 StateBackends（状态后端）。Flink 提供了三种 StateBackends，分别是 MemoryStateBackend、

FsStateBackend 和 RocksDBStateBackend。

- MemoryStateBackend：基于内存的状态管理。特点是速度快、性能高。缺点是一旦机器发生故障，整个内存中存储的状态数据都会丢失，而且内存由于内存大小的限制，这种方式并不会被用在生产环境中。MemoryStateBackend 可以通过 `env.setStateBackend(new MemoryStateBackend(maxStateSize))`，maxStateSize 是一个 Int 类型的参数，用来设置保存 State 可占用的最大内存。
- FsStateBackend：基于文件系统的状态管理。这里的文件系统可以是本地文件系统，也可以是分布式文件系统，如：HDFS、OSS、S3 等。相对于 MemoryStateBackend 来说，基于文件的方式数据安全性更加得到了保障，可以保存的 State 也不会受限于磁盘大小。FsStateBackend 可以使用 `setStateBackend(new FsStateBackend(path))` 来指定，参数 path 表示存储路径。生产环境一般都用分布式文件系统进存储，如：`hdfs://node1:9000/flink/checkpoint/`。
- RocksDBStateBackend：用 RocksDB 来存储 State。RocksDB 是一个 Key-Value 型的数据库，这种方式大家了解即可。



Flink 运行时架构主要包括四个不同的组件，它们会在运行流处理应用程序时协同工作：作业管理器（JobManager）、资源管理器（ResourceManager）、任务管理器（TaskManager），以及分发器（Dispatcher）。因为 Flink 是用 Java 和 Scala 实现的，所以所有组件都会运行在 Java 虚拟机上。每个组件的职责如下：

#### 作业管理器（JobManager）

控制一个应用程序执行的主进程，也就是说，每个应用程序都会被一个不同的 JobManager 所控制执行。JobManager 会先接收到要执行的应用程序，这个应用程序会包括：作业图（JobGraph）、逻辑数据流图（logical dataflow graph）和打包了所有的类、库和其它资源的 JAR 包。JobManager 会把 JobGraph 转换成一个物理层面的数据流图，这个图被叫做“执行图”（ExecutionGraph），包含了所有可以并发执行的任务。JobManager 会向资源管理器（ResourceManager）请求执行任务必要的资源，也就是任务管理器（TaskManager）上的插槽（slot）。一旦它获取到了足够的资源，就会将执行图分发到真正运行它们的 TaskManager 上。而在运行过程中，JobManager 会负责所有需要中央协调的操作，比如说检查点（checkpoints）的协调。

#### 资源管理器（ResourceManager）

主要负责管理任务管理器（TaskManager）的插槽（slot），TaskManger 插槽是 Flink 中定义的处理资源单元。Flink 为不同的环境和资源管理工具提供了不同资源管理器，比如 YARN、Mesos、K8s，以及 standalone 部署。当 JobManager 申请插槽资源时，ResourceManager 会将有空闲插槽的 TaskManager 分配给 JobManager。如果 ResourceManager 没有足够的插槽来满足 JobManager 的请求，它还可以向资源提供平台发起会话，以提供启动 TaskManager 进程的容器。另外，ResourceManager 还负责终止空闲的 TaskManager，释放计算资源。

#### 任务管理器（TaskManager）

Flink 中的工作进程。通常在 Flink 中会有多个 TaskManager 运行，每一个 TaskManager 都包含了一定数量的插槽（slots）。插槽的数量限制了 TaskManager 能够执行的任务数量。启动之后，TaskManager 会向资源管理器注册它的插槽；收到资源管理器的指令后，TaskManager 就会将一个或者多个插槽提供给 JobManager 调用。JobManager 就可以向插槽分配任务（tasks）来执行了。在执行过程中，一个 TaskManager 可以跟其它运行同一应用程序的 TaskManager 交换数据。

#### 分发器（Dispatcher）

可以跨作业运行，它为应用提交提供了 REST 接口。当一个应用被提交执行时，分发器就会启动并将应用移交给一个 JobManager。由于是 REST 接口，所以 Dispatcher 可以作为集群的一个 HTTP 接入点，这样就能够不受防火墙阻挡。Dispatcher 也会启动一个 Web UI，用来方便地展示和监控作业执行的信息。Dispatcher 在架构中可能并不是必需的，这取决于应用提交运行的方式。

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/d9bb60a08e60b95a5cb589dbe2d24f0d-0)

#### Slots

每⼀个 TaskManager(worker)是⼀个 JVM 进程，它可能会在独⽴的线程上执⾏⼀个或多个 subtask。为了控制⼀个 worker 能接收多少个 task，worker 通过 task slot 来进⾏控制（⼀个 worker ⾄少有⼀个 task slot）。每个 task slot 表示 TaskManager 拥有资源的⼀个固定⼤⼩的⼦集。假如⼀个 TaskManager 有三个 slot，那么它会将其管理的内存分成三份给各个 slot。资源 slot 化意味着⼀个 subtask 将不需要跟来⾃其 他 job 的 subtask 竞争被管理的内存，取⽽代之的是它将拥有⼀定数量的内存储备。需要注意的是，这⾥ 不会涉及到 CPU 的隔离，slot ⽬前仅仅⽤来隔离 task 的受管理的内存。

通过调整 task slot 的数量，允许⽤户定义 subtask 之间如何互相隔离。如果⼀个 TaskManager ⼀个 slot，那将意味着每个 task group 运⾏在独⽴的 JVM 中（该 JVM 可能是通过⼀个特定的容器启动的），⽽ ⼀个 TaskManager 多个 slot 意味着更多的 subtask 可以共享同⼀个 JVM。⽽在同⼀个 JVM 进程中的 task 将 共享 TCP 连接（基于多路复⽤）和⼼跳消息。它们也可能共享数据集和数据结构，因此这减少了每个 task 的负载。

Task Slot 是静态的概念，是指 TaskManager 具有的并发执⾏能⼒，可以通过参数 taskmanager.numberOfTaskSlots 进⾏配置，⽽并⾏度 parallelism 是动态概念，即 TaskManager 运⾏ 程序时实际使⽤的并发能⼒，可以通过参数 parallelism.default 进⾏配置。也就是说，假设⼀共有 3 个 TaskManager，每⼀个 TaskManager 中分配 3 个 TaskSlot，也就是每个 TaskManager 可以接收 3 个 task，⼀共 9 个 TaskSlot，如果我们设置 parallelism.default=1，即运⾏程序默认的并⾏度为 1，9 个 TaskSlot 只⽤了 1 个，有 8 个空闲，因此，设置合适的并⾏度才能提⾼效率。

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/6055d685f2ed25c4cae133df48c747aa-0)

#### 程序与数据流

Flink 程序的基础构建模块是 流（streams） 与 转换（transformations）（需要注意的是，Flink 的 DataSet API 所使⽤的 DataSets 其内部也是 stream）。⼀个 stream 可以看成⼀个中间结果，⽽ ⼀个 transformations 是以⼀个或多个 stream 作为输⼊的某种 operation，该 operation 利⽤这些 stream 进⾏计算从⽽产⽣⼀个或多个 result stream。在运⾏时，Flink 上运⾏的程序会被映射成 streaming dataflows，它包含了 streams 和 transformations operators。每⼀个 dataflow 以⼀个 或多个 sources 开始以⼀个或多个 sinks 结束。dataflow 类似 Spark 的 DAG，当然特定形式的环可以 通过 iteration 构建。在⼤部分情况下，程序中的 transformations 跟 dataflow 中的 operator 是⼀⼀ 对应的关系，但有时候，⼀个 transformation 可能对应多个 operator。

#### task 与 operator chains

出于分布式执⾏的⽬的，Flink 将 operator 的 subtask 链接在⼀起形成 task，每个 task 在⼀个线程中 执⾏。将 operators 链接成 task 是⾮常有效的优化：它能减少线程之间的切换和基于缓存区的数据 交换，在减少时延的同时提升吞吐量。链接的⾏为可以在编程 API 中进⾏指定

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/8268c967b91819686ef20b43b72d6ad7-0)

如果你在此之前有学习过 Spark Streaming，我们可以将 Flink 中的术语和 Spark Streaming 中的术语做个类比，方便理解：

| Spark Streaming   | Flink          |
| ----------------- | -------------- |
| DStream           | DataStream     |
| Trasnformation    | Trasnformation |
| Action            | Sink           |
| Task              | SubTask        |
| Pipeline          | Oprator chains |
| DAG               | DataFlow Graph |
| Master + Driver   | JobManager     |
| Worker + Executor | TaskManager    |



当 Flink 集群启动后，首先会启动一个 JobManger 和一个或多个的 TaskManager。由 Client 提交任务给 JobManager，JobManager 再调度任务到各个 TaskManager 去执行，然后 TaskManager 将心跳和统计信息汇报给 JobManager。TaskManager 之间以流的形式进行数据的传输。上述三者均为独立的 JVM 进程。

Client 为提交 Job 的客户端，可以是运行在任何机器上（与 JobManager 环境连通即可）。提交 Job 后，Client 可以结束进程（Streaming 的任务），也可以不结束并等待结果返回。

JobManager 主要负责调度 Job 并协调 Task 做 checkpoint，从 Client 处接收到 Job 和 JAR 包等资源后，会生成优化后的执行计划，并以 Task 的单元调度到各个 TaskManager 去执行。

TaskManager 在启动的时候就设置好了槽位数（Slot），每个 slot 能启动一个 Task，Task 为线程。从 JobManager 处接收需要部署的 Task，部署启动后，与自己的上游建立 Netty 连接，接收数据并处理。

客户端不是运行时和程序执行的一部分，但它用于准备并发送 DataFlow(JobGraph)给 Master(JobManager)，然后，客户端断开连接或者维持连接以等待接收计算结果。



- Window 分类
  - Keyed Window 和 Global Window
  - Time Window 和 Count Window
- Window API

众所周知，如果要对人进行分类的话，按照性别可以分为男和女，按照肤色可以分为黄种人、白种人和黑种人。对于 Window 而言，根据上游数据集的类型可以分为 Keyed Window 和 Global Window，根据业务场景来分，又可以分为 Count Window 和 Time Window。



#### Time Window

基于时间定义的窗口。根据不同业务场景又可以分为滚动窗口(Tumbling Window)、滑动窗口（Sliding Window）和会话窗口（Session Window）三种。

- 滚动窗口（Tumbing Window）：滚动窗口是按照固定时间进行切分，而且所有窗口之间的数据不会重叠，使用时只需要指定一个窗口长度即可。

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/bd1ee76096ba49ae86bde7b9b275a8c8-0)

从上面的示例图中可以看到，滚动窗口的窗口大小（window size）是固定的，而且相邻窗口之间是连续的。现在有这样的业务场：某公司要求每 10 秒统计一次最近 10 秒内各个电商平台的订单数量并输出到大屏幕，这时候就需要用到滚动窗口了，我们只需要将窗口大小设置为 10 秒就可以。我们使用 netcat 发送 Socket 数据来模拟订单流量。

在 `com.shiyanlou.window` 包下创建 `TumblingWindow` Scala Object，代码如下：

```scala
package com.shiyanlou.window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object TumblingWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val textDstream: DataStream[String] = env.socketTextStream("localhost", 9999)

    import org.apache.flink.streaming.api.scala._

    val dataStream: DataStream[(String, Int)] = textDstream
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0) // 按照第0个字段分组
      .timeWindow(Time.seconds(10)) // 设定窗口大小
      .sum(1) // 对第1个字段求和

    dataStream.print().setParallelism(1)

    env.execute("Tumbling Window")
  }
}
```

在上面的代码中，我们监听了 `localhost` 的 9999 端口，将接收到的数据转换为（key, 1）这样的键值对，然后按 key 分组，设定窗口大小为 10 秒，最后针对每个分组里的数据进行求和并输出。

在终端中输入 `nc -l -p 9999`，然后运行程序，在终端中输入以下内容，然后观察控制台输出：

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/fba2b2c5b9a87ea7d30cbddef62a88e3-0)

在终端输入的时候注意控制时间间隔，由于代码中设置的窗口大小是 10 秒，所以会每隔 10 秒打印一次最近 10 秒内输入的数据。最终输出的统计结果和上图中不一致属于正常情况。另外 `timeWindow()` 方法中的时间参数除了 `Time.seconds()`，还有 `Time.days()`、`Time.hours()`、`Time.minutes()` 和 `Time.milliseconds()`，在 idea 中输入的时候会有提示。

- 滑动窗口（Sliding Window）：滑动窗口有两个参数，分别是窗口大小和窗口滑动时间，它是允许不同窗口的元素重叠的（同一个元素可以出现在不同的窗口中）。窗口大小指定数据统计的时间跨度，而滑动时间指定的是相邻两个窗口时间的时间偏移量。当滑动时间小于窗口大小的时候，数据会发生重叠；当滑动窗口大于窗口大小的时候，窗口会出现不连续的情况（部分元素不会纳入统计）；当滑动时间和窗口大小相等的时候，滑动窗口就是滚动窗口，从这个角度来看，滚动窗口是滑动窗口的一个特殊存在。

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/43d5850c6dcfec89d5986a340880a0ae-0)

滑动窗口和滚动窗口在使用过程中的唯一区别就是多了一个滑动大小的参数。假设将上面的业务修改为“公司要求每隔 5 秒统计一次最近 10 秒内的各平台订单量”，那么这里的窗口大小（window size）就是 10 秒，而窗口滑动大小（window slide）是 5 秒，从上图中我们可以看到，当窗口滑动大小小于窗口大小的时候，元素是会重叠的。完整代码如下：

```scala
package com.shiyanlou.window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object SlidlingWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val textDstream: DataStream[String] = env.socketTextStream("localhost", 9999)

    import org.apache.flink.streaming.api.scala._

    val dataStream: DataStream[(String, Int)] = textDstream
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0) // 按照第0个字段分组
      .timeWindow(Time.seconds(10), Time.seconds(5)) // 设定窗口大小、窗口滑动大小
      .sum(1) // 对第1个字段求和

    dataStream.print().setParallelism(1)

    env.execute("Slidling Window")
  }
}
```

- 会话窗口（Session Window）：与前滚动窗口和滑动窗口不同的是，会话窗口没有固定的滑动时间和窗口大小，而是通过一个 session gap 来指定窗口间隔。如果在 session gap 规定的时间内没有活跃数据进入的话，则认为当前窗口结束，下一个窗口开始。session gap 可以理解为相邻元素的最大时间差。

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/f56eed017fe262c916a6f5c1b501f556-0)

代码如下：

```scala
package com.shiyanlou.window

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, ProcessingTimeSessionWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object SessionWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val textDstream: DataStream[String] = env.socketTextStream("localhost", 9999)

    import org.apache.flink.streaming.api.scala._

    val dataStream: DataStream[(String, Int)] = textDstream
      .map((_, 1))
      .keyBy(0)
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))) // 超时时长为5秒
      .sum(1)

    dataStream.print().setParallelism(1)

    env.execute("Session Window")
  }
}
```

在上面的代码中，我们使用 `ProcessingTimeSessionWindows.withGap(Time.seconds(5))` 指定当前窗口为会话窗口，而且最大等待时间为 5 秒，也就是说如果相邻两个元素的抵达时间小于等于 5 秒，则不会触发当前窗口，一旦超过 5 秒未接收到新数据，当前窗口触发计算。大家可以使用 `nc -l -p 9999` 命令启动终端发送数据并观察控制台输出，注意把控相邻元素的时间间隔。

#### Count Window

基于输入数据量定义，与时间无关。Count Window 也可以细分为滚动窗口和滑动窗口，逻辑和 Time Window 中的滚动窗口和滑动窗口的逻辑类似，只是窗口大小和触发条件由时间换成了**相同 Key 元素的数量**。窗口大小是由相同 Key 元素的数量来触发执行，执行时只计算元素数量达到窗口大小的 key 对应的结果。

滚动窗口代码如下：

```scala
package com.shiyanlou.window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object CountTumblingWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val textDstream: DataStream[String] = env.socketTextStream("localhost", 9999)

    import org.apache.flink.streaming.api.scala._

    val dataStream: DataStream[(String, Int)] = textDstream
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .countWindow(3)
      .sum(1)

    dataStream.print().setParallelism(1)

    env.execute("Count Tumbling Window")
  }
}
```

在上面的代码中我们设置的窗口触发条件为 3，也就是相同 Key 元素达到 3 之后就触发计算。注意是**相同 Key 元素的个数**而不是所有元素的总数。在终端输入`nc -l -p 9999`之后依次输入以下内容：

```txt
tianmao
jindong
taobao
tianmao
tianmao
taobao
taobao
jindong
jindong
```

在输入以上内容的时候注意观察控制台输出：

```txt
(tianmao,3)
(taobao,3)
(jindong,3)
```

滑动窗口代码如下：

```scala
package com.shiyanlou.window

import org.apache.flink.streaming.api.scala._

object CountSlidlingWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val textDstream: DataStream[String] = env.socketTextStream("localhost", 9999)

    import org.apache.flink.streaming.api.scala._

    val dataStream: DataStream[(String, Int)] = textDstream
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .countWindow(3, 2)
      .sum(1)

    dataStream.print().setParallelism(1)

    env.execute("Sliding Tumbling Window")
  }
}
```

在上面的代码中，`countWindow()`方法有两个参数，分别是`size`和`slide`，其中`size`为 3，`slide`为 2。也就是说每收到两个相同 Key 的元素就触发一次计算，计算的范围是相邻的 3 个相同元素。

使用 `nc -l -p 9999` 命令启动终端并发送以下内容：

```txt
taobao
taobao
tianmao
taobao
tianmao
taobao
taobao
taobao
```

输出如下：

```txt
(taobao,2)
(tianmao,2)
(taobao,3)
(taobao,3)
```



#### Keyed Window

在前面学习聚合算子的时候我们有提到过 KeyedStream 类型。如果上游数据集的类型是 KeyedStream，则调用`window()`方法，数据会根据 Key 在不同的 Task 实例中分别计算，最后将得到针对每个 Key 的统计结果。

#### Global Window

如果上游数据集是非键值对类型的，则调用`windowAll()`方法，所有的数据都会在窗口算子中一个 Task 中计算，并得到全局的结果。

Keyed Window 和 Global Window 大家作为了解即可，由于使用较少，这里不做赘述。



在流式数据处理中，如何保证数据的全局有序和 Exactly Once（精准一次消费）是非常重要的。虽然数据在上游产生的时候是唯一并且有序的，但是数据从产生到进入 Flink 的过程中，中间可能会由于负载均衡、网络传输、分区等等原因造成数据乱序，为了应对这种情况，所以我们引入了时间语义和 WaterMark 的概念。



在 Flink 中分别将时间分为三种语义，分别是 Event Time（事件生成时间）、Ingestion Time（事件接入时间）Processing Time（事件处理时间）。

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/9680ea762a0ae43c5678f076afbed517-0)

- Event Time：指的是事件产生的时间。通常由事件中的某个时间戳字段来表示，比如用户登录日志中所携带的时间字段、天气信号检测系统中采集到的天气数据中所携带的时间字段。
- Ingestion Time：指的是事件进入 Flink 中的时间。
- Processing Time：指的是时间被处理时的当前系统时间。比如某条数据进入 Flink 之后，我们运行到某个算子时的系统时间，就是 Processing Time。Processing Time 是 Flink 中的默认时间属性。

为了方便理解，这里举个具体的例子。《中国机长》这部电影相信很多同学都看过，它是根据 2018 年 5 月 14 日四川航空 3U8633 航班的真实事件改编的，该电影于 2019 年 9 月 30 日在电影院上映，而小明同学是在 2019 年 10 月 1 日去电影院观看的。在这个案例中，事件真实发生的时间为 2018-05-14，所以 Event Time 应该是 2018-05-14，因为 Event Time 表示的是事件真实发生的时间；进入影院的时间是 2019-09-30，所以 Ingestion Time 就是 2019-09-30；小明去电影院观看的时间是 2019-10-01，所以这里对应的 Processin Time 应该是 2019-10-01，因为它表示的是该时间被处理的时间。

再举一个我们生产环境中的例子。Flink 中接收到一条如下的用户登录日志：

```txt
20210301121533,login,北京,118.112.11.50,0001
```

该日志表示在 `2021-03-01 12:15:33` 有 user_id 为 0001 的用户从北京登录了某网站，而该日志进入 Flink 的时间为 `2021-03-01 12:15:40`，该日志被 Flink 中的某个算子处理的系统时间为 `2021-03-01 12:15:41`。在这个案例中的 Event Time 为 `2021-03-01 12:15:33`，Ingestion Time 为 `2021-03-01 12:15:40`，Processing Time 为 `2021-03-01 12:15:41`。

#### 设置时间语义

Flink 中默认的时间语义是 Processing Time，但是从业务的角度来讲，我们更加关心 Event Time，使用最多的也是 Event Time，只有在 Event Time 不可用的情况下才会勉强使用 Process Time 或 Ingestion Time。所以一般在业务实践中，我们会手动指定时间语义为 Event Time，核心代码如下：

```scala
// 设置使用EventTime
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
```

如果要修改为 Ingestion Time，可以使用下面的代码：

```scala
// 设置使用IngestionTime
env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
```

但是要注意，我们说 Event Time 指的是事件的真实发生时间，通常是由事件中的某个时间戳字段来指定的。但是在上面的代码中，我们只是设置的时间语义为 Event Time，并没有指定哪个字段为时间。



#### Watermark 原理

如果指定了 Event Time 时间语义之后，流数据从产生、到 Flume、到 Kafka 等环节，最终进入 Flink 之后，并不一定是按照数据产生时候的先后顺序依次到的。很可能会因为网络传输延迟、以及 Kafka 中的多个分区问题导致数据乱序。而 Window 在执行计算的时候也不能因为某一条数据迟到而无限期地等下去，所以我们就需要引入 Watermark（水位线）机制来解决这种情况。换句话说，Watermark 就是一个延迟触发机制，专门用于处理事件中的时间乱序问题。

在 Flink 中的窗口处理中，最理想的情况就是属于该窗口的全部数据都能够在窗口关闭之前到达，这样我们就可以进行下一步操作了。但是真实情况并非如此，在窗口快要结束的时候，还有部分属于该窗口的数据没有到达，这时候我们就需要通过 Watermark 指定一个延迟时间，保证在延迟时间到达时，迟到的数据能够被纳入窗口进行计算，这样才能最大程度保证我们的数据准确性。说得具体一点，假设窗口本来会在 5 秒的时候执行计算，但是由于我们设置了延迟时间为 2 秒，那么在时间到 5 秒的时候并不会立即触发窗口计算，而是延迟到 7 秒才触发。那么 Watermark 该如何计算呢？假设进入 Flink 的最大事件时间为 `maxEventTime`，我们所指定的延迟时间为 `t`，那么 `Watermark` = `maxEventTime` - `t`。如果窗口的停止时间小于或者等于 `Watermark`，则窗口被关闭并执行。

理想情况下，进入 Flink 的所有事件都是全局有序的，这时候就不需要设置延迟时间 `t`，所以 Watermark 等于 maxEventTime。当窗口停止时间等于 Watermark 时就会触发计算。如图：

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/81774f98406655f419400b3d1db92ebc-0)

上图表示的是事件 是全局有序的，所以延迟时间为 0，一旦 Watermark 等于 maxEventTime，窗口就触发。第一个窗口中的 maxEventTime 为 5，所以在 Watermark 为 w(5) 的时候就触发窗口；同理，第二个窗口中 maxEventTime 为 10，所以在 w(10) 的时候触发窗口。

然而现实往往是残酷的，数据并不会按照产生的时间顺序依次到达，如图：

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/01879a53627f1fe3b767191ccd203f6c-0)

上图中的事件到达很明显是乱序的，并没有按照产生的先后顺序到达，所以我们需要设置一个延迟时间 `t`，假设 `t=2`。在第一个窗口中，在时间 7 到达之前，maxEventTime 为 5，所以对应的 Watermark 为 `5-2=3`，3 小于窗口的停止时间 5，所以并没有触发窗口。而当 7 到达之后，maxEventTime 为 7，Watermark 为 `7-2=5`，5 等于当前窗口的停止时间 5，所以窗口触发。在第二个窗口，在事件 12 到达之前，maxEventTime 为 11，所以 Watermark 为 `11-2=9`，9 小于该窗口的停止时间 10，所以并没有触发窗口，而当 12 到达之后，maxEventTime 为 12，此时 Watermark 为 `12-2=10`，10 等于当前窗口的停止时间，所以窗口触发。

前面我们分析了单个分区中的 Watermark，在多个分区并行的时候，Watermark 是会有一个对齐机制，要以多个分区中最小的 Watermark 为当前 Task 的 Watermark。可以简单理解为短板效应，或者说跑得快的要等一下跑得慢的。如图：

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/55ae8f04d3a20ce32fa5a2bea7f9e78e-0)

在第一幅图中，从上往下四个分区中的 Watermark 分别为 2、4、3、6，最小的是 2，所以整个 Task 的 Watermark 为 2；第二幅图中，第一个分区中的 4 到了以后，四个 Watermark 分别为 4、4、3、6，此时最小的为 3，所以整个 Task 的 Watermark 为 3；第三幅图中，第二个分区中的 7 到了，四个 Watermark 分别为 4、7、3、6，此时最小的仍然是 3，所以整个 Task 的 Watermark 为 3；第四幅图中，第三个分区的 6 到了以后，四个 Watermark 分别为 4、7、6、6，此时最小的 Watermark 为 4，所以整个 Task 的 Watermark 为 4。

#### Watermark 的使用

前面一直在讲时间语义和 Watermark 的概念，接下来我们具体看一下在代码中是如何使用 Event Time 和 Watermark 的。在 `com.shiyanlou.time` 包下创建 `PeriodicWatermark` Scala object，其中代码如下：

```scala
package com.shiyanlou.time

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object PeriodicWatermark {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val data: DataStream[UserLog] = env.socketTextStream("localhost", 9999)
      .map(line => {
        val arr = line.split(",")

        UserLog(arr(0).toLong, arr(1), arr(2), arr(3), arr(4).toLong)
      })

    data
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserLog](Time.seconds(2)) {
        override def extractTimestamp(element: UserLog): Long = {
          element.time
        }
      })
      .print()

    env.execute("Periodicc Watermark")
  }

  // 日志样例：20210301121533,login,北京,118.112.11.50,0001
  case class UserLog(time: Long, action: String, city: String, ip: String, user_id: Long)
}
```

然后打开终端，输入 `nc -l -p 9999`，然后输入样例：`20210301121533,login,北京,118.112.11.50,0001`，执行程序，观察输出情况。

上面的代码中，我们通过 `env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)` 指定了时间语义为 `Event Time`，在倒数第二行，我们创建了一个样例类 `UserLog`，其中包含了 5 个字段，分别表示日志生成时间，用户行为（登录、登出、点击、浏览等），城市，IP 地址和用户 ID。然后将通过 Socket 获取的字符串通过逗号分割并输出为 `UserLog` 实例。重点是 `assignTimestampsAndWatermarks` 方法， `assignTimestampsAndWatermarks` 的参数是一个 `BoundedOutOfOrdernessTimestampExtractor` 对象，看名字就知道，它表示的是无界流中乱序数据的时间提取器该提取器处理的泛型为 `UserLog`。在 `BoundedOutOfOrdernessTimestampExtractor` 对象中通过 `Time.seconds(2)` 延迟时间为 2 秒，并在 `extractTimestamp` 方法中指定了 `Event Time` 所需要的字段为 `UserLog` 对象的 `time` 字段。

简单来说，和前面几个实验章节中，唯一不同的两个地方就是通过 `env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)` 指定了时间语义为 `Event Time`，通过 `assignTimestampsAndWatermarks` 指定了延迟时间为 2 秒，指定 `Event Time` 需要的时间字段为 `time`。

在 Flink1.9 之前，开发人员如果需要处理批计算和流计算，需要同时掌握两种编程接口，对应的业务代码也是两套。一直到 2019 年阿里巴巴 Blink 团队在 Blink 中实现了 Table API 和 SQL，并将 Blink 贡献给 Flink 社区之后，这一问题才得以解决。由于 Table API 和 SQL 出现的时间较晚，所以功能尚不完善，但是已有功能已经可以解决开发人员的很多困难。

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/0c880e471f8cbe1c2c74fc448d19a0d6-0)

根据上图我们可以看到，Flink 中最底层的编程接口是 `Stateful Stream Processing`，在其的上面一层就是 `DataStream/DataSet API`，实际上我们在前面的实验中所使用的就是 `DataStream/DataSet API`，分别对应流处理 API 和批处理 API。再往上就是 `Table API` 和 `SQL`。越往上层的接口使用越简单方便，越往底层的接口使用更加灵活，但是使用也更加困难，对编程人员的编码能力要求也越高。`Table API` 和 `SQL` 的出现，使得我们可以通过简单的 API 调用和在代码中加入 SQL 就可以完成结构化数据处理，大大提高了开发效率。



要使用 Flink Table API 和 Flink SQL，需要在 pom.xml 文件中新加入两个依赖：

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-planner_2.11</artifactId>
    <version>1.10.0</version>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-api-scala-bridge_2.11</artifactId>
    <version>1.10.0</version>
</dependency>
```

- flink-table-planner：planner 计划器。是 Table API 最主要的部分，提供了运行时环境和生成程序执行计划的 planner。
- flink-table-api-scala-bridge：bridge 桥接器。主要负责 Table API 和 DataStream/DataSet API 的连接支持，按照语言分 java 和 scala 版本。

注意：在引入 Table API 和 SQL 的依赖时候的版本为`1.10.0`，此时 Flink 中的核心依赖版本也应该修改为对应的版本。

```xml
<dependency>
        <groupId>org.apache.flink</groupId>
    <artifactId>flink-scala_2.11</artifactId>
    <version>1.10.0</version>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-scala_2.11</artifactId>
    <version>1.10.0</version>
</dependency>
```

和 DataStream API 以及 DataSet API 一样，Table API 和 SQL 也有相似的编程模型。所以要在代码中使用 Table API 和 SQL 就必须先创建其所需要的执行环境 `TableEnviroment` 对象。在 Flink 1.9 之前可以通过下面这种方式创建：

```scala
// 创建流环境
val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
// 基于流环境创建Table环境
val tableEvn = StreamTableEnvironment.create(streamEnv)
```

在 Flink 1.9 之后还可以使用下面这种方式创建：

```scala
// 创建流环境
val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
// 创建EnvironmentSettings对象
val envSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
// 创建Table环境
val tableEnv = StreamTableEnvironment.create(streamEnv, envSettings)
```

自从 Blink 加入之后，Flink 中就保留了两套 Planner，Flink Planner 被称为 Old Planner，新加入的被称为 Blink Planner。由于 blink 不支持表和 DataSet 之间的转换等，所以官方推荐使用 Old Planner。



#### 创建 Table

在 Flink 中创建表有两种方法：

- 从文件创建（批计算）
- 从 DataStream 创建（流计算）

一般只有批计算，我们才会从文件从文件中创建。在 `/home/shiyanlou` 路径下创建 `userlog.log` 文件来表示用户日志，并加入如下内容：

```txt
20210301121533,login,北京,118.128.11.31,0001
20210301121536,login,上海,10.90.113.150,0002
20210301121544,login,成都,112.112.31.33,0003
20210301121559,login,成都,101.132.93.24,0004
20210301121612,login,上海,189.112.89.78,0005
20210301121638,login,北京,113.52.101.50,0006
```

以上内容的每一行表示一条用户登录的日志，以逗号分隔，从左往右分别表示登录时间、用户行为、登录城市、登录 IP、UserID，IP 地址和城市不对应，请忽略！然后在 `com.shiyanlou.table` 包下创建 `TableTest` object，代码如下：

```scala
package com.shiyanlou.table

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}

object TableTest {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //初始化Table API的上下文环境
    val tableEnv = BatchTableEnvironment.create(env)

    tableEnv
      .connect(new FileSystem().path("/home/shiyanlou/userlog.log"))
      .withFormat(new OldCsv())
      .withSchema(new Schema()
        .field("time", DataTypes.BIGINT())
        .field("action", DataTypes.STRING())
        .field("city", DataTypes.STRING())
        .field("IP", DataTypes.STRING())
        .field("user_id", DataTypes.BIGINT())
      )
      .createTemporaryTable("temp_userlog")

    tableEnv.from("temp_userlog").printSchema()
  }
}
```

上面代码中的 `env` 对象和 `tableEnv` 对象的类型，都是批计算才会用到的。创建好 `tableEnv` 之后，读取了 `/home/shiyanlou/userlog.log` 文件，并通过 `withFormat(new OldCsv())` 指定数据源格式为 `csv`。注意，`csv` 类型的文件指的是以英文逗号分隔的文本文件，并非必须是 `.csv` 扩展名。通过`withSchema`方法指定了 Table 的结构信息，最后打印了 `temp_userlog` 表的结构信息，如下所示：

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/84caeadb27d04a7bf4788f614f6b1b35-0)

通过流计算创建 Table 的代码如下所示：

```scala
// StreamTableTest.scala
package com.shiyanlou.table

import org.apache.flink.table.api.scala._

object StreamTableTest {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.streaming.api.scala._
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(streamEnv)
    val data = streamEnv.socketTextStream("localhost", 9999)
      .map(line => {
        val arr = line.split(",")

        UserLog(arr(0).toLong, arr(1), arr(2), arr(3), arr(4).toLong)
      })

    val table = tableEnv.fromDataStream(data)
    table.printSchema()

    streamEnv.execute()
  }

  case class UserLog(time: Long, action: String, city: String, ip: String, user_id: Long)
}
```

和批计算的方式相比，流计算我们使用了 `StreamExecutionEnvironment` 和 `StreamTableEnvironment` ，在终端运行 `nc -l -p 9999` 之后打印的结果如下所示：

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/c47cabfa3cc19bde9a47206d3b0001e8-0)

有同学可能会好奇，我们并没有通过 Socket 发送数据，它怎么就知道表结构并打印了呢？因为我们在 `map` 方法中已经将其转换为 `UserLog` 类型了，所以 Table 的结构和 UserLog 是一致的。

#### 修改字段名

Flink 中支持按照字段的位置进行字段重命名和通过 `as` 关键字进行字段重命名，但是无论通过哪种方式都需要导入 Flink Table 的隐式转换：

```scala
import org.apache.flink.table.api.scala._
```

基于位置的方式：

```scala
val table = tableEnv.fromDataStream(data, 'time2, 'action2, 'city2, 'ip2, 'user_id2)
```

运行结果如下：

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/a1e52a147358168a03170164fb7080ae-0)

基于 `as` 关键字的方式目前不支持修改全部字段，所以我们修改除 `time` 以外的其它字段：

```scala
val table = tableEnv.fromDataStream(data, 'time, 'action as 'action2, 'city as 'city2, 'ip as 'ip2, 'user_id as 'user_id2)
```

运行结果如下：

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/8cf5a9cc3b64131749d1402997628a0f-0)

#### 查询

假设我们要过滤出城市为北京和成都的用户，并分别统计这两个城市中的用户数量，使用 SQL 应该是这样的：

```sql
select
        city, count(user_id) as cnt
from
        temp_userlog
where
        city = '北京' or city = '成都'
group by
    city
```

对应到 Flink Table API 应该是：

```scala
package com.shiyanlou.table

import org.apache.flink.types.Row

object StreamTableTest {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api.scala._
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(streamEnv)
    val data = streamEnv.socketTextStream("localhost", 9999)
      .map(line => {
        val arr = line.split(",")

        UserLog(arr(0).toLong, arr(1), arr(2), arr(3), arr(4))
      })

    val table = tableEnv.fromDataStream(data)

    val res = table
      .where('city === "北京" || 'city === "成都")
      .groupBy('city)
      .select('city, 'user_id.count as 'cnt )

    tableEnv
      .toRetractStream[Row](res)
      .print()

    tableEnv.execute("Table API")
  }

  case class UserLog(time: Long, action: String, city: String, ip: String, user_id: String)
}
```

其中

```
where('city === "北京" || 'city === "成都")
```

等同于

`filter('city === "北京" || 'city === "成都")`，

```
groupBy('city)
```

等同于

```
groupBy("city")
```

在终端执行 `nc -l -p 9999` 并运行以上代码，然后在终端输入以下内容：

```txt
20210301121533,login,北京,118.128.11.31,0001
20210301121536,login,上海,10.90.113.150,0002
20210301121544,login,成都,112.112.31.33,0003
20210301121559,login,成都,101.132.93.24,0004
20210301121612,login,上海,189.112.89.78,0005
20210301121638,login,北京,113.52.101.50,0006
```

运行结果如下：

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/eb2256c2c16f65a04029f40db2a45a56-0)

最终输出的类型为 `(Boolean, T)`，最前面的布尔值代表的是数据更新类型，True 对应的是 Insert 操作更新的数据，而 False 对应的是 Delete 操作更新的数据。当第一条北京的数据出现时，属于 Insert 操作，当第二条北京的数据出现时，后面的统计结果由 1 变为 2，所以之前为 1 的那条数据就会被 Delete，所以会对应到 False 输出一次；成都对应的数据也是同理，当第一条成都的数据出现时，属于 Insert 操作，而当第二条数据出现的时候，原来的统计结果为 1 的数据会被 Delete，所以会对应为 False。如果只需要输出 True 的数据，可以做如下修改：

```scala
tableEnv
  .toRetractStream[Row](res)
  .filter(_._1 == true)
  .print()
```

重新在控制台输入相同的日志数据，运行结果如下：

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/262bfc18c974dc87053bb56ee5b225bd-0)

![图片描述](https://doc.shiyanlou.com/courses/3423/1561942/f2851fc937eb72bd0792a0e70fd82e91-0)