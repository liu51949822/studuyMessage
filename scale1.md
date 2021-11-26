开始使用 Scala 的最简单的方式是使用交互式 Scala 解释器，只要输入 Scala 表达式，Scala 解释器会立即解释执行该语句并输出结果。当然你也可以使用如 Scala IDE 或 IntelliJ IDEA 集成开发环境。不过本教程开始还是以这种交互式 Scala 解释器为主。

使用 Scala 解释器，首先你需要下载安装 Scala 运行环境。然后在命令行输入 `scala`，则进入 scala 解释器，下图为 Linux 环境下 scala 解释器界面：

![2-3.1-1](https://doc.shiyanlou.com/document-uid702660labid6308timestamp1525419891893.png)

你可以使用 `:help` 命令列出一些常用的 Scala 解释器命令。

退出 Scala 解释器，输入：

```scala
:quit
```

在 `scala >` 提示符下，你可以输入任意的 Scala 表达式，比如输入 `1+2`，解释器显示：

```scala
1+2
```

![2-3.1-2](https://doc.shiyanlou.com/document-uid702660labid6308timestamp1525420016153.png)

这行显示包括：

- 一个由 Scala 解释器自动生成的变量名或者由你指定的变量名用来指向计算出来的结果，比如 `res0` 代表 `result0` 变量。
- 一个冒号，后面紧跟个变量类型比如 `Int`。
- 一个等于号 `=`。
- 计算结果，本例为 `1+2` 的结果 `3`。

`resX` 变量名会用在之后的表达式中，比如：此时 `res0=3`，如果输入 `res0 *3`，则显示 `res1: Int = 9`。



Scala 定义了两种类型的变量 `val` 和 `var`，`val` 类似于 Java 中的 `final` 变量，一旦初始化之后，不可以重新赋值（我们可以称它为 `常变量`）。而 `var` 类似于一般的非 `final` 变量。可以任意重新赋值。

比如定义一个字符串常变量：

```scala
scala> val msg = "Hello,World"
msg: String = Hello,World
```

这个表达式定义了一个 `msg` 变量，为字符串常量。它的类型为 `string`（`java.lang.string`）。可以看到我们在定义这个变量时并不需要像 Java 一样定义其类型，Scala 可以根据赋值的内容推算出变量的类型。这在 Scala 语言中称为 “type inference”（类型推断）。当然如果你愿意，你也可以采用和 Java 一样的方法，明确指定变量的类型，如：

```scala
scala> val msg2:String = "Hello again,world"
msg2: String = Hello again,world
```

不过这样写就显得不像 Scala 风格了。此外 Scala 语句也不需要以分号结尾。如果在命令行中需要分多行输入，Scala 解释器在新行前面显示 `|`，表示该行接着上一行。比如：

```scala
scala> val msg3 =
     | "Hello world 3rd time"
msg3: String = Hello world 3rd time
```



Scala 既是面向对象的编程语言，也是面向函数的编程语言，因此函数在 Scala 语言中的地位和类是同等的。下面的代码定义了一个简单的函数求两个值的最大值：

```scala
scala> def max(x:Int,y:Int) : Int ={
     | if (x >y) x
     | else
     | y
     | }
max: (x: Int, y: Int)Int
```

Scala 函数以 `def` 定义，之后是函数的名称（如 `max`），然后是以逗号分隔的参数。Scala 中变量类型是放在参数和变量的后面，以 `：` 隔开。这样做的一个好处是便于 “type inference”。刚开始有些不习惯（如果你是 Pascal 程序员可能会觉得很亲切）。同样如果函数需要返回值，它的类型也是定义在参数的后面，实际上每个 Scala 函数都有返回值，只是有些返回值类型为 `Unit`，类似于 `void` 类型。

此外每个 Scala 表达式都有返回结果（这一点和 Java，C# 等语言不同），比如 Scala 的 `if else` 语句也是有返回值的，因此函数返回结果无需使用 `return` 语句。实际上在 Scala 代码中应当尽量避免使用 `return` 语句。函数的最后一个表达式的值就可以作为函数的结果进行返回。

同样由于 Scala 的 “type inference” 特点，本例其实无需指定返回值的类型。对于大多数函数 Scala 都可以推测出函数返回值的类型，但目前来说回溯函数（函数调用自身）还是需要指明返回结果类型的。

下面再定义个“没有”返回结果的函数，其它语言可能称这种无返回值的函数为程式。

```scala
scala> def greet() = println("hello,world")
greet: ()Unit
```

`greet` 函数的返回值类型为 `Unit`，表示该函数不返回任何有意义的值，`Unit` 类似于 Java 中的 `void` 类型。这种类型的函数主要用来获得函数的“副作用”，比如本函数的副作用是打印招呼语。

下面的代码使用 `while` 实现一个循环：

```scala
val args = Array("I","like","scala")
var i = 0
while (i < args.length) {
  println (args(i))
  i += 1
}
```

![2-3.4-1](https://doc.shiyanlou.com/document-uid702660labid6308timestamp1525422127718.png)

这里要注意的是 Scala 不支持 `++i` 和 `i++` 运算符，因此需要使用 `i += 1` 来加一。 这段代码看起来和 Java 代码差不多，实际上 `while` 也是一个函数，你自然可以利用 Scala 语言的扩展性，实现 `while` 语句，使它看起来和 Scala 语言自带的关键字一样调用。

Scala 访问数组的语法是使用 `()` 而非 `[]`。

这里介绍了使用 `while` 来实现循环，但这种实现循环的方法并不是最好的 Scala 风格，在下一步介绍使用一种更好的方法来避免通过索引来枚举数组元素。



在上节实验 `使用 while 实现循环` 中使用 `while` 来实现循环，和使用 Java 实现无太大差异，而 Scala 是面向函数的语言，更好的方法是采用“函数式”风格来编写代码。比如上面的循环，使用 `foreach` 方法如下：

```scala
args.foreach(arg => println(arg))
```

![2-3.5-1](https://doc.shiyanlou.com/document-uid702660labid6308timestamp1525422439291.png)

该表达式，调用 `args` 的 `foreach` 方法，传入一个参数，这个参数类型也是一个函数（`lambda` 表达式，和 C# 中概念类似）。这段代码可以再写得精简些，你可以利用 Scala 支持的缩写形式，如果一个函数只有一个参数并且只包含一个表达式，那么你无需明确指明参数。因此上面的代码可以写成：

```scala
args.foreach(println)
```

![2-3.5-2](https://doc.shiyanlou.com/document-uid702660labid6308timestamp1525422456719.png)

Scala 中也提供了一个称为 “for comprehension” 的功能，它比 Java 中的 `for` 功能更强大。“for comprehension”（可称之为 for 表达式）将在后面介绍，这里先使用 `for` 来实现前面的例子：

```scala
for (arg <-args)
  println(arg)
```

![2-3.5-3](https://doc.shiyanlou.com/document-uid702660labid6308timestamp1525422479443.png)

在 Scala 中，你可以使用 `new` 来实例化一个类。当你创建一个对象的实例时，你可以使用数值或类型参数。如果使用类型参数，它的作用类似 Java 或 .Net 的 `Generic` 类型。所不同的是，Scala 使用方括号来指明数据类型参数，而非尖括号。比如：

```scala
val greetStrings = new Array[String](3)
greetStrings(0) = "Hello"
greetStrings(1) = ","
greetStrings(2) = "world!\n"
for(i <- 0 to 2)
  print(greetStrings(i))
```

![2-3.6-1](https://doc.shiyanlou.com/document-uid702660labid6308timestamp1525423084634.png)

可以看到 Scala 使用 `[]` 来为数组指明类型化参数，本例使用 `String` 类型，数组使用 `()` 而非 `[]` 来指明数组的索引。其中的 `for` 表达式中使用到 `0 to 2`，这个表达式演示了 Scala 的一个基本规则，如果一个方法只有一个参数，你可以不用括号和 `.` 来调用这个方法。

因此这里的 `0 to 2`，其实为 `(0).to(2)` 调用的为整数类型的 `to` 方法，`to` 方法使用一个参数。Scala 中所有的基本数据类型也是对象（和 Java 不同），因此 `0` 可以有方法（实际上调用的是 `RichInt` 的方法），这种只有一个参数的方法可以使用操作符的写法（不用 `.` 和括号），实际上 Scala 中表达式 `1+2`，最终解释为 `(1).+(2)+`，这也是 `Int` 的一个方法，和 Java 不同的是，Scala 对方法的名称没有太多的限制，你可以使用符号作为方法的名称。

这里也说明为什么 Scala 中使用 `()` 来访问数组元素，在 Scala 中，数组和其它普遍的类定义一样，没有什么特别之处，当你在某个值后面使用 `()` 时，Scala 将其翻译成对应对象的 `apply` 方法。因此本例中 `greetStrings(1)` 其实是调用 `greetString.apply(1)` 方法。这种表达方法不仅仅只限于数组，对于任何对象，如果在其后面使用 `()`，都将调用该对象的 apply 方法。同样的如果对某个使用 `()` 的对象赋值，比如：

```scala
greetStrings(0) = "Hello"
```

Scala 将这种赋值转换为该对象的 `update` 方法，也就是 `greetStrings.update(0,”hello”)`。因此上面的例子，使用传统的方法调用可以写成：

```scala
val greetStrings = new Array[String](3)
greetStrings.update(0,"Hello")
greetStrings.update(1,",")
greetStrings.update(2,"world!\n")
for(i <- 0 to 2)
  print(greetStrings.apply(i))
```

![2-3.6-2](https://doc.shiyanlou.com/document-uid702660labid6308timestamp1525423104146.png)

从这点来说，数组在 Scala 中并不是某种特殊的数据类型，和普通的类没有什么不同。

不过 Scala 还是提供了初始化数组的简单方法，比如上面的例子数组可以使用如下代码：

```scala
val greetStrings = Array("Hello",",","World\n")
```

![2-3.6-3](https://doc.shiyanlou.com/document-uid702660labid6308timestamp1525423123443.png)

这里使用 `()` 其实还是调用 Array 类的关联对象 Array 的 apply 方法，也就是：

```scala
val greetStrings = Array.apply("Hello",",","World\n")
```

![2-3.6-4](https://doc.shiyanlou.com/document-uid702660labid6308timestamp1525423193052.png)

Scala 也是一个面向函数的编程语言，面向函数的编程语言的一个特点是，调用某个方法不应该有任何副作用，参数一定，调用该方法后，返回一定的结果，而不会去修改程序的其它状态（副作用）。这样做的一个好处是方法和方法之间关联性较小，从而方法变得更可靠和重用性高。使用这个原则也就意味着需要把变量设成不可修改的，这也就避免了多线程访问的互锁问题。

前面介绍的数组，它的元素是可以被修改的。如果需要使用不可以修改的序列，Scala 中提供了 `Lists` 类。和 Java 的 `List` 不同，Scala 的 `Lists` 对象是不可修改的。它被设计用来满足函数编程风格的代码。它有点像 Java 的 `String`，`String` 也是不可以修改的，如果需要可以修改的 `String` 对像，可以使用 `StringBuilder` 类。

比如下面的代码：

```scala
val oneTwo = List(1,2)
val threeFour = List(3,4)
val oneTwoThreeFour = oneTwo ::: threeFour
println (oneTwo + " and " + threeFour + " were not mutated.")
println ("Thus, " + oneTwoThreeFour + " is a new list")
```

![2-3.7-1](https://doc.shiyanlou.com/document-uid702660labid6308timestamp1525423642173.png)

定义了两个 `List` 对象 `oneTwo` 和 `threeFour`，然后通过 `:::` 操作符（其实为 `:::` 方法）将两个列表连接起来。实际上由于 `List` 的不可修改特性，Scala 创建了一个新的 `List` 对象 `oneTwoThreeFour` 来保存两个列表连接后的值。

`List` 也提供了一个 `::` 方法用来向 `List` 中添加一个元素，`::` 方法（操作符）是右操作符，也就是使用 `::` 右边的对象来调用它的 `::` 方法，Scala 中规定所有以 `：` 开头的操作符都是右操作符，因此如果你自己定义以 `：` 开头的方法（操作符）也是右操作符。

如下面使用常量创建一个列表：

```scala
val oneTwoThree = 1 :: 2 ::3 :: Nil
println(oneTwoThree)
```

![2-3.7-2](https://doc.shiyanlou.com/document-uid702660labid6308timestamp1525423696095.png)

调用空列表对象 `Nil` 的 `::` 方法 也就是：

```scala
val oneTwoThree =  Nil.::(3).::(2).::(1)
```

![2-3.7-3](https://doc.shiyanlou.com/document-uid702660labid6308timestamp1525423711665.png)

Scala 的 `List` 类还定义了其它很多很有用的方法，比如 `head`、`last`、`length`、`reverse`、`tail` 等。这里就不一一说明了，具体可以参考 `List` 的文档。

Scala 中另外一个很有用的容器类为 `Tuples`，和 `List` 不同的是，`Tuples` 可以包含不同类型的数据，而 `List` 只能包含同类型的数据。`Tuples` 在方法需要返回多个结果时非常有用。`Tuple` 对应到数学的 `矢量` 的概念。

一旦定义了一个元组，可以使用 `._` 和 `索引` 来访问元组的元素。矢量的分量，注意和数组不同的是，元组的索引从 1 开始。

```scala
val pair = (99,"Luftballons")
println(pair._1)
println(pair._2)
```

![2-3.8-1](https://doc.shiyanlou.com/document-uid702660labid6308timestamp1525423819481.png)

元组的实际类型取决于它的分量的类型，比如上面 `pair` 的类型实际为 `Tuple2[Int,String]`，而 `(‘u’,’r’,”the”,1,4,”me”)` 的类型为 `Tuple6[Char,Char,String,Int,Int,String]`。

目前 Scala 支持的元组的最大长度为 22。如果有需要，你可以自己扩展更长的元组。

Scala Set（集合）是没有重复对象的集合，所有的元素都是唯一的。

Scala 语言的一个设计目标是让程序员可以同时利用面向对象和面向函数的方法编写代码，因此它提供的集合类分成了可以修改的集合类和不可以修改的集合类两大类型。比如 `Array` 总是可以修改内容的，而 `List` 总是不可以修改内容的。类似的情况，`Scala` 也提供了两种 `Sets` 和 `Maps` 集合类。

比如 Scala API 定义了 `Set` 的 `基Trait` 类型 `Set`。`Trait` 的概念类似于 Java 中的 `Interface`，所不同的是 Scala 中的 `Trait` 可以有方法的实现，分两个包定义 `Mutable`（可变）和 `Immutable`（不可变），使用同样名称的子 `Trait`。下图为 `Trait` 和类的基础关系：

![2-3.9-1](https://doc.shiyanlou.com/document-uid162034labid1679timestamp1453867993618.png)

使用 `Set` 的基本方法如下：

```scala
var jetSet = Set ("Boeing","Airbus")
jetSet += "Lear"
println(jetSet.contains("Cessna"))
```

![2-3.9-2](https://doc.shiyanlou.com/document-uid702660labid6308timestamp1525424179809.png)

缺省情况 `Set` 为 `Immutable Set`，如果你需要使用可修改的集合类（`Set` 类型），你可以使用全路径来指明 `Set`，比如 `scala.collection.mutable.Set`。

Scala 提供的另外一个类型为 `Map` 类型，Map（映射）是一种可迭代的 key/value 结构，所有的值都可以通过键来获取。Scala 也提供了 `Mutable` 和 `Immutable` 两种 Map 类型，如下图所示。

![2-3.9-3](https://doc.shiyanlou.com/document-uid162034labid1679timestamp1453868227251.png)

Map 的基本用法如下（Map 类似于其它语言中的关联数组，如 PHP）

```scala
val romanNumeral = Map ( 1 -> "I" , 2 -> "II",
  3 -> "III", 4 -> "IV", 5 -> "V")
println (romanNumeral(4))
```

![2-3.9-4](https://doc.shiyanlou.com/document-uid702660labid6308timestamp1525424195697.png)





Scala 语言的一个特点是支持面向函数编程，因此学习 Scala 的一个重要方面是改变之前的指令式编程思想（尤其是来自 Java 或 C# 背景的程序员），观念要向函数式编程转变。首先在看代码上要认识哪种是指令编程，哪种是函数式编程。实现这种思想上的转变，不仅仅会使你成为一个更好的 Scala 程序员，同时也会扩展你的视野，使你成为一个更好的程序员。

一个简单的原则，如果代码中含有 `var` 类型的变量，这段代码就是传统的指令式编程，如果代码只有 `val` 变量，这段代码就很有可能是函数式代码，因此学会函数式编程关键是不使用 `var` 来编写代码。

来看一个简单的例子：

```scala
def printArgs ( args: Array[String]) : Unit ={
    var i = 0
    while (i < args.length) {
      println (args(i))
      i += 1
    }
}
```

来自 Java 背景的程序员开始写 Scala 代码很有可能写成上面的实现。我们试着去除 `var` 变量，可以写成更符合函数式编程的代码：

```scala
def printArgs ( args: Array[String]) : Unit ={
    for( arg <- args)
      println(arg)
}
```

![2-3.10-1](https://doc.shiyanlou.com/document-uid702660labid6308timestamp1525424671068.png)

或者更简化为：

```scala
def printArgs ( args: Array[String]) : Unit ={
    args.foreach(println)
}
```

![2-3.10-2](https://doc.shiyanlou.com/document-uid702660labid6308timestamp1525424683170.png)

这个例子也说明了尽量少用 `var` 的好处，代码更简洁明了，从而也可以减少错误的发生。因此 Scala 编程的一个基本原则是，能不用 `var`，尽量不用 `var`，能不用 `mutable` 变量，尽量不用 `mutable` 变量，能避免函数的副作用，就尽量不产生副作用。

使用脚本实现某个任务，通常需要读取文件，本节介绍 Scala 读写文件的基本方法。比如下面的例子读取文件的每行，把该行字符长度添加到行首：

```scala
import scala.io.Source

var args = Source.fromFile("/home/hadoop/test.txt") #记得在对应目录创建test.txt 文件，并写入一些内容

for( line <- args.getLines)
    println(line.length + "" + line)
```

![2-3.11-1](https://doc.shiyanlou.com/document-uid702660labid6308timestamp1525424968852.png) 可以看到 Scala 引入包的方式和 Java 类似，也是通过 `import` 语句。文件相关的类定义在 `scala.io` 包中。如果需要引入多个类，Scala 使用 `_` 而非 `*`。



首先介绍 Scala 的类定义，我们以一个简单的例子开始，创建一个计算整数累计校验和的类 `ChecksumAccumulator`。

```scala
class ChecksumAccumulator{
   private var sum = 0
   def add(b:Byte) :Unit = sum += b
   def checksum() : Int = ~ (sum & 0xFF) +1
}
```

![3-3.1-1](https://doc.shiyanlou.com/document-uid702660labid6310timestamp1525425822990.png)

可以看到 Scala 类定义和 Java 非常类似，也是以 `class` 开始，和 Java 不同的，Scala 的缺省修饰符为`public`，也就是如果不带有访问范围的修饰符 `public`、`protected`、`private` 等，Scala 将默认定义为 `public`。类的方法以 `def` 定义开始，要注意的是 Scala 的方法的参数都是 `val`类型，而不是 `var` 类型，因此在函数体内不可以修改参数的值，比如：如果你修改 `add` 方法如下：

```scala
def add(b:Byte) :Unit ={
      b = 1
      sum += b
  }
```

此时编译器会报错：

![3-3.1-2](https://doc.shiyanlou.com/document-uid702660labid6310timestamp1525425844215.png)

类的方法分两种，一种是有返回值的，一种是不含返回值的，没有返回值的主要是利用代码的“副作用”，比如修改类的成员变量的值或者读写文件等。Scala 内部其实将这种函数的返回值定为 `Unit`（类同 Java 的 void 类型），对于这种类型的方法，可以省略掉 `=` 号，因此如果你希望函数返回某个值，但忘了方法定义中的 `=`，Scala 会忽略方法的返回值，而返回 `Unit`。

再强调一下，Scala 代码无需使用 `；` 结尾，也不需要使用 `return` 返回值，函数的最后一行的值就作为函数的返回值。

但如果你需要在一行中书写多个语句，此时需要使用 `；` 隔开，不过不建议这么做。你也可以把一条语句分成几行书写，Scala 编译器大部分情况下会推算出语句的结尾，不过这样也不是一个好的编码习惯。



Scala 比 Java 更加面向对象，这是因为 Scala 不允许类包含静态元素（静态变量或静态方法）。在 Scala 中提供类似功能的是称为“`Singleton`（单例对象）”的对象。在 Scala 中定义 `Singleton` 对象的方法，除了使用 `object` 而非 `class` 关键字外，其它方式和类定义非常类似。下面例子创建一个 `ChecksumAccumulator` 对象：

```scala
object ChecksumAccumulator {
   private val cache = Map [String, Int] ()
   def calculate(s:String) : Int =
      if(cache.contains(s))
         cache(s)
      else {
         val acc = new ChecksumAccumulator
         for( c <- s)
            acc.add(c.toByte)
         val cs = acc.checksum()
         cache += ( s -> cs)
         cs
       }
}
```

这个对象和上一个创建的类 `ChecksumAccumulator` 同名，这在 Scala 中把这个对象称为其同名的类的“伴侣”对象（Companion object)。如果你需要定义类的 `companion` 对象，Scala 要求你把这两个定义放在同一个文件中。类和其 `companion` 对象可以互相访问对方的私有成员。

如果你是 Java 程序员，可以把 `Singleton` 对象看成以前 Java 定义静态成员的地方。你可以使用类似 Java 静态方法的方式调用 `Singleton` 对象的方法，比如下面为这个例子完整的代码：

```scala
import scala.collection.mutable.Map

class ChecksumAccumulator{
   private var sum = 0
   def add(b:Byte) :Unit = sum +=b
   def checksum() : Int = ~ (sum & 0xFF) +1
}

object ChecksumAccumulator {
   private val cache = Map [String, Int] ()
   def calculate(s:String) : Int =
      if(cache.contains(s))
         cache(s)
      else {
         val acc = new ChecksumAccumulator
         for( c <- s)
            acc.add(c.toByte)
         val cs = acc.checksum()
         cache += ( s -> cs)
         cs
       }
}

println ( ChecksumAccumulator.calculate("Welcome to Scala Chinese community"))
```

![3-3.2-1](https://doc.shiyanlou.com/document-uid702660labid6310timestamp1525426620973.png)

Scala 的 `singleton` 对象不仅仅局限于作为静态对象的容器，它在 Scala 中也是头等公民，但仅仅定义 `Singleton` 对象本身不会创建一个新的类型，你不可以使用 `new` 再创建一个新的 `Singleton` 对象（这也是 Singleton 名字的由来），此外和类定义不同的是，`singleton` 对象不可以带参数。类定义参数将在后面的内容中介绍。

回过头来看看我们的第一个例子 “Hello World”：

```scala
object HelloWorld {
  def main(args: Array[String]) {
    println("Hello, world!")
  }
}
```

这是一个最简单的 Scala 程序，`HelloWorld` 是一个 `Singleton` 对象，它包含一个 `main` 方法（可以支持命令行参数）。和 Java 类似，Scala 中任何 `Singleton` 对象，如果包含 `main` 方法，都可以作为应用的入口点。

在这里要说明一点的是，在 Scala 中不要求 `public` 类定义和其文件名同名，不过使用 `public` 类和文件同名还是有它的优点的，你可以根据个人喜好决定是否遵循 Java 文件命名风格。

最后提一下 Scala 的 `Trait` 功能，Scala 的 `Trait` 和 Java 的 `Interface` 相比，可以有方法的实现（这里有点像抽象类，但如果是抽象类，就不会允许继承多个抽象类）。Scala 的 `Trait` 支持类和 `Singleton` 对象以及多个 `Trait` 混合（使用来自这些 `Trait` 中的方法，而不是不违反单一继承的原则）。

如果你是个 Java 程序员，你会发现 Java 支持的基本数据类型，Scala 都有对应的支持，不过 Scala 的数据类型都是对象（比如整数），这些基本类型都可以通过隐式自动转换的形式支持比 Java 基本数据类型更多的方法。

隐式自动转换的概念将在后面介绍，简单的说就是可以为基本类型提供扩展，比如调用 `(-1).abs()`，Scala 发现基本类型 `Int` 没有提供 `abs()` 方法，但可以发现系统提供了从 `Int` 类型转换为 `RichInt` 的隐式自动转换，而 `RichInt` 具有 `abs` 方法，那么 Scala 就自动将 `1` 转换为 `RichInt` 类型，然后调用 `RichInt` 的 `abs` 方法。

Scala 的基本数据类型有：`Byte`、`Short`、`Int`、`Long` 和 `Char`，这些称为整数类型。整数类型加上 `Float` 和 `Double` 称为数值类型。此外还有 `String` 类型，除 `String` 类型在 `java.lang` 包中定义，其它的类型都定义在包 scala 中。比如 `Int` 的全名为 `scala.Int`。实际上 Scala 运行环境自动会载入包 `scala` 和 `java.lang` 中定义的数据类型，你可以直接使用 `Int`、`Short`、`String` 而无需再引入包或是使用全称。

下面的例子给出了这些基本数据类型的字面量用法，由于 Scala 支持数据类型推断，你在定义变量时多数可以不指明数据类型，而是由 Scala 运行环境自动给出变量的数据类型：

```scala
Welcome to Scala 2.11.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_171).
Type in expressions for evaluation. Or try :help.

scala> var hex = 0x5
hex: Int = 5

scala> var hex2 = 0x00ff
hex2: Int = 255

scala> val prog = 0xcafebabel
prog: Long = 3405691582

scala> val littler:Byte = 38
littler: Byte = 38

scala> val big = 1.23232
big: Double = 1.23232

scala> val a = 'A'
a: Char = A

scala> val f ='\u0041'
f: Char = A

scala> val hello = "hello"
hello: String = hello

scala> val longString = """ Welcome to Ultamix 3000. Type "Help" for help."""
longString: String = " Welcome to Ultamix 3000. Type "Help" for help."

scala> val bool = true
bool: Boolean = true

scala>
```

Scala 的基本数据类型的字面量也支持方法。这点和 Java 不同，Scala 中所有的数值字面量也是对象，比如：

```scala
scala> (-2.7).abs
res3: Double = 2.7

scala> -2.7 abs
warning: there were 1 feature warning(s); re-run with -feature for details
res4: Double = 2.7

scala> 0 max 5
res5: Int = 5

scala> 4 to 6
res6: scala.collection.immutable.Range.Inclusive = Range(4, 5, 6)
```

这些方法其实是对于数据类型的 `Rich` 类型的方法，`Rich` 类型将在后面再做详细介绍。



Scala 提供了丰富的运算符用来操作前面介绍的基本数据类型。前面说过，这些运算符（操作符）实际为普通类方法的简化（或者称为美化）表示。比如 `1+2`，实际为 `(1).+(2)`，也就是调用 `Int` 类型的 `+` 方法。

例如：

```scala
scala> val sumMore = (1).+(2)
sumMore: Int = 3
```

实际上类 `Int` 定义了多个 `+` 方法的重载方法（以支持不同的数据类型），比如和 `Long` 类型相加。

`+` 符号是一个运算符，并且是一个中缀运算符。在 Scala 中你可以定义任何方法为一操作符。比如 `String` 的 `IndexOf` 方法也可以使用操作符的语法来书写。例如：

```scala
scala> val s = "Hello, World"
s: String = Hello, World

scala> s indexOf 'o'
res0: Int = 4
```

由此可以看出，运算符在 Scala 中并不是什么特殊的语法，任何 Scala 方法都可以作为操作符来使用。是否是操作符取决于你如何使用这个方法，当你使用 `s.indexOf('o')` 时，`indexOf` 不是一个运算符。而你写成 `s indexOf 'o'`，`indexOf` 就是一个操作符，因为你使用了操作符的语法。

除了类似 `+` 的中缀运算符（操作符在两个操作符之间），还可以有前缀运算符和后缀运算符。顾名思义，前缀运算符的操作符在操作数前面，比如 `-7` 前面的 `-`。后缀运算符的运算符在操作数的后面，比如 `7 toLong` 中的 `toLong`。前缀和后缀操作符都使用一个操作数，而中缀运算符使用前后两个操作数。Scala 在实现前缀和后缀操作符的方法，其方法名都以 `unary_-` 开头。

比如，表达式 `-2.0` 实际上调用 `(2.0).unary_-`方法。

```scala
scala> -2.0
res1: Double = -2.0

scala> (2.0).unary_-
res2: Double = -2.0
```

如果你需要定义前缀方法，你只能使用 `+`、`-`、`!` 和 `~` 四个符号作为前缀操作符。

后缀操作符在不使用 `.` 和括号调用时不带任何参数。在 Scala 中，你可以省略掉没有参数的方法调用的空括号。按照惯例，如果你调用方法是为了利用方法的“副作用”，此时写上空括号，如果方法没有任何副作用（没有修改其它程序状态），你可以省略掉括号。

比如：

```scala
scala> val s = "Hello, World"
s: String = Hello, World

scala> s toLowerCase
warning: there was one feature warning; re-run with -feature for details
res0: String = hello, world
```

具体 Scala 的基本数据类型支持的操作符，可以参考 Scala API 文档。下面以示例介绍一些常用的操作符：

#### 算术运算符（包括 `+`、`–`、`×` 和 `/`）

```scala
scala> 1.2 + 2.3
res0: Double = 3.5

scala> 'b' -'a'
res1: Int = 1

scala> 11 % 4   #取余
res2: Int = 3

scala> 11.0f / 4.0f
res3: Float = 2.75

scala> 2L * 3L
res4: Long = 6
```

#### 关系和逻辑运算符（包括 `>`、`<`、`>=` 和 `!` 等）

```scala
scala> 1 >2
res5: Boolean = false

scala> 1.0 <= 1.0
res6: Boolean = true

scala> val thisIsBoring = !true
thisIsBoring: Boolean = false

scala> !thisIsBoring
res7: Boolean = true

scala> val toBe = true
toBe: Boolean = true

scala> val question = toBe || ! toBe  #表示逻辑或
question: Boolean = true
```

要注意的是，逻辑运算支持“短路运算”，比如 `op1 || op2`，当 `op1 = true`，`op2` 无需再计算，就可以知道结果为 `true`。这时 `op2` 表示式不会执行。例如：

```bash
scala> def salt()= { println("salt");false}
salt: ()Boolean

scala> def pepper() = {println("pepper");true}
pepper: ()Boolean

scala> pepper() && salt()   #表示逻辑与
pepper
salt
res0: Boolean = false

scala> salt() && pepper()
salt
res1: Boolean = false
```

#### 位操作符（包括 `&`、`|`、`^`、`~`）

```scala
scala> 1 & 2
res2: Int = 0

scala> 1 | 2
res3: Int = 3

scala> 1 ^ 2
res4: Int = 3

scala> ~1
res5: Int = -2
```

#### 对象恒等比较

如果需要比较两个对象是否相等，可以使用 `==` 和 `!=` 操作符。

```scala
scala> 1 == 2
res6: Boolean = false

scala> 1 !=2
res7: Boolean = true

scala> List(1,2,3) == List (1,2,3)
res8: Boolean = true

scala> ("he"+"llo") == "hello"
res9: Boolean = true
```

Scala 的 `==` 和 Java 不同，scala 的 `==` 只用于比较两个对象的**值**是否相同。而对于引用类型的比较使用另外的操作符 `eq` 和 `ne`。

#### 操作符的优先级和左右结合性

Scala 的操作符的优先级和 Java 基本相同，如果有困惑时，可以使用 `()` 改变操作符的优先级。操作符一般为左结合，Scala 规定了操作符的结合性由操作符的最后一个字符定义。对于以 `：` 结尾的操作符都是右结合，其它的操作符多是左结合。

例如：`a*b` 为 `a.*(b)`，而 `a:::b` 为 `b.:::(a)`，而 `a:::b:::c` 等价于 `a::: (b ::: c)`，`a*b*c` 等价于 `(a*b)*c`。



Scala 提供了丰富的运算符用来操作前面介绍的基本数据类型。前面说过，这些运算符（操作符）实际为普通类方法的简化（或者称为美化）表示。比如 `1+2`，实际为 `(1).+(2)`，也就是调用 `Int` 类型的 `+` 方法。

例如：

```scala
scala> val sumMore = (1).+(2)
sumMore: Int = 3
```

实际上类 `Int` 定义了多个 `+` 方法的重载方法（以支持不同的数据类型），比如和 `Long` 类型相加。

`+` 符号是一个运算符，并且是一个中缀运算符。在 Scala 中你可以定义任何方法为一操作符。比如 `String` 的 `IndexOf` 方法也可以使用操作符的语法来书写。例如：

```scala
scala> val s = "Hello, World"
s: String = Hello, World

scala> s indexOf 'o'
res0: Int = 4
```

由此可以看出，运算符在 Scala 中并不是什么特殊的语法，任何 Scala 方法都可以作为操作符来使用。是否是操作符取决于你如何使用这个方法，当你使用 `s.indexOf('o')` 时，`indexOf` 不是一个运算符。而你写成 `s indexOf 'o'`，`indexOf` 就是一个操作符，因为你使用了操作符的语法。

除了类似 `+` 的中缀运算符（操作符在两个操作符之间），还可以有前缀运算符和后缀运算符。顾名思义，前缀运算符的操作符在操作数前面，比如 `-7` 前面的 `-`。后缀运算符的运算符在操作数的后面，比如 `7 toLong` 中的 `toLong`。前缀和后缀操作符都使用一个操作数，而中缀运算符使用前后两个操作数。Scala 在实现前缀和后缀操作符的方法，其方法名都以 `unary_-` 开头。

比如，表达式 `-2.0` 实际上调用 `(2.0).unary_-`方法。

```scala
scala> -2.0
res1: Double = -2.0

scala> (2.0).unary_-
res2: Double = -2.0
```

如果你需要定义前缀方法，你只能使用 `+`、`-`、`!` 和 `~` 四个符号作为前缀操作符。

后缀操作符在不使用 `.` 和括号调用时不带任何参数。在 Scala 中，你可以省略掉没有参数的方法调用的空括号。按照惯例，如果你调用方法是为了利用方法的“副作用”，此时写上空括号，如果方法没有任何副作用（没有修改其它程序状态），你可以省略掉括号。

比如：

```scala
scala> val s = "Hello, World"
s: String = Hello, World

scala> s toLowerCase
warning: there was one feature warning; re-run with -feature for details
res0: String = hello, world
```

具体 Scala 的基本数据类型支持的操作符，可以参考 Scala API 文档。下面以示例介绍一些常用的操作符：

#### 算术运算符（包括 `+`、`–`、`×` 和 `/`）

```scala
scala> 1.2 + 2.3
res0: Double = 3.5

scala> 'b' -'a'
res1: Int = 1

scala> 11 % 4   #取余
res2: Int = 3

scala> 11.0f / 4.0f
res3: Float = 2.75

scala> 2L * 3L
res4: Long = 6
```

#### 关系和逻辑运算符（包括 `>`、`<`、`>=` 和 `!` 等）

```scala
scala> 1 >2
res5: Boolean = false

scala> 1.0 <= 1.0
res6: Boolean = true

scala> val thisIsBoring = !true
thisIsBoring: Boolean = false

scala> !thisIsBoring
res7: Boolean = true

scala> val toBe = true
toBe: Boolean = true

scala> val question = toBe || ! toBe  #表示逻辑或
question: Boolean = true
```

要注意的是，逻辑运算支持“短路运算”，比如 `op1 || op2`，当 `op1 = true`，`op2` 无需再计算，就可以知道结果为 `true`。这时 `op2` 表示式不会执行。例如：

```bash
scala> def salt()= { println("salt");false}
salt: ()Boolean

scala> def pepper() = {println("pepper");true}
pepper: ()Boolean

scala> pepper() && salt()   #表示逻辑与
pepper
salt
res0: Boolean = false

scala> salt() && pepper()
salt
res1: Boolean = false
```

#### 位操作符（包括 `&`、`|`、`^`、`~`）

```scala
scala> 1 & 2
res2: Int = 0

scala> 1 | 2
res3: Int = 3

scala> 1 ^ 2
res4: Int = 3

scala> ~1
res5: Int = -2
```

#### 对象恒等比较

如果需要比较两个对象是否相等，可以使用 `==` 和 `!=` 操作符。

```scala
scala> 1 == 2
res6: Boolean = false

scala> 1 !=2
res7: Boolean = true

scala> List(1,2,3) == List (1,2,3)
res8: Boolean = true

scala> ("he"+"llo") == "hello"
res9: Boolean = true
```

Scala 的 `==` 和 Java 不同，scala 的 `==` 只用于比较两个对象的**值**是否相同。而对于引用类型的比较使用另外的操作符 `eq` 和 `ne`。

#### 操作符的优先级和左右结合性

Scala 的操作符的优先级和 Java 基本相同，如果有困惑时，可以使用 `()` 改变操作符的优先级。操作符一般为左结合，Scala 规定了操作符的结合性由操作符的最后一个字符定义。对于以 `：` 结尾的操作符都是右结合，其它的操作符多是左结合。

例如：`a*b` 为 `a.*(b)`，而 `a:::b` 为 `b.:::(a)`，而 `a:::b:::c` 等价于 `a::: (b ::: c)`，`a*b*c` 等价于 `(a*b)*c`。



首先，我们回忆下有理数的定义：一个有理数(rational)可以表示成分数形式：`n/d`，其中 `n` 和 `d` 都是整数（`d` 不可以为 `0`），`n` 称为分子（numerator），`d` 为分母（denominator）。和浮点数相比，有理数可以精确表达一个分数，而不会有误差。

因此我们定义的 `Rational` 类支持上面的有理数的定义。支持有理数的加减乘除，并支持有理数的规范表示，比如 `2/10`，其规范表示为 `1/5`。分子和分母的最小公倍数为 `1`。

有了有理数定义的实现规范，我们可以开始设计类 `Rational`。一个好的起点是考虑用户如何使用这个类，我们已经决定使用 “Immutable” 方式来使用 `Rational` 对象，我们需要用户在定义 `Rational` 对象时提供分子和分母。因此我们可以开始定义 Rational 类如下：

```scala
class Rational(n:Int, d:Int)
```

可以看到，和 Java 不同的是，Scala 的类定义可以有参数，称为`类参数`，如上面的 `n`、`d`。Scala 使用类参数，并把类定义和主构造函数合并在一起，在定义类的同时也定义了类的主构造函数。因此 Scala 的类定义相对要简洁些。

Scala 编译器会编译 Scala 类定义包含的任何不属于类成员和类方法的其它代码，这些代码将作为类的主构造函数。比如，我们定义一条打印消息作为类定义的代码：

```scala
scala> class Rational (n:Int, d:Int) {
     |    println("Created " + n + "/" +d)
     | }
defined class Rational

scala> new Rational(1,2)
Created 1/2
res0: Rational = Rational@22f34036
```

可以看到创建 `Ratiaonal` 对象时，自动执行类定义的代码（主构造函数）

上面的代码创建 `Rational(1，2)`，Scala 编译器打印出 `Rational@22f34036`，这是因为使用了缺省的类的 `toString() 定义`（`Object` 对象的），缺省实现是打印出对象的类名称 + *@* + 16 进制数（对象的地址），显示结果不是很直观，因此我们可以重新定义类的 `toString()` 方法以显示更有意义的字符。

在 Scala 中，你也可以使用 `override` 来重载基类定义的方法，而且必须使用 `override` 关键字表示重新定义基类中的成员。比如：

```scala
scala> class Rational (n:Int, d:Int) {
     |    override def toString = n + "/" +d
     | }
defined class Rational

scala> val x = new Rational(1,3)
x: Rational = 1/3

scala> val y = new Rational(5,7)
y: Rational = 5/7
```

前面说过有理数可以表示为 `n/d`（其中 `d`、`n` 为整数，而 `d` 不能为 `0`）。对于前面的 `Rational` 定义，我们如果使用 `0` 也是可以的。

```scala
scala> new Rational(5,0)
res0: Rational = 5/0
```

怎么解决分母不能为 `0` 的问题呢？面向对象编程的一个优点是实现了数据的封装，你可以确保在其生命周期过程中是有效的。对于有理数的一个前提条件是分母不可以为 `0`，Scala 中定义为传入构造函数和方法的参数的限制范围，也就是调用这些函数或方法的调用者需要满足的条件。

Scala 中解决这个问题的一个方法是使用 `require` 方法。`require` 方法为 `Predef` 对象定义的一个方法，Scala 环境自动载入这个类的定义，因此无需使用 `import` 引入这个对象。因此修改 `Rational` 定义如下：

```scala
scala> class Rational (n:Int, d:Int) {
     |    require(d != 0)
     |    override def toString = n + "/" +d
     | }
defined class Rational

scala> new Rational(5,0)
java.lang.IllegalArgumentException: requirement failed
  at scala.Predef$.require(Predef.scala:211)
  ... 33 elided
```

可以看到，如果再使用 `0` 作为分母，系统将抛出 `IllegalArgumentException` 异常



前面我们定义了 `Rational` 的主构造函数，并检查了输入不允许分母为 `0`。下面我们就可以开始实现两个 `Rational` 对象相加的操作。我们需要实现的是函数化对象，因此 `Rational` 的加法操作应该是返回一个新的 `Rational` 对象，而不是返回被相加的对象本身。我们很可能写出如下的实现：

```scala
class Rational (n:Int, d:Int) {
   require(d!=0)
   override def toString = n + "/" +d
   def add(that:Rational) : Rational =
     new Rational(n*that.d + that.n*d,d*that.d)
}
```

实际上编译器会给出如下编译错误：

```scala
<console>:11: error: value d is not a member of Rational
            new Rational(n*that.d + that.n*d,d*that.d)
                                ^
<console>:11: error: value d is not a member of Rational
            new Rational(n*that.d + that.n*d,d*that.d)
```

这是为什么呢？尽管类参数在新定义的函数的访问范围之内，但仅限于定义类的方法本身（比如之前定义的 `toString` 方法，可以直接访问类参数），但对于 `that` 来说，无法使用 `that.d` 来访问 `d`。因为 `that` 不在定义的类可以访问的范围之内。此时需要定义为类的成员变量。注：后面定义的 `case class` 类型编译器自动把类参数定义为类的属性，这时可以使用 `that.d` 等来访问类参数。

修改 `Rational` 定义，使用成员变量定义如下：

```scala
class Rational (n:Int, d:Int) {
   require(d != 0)
   val number = n
   val denom = d
   override def toString = number + "/" +denom
   def add(that:Rational)  =
     new Rational(
       number * that.denom + that.number* denom,
       denom * that.denom
     )
}
```

要注意的是，我们这里定义成员变量都使用了 `val`，因为我们实现的是 “immutable” 类型的类定义。`number` 和 `denom` 以及 `add` 都可以不定义类型，Scala 编译器能够根据上下文推算出它们的类型。

```scala
scala> val oneHalf = new Rational(1,2)
oneHalf: Rational = 1/2

scala> val twoThirds = new Rational(2,3)
twoThirds: Rational = 2/3

scala> oneHalf add twoThirds
res0: Rational = 7/6

scala> oneHalf.number
res1: Int = 1
```

可以看到，这时就可以使用 `.number` 等来访问类的成员变量。

Scala 也使用 `this` 来引用当前对象本身，一般来说访问类成员时无需使用 `this`，比如实现一个 `lessThan` 方法，下面两种实现是等效的。

第一种：

```scala
def lessThan(that:Rational) =
   this.number * that.denom < that.number * this.denom
```

第二种：

```scala
def lessThan(that:Rational) =
   number * that.denom < that.number * denom
```

但如果需要引用对象自身，`this` 就无法省略，比如下面实现一个返回两个 `Rational` 中比较大的一个值：

```scala
def max(that:Rational) =
      if(lessThan(that)) that else this
```

其中的 `this` 就无法省略。



在定义类时，很多时候需要定义多个构造函数，在 Scala 中，除主构造函数之外的构造函数都称为辅助构造函数（或是从构造函数），比如对于 `Rational` 类来说，如果定义一个整数，就没有必要指明分母，此时只要整数本身就可以定义这个有理数。我们可以为 Rational 定义一个辅助构造函数，`Scala` 定义辅助构造函数使用 `this(…)` 的语法，所有辅助构造函数名称为 `this`。

```scala
def this(n:Int) = this(n,1)
```

所有 Scala 的辅助构造函数的第一个语句都为调用其它构造函数，也就是 `this(…)`。被调用的构造函数可以是主构造函数或是其它构造函数（最终会调用主构造函数）。这样使得每个构造函数最终都会调用主构造函数，从而使得主构造函数成为创建类单一入口点。在 Scala 中也只有主构造函数才能调用基类的构造函数，这种限制有它的优点，使得 Scala 构造函数更加简洁以及提高一致性。



Scala 类定义私有成员的方法也是使用 `private` 修饰符，为了实现 `Rational` 的规范化显示，我们需要使用一个求分子和分母的最大公约数的私有方法 `gcd`。同时我们使用一个私有变量 `g` 来保存最大公约数，修改 `Rational` 的定义：

```scala
class Rational (n:Int, d:Int) {
    require(d != 0)
    private val g = gcd (n.abs,d.abs)
    val number = n/g
    val denom = d/g
    override def toString = number + "/" +denom
    def add(that:Rational)  =
      new Rational(
        number * that.denom + that.number* denom,
        denom * that.denom
      )
    def this(n:Int) = this(n,1)
    private def gcd(a:Int,b:Int):Int =
      if(b == 0) a else gcd(b, a % b)
}
scala> new Rational ( 66,42)
res0: Rational = 11/7
```

注意 gcd 的定义，因为它是个 `回溯` 函数，必须定义返回值类型。Scala 会根据成员变量出现的顺序依次初始化它们，因此 `g` 必须出现在 `number` 和 `denom` 之前。



我们使用 `add` 定义两个 `Rational` 对象的加法。两个 `Rational` 加法可以写成 `x.add(y)` 或者 `x add y`。

即使使用 `x add y` 还是没有 `x + y` 来得简洁。

我们前面说过，在 Scala 中，运算符（操作符）和普通的方法没有什么区别，任何方法都可以写成操作符的语法。比如上面的 `x add y`。

而在 Scala 中对方法的名称也没有什么特别的限制，你可以使用符号作为类方法的名称，比如使用 `+`、`-` 和 `*` 等符号。因此我们可以重新定义 `Rational` 如下：

```scala
class Rational (n:Int, d:Int) {
   require(d != 0)
   private val g = gcd (n.abs,d.abs)
   val numer = n/g
   val denom = d/g
   override def toString = numer + "/" +denom
   def +(that:Rational)  =
     new Rational(
       numer * that.denom + that.numer* denom,
       denom * that.denom
     )
   def * (that:Rational) =
     new Rational( numer * that.numer, denom * that.denom)
   def this(n:Int) = this(n,1)
   private def gcd(a:Int,b:Int):Int =
     if(b == 0) a else gcd(b, a % b)
}
```

![5-3.9-1](https://doc.shiyanlou.com/document-uid702660labid6312timestamp1525658071832.png)

这样就可以使用 `+`、`*` 号来实现 `Rational` 的加法和乘法。`+`、`*` 的优先级是 Scala 预设的，和整数的 `+`、`-`、`*` 和 `/` 的优先级一样。下面为使用 `Rational` 的例子：

```scala
scala> val x = new Rational(1,2)
x: Rational = 1/2

scala> val y = new Rational(2,3)
y: Rational = 2/3

scala> x + y
res0: Rational = 7/6

scala> x + x*y
res1: Rational = 5/6
```

从这个例子也可以看出 Scala 语言的扩展性，你使用 `Rational` 对象就像 `Scala` 内置的数据类型一样。

从前面的例子我们可以看到 Scala 可以使用两种形式的标志符，字符数字和符号。字符数字使用字母或是下划线开头，后面可以接字母或是数字，符号 `$` 在 Scala 中也看作为字母。然而以 `$` 开头的标识符为保留的 Scala 编译器产生的标志符使用，应用程序应该避免使用 `$` 开始的标识符，以免造成冲突。

Scala 的命名规则采用和 Java 类似的 `camel` 命名规则（驼峰命名法），首字符小写，比如 `toString`。类名的首字符还是使用大写。此外也应该避免使用以下划线结尾的标志符以避免冲突。

符号标志符包含一个或多个符号，如 `+`、`:` 和 `?`。对于 `+`、`++`、`:::`、`<`、`?>`、`:->` 之类的符号，Scala 内部实现时会使用转义的标志符。例如对 `:->` 使用 `$colon$minus$greater` 来表示这个符号。因此，如果你需要在 Java 代码中访问 `:->` 方法，你需要使用 Scala 的内部名称 `$colon$minus$greater`。

混合标志符由字符数字标志符后面跟着一个或多个符号组成，比如 `unary_+` 为 Scala 对 `+` 方法的内部实现时的名称。

字面量标志符为使用 `"` 定义的字符串，比如 `"x"`、`"yield"`。你可以在 `"` 之间使用任何有效的 Scala 标志符，Scala 将它们解释为一个 Scala 标志符，一个典型的使用是 `Thread` 的 `yield` 方法，在 Scala 中你不能使用 `Thread.yield()` 是因为 yield 为 Scala 中的关键字，你必须使用 `Thread."yield"()`来使用这个方法。



和 Java 一样，Scala 也支持方法重载，重载的方法参数类型不同却使用同样的方法名称，比如对于 `Rational` 对象，`+` 的对象可以为另外一个 `Rational` 对象，也可以为一个 `Int` 对象，此时你可以重载 `+` 方法以支持和 `Int` 相加。

```scala
def + (i:Int) =
     new Rational (numer + i * denom, denom)
```

![5-3.11-1](https://doc.shiyanlou.com/document-uid702660labid6312timestamp1525660309993.png)

上面我们定义 `Rational` 的加法，并重载 `+` 以支持整数，`r + 2`，但如果我们需要 `2 + r` 如何呢？下面的例子：

```scala
scala> val x = new Rational(2,3)
x: Rational = 2/3

scala> val y = new Rational(3,7)
y: Rational = 3/7

scala> val z = 4
z: Int = 4

scala> x + z
res0: Rational = 14/3

scala> x + 3
res1: Rational = 11/3

scala> 3 + x
<console>:10: error: overloaded method value + with alternatives:
  (x: Double)Double <and>
  (x: Float)Float <and>
  (x: Long)Long <and>
  (x: Int)Int <and>
  (x: Char)Int <and>
  (x: Short)Int <and>
  (x: Byte)Int <and>
  (x: String)String
 cannot be applied to (Rational)
              3 + x
                ^
```

可以看到 `x + 3` 没有问题，`3 + x` 就报错了，这是因为整数类型不支持和 `Rational` 相加。我们不可能去修改 `Int` 的定义（除非你重写 Scala 的 `Int` 定义）以支持 `Int` 和 `Rational` 相加。如果你写过 .Net 代码，这可以通过静态扩展方法来实现，Scala 提供了类似的机制来解决这种问题。

如果 `Int` 类型能够根据需要自动转换为 `Rational` 类型，那么 `3 + x` 就可以相加。Scala 通过 `implicit def` 定义一个隐含类型转换，比如定义由整数到 `Rational` 类型的转换如下：

```scala
implicit def intToRational(x:Int) = new Rational(x)
```

![5-3.12-1](https://doc.shiyanlou.com/document-uid702660labid6312timestamp1525660215244.png)

再重新计算 `r + 2` 和 `2 + r` 的例子：

```scala
scala> val r = new Rational(2,3)
r: Rational = 2/3

scala> r + 2
res0: Rational = 8/3

scala> 2 + r
res1: Rational = 8/3
```

其实此时 `Rational` 的一个 `+` 重载方法是多余的，当 Scala 计算 `2 + r`，发现 `2(Int)` 类型没有可以和 `Rational` 对象相加的方法，Scala 环境就检查 `Int` 的隐含类型转换方法是否有合适的类型转换方法，类型转换后的类型支持 `+ r`，一检查发现定义了由 `Int` 到 `Rational` 的隐含转换方法，就自动调用该方法，把整数转换为 `Rational` 数据类型，然后调用 `Rational` 对象的 `+` 方法。从而实现了 `Rational` 类或是 `Int` 类的扩展。

关于 `implicit def` 的详细介绍将由后面的文章来说明，隐含类型转换在设计 Scala 库时非常有用。



Scala 的所有控制结构都有返回结果，如果你使用过 Java 或 C#，就可能了解 Java 提供的三元运算符 `?:`，它的基本功能和 `if` 一样，都可以返回结果。Scala 在此基础上所有控制结构（`while`、`try`、`if` 等）都可以返回结果。这样做的一个好处是，可以简化代码，如果没有这种特点，程序员常常需要创建一个临时变量用来保存结果。

总的来说，Scala 提供的基本程序控制结构，“麻雀虽小，五脏俱全”，虽然少，但足够满足其他指令式语言（如 Java，C++）所支持的程序控制功能。而且，由于这些指令都有返回结果，可以使得代码更为精简。



Scala 语言的 `if` 的基本功能和其它语言没有什么不同，它根据条件执行两个不同的分支。比如，使用 Java 风格编写下面 Scala 的 if 语句的一个例子：

```scala
var age = 25
var result = ""
if(age>20)
{
  result = "worker"
}else
{
  result = "Student"
}
println(result)
```

![6-3.2-1](https://doc.shiyanlou.com/document-uid702660labid6313timestamp1525660893341.png)

上面代码和使用 Java 实现没有太多区别，看起来不怎么像 Scala 风格，我们重新改写一下，利用 `if` 可以返回结果这个特点。

```scala
var age = 25
val result = if (age > 20) "Worker" else "Student"
println(result)
```

![6-3.2-2](https://doc.shiyanlou.com/document-uid702660labid6313timestamp1525660919936.png)

首先这种代码比前段代码短，更重要的是这段代码使用 `val`而无需使用 `var` 类型的变量。使用 `val` 为函数式编程风格。



Scala 的 `while` 循环和其它语言（如 Java）功能一样，它含有一个条件和一个循环体。只要条件满足，就一直执行循环体的代码。

比如，下面的计算最大公约数的一个实现：

```scala
def gcdLoop (x: Long, y:Long) : Long ={
   var a = x
   var b = y
   while( a != 0) {
      var temp = a
      a = b % a
      b = temp
  }
  b
}
```

![6-3.3-1](https://doc.shiyanlou.com/document-uid702660labid6313timestamp1525661663606.png)

Scala 也有 `do-while` 循环，它和 `while` 循环类似，只是检查条件是否满足是在循环体执行之后检查。

例如：

```scala
var line = ""
do {
   line = readLine()
   println("Read: " + line)
} while (line !="")
```

Scala 的 `while` 和 `do-while` 称为“循环”而不是表达式，是因为它不产生有用的返回值（或是返回值为 `Unit`），可以写成 `()`。`()` 的存在使得 Scala 的 `Unit` 和 Java 的 `void` 类型有所不同。

比如，下面的语句在 Scala 的解释器中执行：

```scala
scala> def greet() { println("hi")}
greet: ()Unit

scala> greet() == ()
<console>:9: warning: comparing values of types Unit and Unit using `==' will always yield true
              greet() == ()
                      ^
hi
res0: Boolean = true
```

可以看到（或者看到警告）`greet()` 的返回值和 `()` 比较结果为 `true`。

注意另外一种可以返回 `Unit` 结果的语句为 `var` 类型赋值语句。如果你使用如下 Java 风格的语句将碰到麻烦：

```scala
while((line = readLine()) != "")
  println("Read: " + line)
```

如果你试图编译或是执行这段代码会有如下警告：

![6-3.3-2](https://doc.shiyanlou.com/document-uid702660labid6313timestamp1525661683312.png)

意思是 `Unit`（赋值语句返回值）和 `String` 做不等比较永远为 `true`。上面的代码会是一个死循环。

正因为 `while` 循环没有值，因此在纯函数化编程中应该避免使用 `while` 循环。Scala 保留 `while` 循环，是因为在某些时候使用循环代码比较容易理解。而如果使用纯函数化编程，需要执行一些重复运行的代码时，通常就需要使用回溯函数来实现，回溯函数通常看起来不是很直观。

比如前面计算最大公约数的函数使用纯函数化编程借助回溯函数实现如下：

```scala
def gcd (x :Long, y:Long) :Long =
   if (y ==0) x else gcd (y, x % y)
```

总的来说，推荐尽量避免在代码中使用 `while` 循环，正如函数化编程要避免使用 `var` 变量一样。而使用 `while` 循环时通常也会使用到 `var` 变量，因此在你打算使用 `while` 循环时需要特别小心，看是否可以避免使用它们。





Scala 中的 `for` 表达式有如一把完成迭代任务的瑞士军刀，它允许你使用一些简单的部件以不同的方法组合完成许多复杂的迭代任务。简单的应用，比如枚举一个整数列表，较复杂的应用可以同时枚举多个不同类型的列表，根据条件过滤元素，并可以生成新的集合。

#### 枚举集合元素

这是使用 `for` 表示式的一个基本用法，和 Java 的 `for` 非常类似，比如下面的代码可以枚举当前目录下所有文件：

```scala
val filesHere = (new java.io.File(".")).listFiles

for(file <-filesHere)
  println(file)
```

![6-3.4.1-1](https://doc.shiyanlou.com/document-uid702660labid6313timestamp1525662101554.png)

其中如 `file <– filesHere` 的语法结构，在 Scala 中称为“生成器（generator）”。本例中，`filesHere` 的类型为 `Array[File]`。每次迭代中，变量 `file` 会初始化为该数组中一个元素，`file` 的 `toString()` 为文件的文件名，因此 `println(file)` 打印出文件名。

Scala 的 `for` 表达式支持所有类型的集合，而不仅仅是数组，比如下面使用 `for` 表达式来枚举一个 `Range` 类型。

```scala
 scala> for(i <- 1 to 4) println("Interation" +i)
Interation 1
Interation 2
Interation 3
Interation 4
```

#### 过滤

某些时候，你可能不想枚举集合中的每一个元素，而是只想迭代某些符合条件的元素。在 Scala 中，你可以为 `for` 表达式添加一个过滤器——在 `for` 的括号内添加一个 `if` 语句，例如：

修改前面枚举文件的例子，改成只列出 `.scala` 文件，示例如下：

```scala
val filesHere = (new java.io.File(".")).listFiles

for( file <-filesHere
   if file.getName.endsWith(".scala")
)  println(file)
```

如果有必要的话，你可以使用多个过滤器，只要添加多个 if 语句即可。比如，为保证前面列出的文件不是目录，可以添加一个 `if`，如下面代码：

```scala
val filesHere = (new java.io.File(".")).listFiles

for( file <-filesHere
   if file.isFile
   if file.getName.endsWith(".scala")
)  println(file)
```

#### 嵌套迭代

`for` 表达式支持多重迭代。下面的例子使用两重迭代，外面的循环枚举 `filesHere`，而内部循环枚举该文件的每一行文字。实现了类似 Unix 中的 `grep` 命令：

```scala
val filesHere = (new java.io.File(".")).listFiles

def fileLines (file : java.io.File) =
   scala.io.Source.fromFile(file).getLines().toList

def grep (pattern: String) =
  for (
    file <- filesHere if file.getName.endsWith(".scala");
    line <- fileLines(file)
    if line.trim.matches(pattern)
  ) println(file + ":" + line.trim)

grep (".*gcd.*")
```

![6-3.4.3-1](https://doc.shiyanlou.com/document-uid702660labid6313timestamp1525662637076.png)

注意上面代码中，两个迭代之间使用了 `;`，如果你使用 `{}` 替代 `for` 的 `()` 括号，你可以不使用 `；` 分隔这两个“生成器”。这是因为，Scala 编译器不推算包含在花括号内的省掉的 `;`。使用 `{}` 改写的代码如下：

```scala
val filesHere = (new java.io.File(".")).listFiles

def fileLines (file : java.io.File) =
   scala.io.Source.fromFile(file).getLines().toList

def grep (pattern: String) =
  for {
    file <- filesHere if file.getName.endsWith(".scala")
    line <- fileLines(file)
    if line.trim.matches(pattern)
  } println(file + ":" + line.trim)

grep (".*gcd.*")
```

![6-3.4.3-2](https://doc.shiyanlou.com/document-uid702660labid6313timestamp1525662755077.png)

这两段代码是等效的。

#### 绑定中间变量

你可能注意到，前面代码使用了多次 `line.trim`。如果 `trim` 是个耗时的操作，你可能希望 `trim` 只计算一次。Scala 允许你使用 `=` 号来绑定计算结果到一个新变量。绑定的作用和 `val` 类似，只是不需要使用 `val` 关键字。例如，修改前面的例子，只计算一次 `trim`，把结果保存在 `trimmed` 变量中。

```scala
val filesHere = (new java.io.File(".")).listFiles

def fileLines (file : java.io.File) =
   scala.io.Source.fromFile(file).getLines().toList

def grep (pattern: String) =
  for {
    file <- filesHere if file.getName.endsWith(".scala")
    line <- fileLines(file)
    trimmed = line.trim
    if trimmed.matches(pattern)
  } println(file + ":" + trimmed)

grep (".*gcd.*")
```

![6-3.4.4-1](https://doc.shiyanlou.com/document-uid702660labid6313timestamp1525663167710.png)

#### 生成新集合

`for` 表达式也可以用来生产新的集合，这是 Scala 的 `for`表达式比 Java 的 `for` 语句功能强大的地方。它的基本语法如下：

```scala
for clauses yield body
```

关键字 `yield` 放在 body 的前面，`for` 每迭代一次，就产生一个 `body`。`yield` 收集所有的 `body` 结果，返回一个 `body` 类型的集合。



Scala 的异常处理和其它语言比如 Java 类似，一个方法可以通过抛出异常而不返回值的方式来终止相关代码的运行。调用函数，可以捕获这个异常作出相应的处理，或者直接退出。在这种情况下，异常会传递给调用函数的调用者，依次向上传递，直到有方法处理这个异常。

#### 抛出异常

Scala 抛出异常的方法和 Java 一样，使用 `throw` 方法。例如，抛出一个新的参数异常：

```scala
throw new IllegalArgumentException
```

尽管看起来似乎有些自相矛盾，Scala 中，`throw` 也是一个表达式，也是有返回值的。比如下面的例子：

```scala
val half =
  if (n % 2 == 0)
    n/2
  else
    throw new RuntimeException("n must be even")
```

当 `n` 为偶数时，`n` 初始化为 `n` 的一半；而如果 `n` 为奇数，将在初始化 `half` 之前就抛出异常。正因为如此，可以把 `throw` 的返回值视作任意类型。

技术上来说，抛出异常的类型为 `Nothing`。对于上面的例子，整个 `if` 表达式的类型为可以计算出值的那个分支的类型。如果 `n` 为 `Int`，那么 `if` 表示式的类型也是 `Int` 类型，而不需要考虑 `throw` 表达式的类型。

#### 捕获异常

Scala 捕获异常的方法和后面介绍的“模式匹配”的使用方法是一致的。比如：

> 注意，这个地方的文件需要自己创建，并将文件目录写到 FileReader 中。

```bash
vi input.txt
```

![6-3.5.2-1](https://doc.shiyanlou.com/document-uid702660labid6313timestamp1525742865841.png)

```scala
import java.io.FileReader
import java.io.FileNotFoundException
import java.io.IOException

try {
  val f = new FileReader("/home/hadoop/input.txt")
} catch {
  case ex: FileNotFoundException => //handle missing file
  case ex: IOException => //handle other I/O error
}
```

![6-3.5.2-2](https://doc.shiyanlou.com/document-uid702660labid6313timestamp1525743130475.png)

模式匹配将在后面介绍，`try-catch` 表达式的基本用法和 Java 一样。如果 `try` 块中代码在执行过程中出现异常，将逐个检测每个 `catch` 块。在上面的例子，如果打开文件出现异常，将先检查是否是 `FileNotFoundException` 异常。如果不是，再检查是否是 `IOException`。如果还不是，再终止 `try-catch` 块的运行，而向上传递这个异常。

> **注意：** 和 Java 异常处理不同的一点是，Scala 不需要你捕获 `checked` 的异常。这点和 C# 一样，也不需要使用 `throw` 来声明某个异常。当然，如果有需要，还是可以通过 `@throw` 来声明一个异常，但这不是必须的。

#### finally 语句

Scala 也支持 `finally` 语句，你可以在 `finally` 块中添加一些代码。这些代码不管 `try` 块是否抛出异常，都会执行。比如，你可以在 `finally` 块中添加代码保证关闭已经打开的文件，而不管前面代码中是否出现异常。

```scala
import java.io.FileReader

val file = new FileReader("/home/hadoop/input.txt")

try {
  //use the file
} finally {
  file.close()
}
```

![6-3.5.3-1](https://doc.shiyanlou.com/document-uid702660labid6313timestamp1525743159724.png)

#### 生成返回值

和大部分 Scala 控制结构一样，Scala 的 `try-catch-finally` 也生成某个值。比如下面的例子尝试分析一个 URL，如果输入的 URL 无效，则使用缺省的 URL 链接地址：

```scala
import java.net.URL
import java.net.MalformedURLException

def urlFor(path:String) =
  try {
    new URL(path)
  } catch {
    case e: MalformedURLException =>
      new URL("http://www.scala-lang.org")
  }
```

![6-3.5.4-1](https://doc.shiyanlou.com/document-uid702660labid6313timestamp1525743312024.png)

通常情况下，`finally` 块用来做些清理工作，而不应该产生结果，但如果在 `finally` 块中使用 `return` 来返回某个值，这个值将覆盖 `try-catch` 产生的结果，比如：

```scala
scala> def f(): Int = try { return 1 } finally { return 2}
f: ()Int

scala> f
res0: Int = 2
```

而下面的代码：

```scala
scala> def g() :Int = try 1 finally 2
<console>:21: warning: a pure expression does nothing in statement position; you may be omitting necessary parentheses
       def g() : Int = try 1 finally 2
                                     ^

g: ()Int

scala> g
res0: Int = 1
```

结果却是 `1`，上面两种情况常常使得程序员产生困惑，因此关键的一点是避免在 `finally` 生成返回值，而只用来做些清理工作，比如关闭文件。

#### Match 表达式

Scala 的 `Match` 表达式支持从多个选择中选取其一，类似其它语言中的 `switch` 语句。通常来说，Scala 的 `match` 表达式支持任意的匹配模式，这种基本模式将在后面介绍。

接下来，为你介绍类似 `switch` 用法的 `match` 表达式，它也是在多个选项中选择其一。

例如，下面的例子从参数中读取食品的名称，然后根据食品的名称，打印出和该食品搭配的食品。比如，输入 `salt`，与之对应的食品为 `pepper`。如果是 `chips`，那么搭配的就是 `salsa` 等等。

```scala
val args = Array("chips")
val firstArg = if (args.length >0 ) args(0) else ""
firstArg match {
  case "salt" => println("pepper")
  case "chips" => println("salsa")
  case "eggs" => println("bacon")
  case _ => println("huh?")
}
```

![6-3.5.5-1](https://doc.shiyanlou.com/document-uid702660labid6313timestamp1525744075929.png)

这段代码和 Java 的 `switch` 相比有几点不同：

- 一是任何类型的常量都可以用在 `case` 语句中，而不仅仅是 `int` 或是枚举类型。
- 二是每个 `case` 语句无需使用 `break`，Scala 不支持 “`fall through`”。
- 三是 Scala 的缺省匹配为 `_`，其作用类似 java 中的 `default`。

而最关键的一点，是 scala 的 `match` 表达式有返回值。上面的代码使用的是 `println` 打印，而实际上你可以使用表达式，比如修改上面的代码如下：

```scala
val firstArg = if (args.length >0 ) args(0) else ""
val friend = firstArg match {
  case "salt" => "pepper"
  case "chips" => "salsa"
  case "eggs" => "bacon"
  case _ => "huh?"
}

println(friend)
```

![6-3.5.5-2](https://doc.shiyanlou.com/document-uid702660labid6313timestamp1525744622605.png)

这段代码和前面的代码是等效的，不同的是后面这段代码 `match` 表达式返回结果。





你也许注意到：到目前为止，我们介绍 Scala 的内置控制结构时，没有提到使用 `break` 和 `continue`。Scala 特地没有在内置控制结构中包含 `break` 和 `continue`，这是因为这两个控制结构和函数字面量有点格格不入，函数字面量我们将在后面介绍。函数字面量和其它类型字面量，比如数值字面量 `4`、`5.6` 相比，他们在 Scala 的地位相同。

我们很清楚 `break` 和 `continue` 在循环控制结构中的作用。Scala 内置控制结构特地去掉了 `break` 和 `continue`，这是为了更好的适应函数化编程。不过，你不用担心，Scala 提供了多种方法来替代 `break` 和 `continue` 的作用。

一个简单的方法，是使用一个 `if` 语句来代替一个 `continue`，使用一个布尔控制量来去除一个 `break`。比如下面的 Java 代码在循环结构中使用 `continue` 和 `break`：

```java
int i = 0;
boolean foundIt = false;
while(i <args.length) {
    if (args[i].startWith("-")) {
        i = i + 1;
        continue;
    }
    if(args[i].endsWith(".scala")){
        foundIt = true;
        break;
    }
    i = i + 1;
}
```

这段 Java 代码实现的功能，是从一组字符串中寻找以 `.scala` 结尾的字符串，但跳过以 `-` 开头的字符串。

下面我们使用 `if` 和 `boolean` 变量，逐句将这段使用 Scala 来实现（不使用 `break` 和 `continue`）如下：

```scala
var i = 0
var foundIt = false
while (i < args.length && !foundIt) {
    if (!args(i).startsWith("-")) {
    if(args(i).endsWith(".scala"))
        foundIt = true
    }
    i = i + 1
}
```

![6-3.6-1](https://doc.shiyanlou.com/document-uid702660labid6313timestamp1525744646313.png)

可以看到，我们使用 `if`（与前面的 `continue` 条件相反）去掉了 `continue`，而重用了 `foundIt` 布尔变量，去掉了 `break`。

这段代码和前面 Java 实现非常类似，并且使用了两个 `var` 变量。使用纯函数化编程的一个方法是去掉 `var` 变量的使用，而递归函数（回溯函数）是用于去除循环结构中使用 `var` 变量时，通常使用的一个方法。

使用递归函数重新实现上面代码的查询功能：

```scala
def searchFrom(i:Int) : Int = {
    if( i >= args.length) -1
    else if (args(i).startsWith("-")) searchFrom (i+1)
    else if (args(i).endsWith(".scala")) i
    else searchFrom(i + 1)
}
     val i = searchFrom(0)
```

![6-3.6-2](https://doc.shiyanlou.com/document-uid702660labid6313timestamp1525745027668.png)

在函数化编程中，使用递归函数来实现循环是非常常见的一种方法，我们应该熟悉递归函数的用法。



