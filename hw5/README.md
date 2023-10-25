### 作业五

本次作业以vscode为环境，以maven为框架实现mapreduce编程，仓库中包含mapreduce的主代码、jar包以及相应的input文件、output结果等文件，可供批改与检查。

#### 目录结构

本次作业的仓库目录结构如下。为了日后作业方便起见，建立仓库Finance_big_data，并建立hw5文件夹存放本次作业的主要内容。

<div style="background-color: #eaf2f8; color: #000; padding: 10px;">
<pre>
Finance_big_data
├────── hw5
|   	├──.vscode
|   	|	└──settings.json//vscode的java setting
|   	├──homwork_5
|   	|	├──src
|   	|	|   └──main/java/com/hw5
|   	|	|		└──homework5.java
|   	|	├──target
|   	|	|    └──homework_5-1.0-SNAPSHOT.jar
|   	|	└──pom.xml
|       ├──input
|   	|    ├──A.csv
|   	|    ├──A.xlsx
|   	|    ├──B.csv
|   	|    └──B.xlsx
|   	├──output
|   	|    ├──_SUCCESS
|       |    └──part-r-00000
|   	├──output.csv
|   	└──REAMDME.md
</pre>
</div>

#### 设计思路

<blockquote style="background-color: #eaf2f8; color: #000; padding: 10px;">
    本次作业的目标是利用已经配置完成的Hadoop伪分布式环境，对于给定的两个输入文件A和B（包含了纳斯达克100指数、标普500指数、道琼斯工业指数的部分成分股数据，其中第一列为三大指数编号（简化为101、102、103），第二列为成分股代码），利用Mapreduce程序进行编程，对两个文件内容进行合并，并对重复内容进行剔除，最后合并为一个包含三大指数成分股的输出文件。输出文件格式与文件A和B格式相同。
</blockquote>

##### 输入输出模块

考虑到本次要输入两个文件，因此在作业中，首先对输入输出模块进行设计

```java
public static void main(String[] args) throws InterruptedException,IOException,ClassNotFoundException{   //main方法
		
		Configuration conf=new Configuration();
		// conf.set("fs.defaultFS", "hdfs://localhost:9000");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage:Merge and duplicate removal <in> [<in>...] <out>");
            System.exit(2);
        }
		Job job=Job.getInstance(conf,"merge");
		job.setJobName("merge");
		job.setJarByClass(homework5.class);
        //上面四句话是设置类信息，方便hadoop从jar文件中找到
		job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReduce.class);
        //设置执行Map与Reduce的类，也就是你之前设置的类名
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//设置Map端输出的数据类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//设置输出数据类型
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		//添加输入输出路径
		job.waitForCompletion(true);
}
```

在上面的部分，首先，建立configuration，连接默认的hdfs文件系统：

```java
Configuration conf=new Configuration();
```

第二步，考虑到输入文件夹下文件不唯一，设置如下的文件读取逻辑：

```java
String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	if (otherArgs.length != 2) {
    System.err.println("Usage:Merge and duplicate removal <in> [<in>...] <out>");
	}
```

设置类的信息，Map类与Reducer类以及它们的输入输出类型：

```java
Job job=Job.getInstance(conf,"merge");
job.setJobName("merge");
job.setJarByClass(homework5.class);
//上面四句话是设置类信息，方便hadoop从jar文件中找到
job.setMapperClass(MyMapper.class);
job.setReducerClass(MyReduce.class);
//设置执行Map与Reduce的类，也就是你之前设置的类名
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(Text.class);
//设置Map端输出的数据类型
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
```

设置输出类型，保证读取到最后一个路径是输出路径，前面的均为输入路径：

```java
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		//添加输入输出路径
		job.waitForCompletion(true);
```

##### Mapper与Reducer类设计

为了保证去重的效果，同时capture到每一行的信息，在Mapper阶段，我们应采用以行号为键，行数据为值的形式。在Reducer阶段，我们将具有相同行号的所有行数据合并，并去重后输出，当我们仅设置一个Reducer节点时，所有的键值对都会被发送到同一个Reducer节点进行处理。

![未命名文件(4)](https://github.com/Maroon1989/Finance_big_data/blob/master/hw5/png/document.jpg)

###### Mapper设计

刚开始的想法，是以表格的第一列为key，第二列的值为value，但是发现逻辑并不正确，这样会导致因为大量key相同会去重到只剩3个。因此考虑把每一行的数据作为mapper output的key，这样就能使得每一行独一无二，达到去重的效果，具体实现代码如下：

```java
public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
		// private Text outKey = new Text();
        // private Text outValue = new Text();
		private static Text text = new Text();
		protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
		//protected void map(数据类型 key,数据类型 value,Context context) throws IOException,InterruptedException{
			
			// String line = value.toString();
			// String[]fields = line.split("\t");
			// if (fields.length == 2){
			// 	if (fields[0] == "101"||fields[0] == "102" ||fields[0] == "103"){
			// 		outKey.set(fields[0]);
			// 		outValue.set(fields[1]);
			// 		context.write(outKey, outValue);
			// 	}
			// }
			// context.write(new Text(line),new Text("1"));	
			text = value;
			context.write(text,new Text(""));
		}
	}
```

###### Reducer设计

这里是把mapper的数据汇总，实现去重的效果。

```java
	public static class MyReduce extends Reducer<Text,Text,Text,Text>{
		// private Text outvalue = new Text();
		
		protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
		
			// List<String> sum = new ArrayList<>();
			// Set<String>uniquevalues = new HashSet<>();
			// for(Text value:values){
			// 	String v = value.toString();
			// 	uniquevalues.add(v);
			// }
			// StringBuilder sb = new StringBuilder();
			// for(String uniquevalue:uniquevalues){
			// 	sb.append(uniquevalue).append("\t");
			// }
			// outvalue.set(sb.toString());
			context.write(key, new Text(""));
			// String[] arr = sum.toArray(new String[0])
			// context.write(key,new String[](sum));
			
		}
	}
```

#### 执行流程

完成java代码的编写后，理论上在vscode应该会生成如下所示的目录：

<div style="background-color: #eaf2f8; color: #000; padding: 10px;">
<pre>
├────── hw5
|   	├──.vscode
|   	|	└──settings.json//vscode的java setting
|   	├──homwork_5
|   	|	├──src
|   	|	|   └──main/java/com/hw5
|   	|	|		└──homework5.java
|   	|	├──target
|   	|	└──pom.xml
|       ├──input
|   	|    ├──A.csv
|   	|    ├──A.xlsx
|   	|    ├──B.csv
|   	|    └──B.xlsx
</pre>
</div>

其中，考虑到excel文件的读取会有数据流读取的bug，输出时可能会导致output的文件出现无法打开的情况（输出非正常二进制流），在本作业中我采用python对源数据进行了一定的处理，将源数据改成csv格式，其中‘utf-8'的格式会相对处理友好。

接下来在hw5目录下执行：

```bash
mvn package
```

这样就会生成homework5.java对应的jar包，我生成的jar包名为homework_5-1.0-SNAPSHOT.jar

然后我们把目录切换到/usr/local/hadoop，仿照实验一的操作，启动hdfs文件系统建立链接：

```bash
sbin/start-dfs.sh
```

之后在hdfs中建立input文件夹，执行代码如下：

```bash
bin/hdfs dfs -mkdir /user
bin/hdfs dfs -mkdir /user/homework5
bin/hdfs dfs -mkdir /user/homework5/input
```

进行上一步操作后，把本地的两个csv输入文件导入hdfs中，执行代码如下：

```bash
bin/hdfs dfs -put /home/hadoop/Workspace/hw5/input/*.csv /user/homework5/input
```

其中，/home/hadoop/Workspace/hw5/input是本地的input文件夹

最后，利用hadoop命令执行mapreduce代码（注意要保证output文件不存在，否则用命令进行删除）：

```bash
bin/hadoop jaomework 5-1.0-SNAPSHOT.jar com.hw5.homework5 /user/homework/home/hadoop/Workspace/hw5/homework_5/target/hw5/input /user/homework5/output
```

执行结果如下：

![51fd19a7cf02d87f8f4461d77052bb6](https://github.com/Maroon1989/Finance_big_data/blob/master/hw5/png/51fd19a7cf02d87f8f4461d77052bb6.png)

![150869e8fb47e2d825a9f201e66246b](https://github.com/Maroon1989/Finance_big_data/blob/master/hw5/png/150869e8fb47e2d825a9f201e66246b.png)

然后执行代码get把output拷贝到本地文件系统上：

```
bin/hdfs dfs -get /user/homework5/output /home/hadoop/Workspace/hw5/output
```

打开output文件的part-r-00000查看输出：

![image-20231025230331921](https://github.com/Maroon1989/Finance_big_data/blob/master/hw5/png/305be12bbb20b568d8a74117d61bb6a.png)

生成是成功的，本次作业完成。

#### 扩展功能

考虑到项目功能的可扩展性，若输出为part-r-00000文件显然不具有实际应用价值，因此在本次作业中新添了csv输出的功能，供尝试。

流程如下：

添加如下代码即可，首先创建csv输出文件，读取我们之前生成的结果文件，并读入csv文件即可：

```java
Configuration conf2 = new Configuration();
        FileSystem fs = FileSystem.get(conf2);

        // 创建CSV输出文件
		Path csvOutputPath = new Path("output.csv");
		BufferedWriter bw = new BufferedWriter(new FileWriter(csvOutputPath.toString()));

        // 读取Hadoop输出的结果文件
        Path outputPath = new Path(otherArgs[otherArgs.length - 1]);
        if (fs.exists(outputPath)) {
            for (FileStatus status : fs.listStatus(outputPath)) {
                if (!status.isDirectory()) {
                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
                    String line;
                    while ((line = br.readLine()) != null) {
						bw.write(line);
						bw.newLine();
                    }
                    br.close();
                }
            }
        }
		bw.close();
        fs.close();
```

生成后，文件夹即如开头所示。

#### 不足之处

在本次作业中，并没能实现excel的导出，频频报错classnofound，可能是因为没有把workbook的class放置在Java的系统class中，希望能在未来弥补。

