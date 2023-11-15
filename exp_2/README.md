## 实验二 MapReduce数据挖掘算法实践

#### 实验背景

在借贷交易中，银行和其他金融机构通常提供资⾦给借款⼈，期望借款⼈能够按时还款本金和利息。然而，由于各种原因，有时借款人可能⽆法按照合同规定的方式履行还款义务，从而导致贷款违约。本次实验以银行贷款违约为背景，选取了约30万条贷款信息 。

#### 数据来源

https://www.kaggle.com/datasets/mishra5001/credit-card/data

#### 目录结构

本次实验的仓库目录结构如下。

<div style="background-color: #eaf2f8; color: #000; padding: 10px;">
<pre>
Finance_big_data
├────── exp_2
|   	├──.vscode
|   	|	└──settings.json//vscode的java setting
|   	├──exp_2_1
|   	|	├──src
|   	|	|   └──main/java/com/exp_2_1
|   	|	|		└──exp_2_1.java
|   	|	├──target
|   	|	|    └──exp_2_1-1.0-SNAPSHOT.jar
|   	|	└──pom.xml
|    	|	|
|   	|	└──output
|       |	     ├──_SUCCESS
|       |	     └──part-r-00000
|   	├──exp_2_2
|   	|	├──src
|   	|	|   └──main/java/com/exp_2_2
|   	|	|		└──exp_2_2.java
|   	|	├──target
|   	|	|    └──exp_2_2-1.0-SNAPSHOT.jar
|   	|	├──pom.xml
|    	|	|
|   	|	└──output
|       |	     ├──_SUCCESS
|       |	     └──part-r-00000
|   	├──exp_2_3/KNN/knn
|   	|	├──src
|   	|	|   └──main/java
|   	|	|       ├──com/knn
|       |	|       |    	├──Instance.java
|       |	|       |       └──KNN.java
|       |	|       └──king/Utils
|       |	|               ├──Distance.java
|       |	|               └──ListWritable.java
|   	|	├──target
|   	|	|    └──knn-1.0-SNAPSHOT.jar
|   	|	└──pom.xml
|       |	├──test_original.txt
|       |	├──test.txt
|       |	├──train.txt
|       |	├──evaluate.py
|       |	├──train_test.split.ipynb
|    	|	|
|   	|	└──output
|       |	     ├──_SUCCESS
|       |	     └──part-r-00000
|       ├──input
|   	|    ├──application.csv
|   	|    └──columns_description.csv
|   	└──README.md
</pre>
</div>


#### 任务内容

##### 任务一

<blockquote style="background-color: #eaf2f8; color: #000; padding: 10px;">
    编写MapReduce程序，统计数据集中违约和非违约的数量，按照标签TARGET进⾏输出，即1代表有违约的情况出现，0代表其他情况。
</blockquote>

##### 设计思路

本题实质上是一个类似于wordcount问题，通过对target进行统计然后输出结果，但是并不需要进行排序。因此可以仿照wordcount的思路进行代码的编写。

##### Mapper 设计

首先以文件的行号为key，内容为value进行输入。考虑到target是文件的最后一列，因此在对每一行内容进行字段分割时，取最后一个作为Mapper输出的键值对的键，并对这一行的target值的value设为1，方便在Reducer进行求和统计。

具体代码实现如下：

```java
public static class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		private static Text text = new Text();
		protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			if(key.get()==0)
            {
                return;
            }
			String line = value.toString();
			String[]fields = line.split(",");
            text.set(fields[fields.length-1]);
			context.write(text,new IntWritable(1));
		}
	}
```

注意到，文件的输入会包括列名，因此当key.get()为0时，要跳过，直接return掉这次的map。

##### Reducer设计

承接上面的Mapper设计，在接收到键值对后，Reducer要对相同的键进行value求和运算，最后输出。

代码设计如下：

```java
public static class MyReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
		IntWritable total = new IntWritable();
		protected void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
            int sum = 0;
			for(IntWritable value:values)
            {
                int i = value.get();
                sum+=1;
            }
            total.set(sum);
            context.write(key,total);
		}
	}
```

##### 执行流程

在补充完输入输出部分后，接下来在exp_2_1部分下执行：

```
mvn package
```

执行命令后，就会生成exp_2_1-1.0-SNAPSHOT.jar

然后我们把目录切换到/usr/local/hadoop，仿照实验一的操作，启动hdfs文件系统建立链接：

```bash
sbin/start-dfs.sh
```

之后在hdfs中建立input文件夹，执行代码如下：

```bash
bin/hdfs dfs -mkdir /user
bin/hdfs dfs -mkdir /user/exp_2
bin/hdfs dfs -mkdir /user/exp_2/input
```

进行上一步操作后，把本地的application.csv导入到hdfs中，执行代码如下：

```bash
bin/hdfs dfs -put /home/hadoop/Workspace/exp_2/exp_2_1/input/application_data.csv /user/exp_2/input
```

最后，利用hadoop命令执行mapreduce代码（注意要保证output文件不存在，否则用命令进行删除）：

```bash
bin/hadoop jar /home/hadoop/Workspace/exp_2/exp_2_1/exp_2_2/target/exp_2_1-1.0-SNAPSHOT.jar com.exp_2_1.exp_2_1 /user/exp_2/input /user/exp_2/exp_2_1/output
```

执行结果如下：

![image-20231114175948156](https://github.com/Maroon1989/Finance_big_data/blob/master/exp_2/image/image-1.png)

执行结果successful，且output如下所示：

![image-20231114180035553](https://github.com/Maroon1989/Finance_big_data/blob/master/exp_2/image/image-2.png)

可以看到数据集存在一定的unbalanced问题，考虑在后续任务中进行处理。

##### 任务二

<blockquote style="background-color: #eaf2f8; color: #000; padding: 10px;">
编写MapReduce程序，统计⼀周当中每天申请贷款的交易数WEEKDAY_APPR_PROCESS_START，并按照交易数从大到小进行排序。
</blockquote>

##### 设计思路

本题实质上也是一个类似于wordcount问题，通过对WEEKDAY_APPR_PROCESS_START进行统计然后输出结果，与任务一不同的是，这个任务涉及到了排序的问题，因此步骤繁琐了许多。

本任务通过以下思路进行编写：

步骤一：设置第一个job，用于处理默认的wordcount任务，输入为源文件，输出到一个临时文件夹。

步骤二：设置第二个job，用于处理排序问题，输入为上一步创建的临时文件夹，输出是output文件夹并删除临时文件夹。

##### 步骤一

在这一步中，内容与上一步的wordcount类似的任务并无太大区别，在此不再赘述，展示这一个job对应的main函数部分：

```java
Configuration conf=new Configuration();
		Path tempdir = new Path("/user/exp_2/exp_2_2/tmp");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage:Merge and duplicate removal <in> <out>");
            System.exit(2);
        }
		Job job=Job.getInstance(conf,"merge");
		job.setJobName("merge");
		job.setJarByClass(exp_2_2.class);
		job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, tempdir);
		job.waitForCompletion(true);
```

与任务一不同的是，这一步骤设计了临时文件夹作为输出，为的是方便后面的排序。

##### 步骤二

在这一步骤中目标是实现排序。

###### 设计思路

在mapreduce中进行排序，实际上就是控制键值对到达Reducer的顺序。简单来说，就是需要设计一个排序器，在Mapper完成任务后，对键值对进行控制，使得Reducer能有序处理任务。然而，目前mapreduce的排序函数编写大多针对键排序，因此需要先通过InverseMapper将步骤一中的键值对进行反转，然后执行排序，最后输入到Reducer中进行键值对反转成目标状态。

###### InverseMapper说明

可以通过以下代码import InverseMapper：

```java
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
```

InverseMapper是apache开发的一个template，可以兼容多种数据格式，适用于键值对交换的Mapper任务，其源码如下：

```java
public class InverseMapper<K, V>extends MapReduceBase implements Mapper<K, V, V, K> {
/** The inverse function.  Input keys and values are swapped.*/
	public void map(K key, V value,OutputCollector<V, K> output, Reporter reporter)throws IOException {
		output.collect(value, key);
	}
}
```

###### 排序设计

代码如下：

```java
public static class IntWritableDecreasingComparator extends IntWritable.Comparator {
          public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
          }
          
          public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
              return -super.compare(b1, s1, l1, b2, s2, l2);
          }
}
```

###### SecondReduce设计

在这一步中，只需对value-key进行反转即可，注意到Reducer已经把键值对进行聚合，因此循环遍历每一个key对应的value并输出即可。

具体代码实现如下：

```java
public static class SecondReduce extends Reducer<IntWritable,Text,Text,IntWritable>{
		// private Text outvalue = new Text();
		IntWritable total = new IntWritable();
		protected void reduce(IntWritable key,Iterable<Text>values,Context context) throws IOException,InterruptedException{
			for(Text value:values){
				context.write(value,key);
			}
		}
	}
```

##### 执行流程

在补充完输入输出部分后，接下来在exp_2_2部分下执行：

```
mvn package
```

执行命令后，就会生成exp_2_2-1.0-SNAPSHOT.jar

然后我们把目录切换到/usr/local/hadoop，仿照实验一的操作，启动hdfs文件系统建立链接：

```bash
sbin/start-dfs.sh
```

之后在hdfs中建立input文件夹，执行代码如下：

```bash
bin/hdfs dfs -mkdir /user
bin/hdfs dfs -mkdir /user/exp_2
bin/hdfs dfs -mkdir /user/exp_2/input
```

最后，利用hadoop命令执行mapreduce代码（注意要保证output文件不存在，否则用命令进行删除）：

```bash
bin/hadoop jar /home/hadoop/Workspace/exp_2/exp_2_1/exp_2_2/target/exp_2_2-1.0-SNAPSHOT.jar com.exp_2_2.exp_2_2 /user/exp_2/input /user/exp_2/exp_2_2/output
```

执行结果如下：

![image-20231114200503117](https://github.com/Maroon1989/Finance_big_data/blob/master/exp_2/image/image-3.png)

执行结果successful，且output如下所示：

![image-20231114200558633](https://github.com/Maroon1989/Finance_big_data/blob/master/exp_2/image/image-4.png)

可以看到目标已经实现。

##### 任务三

<blockquote style="background-color: #eaf2f8; color: #000; padding: 10px;">
根据application_data.csv中的数据，基于MapReduce建立贷款违约检测模型，并评估实验结果的准确率。
</blockquote>

##### 设计思路

本任务是mapreduce的综合应用问题。在这个问题中，考虑到mapreduce基本局限于分布式的机器学习算法，因此采用了knn实现贷款违约检测模型的建立。

在经过网络的学习后，本次任务参考了黄老师的KNN算法的思路，将代码分为了几个文件执行，这是为了方便代码的实现，然而书上代码Hadoop以及Java版本太低，进行了一定的改进：

1. Instance.java：这是为了简化文件的读取，将文件的特征与label分别存储起来到Instance类中；
2. KNN.java：主java文件，存放Mapper和Reducer进行KNN的核心处理；
3. Distance.java：用于计算样本之间的距离，是KNN算法的核心部分；
4. ListWritable：用于进行KNN找最近邻算法时邻居确定时待选label的存放。

##### Instance.java

代码部分

```
public class Instance {
	private double[] attributeValue;
    private double lable;
    public Instance(String line){
    	String[] value = line.split(" ");
    	attributeValue = new double[value.length - 1];
    	for(int i = 0;i < attributeValue.length;i++){
    		attributeValue[i] = Double.parseDouble(value[i]);
    	}
    	lable = Double.parseDouble(value[value.length - 1]);
    }
    
    public double[] getAtrributeValue(){
    	return attributeValue;
    }
    
    public double getLable(){
    	return lable;
    }
}
```

##### Distance.java

此代码用于计算两个样本间的欧式距离，代码构成如下：

```Java
public class Distance {
	public static double EuclideanDistance(double[] a,double[] b) throws Exception{
		if(a.length != b.length)
			throw new Exception("size not compatible!");
		double sum = 0.0;
        for(int i = 0;i < a.length;i++){
			sum += Math.pow(a[i] - b[i], 2);
        }
		return Math.sqrt(sum);
	}
}
```

##### ListWritable.java

这个代码源于GitHub，是考虑到KNN算法中label进行修改的简便性设计的一个类，在此处进行迁移，方便后续KNN算法的编写，在后续的使用中会用此类继承DoubleWritable。

##### KNN.java

根据KNN的算法思路，只需在训练集中找到和测试集距离最近的赋予label即可。

首先，创建训练集的全部set，也即读取训练集文件：

```java
FileSystem fs = FileSystem.get(conf);
			FileStatus[] fileStatuses = fs.listStatus(trainFolder);
			BufferedReader br=null;
			for (FileStatus fileStatus : fileStatuses) {
				if (!fileStatus.isDirectory()) {
					try {
						br = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));
						String line;
						while ((line = br.readLine()) != null) {
							Instance trainInstance = new Instance(line);
							trainSet.add(trainInstance);
						}
						br.close();
					} catch(FileNotFoundException e) {
						throw new RuntimeException("文件未找到: " + e.getMessage());
					}
				}
    		}
```

其次，设定输入k个neighbour，即每次test时会创建10个可能的label值，然后在label内找到最频繁的会作为最终的label值。因此，先进行创建的初始化：

```java
ArrayList<Double> distance = new ArrayList<Double>(k);
			ArrayList<DoubleWritable> trainLable = new ArrayList<DoubleWritable>(k);
			for(int i = 0;i < k;i++){
				distance.add(Double.MAX_VALUE);
				trainLable.add(new DoubleWritable(-1.0));
			}
```

其次，遍历训练集，观察训练集中是否有距离小于distance中的当前值，如果有，则同时替换可能的label和当前distance，不断遍历，这样就能找到top-k的可能label：

```java
Instance testInstance = new Instance(textLine.toString());
			for(int i = 0;i < trainSet.size();i++){
				try {
					double dis = Distance.EuclideanDistance(trainSet.get(i).getAtrributeValue(), testInstance.getAtrributeValue());
					int index = indexOfMax(distance);
					if(dis < distance.get(index)){
						distance.remove(index);
					    trainLable.remove(index);
					    distance.add(dis);
					    trainLable.add(new DoubleWritable(trainSet.get(i).getLable()));
					}
				} catch (Exception e) {
					e.printStackTrace();
				}		
			}			
```

然后Mapper处理完后，在Reducer处进行最频繁label的选择：

```java
for(ListWritable<DoubleWritable> val: kLables){
				try {
					predictedLable = valueOfMostFrequent(val);
					break;
				} catch (Exception e) {
					e.printStackTrace();
				}
}
```

##### 执行流程

在补充完输入输出部分后，接下来在exp_2_3/KNN/knn部分下执行：

```
mvn package
```

执行命令后，就会生成knn-1.0-SNAPSHOT.jar

然后我们把目录切换到/usr/local/hadoop，仿照实验一的操作，启动hdfs文件系统建立链接：

```bash
sbin/start-dfs.sh
```

之后在hdfs中建立input文件夹，执行代码如下：

```bash
bin/hdfs dfs -mkdir /user
bin/hdfs dfs -mkdir /user/exp_2
bin/hdfs dfs -mkdir /user/exp_2/exp_2_3
bin/hdfs dfs -mkdir /user/exp_2/exp_2_3/input_1
bin/hdfs dfs -mkdir /user/exp_2/exp_2_3/input_2
```

注意到这里创建了两个input文件夹，其中input_1用于测试集存放，input_2用于训练集存放

然后我们将源数据分割成训练集和测试集，注意到测试集为了mask label，这里将label掩盖成'-1'，事实上代码中并不需要这样处理，这里仅作演示：

1. 在分割前，首先进行数据的预处理，为了实验的方便，本次实验全部采用了连续型特征：

```python
df=pd.read_csv('/home/hadoop/Workspace/exp_2/exp_2_1/input/application_data.csv')
df=df[['SK_ID_CURR','TARGET','AMT_INCOME_TOTAL','NAME_EDUCATION_TYPE','AMT_CREDIT','NAME_HOUSING_TYPE','NAME_FAMILY_STATUS','NAME_INCOME_TYPE','NAME_CONTRACT_TYPE','REGION_POPULATION_RELATIVE','DAYS_BIRTH','DAYS_EMPLOYED','DAYS_REGISTRATION','DAYS_ID_PUBLISH','WEEKDAY_APPR_PROCESS_START','HOUR_APPR_PROCESS_START','ORGANIZATION_TYPE']]
```

2. 查看缺失值情况并进行缺失值处理（采用median policy）：

```python
df.isnull().sum()
for i in df.columns:
    if df[i].dtypes == 'object':
        df[i].fillna(df[i].mode()[0], inplace=True)
    else:
        df[i].fillna(df[i].median(), inplace=True)
print(df)
```

3. 对数据进行标准化处理，这里考虑到KNN的适用性，使用StandardScaler policy：

```python
def normalize_df(df,normalization='standard'):
    if normalization == 'standard':
        normalizer = preprocessing.StandardScaler()
    else:
        raise ValueError('No such option')
    numeric_cols = df.select_dtypes(include=['float64','int64']).columns
    numeric_cols  = numeric_cols[2:]
    print(numeric_cols)
    df[numeric_cols] = normalizer.fit_transform(df[numeric_cols])
    return df
normalize_df(df,normalization='standard')
```

4. 相关性检验

```python
ax = sns.heatmap(df.corr(),annot=False)
plt.show()
```

结果如下：

![image-20231114235426873](https://github.com/Maroon1989/Finance_big_data/blob/master/exp_2/image/image-5.png)

5. 进行分割与label替换

```python
X  = df.drop(['SK_ID_CURR','TARGET'],axis=1)
y = df.TARGET
X_train,X_test,y_train,y_test=train_test_split(X,y,test_size=0.2,random_state=1)
y_t = pd.DataFrame({'y':[-1]*len(y_test)},index=y_test.index)
train_data = pd.concat([X_train, y_train], axis=1)
test_data = pd.concat([X_test, y_t], axis=1)
test_data_2 = pd.concat([X_test, y_test], axis=1)
train_data.to_csv("train.txt", index=False, header=None, sep=' ')
test_data.to_csv("test.txt", index=False, header=None, sep=' ')
test_data.to_csv("test_original.txt",index=False, header=None, sep=' ')
```

生成三个txt文件，其中train.txt是训练集文件，test.txt是放入input_1中的测试集文件，test_original.txt是原始测试集文件，目的是为了与算法运行结果进行对比与评估。

之后仿照之前实验的代码，把train.txt与test.txt放入input_2与input_1中，执行下面代码即可：

```bash
bin/hadoop jar /home/hadoop/Workspace/exp_2/exp_2_1/exp_2_3/KNN/knn/target/knn-1.0-SNAPSHOT.jar com.knn.KNN /user/exp_2/exp_2_3/knn/input_1 /user/exp_2/exp_2_3/knn/output /user/exp_2/exp_2_3/knn/input_2 10
```

其中10是‘k’值，也即top-k的范围。

最后，利用hadoop命令执行mapreduce代码（注意要保证output文件不存在，否则用命令进行删除）：

```bash
bin/hadoop jar /home/hadoop/Workspace/exp_2/exp_2_1/exp_2_2/target/exp_2_2-1.0-SNAPSHOT.jar com.exp_2_2.exp_2_2 /user/exp_2/input /user/exp_2/exp_2_2/output
```

执行结果如下：

![image-20231115002920025](https://github.com/Maroon1989/Finance_big_data/blob/master/exp_2/image/image-6.png)

可见执行成功！

###### evaluate

提交的作业中有一份evaluate.py可以进行测试集的evaluation

具体而言实现了以下几个部分：

1. cal_accuracy

```python
def cal_accuracy(y_pred, y_true):
    return accuracy_score(y_true,y_pred)
```

2. cal_f1

```python
def cal_f1(y_pred, y_true):
    return f1_score(y_true,y_pred)
```

3. cal_recall

```python
def cal_recall(y_pred, y_true):
    return recall_score(y_true,y_pred)
```

4. cal_precision

```python
def cal_precison(y_pred, y_true):
    return precision_score(y_true,y_pred)
```

5. roc_auc_score

```python
print(roc_auc_score(last_column_array,data_array))
```

6. classification_report

```python
print(classification_report(last_column_array,data_array))
```

结果如下：

![26ace65c163d5e5056da97496bb170e](https://github.com/Maroon1989/Finance_big_data/blob/master/exp_2/image/image-7.png)

可以看到目标已经实现。

#### 可扩展之处

从数据集的结构可以看到，1的标签数量非常少，说明数据集非常不平衡，所学习到的模型必然面临着过拟合、泛化性较差等问题，这需要进一步使用数据预处理方法去缓解问题。在kaggle community中，事实上有这样的解决办法。







