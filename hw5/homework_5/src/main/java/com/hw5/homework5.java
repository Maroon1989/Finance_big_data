package com.hw5;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.BufferedWriter;
// import java.io.FileReader;
import java.io.FileWriter;
// import java.io.IOException;
// import java.util.HashMap;
// import java.util.HashSet;
// import java.util.Map;
// import java.util.Set;
// import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
// import org.apache.poi.ss.usermodel.Cell;
// import org.apache.poi.ss.usermodel.CellType;
// import org.apache.poi.ss.usermodel.Row;
// import org.apache.poi.ss.usermodel.Sheet;
// import org.apache.poi.ss.usermodel.Workbook;
// import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.hadoop.fs.FileStatus;
// import java.io.BufferedReader;
import java.io.InputStreamReader;
// import java.io.OutputStream;
// import java.util.ArrayList;
// import java.util.List;

public class homework5 {
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
		//设置Map端输出的数据类型，这两句话也可以不写
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//设置输出数据类型，这两句话必须写
		
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		//添加输入输出路径
		
		
		job.waitForCompletion(true);
		// 将输出文件转换为xlsx格式
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
    }
}
