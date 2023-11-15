package com.exp_2_2;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.naming.Context;
import javax.swing.text.html.HTMLDocument.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.fs.FileStatus;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
// List<String> columnNames = Arrays.asList(


public class exp_2_2 {
    public static class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		// private Text outKey = new Text();
        // private Text outValue = new Text();
		private static Text text = new Text();
		protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
		//protected void map(数据类型 key,数据类型 value,Context context) throws IOException,InterruptedException{
			if(key.get()==0)
            {
                return;
            }
			String line = value.toString();
			String[]fields = line.split(",");
            text.set(fields[25]);
			context.write(text,new IntWritable(1));
		}
	}
	
	public static class MyReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		// private Text outvalue = new Text();
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
	public static class SecondReduce extends Reducer<IntWritable,Text,Text,IntWritable>{
		// private Text outvalue = new Text();
		IntWritable total = new IntWritable();
		protected void reduce(IntWritable key,Iterable<Text>values,Context context) throws IOException,InterruptedException{
			for(Text value:values){
				context.write(value,key);
			}
		}
	}
	public static class IntWritableDecreasingComparator extends IntWritable.Comparator {
          public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
          }
          
          public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
              return -super.compare(b1, s1, l1, b2, s2, l2);
          }
      }
	
	public static void main(String[] args) throws InterruptedException,IOException,ClassNotFoundException{   //main方法
		
		Configuration conf=new Configuration();
		// conf.set("fs.defaultFS", "hdfs://localhost:9000");
		Path tempdir = new Path("/user/exp_2/exp_2_2/tmp");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage:Merge and duplicate removal <in> <out>");
            System.exit(2);
        }
		Job job=Job.getInstance(conf,"merge");
		job.setJobName("merge");
		job.setJarByClass(exp_2_2.class);
        //上面四句话是设置类信息，方便hadoop从jar文件中找到

		job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReduce.class);
        //设置执行Map与Reduce的类，也就是你之前设置的类名
        
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//设置Map端输出的数据类型，这两句话也可以不写
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//设置输出数据类型，这两句话必须写
		
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		// job.setMapperClass(InverseMapper.class);
		FileOutputFormat.setOutputPath(job, tempdir);
		//添加输入输出路径
		
		// job.setSortComparatorClass(IntWritableDecreasingComparator.class);
		job.waitForCompletion(true);
		Job sortjob = new Job(conf, "sort");
		// Path tmpdir_2 = new Path("/user/exp_2/exp_2_2/tmp_2");
		sortjob.setJarByClass(exp_2_2.class);
		FileInputFormat.addInputPath(sortjob, tempdir);
		sortjob.setInputFormatClass(SequenceFileInputFormat.class);
		sortjob.setMapperClass(InverseMapper.class);
		sortjob.setReducerClass(SecondReduce.class);
		sortjob.setMapOutputKeyClass(IntWritable.class);
		sortjob.setMapOutputValueClass(Text.class);
		sortjob.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(sortjob,new Path(otherArgs[otherArgs.length - 1]));
		sortjob.setOutputKeyClass(Text.class);
		sortjob.setOutputValueClass(IntWritable.class);
		// sortjob.setOutputFormatClass(SequenceFileOutputFormat.class);
		sortjob.setSortComparatorClass(IntWritableDecreasingComparator.class);
		// sortjob.setReducerClass(SecondReduce.class);
		sortjob.waitForCompletion(true);
		FileSystem.get(conf).delete(tempdir);

		// Job reverjob = new Job(conf, "reverse");
		// reverjob.setJarByClass(exp_2_2.class);
		// FileInputFormat.addInputPath(reverjob, tmpdir_2);
		// reverjob.setInputFormatClass(SequenceFileInputFormat.class);
		// reverjob.setMapperClass(InverseMapper.class);
		// reverjob.setNumReduceTasks(1);
		// FileOutputFormat.setOutputPath(reverjob,
		// 			new Path(otherArgs[otherArgs.length - 1]));
		// reverjob.setOutputKeyClass(Text.class);
		// reverjob.setOutputValueClass(IntWritable.class);
		// // sortjob.setSortComparatorClass(IntWritableDecreasingComparator.class);

		// sortjob.waitForCompletion(true);

		FileSystem.get(conf).delete(tempdir);
		System.exit(0);
		// 将输出文件转换为xlsx格式
        // Configuration conf2 = new Configuration();
        // FileSystem fs = FileSystem.get(conf2);

        // // 创建Workbook对象
        // Workbook workbook = new SXSSFWorkbook();

        // // 创建Sheet对象
        // Sheet sheet = workbook.createSheet("Sheet1");

        // int rowIndex = 0;
        // int cellIndex = 0;

        // // 读取Hadoop输出的结果文件
        // Path outputPath = new Path(args[1]);
        // if (fs.exists(outputPath)) {
        //     for (FileStatus status : fs.listStatus(outputPath)) {
        //         if (!status.isDirectory()) {
        //             BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
        //             String line;
        //             while ((line = br.readLine()) != null) {
        //                 Row row = sheet.createRow(rowIndex++);
        //                 Cell cell = row.createCell(cellIndex);
        //                 cell.setCellValue(line);
        //             }
        //             br.close();
        //         }
        //     }
        // }

        // // 保存为xlsx文件
        // Path excelOutputPath = new Path("output.xlsx");
        // OutputStream outputStream = fs.create(excelOutputPath);
        // workbook.write(outputStream);
        // outputStream.close();
        // workbook.close();

        // fs.close();
    }
}
