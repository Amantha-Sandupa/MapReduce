
import java.io.*;
import java.util.*;

// below packages are present in "hadoop-common.jar" file
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
//below packages present in "hadoop-mapreduce-client-core.jar" file
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;




public class Main {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		
	
		Configuration config = new Configuration ();
		
		Job job = Job.getInstance(config, "Main");
		
//		(we are telling hadoop to use this classes while ecexuting map reduce jobs )
		//set the main class
		job.setJarByClass(Main.class);
		//set mapper cls
		job.setMapperClass(Map.class);
		//set reducer clz
		job.setReducerClass(Reduce.class);
		
		//set output key clz
		job.setOutputKeyClass(Text.class);
		//set output value clz
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path outputPath = new Path(args[1]);
		
		//configure the input/output path from the filesystem into job
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
			// hadoop jar filename.jar /inputfile /outputpath 
		
	}
	
		public static class Map extends Mapper <LongWritable, Text, Text, IntWritable> {
			//mapper class  
//			input key - LongWritable
//			input value - Text
//			output key - Text
//			output value - intWritable
			
			public void map(LongWritable key, Text value,
					Context context) throws IOException, InterruptedException{
				// this is my test class 4, is
				
				// key : byte offset (thats why we are using LongWritable)
				// value : this is my test class 4
				String line =value.toString();
				
				StringTokenizer token = new StringTokenizer(line);
				
				while (token.hasMoreTokens()){// checking the tokenizer got any more words or not
					value.set(token.nextToken()); // assigning to next word into value variable
						// this 
						// is 
						// my 
						// test
						// class
						// 4
					
					context.write(value, new IntWritable(1));
						// this, 1
						// is, 1
						// my, 1
						// test, 1
						// class, 1
						// 4, 1
						// is, 1
				}
			}
		}
 

		public static class Reduce extends Reducer <Text, IntWritable, Text, IntWritable>{
			//mapper class  
			//input key - Text
			//input value - IntWritable
			//output key - Text
			//output value - IntWritable
			
			public void reduce(Text key, Iterable<IntWritable> values,
					Context context) throws IOException, InterruptedException{
				
				// this, (1)
				// is, (1 , 1) this is the input for the reduer
				// my, (1)
				// test, (1)
				// class, (1)
				// 4, (1) 
				
				
				int sum = 0;
				
					//sum = 1
					//sum = 2
					//sum = 1
					//sum = 1
					//sum = 1
					//sum = 1
					
				for (IntWritable x: values){
					sum+=x.get();
				}
				context.write(key, new IntWritable(sum));
					// this, 1
					// is, 2
					// my, 1
					// test, 1
					// class, 1
					// 4, 1
			}
		}

		// data types in hadoop are specifically design for destributed file sysytems
		// generic datatypes in java cannot use in the mapreduce 
		
		
		
}

