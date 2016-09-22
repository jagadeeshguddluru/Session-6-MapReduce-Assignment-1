# Session-6-MapReduce-Assignment-1
package p;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Olymap extends Mapper<LongWritable,Text,Text,IntWritable>  {
	Text country=new Text();
	IntWritable totalmedals=new IntWritable();
	int i=0;
	public void map(LongWritable key,Text value,Context c) throws IOException, InterruptedException{
		String s=value.toString();
		String s1[]=s.split("\t");
		if(s1[5].equalsIgnoreCase("Swimming")){
			country.set(s1[2]);
			i=Integer.parseInt(s1[9]);
			totalmedals.set(i);
			c.write(country, totalmedals);
		}
	}

}


package p;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Olyred extends Reducer<Text,IntWritable,Text,IntWritable> {
public void reduce(Text value,Iterable<IntWritable> key,Context c) throws IOException, InterruptedException{
	int sum=0;
	for(IntWritable x:key)
		sum=sum+x.get();
	c.write(value, new IntWritable(sum));	
}
}


package p;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Olydriver {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		Job j=Job.getInstance(conf, "p.Olydriver.class");
		j.setMapperClass(p.Olymap.class);
		j.setReducerClass(p.Olyred.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		j.setJarByClass(p.Olydriver.class);
		j.setJobName("Olydriver");
		FileInputFormat.addInputPath(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));
		System.exit(j.waitForCompletion(true)?0:1);
	}

}

