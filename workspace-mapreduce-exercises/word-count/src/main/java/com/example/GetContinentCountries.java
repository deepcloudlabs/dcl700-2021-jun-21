package com.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GetContinentCountries {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "get continents # of countries");
		job.setJarByClass(GetContinents.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path outputPath = new Path(args[1]);
		FileSystem fs = outputPath.getFileSystem(new Configuration());
		if (fs.exists(outputPath))
			fs.delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setMapperClass(ContinentIntMapper.class);
		job.setCombinerClass(ContinentIntReducer.class);
		job.setReducerClass(ContinentIntReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

class ContinentIntMapper extends Mapper<Object, Text, Text, IntWritable> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\",\"");
		context.write(new Text(tokens[2]), new IntWritable(1));
	}
}

class ContinentIntReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int numOfCountriesInContinent = 0;
		for (IntWritable value : values) {
			numOfCountriesInContinent += value.get();
		}
		context.write(key, new IntWritable(numOfCountriesInContinent));
	}
}