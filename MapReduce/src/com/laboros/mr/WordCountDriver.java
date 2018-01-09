package com.laboros.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.laboros.mapper.WordCountMapper;
import com.laboros.reducer.WordCountReducer;

public class WordCountDriver extends Configured implements Tool {

	public static void main(String[] args) {

		// Total 3 steps
		// Validation
		if (args.length < 2) {
			System.out
					.println("Java Usage "
							+ WordCountDriver.class.getName()
							+ " [configuration] /path/to/hdfs/file " +
							"/path/to/hdfs/destination/dir");
			return;
		}
		// Loading Configuration
		Configuration conf = new Configuration(Boolean.TRUE);
		// Invoke ToolRunner.run

		try {
			int i = ToolRunner.run(conf, new WordCountDriver(), args);
			if (i == 0) {
				System.out.println("SUCCESS");
			}
		} catch (Exception e) {
			System.out.println("FAILED");
			e.printStackTrace();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		//Total 10 steps
		
		//step -1 : Getting the configuration
		//This configuration will set the parameters in the ToolRunner.run
		//Getting the configuration from super
		
		Configuration conf = super.getConf(); 
		
		//step-2: Create Job Instance
		Job wordCountJob = Job.getInstance(conf, WordCountDriver.class.getName());
		
		//Step-3: Setting the classpath of the mapper class jar name 
		//on the datanode
		wordCountJob.setJarByClass(WordCountDriver.class);
		
		//step-4: Setting the input
		final String hdfsInput = args[0];
		final Path hdfsInputPath = new Path(hdfsInput);
		TextInputFormat.addInputPath(wordCountJob, hdfsInputPath);
		wordCountJob.setInputFormatClass(TextInputFormat.class);
		
		//step-5: Setting the output
		
		final String hdfsOutput=args[1];
		final Path hdfsOutputPath = new Path(hdfsOutput);
		TextOutputFormat.setOutputPath(wordCountJob, hdfsOutputPath);
		wordCountJob.setOutputFormatClass(TextOutputFormat.class);
		
		//Step-6: Setting the Mapper
		
		wordCountJob.setMapperClass(WordCountMapper.class);
		
		//step-7: Setting the Reducer
		wordCountJob.setReducerClass(WordCountReducer.class);
		//step-8: SOME THING
		
		wordCountJob.setMapOutputKeyClass(Text.class);
		wordCountJob.setMapOutputValueClass(IntWritable.class);
		
		//step-9: Some Thing -2 
		
		wordCountJob.setOutputKeyClass(Text.class);
		wordCountJob.setOutputValueClass(IntWritable.class);
		//Step: 10 : Trigger Method
		wordCountJob.waitForCompletion(Boolean.TRUE);
		return 0;
	}

}
