package com.laboros.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.laboros.mapper.WeatherMapper;
import com.laboros.reducer.WeatherReducer;

public class WeatherDriver extends Configured implements Tool {

	public static void main(String[] args) {

		// Total 3 steps
		// Validation
		if (args.length < 2) {
			System.out
					.println("Java Usage "
							+ WeatherDriver.class.getName()
							+ " [configuration] /path/to/hdfs/file " +
							"/path/to/hdfs/destination/dir");
			return;
		}
		// Loading Configuration
		Configuration conf = new Configuration(Boolean.TRUE);
		// Invoke ToolRunner.run

		try {
			int i = ToolRunner.run(conf, new WeatherDriver(), args);
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
		Job weatherJob = Job.getInstance(conf, WeatherDriver.class.getName());
		
		//Step-3: Setting the classpath of the mapper class jar name 
		//on the datanode
		weatherJob.setJarByClass(WeatherDriver.class);
		
		//step-4: Setting the input
		final String hdfsInput = args[0];
		final Path hdfsInputPath = new Path(hdfsInput);
		TextInputFormat.addInputPath(weatherJob, hdfsInputPath);
		weatherJob.setInputFormatClass(TextInputFormat.class);
		
		//step-5: Setting the output
		
		final String hdfsOutput=args[1];
		final Path hdfsOutputPath = new Path(hdfsOutput);
		TextOutputFormat.setOutputPath(weatherJob, hdfsOutputPath);
		weatherJob.setOutputFormatClass(TextOutputFormat.class);
		
		//Step-6: Setting the Mapper
		
		weatherJob.setMapperClass(WeatherMapper.class);
		
		//step-7: Setting the Reducer
		weatherJob.setReducerClass(WeatherReducer.class);
		//step-8: Set the Mapper Output Key and Value classes
		
		weatherJob.setMapOutputKeyClass(Text.class);
		weatherJob.setMapOutputValueClass(Text.class);
		
		//step-9: Set the Reducer Output Key and Value classes 
		
//		weatherJob.setOutputKeyClass(Text.class);
//		weatherJob.setOutputValueClass(IntWritable.class);
		//Step: 10 : Trigger Method
		weatherJob.waitForCompletion(Boolean.TRUE);
		return 0;
	}

}
