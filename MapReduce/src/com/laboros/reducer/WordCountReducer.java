package com.laboros.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(
			Text key,
			java.lang.Iterable<IntWritable> values,
			Context context)
 			throws java.io.IOException, InterruptedException 
 			{
		//key -- BEAR
		//values -- {1,2}
		
//		String outputKey=key.toString();
		int count=0;
		
		for (IntWritable intWritable : values) {
			count += intWritable.get();
		}
		
		context.write(key, new IntWritable(count));
	};
}
