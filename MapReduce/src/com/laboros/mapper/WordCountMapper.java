package com.laboros.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Context context)
			throws java.io.IOException, InterruptedException 
			{
		//key  --- 0
		//value --- DEER RIVER RIVER
		
//		final long lineOffset= key.get();
		final String iLine = value.toString();
		
		if(StringUtils.isNotEmpty(iLine))
		{
			final String[] tokens = StringUtils.splitPreserveAllTokens(iLine, " ");
			//tokens[0] -- DEER
			//tokens[1] --- RIVER
			//tokens[2] --- RIVER
			
			for (String token : tokens) {
				context.write(new Text(token), new IntWritable(1));
			}
		}
	};
}








