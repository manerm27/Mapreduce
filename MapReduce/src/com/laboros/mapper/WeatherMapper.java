package com.laboros.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * @author svaduka Compute the max temperature recorded in the given year, along
 *         with identify the date, value
 * 
 */
public class WeatherMapper extends Mapper<LongWritable, Text, Text, Text> {

	String fileName = null;

	protected void setup(
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>.Context context)
			throws java.io.IOException, InterruptedException {
		FileSplit fileSplit=(FileSplit)context.getInputSplit();
		fileName=fileSplit.getPath().getName();
	};

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		// key -- 0
		// value -- 27516 20140101 2.424 -156.61 71.32 -16.6 -18.7 -17.7 -17.7
		// 0.0 0.00 C -17.8 -19.4 -18.7 83.8 73.5 80.8 -99.000 -99.000 -99.000
		// -99.000 -99.000 -9999.0 -9999.0 -9999.0 -9999.0 -9999.0

		// step-1: Convert into String

		final String iLine = value.toString();
		// Step-2 : Check for nulls
		if (StringUtils.isNotEmpty(iLine)) {
			// Step-3: Identify the type of data : Fixed Width
			// Go with length
			// I Need
			// 1: Date
			// 2: Year
			// 3: Max_temp

			String date = StringUtils.substring(iLine, 6, 14);// 20140101
			String year = StringUtils.substring(date, 0, 4);
			String max_temp = StringUtils.substring(iLine, 38, 45);

			// Identify the map output key
			// Identify the map output value

			context.write(new Text(year), new Text(date + "_" + max_temp+"_"+fileName));

		}

	};
}
