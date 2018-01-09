package com.laboros.reducer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, java.lang.Iterable<Text> values,
			Context context) throws java.io.IOException, InterruptedException {

		// key -- 2014
		// value {20141112_11,20140101_23,20140112_22.5}

		// year max_temp_recorded_date max_temp
		String max_temp_recorded_date="";
		float max_temp = Float.MIN_VALUE;
		String fileName = null;
		for (Text date_temp : values) {
			final String[] tokens = StringUtils.splitPreserveAllTokens(
					date_temp.toString(), "_");
			
			//tokens[0] = 20141112
			//tokens[1] = 11

			float temp=Float.parseFloat(tokens[1]);
			if(max_temp<temp)
			{
				max_temp=temp;
				max_temp_recorded_date=tokens[0];
				fileName=tokens[2];
			}
			
		}
		context.write(key,new Text(max_temp_recorded_date+"\t"+max_temp+"\t"+fileName));
	};
}
