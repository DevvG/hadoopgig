package com.techlearners.app.logprocessor.v1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class LogProcessorReducev1 extends Reducer<Text, LongWritable, Text, LongWritable>{
    LongWritable result = new LongWritable();
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for(LongWritable value : values){
			sum += value.get();
		}
		result.set(sum);
		context.write(key, result);
	}

	

}
