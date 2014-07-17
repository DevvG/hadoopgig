package com.techlearners.app.logprocessor.v1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogProcessorMapv1 extends Mapper<LongWritable,Text,Text,LongWritable>{

@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
	         LongWritable tempValue = null;
		     String input = value.toString();
		     String tempKey = null;
		     if(input.indexOf('-') != -1){
		    	 tempKey = input.substring(0,(input.indexOf('-')-1)).trim();
		     }
		     if(input.indexOf("200") != -1){
		    	 tempValue = new LongWritable();
		    	 tempValue.set(1);
		     }
		     if(tempKey != null && tempValue != null){
		    	 context.write(new Text(tempKey), tempValue);
		     }
		     
		     
		     
	}
}
