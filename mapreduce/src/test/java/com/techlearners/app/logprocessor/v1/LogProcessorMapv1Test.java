package com.techlearners.app.logprocessor.v1;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.*;
import org.junit.Before;
import org.junit.Test;

import com.techlearners.app.logprocessor.v1.LogProcessorMapv1;
import com.techlearners.app.logprocessor.v1.LogProcessorReducev1;

public class LogProcessorMapv1Test {
	
	MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver = null;
	MapDriver<LongWritable, Text, Text, LongWritable> mapDriver = null;
	ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver = null;
	LogProcessorMapv1 mapper = new LogProcessorMapv1();
	LogProcessorReducev1 reducer = new LogProcessorReducev1();
	@Before
	public void setUp(){
		mapDriver = new MapDriver<LongWritable, Text, Text, LongWritable>();
		mapDriver.setMapper(mapper);
		reduceDriver = new ReduceDriver<Text, LongWritable, Text, LongWritable>();
		reduceDriver.setReducer(reducer);
		mapReduceDriver=new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable>();
		
		
	}

	@Test
	public void pass_map_withOkResponse() throws IOException {
		mapDriver.withInput(new LongWritable(1), new Text("unicomp6.unicomp.net - - [01/Jul/1995:00:02:17 -0400] \"GET /facilities/lcc.html HTTP/1.0\" 200 2489"));
		mapDriver.withOutput(new Text("unicomp6.unicomp.net"), new LongWritable(1));
		mapDriver.runTest();
	}
   
	@Test
	public void pass_reduce_withOkResponse() throws IOException {
		List<LongWritable> values = new ArrayList<LongWritable>();
		values.add(new LongWritable(1));
		values.add(new LongWritable(1));
		reduceDriver.withInput(new Text("unicomp6.unicomp.net"), values);
		reduceDriver.withOutput(new Text("unicomp6.unicomp.net"), new LongWritable(2));
		reduceDriver.runTest();
		
	}
	@Test
	public void pass_mapreducejob(){
		mapReduceDriver.withMapper(mapper).
		withInput(new LongWritable(1), new Text("unicomp6.unicomp.net - - [01/Jul/1995:00:02:17 -0400] \"GET /facilities/lcc.html HTTP/1.0\" 200 2489"))
		.withInput(new LongWritable(2), new Text("unicomp6.unicomp.net - - [01/Jul/1995:00:02:17 -0400] \"GET /facilities/lcc.html HTTP/1.0\" 200 2489"))
		.withInput(new LongWritable(2), new Text("unicomp6.unicomp.net - - [01/Jul/1995:00:02:17 -0400] \"GET /facilities/lcc.html HTTP/1.0\" 404 2489"))
		.withReducer(reducer).withOutput(new Text("unicomp6.unicomp.net"), new LongWritable(2));
	}
	
}
