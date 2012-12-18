/**
 * 
 */
package com.gotoalberto.mongohadoopspringappexample.test.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;

//import org.springframework.data.hadoop.mapreduce.ToolRunner;

/**
 * @author consultor_amaris
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/spring/applicationContext-test.xml" })
public class JobTest {
	
	@Autowired
	Configuration fromMongoConfiguration;
	
	@Autowired
	Configuration fromFileConfiguration;

	protected Logger log = Logger.getLogger(this.getClass());

	@Test
	public void testJobFromFile() throws Exception {

		Job job = new Job(fromFileConfiguration, "fromFileJob");

		job.waitForCompletion(false);
		
	}
	
	@Test
	public void testJobFromMongo() throws Exception {
		
		Job job = new Job(fromMongoConfiguration, "fromMongoJob");
		
		job.waitForCompletion(false);
		
	}
		
}
