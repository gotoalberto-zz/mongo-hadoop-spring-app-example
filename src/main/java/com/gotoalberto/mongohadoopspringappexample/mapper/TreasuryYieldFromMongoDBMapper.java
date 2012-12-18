/**
*    mongo-hadoop-spring-app-example
*    Copyright (C) 2012
*    @author Alberto Gomez Toribio
*    
*    Linkedin: http://linkedin.com/in/gotoalberto
*	 Twitter: http://twitter.com/gotoalberto
*
*    This program is free software: you can redistribute it and/or modify
*    it under the terms of the GNU General Public License as published by
*    the Free Software Foundation, either version 3 of the License, or
*    (at your option) any later version.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU General Public License for more details.
*
*    You should have received a copy of the GNU General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
**/
package com.gotoalberto.mongohadoopspringappexample.mapper;

// Mongo

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The treasury yield mapper.
 */

public class TreasuryYieldFromMongoDBMapper extends
		Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

	private IntWritable year = new IntWritable();
	private DoubleWritable value = new DoubleWritable();

	

	 public void map(LongWritable key, Text textline,
	 OutputCollector<IntWritable, DoubleWritable> output,
	 Reporter reporter) throws IOException {
	 String line = textline.toString();
	 StringTokenizer itr = new StringTokenizer(line, "#");
	 while (itr.hasMoreTokens()) {
	
	 String sYear = itr.nextToken();
	 String sValue = itr.nextToken();
	
	 year.set(Integer.parseInt(sYear));
	 value.set(Double.parseDouble(sValue));
	
	 output.collect(year, value);
	 }
	 }

}