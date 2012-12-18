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
package com.gotoalberto.mongohadoopspringappexample.reducer;

// Mongo

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The treasury yield reducer.
 */
public class TreasuryYieldReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
	@Override
	public void reduce(final IntWritable pKey, final Iterable<DoubleWritable> pValues, final Context pContext)
			throws IOException, InterruptedException {
		int count = 0;
		double sum = 0;
		for (final DoubleWritable value : pValues) {
			LOG.debug("Key: " + pKey + " Value: " + value);
			sum += value.get();
			count++;
		}

		final double avg = sum / count;

		LOG.debug("Average 10 Year Treasury for " + pKey.get() + " was " + avg);

		pContext.write(pKey, new DoubleWritable(avg));
	}

	private static final Log LOG = LogFactory.getLog(TreasuryYieldReducer.class);

}
