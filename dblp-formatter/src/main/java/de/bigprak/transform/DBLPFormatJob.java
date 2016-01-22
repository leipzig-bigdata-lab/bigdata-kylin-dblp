package de.bigprak.transform;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.TITLE;

import akka.io.Tcp.Write;
import de.bigprak.transform.schema.Title;
import scala.collection.parallel.ParIterableLike.FlatMap;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink program.
 * <li>use Tuple data types.
 * <li>write and use user-defined functions.
 * </ul>
 *
 */
public class DBLPFormatJob {

	//
	//	Program
	//
	private final static String LINE_DELIMITTER = "\n";
	private final static String FIELD_DELIMITTER = "\\,";
	
	
	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		CsvReader publicationsReader = env.readCsvFile("dblp-src/publications.csv")
				.fieldDelimiter(FIELD_DELIMITTER)
				.lineDelimiter(LINE_DELIMITTER);
		CsvReader collectionsReader = env.readCsvFile("dblp-src/collections.csv")
				.fieldDelimiter(FIELD_DELIMITTER)
				.lineDelimiter(LINE_DELIMITTER);
		
		//transform dimensions
		//titles
		DataSet<Tuple2<Long, String>> titles = publicationsReader
			.includeFields(true, false, false, true)
			.types(Long.class, String.class)
			.distinct(1);
		
		//document types
		DataSet<Tuple3<Long, String, String>> documentTypeSetA = publicationsReader
			.includeFields(true, true)
			.types(Long.class, String.class, String.class)
			.distinct(1);
		
		//authors
		DataSet<Tuple3<Long, String, Long>> authors = publicationsReader
			.includeFields(true, false, false, false, false, false, true)
			.types(Long.class, String.class, Long.class)
			.distinct(1);
		
		//time
		DataSet<Tuple5<Long, Long, Long, Long, Long>> times = publicationsReader
			.includeFields(true, false, false, false, true)
			.types(Long.class, Long.class, Long.class, Long.class, Long.class)
			.distinct(1)
			.flatMap(new DecadecCalculator());
		
		//venue series
		DataSet<Tuple3<Long, String, String>> venueSeries = collectionsReader
			.includeFields(true, false, false, true)
			.types(Long.class, String.class, String.class)
			.distinct(1);
		
		//extract document types from collections csv and append to document_type.csv
		DataSet<Tuple3<Long, String, String>> documentTypeSetB = collectionsReader
			.includeFields(true, true)
			.types(Long.class, String.class, String.class)
			.distinct(1);
		
		saveDataSetAsCsv("dblp-target/title.csv", titles);
		saveDataSetAsCsv("dblp-target/time.csv", times);
		saveDataSetAsCsv("dblp-target/venue_series.csv", venueSeries);
		saveDataSetAsCsv("dblp-target/author.csv", authors);
		saveDataSetAsCsv("dblp-target/document_type.csv", documentTypeSetA.union(documentTypeSetB));
		
		env.execute();
	}
	
	private static <T extends Tuple> DataSink<?> saveDataSetAsCsv(String path, DataSet<T> dataSet)
	{
		return DataSetUtils.zipWithUniqueId(dataSet)
			.flatMap(new IdGenerator<T>())
			.writeAsCsv(path, LINE_DELIMITTER, FIELD_DELIMITTER, WriteMode.OVERWRITE)
			.setParallelism(1) //save output as 1 file
			.sortLocalOutput(0, Order.ASCENDING);
	}
	
	public static final class IdGenerator<T extends Tuple> implements FlatMapFunction<Tuple2<Long, T>, T>
	{
		/**
		 * 
		 */
		private static final long serialVersionUID = 7218101125158000353L;

		@Override
		public void flatMap(Tuple2<Long, T> value, Collector<T> out) throws Exception {
			Long id = value.f0;
			T output = value.f1;
			output.setField(id, 0);
			out.collect(output);
		}
	}
	
	public static final class DecadecCalculator implements FlatMapFunction<Tuple5<Long, Long, Long, Long, Long>, Tuple5<Long, Long, Long, Long, Long>>
	{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1375521501446745137L;

		@Override
		public void flatMap(Tuple5<Long, Long, Long, Long, Long> value,
				Collector<Tuple5<Long, Long, Long, Long, Long>> out) throws Exception {
			// TODO Auto-generated method stub
			Long year = value.f1;
			year = (year % 100) / 10;
			Long decade = year * 10;
			value.setField(decade, 4);
			if(value.f4 == null)
				System.out.println(year + " " + decade);
			out.collect(value);
		}
	}
}
