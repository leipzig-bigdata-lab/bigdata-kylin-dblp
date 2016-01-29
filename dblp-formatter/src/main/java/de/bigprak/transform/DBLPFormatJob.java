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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.TITLE;

import com.google.inject.util.Types;

import akka.io.Tcp.Write;
import de.bigprak.transform.schema.Publication;
import de.bigprak.transform.schema.Title;
import scala.collection.parallel.ParIterableLike.FlatMap;
import scala.math.Numeric.FloatAsIfIntegral;

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
		
		
		//titles
		DataSet<Tuple3<Long, String, String>> titles = generateUniqueIds(publicationsReader
			.includeFields(true, false, true, true)
			.types(Long.class, String.class, String.class))
			.project(0,2,1);//swap parameters
		
		//document types
		DataSet<Tuple3<Long, String, String>> documentTypeSetA = publicationsReader
			.includeFields(true, true)
			.types(Long.class, String.class, String.class);
		
		//authors
		DataSet<Tuple3<Long, String, Long>> authors = generateUniqueIds(publicationsReader
			.includeFields(true, false, false, false, false, false, true)
			.types(Long.class, String.class, Long.class)
			.flatMap(new Splitter<Tuple3<Long, String, Long>>(1)).distinct(1));
		
		//time
		DataSet<Tuple5<Long, Long, Long, Long, Long>> times = generateUniqueIds(publicationsReader
			.includeFields(true, false, false, false, true)
			.types(Long.class, Long.class, Long.class, Long.class, Long.class)
			.flatMap(new DecadeCalculator()).distinct(1));
		
		//venue series
		DataSet<Tuple3<Long, String, String>> venueSeries = generateUniqueIds(collectionsReader
			.includeFields(true, false, true, true)
			.types(Long.class, String.class, String.class))
			.project(0,2,1);
		venueSeries = venueSeries.flatMap(new KeyCleaner()); //remove ref: from venueseries key
		
		//extract document types from collections csv and append to document_type.csv
		DataSet<Tuple3<Long, String, String>> documentTypeSetB = collectionsReader
			.includeFields(true, true)
			.types(Long.class, String.class, String.class);
		
		DataSet<Tuple3<Long, String, String>> documentTypes = generateUniqueIds(documentTypeSetA.union(documentTypeSetB).distinct(1));
		
//		DataSet<Tuple8<Long, String, String, String, Long, String, String, String>> pubs = publicationsReader
//				.includeFields(true, true, true, true, true, true, true, true)
//				.ignoreInvalidLines()
//				.types(Long.class, String.class, String.class, String.class, Long.class, String.class, String.class, String.class);
		
		DataSet<Tuple7<Long, String, String, String, Long, String, String>> pubs = publicationsReader
				.includeFields(true, true, true, true, true, true, true)
				.types(Long.class, String.class, String.class, String.class, Long.class, String.class, String.class);
		
		
		DataSet<Tuple2<Long, Long>> timeJoin = pubs.coGroup(times).where(4).equalTo(1).with(new Join());
		DataSet<Tuple2<Long, Long>> titleJoin = pubs.coGroup(titles).where(2).equalTo(2).with(new Join());
		DataSet<Tuple2<Long, Long>> documentTypeJoin = pubs.coGroup(documentTypes).where(2).equalTo(1).with(new Join());
		DataSet<Tuple2<Long, Long>> venueSeriesJoin = pubs.coGroup(venueSeries).where(5).equalTo(2).with(new Join());
		DataSet<Tuple2<Long, Long>> authorJoin = pubs.flatMap(new Splitter<Tuple7<Long, String, String, String, Long, String, String>>(6)).coGroup(authors).where(6).equalTo(1).with(new Join());
		
//		DataSet<Tuple10<Long, String, Long, Long, Long, Long, Long, Long, Long, Long>> publications = publicationsReader
//				.includeFields(true, false, true)
//				.types(Long.class, String.class, Long.class, Long.class, Long.class, Long.class, Long.class, Long.class, Long.class, Long.class);
		
		saveDataSetAsCsv("dblp-target/title.csv", titles);
		saveDataSetAsCsv("dblp-target/time.csv", times);
		saveDataSetAsCsv("dblp-target/venue_series.csv", venueSeries);
		saveDataSetAsCsv("dblp-target/author.csv", authors);
		saveDataSetAsCsv("dblp-target/document_type.csv", documentTypes);
		saveDataSetAsCsv("dblp-target/author_publication_map.csv", authorJoin);
//		saveDataSetAsCsv("dblp-target/publication.csv", publications);
		
		env.execute();
	}
	
	private static <T extends Tuple> DataSet<T> generateUniqueIds(DataSet<T> dataSet)
	{
		return DataSetUtils.zipWithUniqueId(dataSet)
				.flatMap(new IdExtractor<T>());
	}
	
	private static <T extends Tuple> DataSink<?> saveDataSetAsCsv(String path, DataSet<T> dataSet, int... indicesForDistinction)
	{
		return 
			dataSet
			.distinct(indicesForDistinction)
			.writeAsCsv(path, LINE_DELIMITTER, FIELD_DELIMITTER, WriteMode.OVERWRITE)
			.setParallelism(1) //save output as 1 file
			.sortLocalOutput(1, Order.ASCENDING);
	}
	
	public static final class Join<T0 extends Tuple, T1 extends Tuple> implements CoGroupFunction<T0, T1, Tuple2<Long,Long>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 5479225801717280691L;

		@Override
		public void coGroup(Iterable<T0> leftElements, 
				Iterable<T1> rightElements, 
				Collector<Tuple2<Long,Long>> out)
				throws Exception {
			
			Iterator<T1> it = rightElements.iterator();
			Long id = 0L;
			if(!it.hasNext())
				id = -1L;
			else
				id = it.next().getField(0);
			
			for (Tuple leftElem : leftElements) {
				out.collect(new Tuple2<Long, Long>((Long) leftElem.getField(0), id));
			}
		}
	}
	
	public static final class IdExtractor<T extends Tuple> implements FlatMapFunction<Tuple2<Long, T>, T>
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
	
	public static final class DecadeCalculator implements FlatMapFunction<Tuple5<Long, Long, Long, Long, Long>, Tuple5<Long, Long, Long, Long, Long>>
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
	
	
	//splits strings seperated by |
	public static final class Splitter<T extends Tuple> implements FlatMapFunction<T, T>
	{
		/**
		 * 
		 */
		private static final long serialVersionUID = 2867536301386194426L;
		
		private int index;
		
		public Splitter(int index) {
			super();
			this.index = index;
		}
		
		@Override
		public void flatMap(T value, Collector<T> out) throws Exception {
			T newValue = value;
			List<String> splittedValues = Arrays.asList(value.getField(index).toString().split("[|]"));
			for(String splittedValue : splittedValues)
			{
				newValue.setField(splittedValue, index);
				out.collect(newValue);
			}
		}
	}
	
	public static final class KeyCleaner implements FlatMapFunction<Tuple3<Long, String, String>, Tuple3<Long, String, String>>
	{

		/**
		 * 
		 */
		private static final long serialVersionUID = 310145510485555611L;

		@Override
		public void flatMap(Tuple3<Long, String, String> value, Collector<Tuple3<Long, String, String>> out)
				throws Exception {
			//add "ref:"
			if(!value.f2.contains("ref:"))
				value = new Tuple3<Long, String, String>(value.f0, value.f1, "ref:" + value.f2);
			out.collect(value);
		}
		
	}
	
}
