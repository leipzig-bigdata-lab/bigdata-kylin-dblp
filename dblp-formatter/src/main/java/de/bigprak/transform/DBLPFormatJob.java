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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.TITLE;

import akka.io.Tcp.Write;
import de.bigprak.transform.schema.Title;

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
		CsvReader publicationsReader = env.readCsvFile("dblp-src/publications.csv");
		CsvReader collectionsReader = env.readCsvFile("dblp-src/collections.csv");
		
		//transform dimensions
		//titles
		publicationsReader.fieldDelimiter(FIELD_DELIMITTER)
			.lineDelimiter(LINE_DELIMITTER)
			.includeFields(true, false, false, true)
			.types(Integer.class, String.class)
			.distinct(1)
			.writeAsCsv("dblp-target/title.csv", LINE_DELIMITTER, FIELD_DELIMITTER, WriteMode.OVERWRITE)
			.setParallelism(1)
			.sortLocalOutput(0, Order.ASCENDING);
		
		//document types
		DataSet<Tuple3<Integer, String, String>> documentTypeSetA = publicationsReader.fieldDelimiter(FIELD_DELIMITTER)
			.lineDelimiter(LINE_DELIMITTER)
			.includeFields(true, true)
			.types(Integer.class, String.class, String.class)
			.distinct(1);
		
		//authors
		publicationsReader.fieldDelimiter(FIELD_DELIMITTER)
			.lineDelimiter(LINE_DELIMITTER)
			.includeFields(true, false, false, false, false, false, true)
			.types(Integer.class, String.class, Integer.class)
			.distinct(1)
			.writeAsCsv("dblp-target/author.csv", LINE_DELIMITTER, FIELD_DELIMITTER, WriteMode.OVERWRITE)
			.setParallelism(1)
			.sortLocalOutput(0, Order.ASCENDING);
		
		//time
		publicationsReader.fieldDelimiter(FIELD_DELIMITTER)
			.lineDelimiter(LINE_DELIMITTER)
			.includeFields(true, false, false, false, true)
			.types(Integer.class, Integer.class, Integer.class, Integer.class, Integer.class)
			.distinct(1)
			.writeAsCsv("dblp-target/time.csv", LINE_DELIMITTER, FIELD_DELIMITTER, WriteMode.OVERWRITE)
			.setParallelism(1)
			.sortLocalOutput(0, Order.ASCENDING);
		
		//venue series
		collectionsReader.fieldDelimiter(FIELD_DELIMITTER)
			.lineDelimiter(LINE_DELIMITTER)
			.includeFields(true, false, false, true)
			.types(Integer.class, String.class, String.class)
			.distinct(1)
			.writeAsCsv("dblp-target/venue_series.csv", LINE_DELIMITTER, FIELD_DELIMITTER, WriteMode.OVERWRITE)
			.setParallelism(1)
			.sortLocalOutput(0, Order.ASCENDING);
		
		//extract document types from collections csv and append to document_type.csv
		DataSet<Tuple3<Integer, String, String>> documentTypeSetB = collectionsReader.fieldDelimiter(FIELD_DELIMITTER)
			.lineDelimiter(LINE_DELIMITTER)
			.includeFields(true, true)
			.types(Integer.class, String.class, String.class)
			.distinct(1);
		documentTypeSetA.union(documentTypeSetB)
			.writeAsCsv("dblp-target/document_type.csv", LINE_DELIMITTER, FIELD_DELIMITTER, WriteMode.OVERWRITE)
			.setParallelism(1)
			.sortLocalOutput(0, Order.ASCENDING);
		
		env.execute();
	}
}
