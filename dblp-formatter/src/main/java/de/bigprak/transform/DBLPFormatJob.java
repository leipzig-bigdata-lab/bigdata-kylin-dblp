package de.bigprak.transform;

import org.apache.flink.api.common.operators.Order;

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
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import de.bigprak.transform.cogroup.Join;
import de.bigprak.transform.cogroup.PublicationMerge;
import de.bigprak.transform.flatmap.DecadeCalculator;
import de.bigprak.transform.flatmap.IdExtractor;
import de.bigprak.transform.flatmap.KeyCleaner;
import de.bigprak.transform.flatmap.Splitter;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

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
	private static String LINE_DELIMITTER;
	private static String FIELD_DELIMITTER;
	private final static char QUOTE_CHAR = '"';
	private static String SOURCE_PATH;
	private static String TARGET_PATH;
	
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		//get parameters from command line
		processArgs(args);
		try {
			// set up the execution environment
			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			//read source files and split fields
			CsvReader publicationsReader = env.readCsvFile(SOURCE_PATH + "dblp-publications.csv")
					.parseQuotedStrings(QUOTE_CHAR)
					.fieldDelimiter(FIELD_DELIMITTER)
					.lineDelimiter(LINE_DELIMITTER)
					.ignoreInvalidLines();
			CsvReader collectionsReader = env.readCsvFile(SOURCE_PATH + "dblp-collections.csv")
					.parseQuotedStrings(QUOTE_CHAR)
					.fieldDelimiter(FIELD_DELIMITTER)
					.lineDelimiter(LINE_DELIMITTER)
					.ignoreInvalidLines();
			
			//extract citings from source files 
			DataSet<Tuple2<Long, String>> cites = publicationsReader
					.includeFields(true, false, false, false, false, false, false, true)
					.ignoreInvalidLines()
					.types(Long.class, String.class)
					.flatMap(new Splitter<Tuple2<Long, String>>(1))
					.flatMap(new KeyCleaner<Tuple2<Long, String>>(1));
			
			//extract data for title table from source files 
			DataSet<Tuple3<Long, String, String>> titles = generateUniqueIds(publicationsReader
				.includeFields(true, false, true, true)
				.ignoreInvalidLines()
				.types(Long.class, String.class, String.class))
				.project(0,2,1);//swap parameters
			
			//extract data for author table from source files 
			DataSet<Tuple3<Long, String, Long>> authors = generateUniqueIds(publicationsReader
				.includeFields(true, false, false, false, false, false, true)
				.ignoreInvalidLines()
				.types(Long.class, String.class, Long.class)
				.flatMap(new Splitter<Tuple3<Long, String, Long>>(1)) //split authors
				.distinct(1)); //eliminate duplicates
			
			//extract data for time table from source files 
			DataSet<Tuple5<Long, Long, Long, Long, Long>> times = generateUniqueIds(publicationsReader
				.includeFields(true, false, false, false, true)
				.ignoreInvalidLines()
				.types(Long.class, Long.class, Long.class, Long.class, Long.class)
				.flatMap(new DecadeCalculator()) //calculate specific decade and fill decade field
				.distinct(1)); //eliminate duplicates
			
			//extract data for venue series table from source files
			DataSet<Tuple3<Long, String, String>> venueSeries = generateUniqueIds(collectionsReader
				.includeFields(true, false, true, true)
				.ignoreInvalidLines()
				.types(Long.class, String.class, String.class))
				.project(0,2,1); //swap fields
			
			//there are document types in publications and collections.csv so have to be merged
			//extract data for document type table from publication csv
			DataSet<Tuple3<Long, String, String>> documentTypeSetA = publicationsReader
				.includeFields(true, true)
				.ignoreInvalidLines()
				.types(Long.class, String.class, String.class);
			//extract document types from collections csv and append to document_type.csv
			DataSet<Tuple3<Long, String, String>> documentTypeSetB = collectionsReader
				.includeFields(true, true).ignoreInvalidLines()
				.types(Long.class, String.class, String.class);
			DataSet<Tuple3<Long, String, String>> documentTypes = generateUniqueIds(documentTypeSetA.union(documentTypeSetB).distinct(1));
			
			//get all fields except of cites from publications.csv as preparation for joins
			DataSet<Tuple7<Long, String, String, String, Long, String, String>> pubs = publicationsReader
					.includeFields(true, true, true, true, true, true, true)
					.ignoreInvalidLines()
					.types(Long.class, String.class, String.class, String.class, Long.class, String.class, String.class)
					.flatMap(new KeyCleaner<Tuple7<Long, String, String, String, Long, String, String>>(5));//remove :ref from venue series
			
			//join the dataset with extracted dimension 'tables'
			//join on names, titles, ...
			//get a map of fact table id and dimension table id
			DataSet<Tuple2<Long, Long>> timeJoin = pubs.coGroup(times).where(4).equalTo(1).with(new Join());
			DataSet<Tuple2<Long, Long>> titleJoin = pubs.coGroup(titles).where(2).equalTo(2).with(new Join());
			DataSet<Tuple2<Long, Long>> documentTypeJoin = pubs.coGroup(documentTypes).where(1).equalTo(1).with(new Join());
			DataSet<Tuple2<Long, Long>> venueSeriesJoin = pubs.coGroup(venueSeries).where(5).equalTo(2).with(new Join());
			DataSet<Tuple2<Long, Long>> authorJoin = pubs.flatMap(new Splitter<Tuple7<Long, String, String, String, Long, String, String>>(6)).coGroup(authors).where(6).equalTo(1).with(new Join());
			DataSet<Tuple2<Long, Long>> citeJoin = pubs.coGroup(cites).where(2).equalTo(1).with(new Join());
			
			//prepare fact table 
			DataSet<Tuple9<Long, Long, Long, Long, Long, Long, Long, Long, Long>> publications = publicationsReader
					.includeFields(true)
					.ignoreInvalidLines()
					.types(Long.class, Long.class, Long.class, Long.class, Long.class, Long.class, Long.class, Long.class, Long.class);
			
			//merge joins
			//join on fact table ids, then dispose joins except of author map
			publications = publications.coGroup(timeJoin).where(0).equalTo(0).with(new PublicationMerge(3));
			publications = publications.coGroup(titleJoin).where(0).equalTo(0).with(new PublicationMerge(1));
			publications = publications.coGroup(venueSeriesJoin).where(0).equalTo(0).with(new PublicationMerge(4));
			publications = publications.coGroup(documentTypeJoin).where(0).equalTo(0).with(new PublicationMerge(2));
			
			//save dimension and fact table/s
			saveDataSetAsCsv(TARGET_PATH + "publication_citing_map/publication_citing_map.csv", citeJoin, 0);
			saveDataSetAsCsv(TARGET_PATH + "title/title.csv", titles, 0);
			saveDataSetAsCsv(TARGET_PATH + "time/time.csv", times, 0);
			saveDataSetAsCsv(TARGET_PATH + "venue_series/venue_series.csv", venueSeries, 0);
			saveDataSetAsCsv(TARGET_PATH + "author/author.csv", authors, 0);
			saveDataSetAsCsv(TARGET_PATH + "document_type/document_type.csv", documentTypes, 0);
			saveDataSetAsCsv(TARGET_PATH + "author_publication_map/author_publication_map.csv", authorJoin, 0);
			saveDataSetAsCsv(TARGET_PATH + "publication/publication.csv", publications,0);
			
			env.execute();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	protected static ArgumentParser createArgsParser() {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("pub-importer");
        parser.version(DBLPFormatJob.class.getPackage().getImplementationVersion());
        parser.addArgument("-V", "--version").action(Arguments.version());

        parser.addArgument("-ld")
              .help("line delimitter")
              .type(String.class)
              .setDefault("\n");

        parser.addArgument("-fd")
	        .help("field delimitter")
	        .type(String.class)
	        .setDefault(",");
        
        parser.addArgument("-source")
        	.help("source path")
        	.required(true);

        parser.addArgument("-target")
        	.help("target path, containing files will be overwritten")
        	.required(true);

        return parser;
    }
	
	private static void processArgs(String[] rawArgs)
	{
        ConsoleAppender appender = new ConsoleAppender(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN), "System.err");
        appender.setThreshold(Level.WARN);
        Logger.getRootLogger().addAppender(appender);
        Namespace args = createArgsParser().parseArgsOrFail(rawArgs);
        
        FIELD_DELIMITTER = args.getString("fd");
        LINE_DELIMITTER = args.getString("ld");
        SOURCE_PATH = args.getString("source");
        TARGET_PATH = args.getString("target");
        
        if(!SOURCE_PATH.endsWith("/"))
        {
        	SOURCE_PATH += "/";
        }
        if (!TARGET_PATH.endsWith("/"))
        {
        	TARGET_PATH += "/";
        }
	}
	
	private static <T extends Tuple> DataSet<T> generateUniqueIds(DataSet<T> dataSet) throws Exception
	{
		return DataSetUtils.zipWithUniqueId(dataSet)
				.flatMap(new IdExtractor<T>());
	}
	
	private static <T extends Tuple> DataSink<?> saveDataSetAsCsv(String path, DataSet<T> dataSet, int sortIndex)
	{
		return 
			dataSet
			.writeAsCsv(path, LINE_DELIMITTER, FIELD_DELIMITTER, WriteMode.OVERWRITE)
			.setParallelism(1) //save output as 1 file
			.sortLocalOutput(sortIndex, Order.ASCENDING);
	}
}
