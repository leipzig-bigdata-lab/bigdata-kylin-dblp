package de.bigprak.transform;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.swing.SortOrder;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
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
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
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

import com.codahale.metrics.CsvReporter;

import de.bigprak.transform.cogroup.Count;
import de.bigprak.transform.cogroup.Join;
import de.bigprak.transform.cogroup.PublicationMerge;
import de.bigprak.transform.csv.CSVRecordToTupleMap;
import de.bigprak.transform.csv.CommonsCSVInputFormat;
import de.bigprak.transform.csv.CommonsCSVOutputFormat;
import de.bigprak.transform.flatmap.AppendTupleFields;
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
	private static String TYPE;
	private static Map<String, Map<String, Integer>> libMap = new HashMap<>();
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		//get parameters from command line
		processArgs(args);
		try {
			// set up the execution environment
			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			boolean isDBLP = "dblp".equals(TYPE);
			
	        CSVFormat csvFormat = CSVFormat.newFormat(',')
	                .withRecordSeparator('\n')
	                .withEscape('\\')
	                .withQuoteMode(QuoteMode.NONE)
	                .withNullString("");
			
			//read sources
			DataSource<CSVRecord> publicationsReader = env.readFile(new CommonsCSVInputFormat(csvFormat), SOURCE_PATH + "/" + TYPE + "-publications.csv");
			DataSource<CSVRecord> collectionsReader = null;
			DataSet<Tuple2<Long, String>> cites = null;
			DataSet<Tuple2<Long, Long>> acmCiteCount = null;
			DataSet<Tuple3<Long, String, String>> venueSeries = null;
			DataSet<Tuple2<Long, String>> documentTypeSetA = null;
			DataSet<Tuple2<Long, String>> documentTypeSetB = null;
			DataSet<Tuple3<Long, String, String>> documentTypes = null;
			DataSet<Tuple3<Long, String, String>> titles = null;
			DataSet<Tuple3<Long, String, Long>> authors = null;
			DataSet<Tuple5<Long, Long, Long, Long, Long>> times = null;
			DataSet<Tuple7<Long, String, String, String, Long, String, String>> pubs = null;
			
			//since dblp and acm have different indices, special handling for each is necessary
			if(isDBLP)
			{
				collectionsReader = env.readFile(new CommonsCSVInputFormat(csvFormat), SOURCE_PATH + "/" + TYPE + "-collections.csv");
			
				//extract citings from source files
				//ACM already counted the cites and has no list of cites stored
				cites = publicationsReader
						.map(new CSVRecordToTupleMap<Tuple2<Long,String>>(new int[] {0, 7}, new Class[] {Long.class, String.class}))
						.returns("Tuple2<Long, String>")
						.flatMap(new Splitter<Tuple2<Long, String>>(1))
						.flatMap(new KeyCleaner<Tuple2<Long, String>>(1));
				
				//extract data for venue series table from source files
				//ACM does not contain venues
				venueSeries = generateUniqueIds(collectionsReader
						.map(new CSVRecordToTupleMap<Tuple3<Long, String, String>>(new int[] {0, 3, 2}, new Class[] {Long.class, String.class, String.class}))
						.returns("Tuple3<Long, String, String>"));
			
				//there are document types in publications and collections.csv so have to be merged
				//extract data for document type table from publication csv
				documentTypeSetA = publicationsReader
						.map(new CSVRecordToTupleMap<Tuple2<Long, String>>(new int[] {0, 1}, new Class[] {Long.class, String.class}))
						.returns("Tuple2<Long, String>");
				
				//extract document types from collections csv and append to document_type.csv
				documentTypeSetB = collectionsReader
						.map(new CSVRecordToTupleMap<Tuple2<Long, String>>(new int[] {0, 1}, new Class[] {Long.class, String.class}))
						.returns("Tuple2<Long, String>");
			
				documentTypes = generateUniqueIds(
						documentTypeSetA
						.union(documentTypeSetB)
						.distinct(1))
						.flatMap(new AppendTupleFields<Tuple2<Long,String>, Tuple3<Long, String, String>>(new Tuple3<Long, String, String>(-1L, "-1", "-1")))
						.returns("Tuple3<Long, String, String>");
				
				//extract data for title table from source files 
				titles = generateUniqueIds(publicationsReader
						.map(new CSVRecordToTupleMap<Tuple3<Long, String, String>>(new int[] {0, 3, 2}, new Class[] {Long.class, String.class, String.class}))
						.returns("Tuple3<Long, String, String>"));
				
				//extract data for author table from source files 
				authors = generateUniqueIds(publicationsReader
						.map(new CSVRecordToTupleMap<Tuple2<Long, String>>(new int[] {0, 6}, new Class[] {Long.class, String.class}))
						.returns("Tuple2<Long, String>")
						.flatMap(new AppendTupleFields<Tuple2<Long, String>, Tuple3<Long, String, Long>>(new Tuple3<Long, String, Long>(-1L, "-1", -1L)))
						.returns("Tuple3<Long, String, Long>")
						.flatMap(new Splitter<Tuple3<Long, String, Long>>(1)) //split authors
						.distinct(1)); //eliminate duplicates
				
				//extract data for time table from source files 
				times = generateUniqueIds(publicationsReader
						.map(new CSVRecordToTupleMap<Tuple2<Long, Long>>(new int[] {0, 4}, new Class[] {Long.class, Long.class}))
						.returns("Tuple2<Long, Long>")
						.flatMap(new AppendTupleFields<Tuple2<Long, Long>, Tuple5<Long, Long, Long, Long, Long>>(new Tuple5<Long, Long, Long, Long, Long>(-1L, -1L, -1L, -1L, -1L)))
						.returns("Tuple5<Long, Long, Long, Long, Long>")
						.flatMap(new DecadeCalculator()) //calculate specific decade and fill decade field
						.distinct(1)); //eliminate duplicates
				
				//get all fields except of cites from publications.csv as preparation for joins
				pubs = publicationsReader
						.map(new CSVRecordToTupleMap<Tuple7<Long, String, String, String, Long, String, String>>(new int[] {0, 1, 2, 3, 4, 5, 6}, new Class[] {Long.class, String.class, String.class, String.class, Long.class, String.class, String.class}))
						.returns("Tuple7<Long, String, String, String, Long, String, String>")
						.flatMap(new KeyCleaner<Tuple7<Long, String, String, String, Long, String, String>>(5));//remove :ref from venue series
			} else {
				acmCiteCount = publicationsReader
						.map(new CSVRecordToTupleMap<Tuple2<Long,Long>>(new int[] {0, 6}, new Class[] {Long.class, Long.class}))
						.returns("Tuple2<Long, Long>"); 
				
				//extract data for title table from source files 
				titles = generateUniqueIds(publicationsReader
						.map(new CSVRecordToTupleMap<Tuple3<Long, String, String>>(new int[] {0, 2, 1}, new Class[] {Long.class, String.class, String.class}))
						.returns("Tuple3<Long, String, String>"));
				
				//extract data for author table from source files 
				authors = generateUniqueIds(publicationsReader
						.map(new CSVRecordToTupleMap<Tuple2<Long, String>>(new int[] {0, 5}, new Class[] {Long.class, String.class}))
						.returns("Tuple2<Long, String>")
						.flatMap(new AppendTupleFields<Tuple2<Long, String>, Tuple3<Long, String, Long>>(new Tuple3<Long, String, Long>(-1L, "-1", -1L)))
						.returns("Tuple3<Long, String, Long>")
						.flatMap(new Splitter<Tuple3<Long, String, Long>>(1)) //split authors
						.distinct(1)); //eliminate duplicates
				
				//extract data for time table from source files 
				times = generateUniqueIds(publicationsReader
						.map(new CSVRecordToTupleMap<Tuple2<Long, Long>>(new int[] {0, 3}, new Class[] {Long.class, Long.class}))
						.returns("Tuple2<Long, Long>")
						.flatMap(new AppendTupleFields<Tuple2<Long, Long>, Tuple5<Long, Long, Long, Long, Long>>(new Tuple5<Long, Long, Long, Long, Long>(-1L, -1L, -1L, -1L, -1L)))
						.returns("Tuple5<Long, Long, Long, Long, Long>")
						.flatMap(new DecadeCalculator()) //calculate specific decade and fill decade field
						.distinct(1)); //eliminate duplicates
				
				//get all fields except of cites from publications.csv as preparation for joins
				pubs = publicationsReader
						.map(new CSVRecordToTupleMap<Tuple7<Long, String, String, String, Long, String, String>>(new int[] {0, 0, 1, 2, 3, 4, 5}, new Class[] {Long.class, String.class, String.class, String.class, Long.class, String.class, String.class}))
						.returns("Tuple7<Long, String, String, String, Long, String, String>")
						.flatMap(new KeyCleaner<Tuple7<Long, String, String, String, Long, String, String>>(5));//remove :ref from venue series
			}
			
			
			
			
			
			//join the dataset with extracted dimension 'tables'
			//join on names, titles, ...
			//get a map of fact table id and dimension table id
			DataSet<Tuple2<Long, Long>> timeJoin = pubs.coGroup(times).where(4).equalTo(1).with(new Join());
			DataSet<Tuple2<Long, Long>> titleJoin = pubs.coGroup(titles).where(2).equalTo(2).with(new Join());
			DataSet<Tuple2<Long, Long>> authorJoin = pubs.flatMap(new Splitter<Tuple7<Long, String, String, String, Long, String, String>>(6)).coGroup(authors).where(6).equalTo(1).with(new Join());
			DataSet<Tuple2<Long, Long>> citeJoin = null;
			DataSet<Tuple2<Long, Long>> documentTypeJoin = null;
			DataSet<Tuple2<Long, Long>> venueSeriesJoin = null;
			if(isDBLP)
			{
				citeJoin = pubs.coGroup(cites).where(2).equalTo(1).with(new Count());
				documentTypeJoin = pubs.coGroup(documentTypes).where(1).equalTo(1).with(new Join());
				venueSeriesJoin = pubs.coGroup(venueSeries).where(5).equalTo(2).with(new Join());
			} else {
//				citeJoin = pubs.coGroup(acmCiteCount).where(0).equalTo(0).with(new Join());
			}
			
			//prepare fact table 
			DataSet<Tuple9<Long, Long, Long, Long, Long, Long, Long, Long, Long>> publications = publicationsReader
					.map(new CSVRecordToTupleMap<Tuple1<Long>>(new int[] {0}, new Class[] {Long.class}))
					.returns("Tuple1<Long>")
					.flatMap(new AppendTupleFields<Tuple1<Long>, Tuple9<Long, Long, Long, Long, Long, Long, Long, Long, Long>>(new Tuple9<Long, Long, Long, Long, Long, Long, Long, Long, Long>(-1L, -1L, -1L, -1L, -1L, 0L, 0L, 0L, 0L)))
					.returns("Tuple9<Long, Long, Long, Long, Long, Long, Long, Long, Long>");
			
			//merge joins
			//join on fact table ids, then dispose joins except of author map
			if(isDBLP) {
				publications = publications.coGroup(citeJoin).where(0).equalTo(0).with(new PublicationMerge(5));
				publications = publications.coGroup(venueSeriesJoin).where(0).equalTo(0).with(new PublicationMerge(4));
				publications = publications.coGroup(documentTypeJoin).where(0).equalTo(0).with(new PublicationMerge(2));
			} else {
				publications = publications.coGroup(acmCiteCount).where(0).equalTo(0).with(new PublicationMerge(7));
			}
			publications = publications.coGroup(timeJoin).where(0).equalTo(0).with(new PublicationMerge(3));
			publications = publications.coGroup(titleJoin).where(0).equalTo(0).with(new PublicationMerge(1));
			if(isDBLP)
			
			//save dimension and fact table/s
			if(isDBLP) {
				saveDataSetAsCsv(TARGET_PATH + "/" + TYPE + "/" + "document_type/document_type.csv", documentTypes, 0, csvFormat);
				saveDataSetAsCsv(TARGET_PATH + "/" + TYPE + "/" + "venue_series/venue_series.csv", venueSeries, 0, csvFormat);
			}
			saveDataSetAsCsv(TARGET_PATH + "/" + TYPE + "/" + "title/title.csv", titles, 0, csvFormat);
			saveDataSetAsCsv(TARGET_PATH + "/" + TYPE + "/" + "time/time.csv", times, 0, csvFormat);
			saveDataSetAsCsv(TARGET_PATH + "/" + TYPE + "/" + "author/author.csv", authors, 0, csvFormat);
			saveDataSetAsCsv(TARGET_PATH + "/" + TYPE + "/" + "publication_author_map/publication_author_map.csv", authorJoin, 0, csvFormat);
			saveDataSetAsCsv(TARGET_PATH + "/" + TYPE + "/" + "publication/publication.csv", publications, 0, csvFormat);
			
			env.execute();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	protected static ArgumentParser createArgsParser() {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("pub-importer");
        parser.version(DBLPFormatJob.class.getPackage().getImplementationVersion());
        parser.addArgument("-V", "--version").action(Arguments.version());

        parser.addArgument("-type")
        	.help("Use ACM or DBLP")
        	.choices("acm", "dblp")
        	.type(String.class)
        	.required(true);
        
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
        
        TYPE = args.getString("type");
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
	
	private static <T extends Tuple> DataSink<?> saveDataSetAsCsv(String path, DataSet<T> dataSet, int sortIndex, CSVFormat format)
	{
		return 
			dataSet
			.sortPartition(sortIndex, Order.ASCENDING)
			.write(new CommonsCSVOutputFormat<T>(format), path, WriteMode.OVERWRITE)
			.setParallelism(1); //save output as 1 file;
	}
}
