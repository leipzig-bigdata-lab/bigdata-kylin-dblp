package bigdata;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import javax.xml.stream.XMLStreamException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class Main {
    public static CSVFormat createHiveCsvFormat() {
        return CSVFormat.newFormat(',')
                        .withRecordSeparator('\n')
                        .withEscape('\\')
                        .withQuoteMode(QuoteMode.NONE)
                        .withNullString("");
    }

    public static void main(String[] args) {
        if(args.length < 2) {
            System.err.println("Usage: dblp-importer /local/dblp.xml|stdin /path/in/hdfs");
            System.exit(1);
            return;
        }

        // for local debugging only
        //System.setProperty("HADOOP_USER_NAME", "bigprak");

        // init log4j to output to stderr
        ConsoleAppender appender = new ConsoleAppender(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN), "System.err");
        appender.setThreshold(Level.WARN);
        Logger.getRootLogger().addAppender(appender);

        String file = args[0];
        Path hdfsPath = new Path(args[1]);

        InputStream dblpInput;
        try {
            if("stdin".equals(file)) {
                dblpInput = System.in;
            } else {
                dblpInput = new FileInputStream(file);
            }
        } catch(FileNotFoundException e) {
            System.err.println("Input file not found: " + e);
            System.exit(2);
            return;
        }

        try {
            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            FileSystem fs = FileSystem.get(new URI("hdfs://localhost"), conf);
            fs.mkdirs(hdfsPath);

            FSDataOutputStream publicationStream = fs.create(new Path(hdfsPath, "dblp-publications.csv"));
            FSDataOutputStream collectionStream = fs.create(new Path(hdfsPath, "dblp-collections.csv"));

            Writer publicationWriter = new BufferedWriter(new OutputStreamWriter(publicationStream, StandardCharsets.UTF_8));
            Writer collectionWriter = new BufferedWriter(new OutputStreamWriter(collectionStream, StandardCharsets.UTF_8));

            try(CSVPrinter publicationCSVPrinter = new CSVPrinter(publicationWriter, createHiveCsvFormat());
                CSVPrinter collectionCSVPrinter = new CSVPrinter(collectionWriter, createHiveCsvFormat())) {
                System.out.print("Starting import ...");
                long importedEntries = bigdata.dblp.Parser.parse(dblpInput, publicationCSVPrinter, collectionCSVPrinter, new ProgressReporter());
                System.out.format("%n... finished! Imported %,d entries.%n", importedEntries);
            }
        } catch(IOException | URISyntaxException e) {
            System.err.println("Error writing to hdfs: " + e);
            System.exit(2);
            return;
        } catch(XMLStreamException e) {
            System.err.println("Error while parsing dblp.xml: " + e);
            System.exit(3);
            return;
        }

        System.exit(0);
    }
}
