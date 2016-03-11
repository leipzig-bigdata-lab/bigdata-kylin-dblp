package bigdata;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

import javax.xml.stream.XMLStreamException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;

/**
 * Implements the CLI for the pub-importer
 *
 * Uses the hadoop hdfs and apache commons csv lib to write the parsed
 * xml directly into the hdfs. Supports dblp and acm.
 */
public class Main {
    public static void main(String[] rawArgs) {
        // force english output in cli help messages
        Locale.setDefault(new Locale("en", "US"));

        // for local debugging only
        //System.setProperty("HADOOP_USER_NAME", "bigprak");

        // init log4j to output to stderr
        ConsoleAppender appender = new ConsoleAppender(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN), "System.err");
        appender.setThreshold(Level.WARN);
        Logger.getRootLogger().addAppender(appender);

        Namespace args = createArgsParser().parseArgsOrFail(rawArgs);
        String command = args.getString("command");

        try {
            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            FileSystem fs = FileSystem.get(new URI("hdfs://localhost"), conf);

            if("dblp".equals(command) || "acm".equals(command)) {
                CSVFormat format = createCsvFormat(args.getString("csv_format"));
                InputStream inputStream = args.get("input_file");
                Path hdfsPath = new Path(args.getString("output_folder"));
                fs.mkdirs(hdfsPath);

                if("dblp".equals(command)) {
                    FSDataOutputStream publicationStream = fs.create(new Path(hdfsPath, "dblp-publications.csv"));
                    FSDataOutputStream collectionStream = fs.create(new Path(hdfsPath, "dblp-collections.csv"));

                    Writer publicationWriter = new BufferedWriter(new OutputStreamWriter(publicationStream, StandardCharsets.UTF_8));
                    Writer collectionWriter = new BufferedWriter(new OutputStreamWriter(collectionStream, StandardCharsets.UTF_8));

                    try(CSVPrinter publicationCSVPrinter = new CSVPrinter(publicationWriter, format);
                        CSVPrinter collectionCSVPrinter = new CSVPrinter(collectionWriter, format)) {
                        System.out.print("Starting import ...");
                        long importedEntries = bigdata.dblp.Parser.parse(inputStream, publicationCSVPrinter, collectionCSVPrinter, new ProgressReporter());
                        System.out.format("%n... finished! Imported %,d entries.%n", importedEntries);
                    }
                } else { // acm
                    FSDataOutputStream publicationStream = fs.create(new Path(hdfsPath, "acm-publications.csv"));

                    Writer publicationWriter = new BufferedWriter(new OutputStreamWriter(publicationStream, StandardCharsets.UTF_8));

                    try(CSVPrinter publicationCSVPrinter = new CSVPrinter(publicationWriter, format)) {
                        System.out.print("Starting import ...");
                        long importedEntries = bigdata.acm.Parser.parse(inputStream, publicationCSVPrinter, new ProgressReporter());
                        System.out.format("%n... finished! Imported %,d entries.%n", importedEntries);
                    }
                }
            } else if("csv".equals(command)) {
                Path hdfsInput = new Path(args.getString("input_file"));
                Path hdfsOutput = new Path(args.getString("output_file"));
                long limit = args.getLong("limit");

                FSDataInputStream inputStream = fs.open(hdfsInput);
                FSDataOutputStream outputStream = fs.create(hdfsOutput);

                CSVFormat inputFormat = null, outputFormat = null;
                if(args.getBoolean("quoted_to_escaped")) {
                    inputFormat = createCsvFormat("quoted");
                    outputFormat = createCsvFormat("escaped");
                } else if(args.getBoolean("escaped_to_quoted")) {
                    inputFormat = createCsvFormat("escaped");
                    outputFormat = createCsvFormat("quoted");
                }

                Reader inputReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
                Writer outputWriter = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));

                try(CSVParser parser = new CSVParser(inputReader, inputFormat);
                    CSVPrinter printer = new CSVPrinter(outputWriter, outputFormat)) {
                    long i = 0;
                    ProgressReporter progress = new ProgressReporter();

                    System.out.print("Starting conversion ...");
                    for(CSVRecord record : parser) {
                        printer.printRecord(record);

                        progress.report(i++);
                        if(limit > 0 && limit <= i) {
                            break;
                        }
                    }
                    System.out.format("%n... finished! Converted %,d lines.%n", i);
                }
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

    protected static ArgumentParser createArgsParser() {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("pub-importer");
        parser.version(Main.class.getPackage().getImplementationVersion());
        parser.addArgument("-V", "--version").action(Arguments.version());

        Subparsers subparsers = parser.addSubparsers()
            .dest("command")
            .title("subcommands")
            .metavar("COMMAND");

        Subparser dblpParser = subparsers.addParser("dblp");
        dblpParser.help("import original dblp.xml into hdfs");
        dblpParser.addArgument("input_file")
                  .help("input file, defaults to stdin")
                  .nargs("?")
                  .type(FileInputStream.class)
                  .setDefault(System.in);
        dblpParser.addArgument("output_folder")
                  .help("destination folder in local hdfs");
        dblpParser.addArgument("--csv-format")
                  .help("formatting of the csv files, use escaped for hive")
                  .required(true)
                  .choices("quoted", "escaped");

        Subparser acmParser = subparsers.addParser("acm");
        acmParser.help("import ACM.xml into hdfs");
        acmParser.addArgument("input_file")
                 .help("input file, defaults to stdin")
                 .nargs("?")
                 .type(FileInputStream.class)
                 .setDefault(System.in);
        acmParser.addArgument("output_folder")
                 .help("destination folder in local hdfs");
        acmParser.addArgument("--csv-format")
                 .help("formatting of the csv files, use escaped for hive")
                 .required(true)
                 .choices("quoted", "escaped");

        Subparser csvParser = subparsers.addParser("csv");
        csvParser.help("convert csv files between differernt formats");
        csvParser.addArgument("input_file")
                 .help("input file in local hdfs");
        csvParser.addArgument("output_file")
                 .help("output file in local hdfs");
        MutuallyExclusiveGroup conversionArguments = csvParser.addMutuallyExclusiveGroup();
        conversionArguments.required(true);
        conversionArguments.addArgument("--quoted-to-escaped")
                           .help("convert from quoted csv to escaped csv (hive)")
                           .action(Arguments.storeTrue());
        conversionArguments.addArgument("--escaped-to-quoted")
                           .help("convert from escaped csv (hive) to quoted csv")
                           .action(Arguments.storeTrue());
        csvParser.addArgument("--limit")
                 .metavar("N")
                 .help("only convert and copy the first N entries")
                 .type(Long.class)
                 .setDefault(new Long(0));

        return parser;
    }

    public static CSVFormat createCsvFormat(String mode) {
        if("quoted".equals(mode)) {
            return CSVFormat.newFormat(',')
                            .withRecordSeparator('\n')
                            .withQuote('"')
                            .withQuoteMode(QuoteMode.MINIMAL)
                            .withNullString("");
        } else if("escaped".equals(mode)) {
            return CSVFormat.newFormat(',')
                            .withRecordSeparator('\n')
                            .withEscape('\\')
                            .withQuoteMode(QuoteMode.NONE)
                            .withNullString("");
        } else {
            throw new IllegalArgumentException("invalid csv mode: " + mode);
        }
    }
}
