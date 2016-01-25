package bigdata;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

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

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;

public class Main {
    public static CSVFormat createHiveCsvFormat() {
        return CSVFormat.newFormat(',')
                        .withRecordSeparator('\n')
                        .withEscape('\\')
                        .withQuoteMode(QuoteMode.NONE)
                        .withNullString("");
    }

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
        InputStream inputStream = args.get("input_file");
        Path hdfsPath = new Path(args.getString("output_folder"));

        try {
            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            FileSystem fs = FileSystem.get(new URI("hdfs://localhost"), conf);
            fs.mkdirs(hdfsPath);

            if("dblp".equals(command)) {
                FSDataOutputStream publicationStream = fs.create(new Path(hdfsPath, "dblp-publications.csv"));
                FSDataOutputStream collectionStream = fs.create(new Path(hdfsPath, "dblp-collections.csv"));

                Writer publicationWriter = new BufferedWriter(new OutputStreamWriter(publicationStream, StandardCharsets.UTF_8));
                Writer collectionWriter = new BufferedWriter(new OutputStreamWriter(collectionStream, StandardCharsets.UTF_8));

                try(CSVPrinter publicationCSVPrinter = new CSVPrinter(publicationWriter, createHiveCsvFormat());
                    CSVPrinter collectionCSVPrinter = new CSVPrinter(collectionWriter, createHiveCsvFormat())) {
                    System.out.print("Starting import ...");
                    long importedEntries = bigdata.dblp.Parser.parse(inputStream, publicationCSVPrinter, collectionCSVPrinter, new ProgressReporter());
                    System.out.format("%n... finished! Imported %,d entries.%n", importedEntries);
                }
            } else if("acm".equals(command)) {
                FSDataOutputStream publicationStream = fs.create(new Path(hdfsPath, "acm-publications.csv"));

                Writer publicationWriter = new BufferedWriter(new OutputStreamWriter(publicationStream, StandardCharsets.UTF_8));

                try(CSVPrinter publicationCSVPrinter = new CSVPrinter(publicationWriter, createHiveCsvFormat())) {
                    System.out.print("Starting import ...");
                    long importedEntries = bigdata.acm.Parser.parse(inputStream, publicationCSVPrinter, new ProgressReporter());
                    System.out.format("%n... finished! Imported %,d entries.%n", importedEntries);
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

        Subparser acmParser = subparsers.addParser("acm");
        acmParser.help("import ACM.xml into hdfs");
        acmParser.addArgument("input_file")
                 .help("input file, defaults to stdin")
                 .nargs("?")
                 .type(FileInputStream.class)
                 .setDefault(System.in);
        acmParser.addArgument("output_folder")
                 .help("destination folder in local hdfs");

        return parser;
    }
}
