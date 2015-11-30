package bigdata.dblp;

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
import java.util.HashSet;
import java.util.Set;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLResolver;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

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

import bigdata.dblp.records.Collection;
import bigdata.dblp.records.Publication;

public class Parser {
    private static Set<String> validProperties;

    static {
        validProperties = new HashSet<>();
        validProperties.add("title");
        validProperties.add("year");
        validProperties.add("journal");
        validProperties.add("volume");
        validProperties.add("number");
        validProperties.add("booktitle");
        validProperties.add("crossref");
        validProperties.add("author");
        validProperties.add("cite");
    }

    public static long parse(InputStream dblpInput, CSVPrinter publicationOutput, CSVPrinter collectionOutput) throws XMLStreamException, IOException {
        long importedEntries = 0;

        // disable default entity resolution limit (64000)
        System.getProperties().setProperty("jdk.xml.entityExpansionLimit", "0");

        XMLInputFactory factory = XMLInputFactory.newInstance();
        factory.setProperty(XMLInputFactory.IS_REPLACING_ENTITY_REFERENCES, true);
        factory.setXMLResolver(new XMLResolver() {
            @Override
            public Object resolveEntity(String publicID, String systemID, String baseURI, String namespace) throws XMLStreamException {
                if(systemID.equals("dblp.dtd")) {
                    InputStream dtd = Parser.class.getResourceAsStream("/dblp.dtd");
                    if(dtd == null) {
                        throw new RuntimeException("dblp.dtd not found!");
                    }
                    return dtd;
                }
                return null;
            }
        });
        XMLStreamReader reader = factory.createXMLStreamReader(dblpInput);

        Publication currentPublication = null;
        Collection currentCollection = null;
        String currentProperty = null;
        String currentPropertyBuffer = "";

        while(reader.hasNext()) {
            switch(reader.next()) {
                case XMLStreamConstants.START_ELEMENT:
                    String startElement = reader.getLocalName();
                    if(Publication.isValidType(startElement)) {
                        currentPublication = new Publication(startElement);
                        currentPublication.id = reader.getAttributeValue(null, "key");
                    } else if(Collection.isValidType(startElement)) {
                        currentCollection = new Collection(startElement);
                        currentCollection.id = reader.getAttributeValue(null, "key");
                    } else if((currentPublication != null || currentCollection != null)
                                && isValidProperty(startElement)) {
                        currentProperty = startElement;
                    }
                    break;

                case XMLStreamConstants.CHARACTERS:
                    if(currentProperty != null) {
                        currentPropertyBuffer += reader.getText();
                    }
                    break;

                case XMLStreamConstants.END_ELEMENT:
                    String endElement = reader.getLocalName();
                    if(Publication.isValidType(endElement)) {
                        if(currentPublication.isComplete()) {
                            currentPublication.toCsv(publicationOutput);
                            importedEntries = progressBar(importedEntries);
                            if(importedEntries > 1000) return importedEntries;
                        }
                        currentPublication = null;
                    } else if(Collection.isValidType(endElement)) {
                        if(currentCollection.isComplete()) {
                            currentCollection.toCsv(collectionOutput);
                            importedEntries = progressBar(importedEntries);
                        }
                        currentCollection = null;
                    } else if(endElement.equals(currentProperty)) {
                        if(currentPublication != null) {
                            switch(currentProperty) {
                                case "title":
                                    currentPublication.title = currentPropertyBuffer;
                                    break;
                                case "year":
                                    try {
                                        currentPublication.year = Integer.valueOf(currentPropertyBuffer);
                                    } catch(NumberFormatException e) {} // ignore publications with invalid year
                                    break;
                                case "journal":
                                case "booktitle":
                                    currentPublication.journal = currentPropertyBuffer;
                                    break;
                                case "crossref":
                                    currentPublication.crossref = currentPropertyBuffer;
                                    break;
                                case "volume":
                                    try {
                                        currentPublication.volume = Integer.valueOf(currentPropertyBuffer);
                                    } catch(NumberFormatException e) {} // ignore invalid volume
                                    break;
                                case "number":
                                    try {
                                        currentPublication.issue = Integer.valueOf(currentPropertyBuffer);
                                    } catch(NumberFormatException e) {} // ignore invalid issue
                                    break;
                                case "author":
                                    currentPublication.authors.add(currentPropertyBuffer);
                                    break;
                                case "cite":
                                    if(!"...".equals(currentPropertyBuffer)) {
                                        currentPublication.cites.add(currentPropertyBuffer);
                                    }
                                    break;
                            }
                        } else if(currentCollection != null) {
                            switch(currentProperty) {
                                case "title":
                                    currentCollection.title = currentPropertyBuffer;
                                    break;
                            }
                        }
                        currentProperty = null;
                        currentPropertyBuffer = "";
                    }
                    break;
            }
        }

        return importedEntries;
    }

    protected static long progressBar(long counter) {
        if(counter % 200000 == 0) {
            System.out.format("%n%,11d -", counter);
            System.out.flush();
        } else if(counter % 3125 == 0) {
            System.out.print('-');
            System.out.flush();
        }

        return counter + 1;
    }

    protected static boolean isValidProperty(String name) {
        return validProperties.contains(name);
    }

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
                long importedEntries = parse(dblpInput, publicationCSVPrinter, collectionCSVPrinter);
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
