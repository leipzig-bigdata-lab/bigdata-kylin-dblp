package bigdata.dblp;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLResolver;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

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

    public static void parse(InputStream dblpInput, Writer publicationOutput, Writer collectionOutput) throws XMLStreamException, IOException {
        // disable default entity resolution limit (64000)
        System.getProperties().setProperty("jdk.xml.entityExpansionLimit", "0");

        XMLInputFactory factory = XMLInputFactory.newFactory();
        factory.setProperty(XMLInputFactory.IS_REPLACING_ENTITY_REFERENCES, true);
        factory.setXMLResolver(new XMLResolver() {
            @Override
            public Object resolveEntity(String publicID, String systemID, String baseURI, String namespace) throws XMLStreamException {
                if(systemID.equals("dblp.dtd")) {
                    return Parser.class.getResourceAsStream("resources/dblp.dtd");
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
                            publicationOutput.append(currentPublication.toString());
                            publicationOutput.append('\n');
                        }
                        currentPublication = null;
                    } else if(Collection.isValidType(endElement)) {
                        if(currentCollection.isComplete()) {
                            collectionOutput.append(currentCollection.toString());
                            collectionOutput.append('\n');
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
    }

    protected static boolean isValidProperty(String name) {
        return validProperties.contains(name);
    }

    public static void main(String[] args) {
        String file;

        if(args.length < 1) {
            System.err.println("Usage: dblp-importer /local/dblp.xml|stdin");
            System.exit(1);
            return;
        }

        file = args[0];

        InputStream dblpInput;
        Writer publicationOutput;
        Writer collectionOutput;
        try {
            if("stdin".equals(file)) {
                dblpInput = System.in;
            } else {
                dblpInput = new FileInputStream(file);
            }
            publicationOutput = new OutputStreamWriter(new FileOutputStream(file + "-pub"), StandardCharsets.UTF_8);
            collectionOutput = new OutputStreamWriter(new FileOutputStream(file + "-col"), StandardCharsets.UTF_8);
        } catch (FileNotFoundException e) {
            System.err.println("File or Directory not found: " + e);
            System.exit(2);
            return;
        }

        try {
            parse(dblpInput, publicationOutput, collectionOutput);
        } catch (XMLStreamException | IOException e) {
            System.err.println("Error while parsing dblp.xml: " + e);
            System.exit(3);
            return;
        }

        System.exit(0);
    }
}
