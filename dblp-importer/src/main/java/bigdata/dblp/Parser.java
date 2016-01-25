package bigdata.dblp;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLResolver;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.csv.CSVPrinter;

import bigdata.ProgressReporter;
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

    public static long parse(InputStream dblpInput, CSVPrinter publicationOutput, CSVPrinter collectionOutput, ProgressReporter progress) throws XMLStreamException, IOException {
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
                            progress.report(importedEntries++);
                        }
                        currentPublication = null;
                    } else if(Collection.isValidType(endElement)) {
                        if(currentCollection.isComplete()) {
                            currentCollection.toCsv(collectionOutput);
                            progress.report(importedEntries++);
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

    protected static boolean isValidProperty(String name) {
        return validProperties.contains(name);
    }
}
