package bigdata.acm;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.csv.CSVPrinter;

import bigdata.ProgressReporter;
import bigdata.acm.records.Publication;

/**
 * Parse the acm.xml that was used in the data-warehouse practicum at Leipzig
 * University using a StAX-Parser and write the resulting Publications as CSVs
 * to the given CSVPrinter.
 */
public class Parser {
    private static Set<String> validProperties;

    static {
        validProperties = new HashSet<>();
        validProperties.add("Title");
        validProperties.add("Venue");
        validProperties.add("VenueSeries");
        validProperties.add("Name");
        validProperties.add("CitingPub");
    }

    public static long parse(InputStream dblpInput, CSVPrinter publicationOutput, ProgressReporter progress) throws XMLStreamException, IOException {
        long importedEntries = 0;

        Pattern volumeIssuePattern = Pattern.compile("volume\\s+(?<volume>[0-9]+)\\s+,\\s+issue\\s+(?<issue>[0-9]+)\\s+.*", Pattern.CASE_INSENSITIVE);

        XMLInputFactory factory = XMLInputFactory.newInstance();
        factory.setProperty(XMLInputFactory.IS_REPLACING_ENTITY_REFERENCES, true);
        XMLStreamReader reader = factory.createXMLStreamReader(dblpInput);

        Publication currentPublication = null;
        String currentProperty = null;
        String currentPropertyBuffer = "";

        while(reader.hasNext()) {
            switch(reader.next()) {
                case XMLStreamConstants.START_ELEMENT:
                    String startElement = reader.getLocalName();
                    if(Publication.isValidType(startElement)) {
                        currentPublication = new Publication();
                        currentPublication.id = reader.getAttributeValue(null, "id");
                    } else if(currentPublication != null && isValidProperty(startElement)) {
                        currentProperty = startElement;
                        switch(currentProperty) {
                            case "Venue":
                                try {
                                    currentPublication.year = Integer.valueOf(reader.getAttributeValue(null, "year"));
                                } catch(NumberFormatException e) {} // ignore publications with invalid year
                                break;
                        }
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
                    } else if(endElement.equals(currentProperty)) {
                        if(currentPublication != null) {
                            switch(currentProperty) {
                                case "Title":
                                    currentPublication.title = currentPropertyBuffer;
                                    break;
                                case "Venue":
                                    Matcher m = volumeIssuePattern.matcher(currentPropertyBuffer);
                                    if(m.matches()) {
                                        try {
                                            currentPublication.volume = Integer.valueOf(m.group("volume"));
                                            currentPublication.issue = Integer.valueOf(m.group("issue"));
                                        } catch(NumberFormatException e) {} // ignore invalid volume or issue (should not happen)
                                    }
                                    break;
                                case "VenueSeries":
                                    currentPublication.journal = currentPropertyBuffer.trim();
                                    break;
                                case "Name":
                                    currentPublication.authors.add(currentPropertyBuffer);
                                    break;
                                case "CitingPub":
                                    currentPublication.numCitings += 1;
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
