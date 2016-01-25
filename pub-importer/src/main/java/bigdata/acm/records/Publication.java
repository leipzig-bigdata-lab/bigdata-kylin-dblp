package bigdata.acm.records;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.csv.CSVPrinter;

public class Publication {
    public String id = null;
    public String title = null;
    public List<String> authors;
    public int year = 0;
    public String journal = null;
    public int volume = 0;
    public int issue = 0;
    public int numCitings = 0;

    public Publication() {
        authors = new ArrayList<>();
    }

    public boolean isComplete() {
        return id != null && title != null && year > 0 && !authors.isEmpty();
    }

    public String getVenue() {
        if(journal != null && (volume > 0 || issue > 0)) {
            return journal.replace('#', ':') + "#" + volume + "#" + issue;
        } else if(journal != null) {
            return journal;
        } else {
            return "";
        }
    }

    static long sequenceNumber = 0;
    public void toCsv(CSVPrinter printer) throws IOException {
        printer.printRecord(++sequenceNumber, id, title, year, getVenue(), saveJoin(authors), numCitings);
    }

    private String saveJoin(Iterable<String> list) {
        StringBuilder builder = new StringBuilder();
        for(String string : list) {
            if(builder.length() != 0) {
                builder.append('|');
            }
            builder.append(string.replace('|', '-'));
        }
        return builder.toString();
    }

    public static boolean isValidType(String type) {
        return validTypes.contains(type);
    }

    private static Set<String> validTypes;
    static {
        validTypes = new HashSet<>();
        validTypes.add("Publication");
    }
}
