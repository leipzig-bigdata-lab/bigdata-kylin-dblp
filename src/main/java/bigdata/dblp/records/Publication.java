package bigdata.dblp.records;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Publication {
    public String type;
    public String id = null;
    public String title = null;
    public List<String> authors;
    public int year = 0;
    public String journal = null;
    public String crossref = null;
    public int volume = 0;
    public int issue = 0;
    public List<String> cites;

    public Publication(String type) {
        this.type = type;
        authors = new ArrayList<>();
        cites = new ArrayList<>();
    }

    public boolean isComplete() {
        return id != null && title != null && year > 0 && !authors.isEmpty();
    }

    public String getVenue() {
        if(crossref != null) {
            return "ref:" + crossref;
        } else if(journal != null && (volume > 0 || issue > 0)) {
            return journal + "#" + volume + "#" + issue;
        } else if(journal != null) {
            return journal;
        } else {
            return "";
        }
    }

    @Override
    public String toString() {
        StringBuilder ret = new StringBuilder();
        ret.append(type);
        ret.append(" (");
        ret.append(id);
        ret.append("): ");
        ret.append(title);
        ret.append(" by ");
        ret.append(String.join(", ", authors));
        ret.append(" (");
        ret.append(year);
        ret.append(", ");
        ret.append(getVenue());
        ret.append(")");
        if(!cites.isEmpty()) {
            ret.append(" cites ");
            ret.append(String.join(", ", cites));
        }
        return ret.toString();
    }

    public static boolean isValidType(String type) {
        return validTypes.contains(type);
    }

    private static Set<String> validTypes;
    static {
        validTypes = new HashSet<>();
        validTypes.add("article");
        validTypes.add("inproceedings");
        validTypes.add("incollection");
        //validTypes.add("phdthesis");
        //validTypes.add("mastersthesis");
    }
}
