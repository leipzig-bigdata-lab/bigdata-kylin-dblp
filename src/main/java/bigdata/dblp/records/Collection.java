package bigdata.dblp.records;

import java.util.HashSet;
import java.util.Set;

public class Collection {
    public String type;
    public String id = null;
    public String title = null;

    public Collection(String type) {
        this.type = type;
    }

    public boolean isComplete() {
        return id != null && title != null;
    }

    @Override
    public String toString() {
        StringBuilder ret = new StringBuilder();
        ret.append(type);
        ret.append(" (");
        ret.append(id);
        ret.append("): ");
        ret.append(title);
        return ret.toString();
    }

    public static boolean isValidType(String type) {
        return validTypes.contains(type);
    }

    private static Set<String> validTypes;
    static {
        validTypes = new HashSet<>();
        validTypes.add("proceedings");
        validTypes.add("book");
    }
}
