package de.bigprak.transform.csv;

import java.lang.reflect.Constructor;

import org.apache.commons.csv.CSVRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;

/**
 * Extract certain columns from the {@link CSVRecord} and store them in a {@link Tuple}.
 *
 * @param <T> One of {@link Tuple1}, {@link Tuple2}, ...
 */
public class CSVRecordToTupleMap<T extends Tuple> implements MapFunction<CSVRecord, T> {
    private static final long serialVersionUID = 1L;

    private int[] indices;
    private Class<?>[] types;

    /**
     * Specify the columns to extract.
     *
     * All types must have an one-argument constructor that takes a {@link String} and
     * match the generic T when put into a Tuple of the correct size.
     *
     * Example:
     * <pre><code>
     * // assume T = Tuple3&lt;Long, String, Boolean&gt;
     * csv = "Peter,22,1456076785,true,Hallo World";
     * indices = {2, 0, 3};
     * types = {Long.class, String.class, Boolean.class};
     * </code></pre>
     *
     * @param indices Select which columns to extract (0-based)
     * @param types Specify the type of each column
     * @throws IllegalArgumentException When indices and types differ in length
     */
    public CSVRecordToTupleMap(int[] indices, Class<?>[] types) {
        if(indices.length != types.length) {
            throw new IllegalArgumentException("indices and types have to be of same length");
        }

        this.indices = indices;
        this.types = types;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T map(CSVRecord record) throws Exception {
        Tuple tuple = Tuple.getTupleClass(indices.length).newInstance();

        for(int i = 0; i < indices.length; ++i) {
            if(indices[i] >= record.size()) {
                throw new RuntimeException("Requested column " + indices[i] + " but record only has " + record.size() + "columns");
            }

            Object value = null;

            try {
                Constructor<?> constructor = types[i].getConstructor(String.class);
                value = constructor.newInstance(record.get(indices[i]));
            } catch(Exception e) {
                throw new RuntimeException("Could not convert column " + indices[i] + " to type " + types[i].getName(), e);
            }

            tuple.setField(value, i);
        }

        return (T) tuple;
    }
}
