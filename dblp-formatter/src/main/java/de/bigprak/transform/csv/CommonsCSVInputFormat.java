package de.bigprak.transform.csv;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.core.fs.Path;

/**
 * InputFormat for parsing CSVs
 *
 * Parse CSVs using {@link DelimitedInputFormat} and the Apache Commons
 * {@link CSVParser}, which supports configurable {@link CSVFormat}s.
 * The record separator is not configurable and always {@code '\n'}
 * (or alternatively {@code "\r\n"}).
 */
public class CommonsCSVInputFormat extends DelimitedInputFormat<CSVRecord> {
    private static final long serialVersionUID = 1L;

    // this will be serialized and send to all workers
    private CSVFormat format;

    public CommonsCSVInputFormat(CSVFormat format) {
        super();
        this.format = format;
    }
    public CommonsCSVInputFormat(Path filePath, CSVFormat format) {
        super(filePath);
        this.format = format;
    }

    @Override
    public CSVRecord readRecord(CSVRecord reuse, byte[] bytes, int offset, int numBytes) throws IOException {
        // using streams instead of strings avoids unnecessary copies
        ByteArrayInputStream stream = new ByteArrayInputStream(bytes, offset, numBytes);
        InputStreamReader reader = new InputStreamReader(stream, StandardCharsets.UTF_8);

        // try to parse the record as exactly one line of csv (already splitted by DelimitedInputFormat)
        try(CSVParser parser = new CSVParser(reader, format)) {
            Iterator<CSVRecord> it = parser.iterator();
            if(it.hasNext()) {
                return it.next();
            }
        }

        throw new IOException("Error parsing the following csv line: " + new String(bytes, StandardCharsets.UTF_8));
    }
}
