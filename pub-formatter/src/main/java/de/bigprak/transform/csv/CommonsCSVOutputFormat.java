package de.bigprak.transform.csv;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.fs.Path;

/**
 * OutputFormat for writing CSVs
 *
 * Write CSVs using the Apache Commons {@link CSVPrinter}, which supports
 * configurable {@link CSVFormat}s.
 */
public class CommonsCSVOutputFormat<T extends Tuple> extends FileOutputFormat<T> {
    private static final long serialVersionUID = 1L;

    // this will be serialized and send to all workers
    private CSVFormat format;

    // this will NOT be serialized to workers
    private transient CSVPrinter printer;

    public CommonsCSVOutputFormat(CSVFormat format) {
        super();
        this.format = format;
    }

    public CommonsCSVOutputFormat(Path outputPath, CSVFormat format) {
        super(outputPath);
        this.format = format;
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);

        Writer out = new OutputStreamWriter(new BufferedOutputStream(this.stream, 4096), StandardCharsets.UTF_8);
        printer = new CSVPrinter(out, format);
    }

    @Override
    public void close() throws IOException {
        if(printer != null) {
            printer.close();
        }

        super.close();
    }

    @Override
    public void writeRecord(T tuple) throws IOException {
        int columns = tuple.getArity();

        for(int i = 0; i < columns; ++i) {
            printer.print(tuple.getField(i));
        }

        printer.println();
    }
}
