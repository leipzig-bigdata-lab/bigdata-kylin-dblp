package de.bigprak.transform.csv;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

public class CSVParsingTest {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        CSVFormat csvFormat = CSVFormat.newFormat(',')
                .withEscape('\\')
                .withQuoteMode(QuoteMode.NONE)
                .withNullString("");

        File in = File.createTempFile("de.bigprak.transform.csv", "CSVParsingTest-in");
        File out = File.createTempFile("de.bigprak.transform.csv", "CSVParsingTest-out");
        in.deleteOnExit();
        out.deleteOnExit();
        out.delete();

        // Extract (in this order) the id, the first Name and the message;
        // ignore second name and time
        String csv = "Peter,Franz,1,Hallo Franz\\, wie geht es dir?,1456076785\n" +
                     "Franz,Peter,2,Sehr gut\\, danke\\, und dir?,1456076892\n" +
                     "Peter,Franz,3,Alles in Ordnung!,1456076899";
        int[] indices = {2,0,3};
        Class<?>[] types = {Long.class, String.class, String.class};

        FileUtils.writeStringToFile(in, csv, StandardCharsets.UTF_8);

        env.readFile(new CommonsCSVInputFormat(csvFormat), in.getAbsolutePath())
           .map(new CSVRecordToTupleMap<Tuple3<Long, String, String>>(indices, types))
           // necessary, because flink cannot deduce return type from generic Mapper
           .returns("Tuple3<Long, String, String>")
           .writeAsCsv(out.getAbsolutePath());

        env.setParallelism(1);
        env.execute();

        String result = FileUtils.readFileToString(out, StandardCharsets.UTF_8);

        // show results
        System.out.println();
        System.out.println("Input csv: ");
        System.out.println(csv);

        System.out.println();
        System.out.println("Extraction parameters: " + Arrays.toString(indices) + " (" + Arrays.toString(types) + ")");

        System.out.println();
        System.out.println("Resulting csv: ");
        System.out.println(result);
    }
}
