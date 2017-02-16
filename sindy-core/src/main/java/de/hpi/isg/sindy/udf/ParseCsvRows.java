package de.hpi.isg.sindy.udf;

import de.hpi.isg.sindy.data.IntObjectTuple;
import de.hpi.isg.sindy.io.CsvParser;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Parses a CSV row into a {@link IntObjectTuple}.
 *
 * @author Sebastian Kruse
 */
@FunctionAnnotation.ForwardedFields("a -> a")
public class ParseCsvRows implements MapFunction<IntObjectTuple<String>, IntObjectTuple<String[]>> {

    private static final long serialVersionUID = 1377116120504051734L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(ParseCsvRows.class);

    private final IntObjectTuple<String[]> outputTuple = new IntObjectTuple<>();

    private final CsvParser parser;

    public ParseCsvRows(final char fieldSeparator, final char quoteChar, String nullString) {
        this.parser = new CsvParser(fieldSeparator, quoteChar, nullString);
    }

    @Override
    public IntObjectTuple<String[]> map(final IntObjectTuple<String> fileLine)
            throws Exception {

        final List<String> fields = this.parser.parse(fileLine.b);
        final String[] fieldArray = fields.toArray(new String[fields.size()]);
        fields.toArray(fieldArray);

        this.outputTuple.a = fileLine.a;
        this.outputTuple.b = fieldArray;
        return this.outputTuple;

    }
}
