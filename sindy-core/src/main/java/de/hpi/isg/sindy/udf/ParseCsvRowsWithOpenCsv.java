package de.hpi.isg.sindy.udf;

import au.com.bytecode.opencsv.CSVParser;
import de.hpi.isg.sindy.data.IntObjectTuple;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Parses a CSV row into a {@link IntObjectTuple}.
 *
 * @author Sebastian Kruse
 */
@FunctionAnnotation.ForwardedFields("a -> a")
public class ParseCsvRowsWithOpenCsv extends RichFlatMapFunction<IntObjectTuple<String>, IntObjectTuple<String[]>> {

    private static final long serialVersionUID = 1377116120504051734L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(ParseCsvRowsWithOpenCsv.class);

    private final IntObjectTuple<String[]> outputTuple = new IntObjectTuple<>();

    private CSVParser csvParser;

    private final Int2IntMap numFieldsPerFile;

    private final int maxFields;

    private final boolean isSupressingEmptyCells;

    // CSV parser settings
    private final char quoteChar;
    private final char escapeChar;
    private final boolean strictQuotes;
    private final boolean ignoreLeadingWhiteSpace;
    private final char separator;
    private final String nullString;
    private final boolean isDropDifferingLines;

    /**
     * Creates a new instance.
     *
     * @param separator               the CSV separator
     * @param quoteChar               is the character that is used to quote fields (although unquoted fields are allowed as well)
     * @param escapeChar              a possible CSV escape character
     * @param strictQuotes            ignore characters outside of quotes
     * @param ignoreLeadingWhiteSpace ignore leading white space when quotes are present
     * @param isDropDifferingLines    whether differing or unparsable lines should simply be ignored
     * @param isSupressingEmptyCells  tells whether null fields will be forwarded by this operator or surpressed
     * @param nullString              the {@link String} representation of null values or {@code null} if none
     * @param maxColumns              is the maximum number of fields to extract from each line (the checkings still apply, though; always the
     *                                first fields will be taken)
     * @param numFieldsPerFile        is a mapping of file IDs to the number of expected fields contained within each row of the respective file
     */
    public ParseCsvRowsWithOpenCsv(char separator, char quoteChar, char escapeChar, boolean strictQuotes, boolean ignoreLeadingWhiteSpace,
                                   boolean isDropDifferingLines, final Int2IntMap numFieldsPerFile, final int maxColumns, final String nullString,
                                   boolean isSupressingEmptyCells) {
        this.separator = separator;
        this.quoteChar = quoteChar;
        this.escapeChar = escapeChar;
        this.strictQuotes = strictQuotes;
        this.ignoreLeadingWhiteSpace = ignoreLeadingWhiteSpace;
        this.isDropDifferingLines = isDropDifferingLines;
        this.numFieldsPerFile = numFieldsPerFile;
        this.maxFields = maxColumns;
        this.nullString = nullString;
        this.isSupressingEmptyCells = isSupressingEmptyCells && this.nullString != null;

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.csvParser = new CSVParser(this.separator, this.quoteChar, this.escapeChar, this.strictQuotes, this.ignoreLeadingWhiteSpace);
    }

    @Override
    public void flatMap(IntObjectTuple<String> fileLine, Collector<IntObjectTuple<String[]>> collector) throws Exception {
        String[] fields;
        int fileId = fileLine.a;
        String row = fileLine.b;

        // Parse and check the correctness.
        try {
            fields = this.csvParser.parseLine(row);
        } catch (IOException e) {
            if (this.isDropDifferingLines) return;
            throw new RuntimeException(String.format("Could not parse tuple %s.", fileLine), e);
        }
        if (this.numFieldsPerFile != null) {
            int numRequiredFields = this.numFieldsPerFile.get(fileId);
            if (fields.length != numRequiredFields) {
                if (this.isDropDifferingLines) return;
                throw new RuntimeException(String.format(
                        "Illegal number of fields in %s (expected %d, found %d).", fileLine, numRequiredFields, fields.length
                ));
            }
        }

        // Check and replace null values.
        if (this.nullString != null) {
            for (int i = 0; i < fields.length; i++) {
                if (this.nullString.equals(fields[i])) fields[i] = null;
            }
        }

        this.outputTuple.a = fileId;
        this.outputTuple.b = fields;
        collector.collect(this.outputTuple);
    }

}
