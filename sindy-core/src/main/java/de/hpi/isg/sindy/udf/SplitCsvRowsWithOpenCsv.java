package de.hpi.isg.sindy.udf;

import au.com.bytecode.opencsv.CSVParser;
import de.hpi.isg.sindy.data.IntObjectTuple;
import de.hpi.isg.sindy.util.NullValueCounter;
import de.hpi.isg.sindy.util.TableHeightAccumulator;
import de.hpi.isg.sindy.util.TableWidthAccumulator;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Objects;

/**
 * This function splits a given CSV row into its fields. The given first integer field will be offset by the field
 * index.
 *
 * @author Sebastian Kruse
 */
public class SplitCsvRowsWithOpenCsv extends RichFlatMapFunction<IntObjectTuple<String>, IntObjectTuple<String>> {

    private static final long serialVersionUID = 1377116120504051734L;

    private final IntObjectTuple<String> outputTuple = new IntObjectTuple<>();

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

    private TableWidthAccumulator tableWidthAccumulator;
    private TableHeightAccumulator tableHeightAccumulator;
    private NullValueCounter nullValueCounter;

    /**
     * Creates a new instance without limitation of used fields.
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
    public SplitCsvRowsWithOpenCsv(char separator, char quoteChar, char escapeChar, boolean strictQuotes, boolean ignoreLeadingWhiteSpace,
                                   boolean isDropDifferingLines, final Int2IntMap numFieldsPerFile, final int maxColumns, final String nullString,
                                   boolean isSupressingEmptyCells) {

        this.separator = separator;
        this.quoteChar = quoteChar;
        this.escapeChar = escapeChar;
        this.strictQuotes = strictQuotes;
        this.ignoreLeadingWhiteSpace = ignoreLeadingWhiteSpace;
        this.isDropDifferingLines = isDropDifferingLines;
        this.numFieldsPerFile = numFieldsPerFile == null && isDropDifferingLines ? new Int2IntOpenHashMap() : numFieldsPerFile;
        if (this.numFieldsPerFile != null) this.numFieldsPerFile.defaultReturnValue(-1);
        this.maxFields = maxColumns;
        this.nullString = nullString;
        this.isSupressingEmptyCells = isSupressingEmptyCells && this.nullString != null;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.csvParser = new CSVParser(this.separator, this.quoteChar, this.escapeChar, this.strictQuotes, this.ignoreLeadingWhiteSpace);
        this.tableWidthAccumulator = new TableWidthAccumulator();
        this.getRuntimeContext().addAccumulator(TableWidthAccumulator.DEFAULT_KEY, this.tableWidthAccumulator);
        this.tableHeightAccumulator = new TableHeightAccumulator();
        this.getRuntimeContext().addAccumulator(TableHeightAccumulator.DEFAULT_KEY, this.tableHeightAccumulator);
        this.nullValueCounter = new NullValueCounter();
        this.getRuntimeContext().addAccumulator(NullValueCounter.DEFAULT_KEY, this.nullValueCounter);
    }

    @Override
    public void flatMap(final IntObjectTuple<String> fileLine, final Collector<IntObjectTuple<String>> out)
            throws Exception {

        final int fileId = fileLine.a;
        this.outputTuple.a = fileId;
        final String row = fileLine.b;

        String[] fields;
        int numFields;
        // Parse and check the correctness.
        try {
            fields = this.csvParser.parseLine(row);
            numFields = (this.maxFields >= 0) ? Math.min(this.maxFields, fields.length) : fields.length;
            this.tableWidthAccumulator.setNumColumns(fileId, numFields);
        } catch (Exception e) {
            if (this.isDropDifferingLines) return;
            throw new RuntimeException(String.format("Could not parse tuple %s.", fileLine), e);
        }

        if (this.numFieldsPerFile != null) {
            int numRequiredFields = this.numFieldsPerFile.get(fileId);
            if (numRequiredFields == -1) {
                // If we should drop differing lines but don't know how many lines to expect, we make a best guess and
                // assume the first seen line to be correct.
                this.numFieldsPerFile.put(fileId, fields.length);
            } else if (fields.length != numRequiredFields) { // We explicitly do not use numFields, because we are interested in the file integrity.
                if (this.isDropDifferingLines) return;
                throw new RuntimeException(String.format(
                        "Illegal number of fields in %s (expected %d, found %d).", fileLine, numRequiredFields, fields.length
                ));
            }
        }

        // At this point, we have a valid tuple and count it.
        this.tableHeightAccumulator.add(fileId);

        // Forward the parsed values.
        for (int fieldIndex = 0; fieldIndex < numFields; fieldIndex++) {
            String field = fields[fieldIndex];
            if (Objects.equals(field, this.nullString)) {
                if (this.isSupressingEmptyCells) {
                    this.outputTuple.a++;
                    this.nullValueCounter.add(fileId + fieldIndex);
                    continue;
                } else {
                    field = "\1";
                }
            }
            this.outputTuple.b = field;
            out.collect(this.outputTuple);
            this.outputTuple.a++;
        }
    }

}
