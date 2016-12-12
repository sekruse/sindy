package de.hpi.isg.sindy.udf;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * This function creates for each incoming attribute group a set of overlap evidences, e.g. [1, 2, 3] -> (1, 1, [2, 3], [1, 1]),
 * (2, 1, [1, 3], [1, 1]), (3, 1, [1, 2], [1, 1]).
 * <p/>
 * <ul>
 * <li><b>Input:</b> <i>attributes</i></li>
 * <li><b>Output:</b> <i>(base attribute, base count, overlapping attributes, overlapping count)</i></li>
 * </ul>
 * 
 * @author Sebastian Kruse
 */
public class OverlapListCreator implements FlatMapFunction<int[], Tuple4<Integer, Integer, int[], int[]>> {

    // TODO: Since the overlap is symmetric, we could almost halve the output.

    private static final long serialVersionUID = -7892494553637141336L;

    private final static int[] EMPTY_ARRAY = {};

    private final static Integer ONE = 1;

    private final Tuple4<Integer, Integer, int[], int[]> outputTuple = new Tuple4<>();

    @Override
    public void flatMap(final int[] attributeArray,
            final Collector<Tuple4<Integer, Integer, int[], int[]>> out) throws Exception {

        int[] overlappingAttributes, overlappingCounts;
        if (attributeArray.length == 1) {
            overlappingAttributes = EMPTY_ARRAY;
            overlappingCounts = EMPTY_ARRAY;
        } else {
            overlappingAttributes = new int[attributeArray.length - 1];
            System.arraycopy(attributeArray, 1, overlappingAttributes, 0, overlappingAttributes.length);
            overlappingCounts = new int[attributeArray.length - 1];
            Arrays.fill(overlappingCounts, 1);
        }
        this.outputTuple.f0 = attributeArray[0];
        this.outputTuple.f1 = ONE;
        this.outputTuple.f2 = overlappingAttributes;
        this.outputTuple.f3 = overlappingCounts;
        out.collect(this.outputTuple);

        for (int attributeIndex = 0; attributeIndex < overlappingAttributes.length; attributeIndex++) {
            // Swap base attribute and overlappingAttributes[attributeIndex]
            int temp = overlappingAttributes[attributeIndex];
            overlappingAttributes[attributeIndex] = this.outputTuple.f0;
            this.outputTuple.f0 = temp;

            out.collect(this.outputTuple);
        }

    }

}
