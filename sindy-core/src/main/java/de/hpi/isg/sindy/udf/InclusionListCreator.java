package de.hpi.isg.sindy.udf;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * This function creates for each incoming attribute group a set of IND candidates, e.g. [1, 2, 3] -> (1, [2, 3]), (2,
 * [1, 3]), (3, [1, 2]).
 * <p>
 * <ul>
 * <li><b>Input:</b> <i>attributes</i></li>
 * <li><b>Output:</b> <i>(included attribute, including attributes)</i></li>
 * </ul>
 *
 * @author Sebastian Kruse
 */
public class InclusionListCreator implements FlatMapFunction<int[], Tuple3<Integer, Integer, int[]>> {

    private static final long serialVersionUID = -7892494553637141336L;

    private final static int[] EMPTY_CANDIDATES = {};

    private final static Integer ONE = 1;

    private final Tuple3<Integer, Integer, int[]> outputTuple = new Tuple3<>();

    @Override
    public void flatMap(final int[] attributeArray,
                        final Collector<Tuple3<Integer, Integer, int[]>> out) throws Exception {

        int[] overlappingAttributes, overlappingCounts;
        if (attributeArray.length == 1) {
            overlappingAttributes = EMPTY_CANDIDATES;
        } else {
            overlappingAttributes = new int[attributeArray.length - 1];
            System.arraycopy(attributeArray, 1, overlappingAttributes, 0, overlappingAttributes.length);
            overlappingCounts = new int[attributeArray.length - 1];
            Arrays.fill(overlappingCounts, 1);
        }
        this.outputTuple.f0 = attributeArray[0];
        this.outputTuple.f1 = ONE;
        this.outputTuple.f2 = overlappingAttributes;
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
