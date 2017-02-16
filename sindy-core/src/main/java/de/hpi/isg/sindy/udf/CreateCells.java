package de.hpi.isg.sindy.udf;

import de.hpi.isg.sindy.data.IntObjectTuple;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Puts the given source and column index into a singleton array, so that the a following reduce on the string value is
 * combinable.
 * <p/>
 * <ul>
 * <li><b>Input:</b> <i>(attribute, value)</i></li>
 * <li><b>Output:</b> <i>(value, [attribute])</i></li>
 * </ul>
 * 
 * @author Sebastian Kruse
 */
@FunctionAnnotation.ForwardedFields("b -> 0")
public class CreateCells extends RichMapFunction<IntObjectTuple<String>, Tuple2<String, int[]>> {

    private static final long serialVersionUID = -7633633076344649301L;

    private final int[] reuseColumnIdentifierArray = new int[1];
    private final Tuple2<String, int[]> outputTuple = new Tuple2<>();

    @Override
    public Tuple2<String, int[]> map(final IntObjectTuple<String> inputTuple)
            throws Exception {

        this.reuseColumnIdentifierArray[0] = inputTuple.a;
        this.outputTuple.f0 = inputTuple.b;
        this.outputTuple.f1 = this.reuseColumnIdentifierArray;
        return this.outputTuple;
    }

}
