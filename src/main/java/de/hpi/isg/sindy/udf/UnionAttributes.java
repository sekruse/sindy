package de.hpi.isg.sindy.udf;

import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation.ReadFields;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * From the incoming bucket element groups, this function groups all attribute IDs and discards the value.
 * <p/>
 * <ul>
 * <li><b>Input:</b> <i>(attribute, value)</i></li>
 * <li><b>Output:</b> <i>attributes</i></li>
 * </ul>
 * </p>
 * Expects the value field as reduce key.
 * 
 * @author Sebastian Kruse
 */
@ReadFields("0")
public class UnionAttributes extends RichGroupReduceFunction<Tuple2<Integer, String>, int[]> {

    private static final long serialVersionUID = -1694727005084300224L;

    private IntSortedSet attributeCollector;

    @Override
    public void open(final Configuration parameters) throws Exception {

        super.open(parameters);
        this.attributeCollector = new IntAVLTreeSet();

    }

    @Override
    public void reduce(final Iterable<Tuple2<Integer, String>> bucketElements,
            final Collector<int[]> out) throws Exception {

        this.attributeCollector.clear();
        for (final Tuple2<Integer, String> bucketElement : bucketElements) {
            this.attributeCollector.add(bucketElement.f0);
        }

        final int[] resultArray = this.attributeCollector.toIntArray();
        out.collect(resultArray);

    }

    // @Override
    // public void combine(Iterator<Tuple2<ColumnIdentifier, String>> values,
    // Collector<Tuple2<ColumnIdentifier, String>> out) throws Exception {
    // // TODO Auto-generated method stub
    // super.combine(values, out);
    // }
    //

}
