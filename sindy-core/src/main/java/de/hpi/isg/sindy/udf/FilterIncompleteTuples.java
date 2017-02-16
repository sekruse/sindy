package de.hpi.isg.sindy.udf;


import de.hpi.isg.sindy.data.IntObjectTuple;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * This function retains only those tuples that have no empty or {@code null} field.
 *
 * @author Sebastian Kruse
 */
public class FilterIncompleteTuples implements FilterFunction<IntObjectTuple<String[]>> {

    @Override
    public boolean filter(IntObjectTuple<String[]> tuple) throws Exception {
        for (String field : tuple.b) {
            if (field == null || field.isEmpty()) {
                return false;
            }
        }
        return true;
    }
}
