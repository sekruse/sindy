package de.hpi.isg.sindy.udf;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Merges overlap lists via multi-union. Note, that this function does not provide sorted overlapping attributes but
 * also does not expect them.
 *
 * Created by basti on 8/7/15.
 */
@RichGroupReduceFunction.Combinable
@FunctionAnnotation.ForwardedFields("0")
public class GroupMultiUnionOverlapLists
        extends RichGroupReduceFunction<Tuple4<Integer, Integer, int[], int[]>, Tuple4<Integer, Integer, int[], int[]>> {

    /**
     * Aggregates the counts of each encountered attribute.
     */
    private Int2IntOpenHashMap overlapCountAccumulator = new Int2IntOpenHashMap();
    {
        this.overlapCountAccumulator.defaultReturnValue(0);
    }

    private Tuple4<Integer, Integer, int[], int[]> outputTuple = new Tuple4<>();

    @Override
    public void reduce(Iterable<Tuple4<Integer, Integer, int[], int[]>> inputAttributeLists,
                       Collector<Tuple4<Integer, Integer, int[], int[]>> collector)
            throws Exception {

        Iterator<Tuple4<Integer, Integer, int[], int[]>> inputIterator = inputAttributeLists.iterator();
        Tuple4<Integer, Integer, int[], int[]> firstElement = inputIterator.next();

        // Short-cut if there is only a single element.
        if (!inputIterator.hasNext()) {
            collector.collect(firstElement);
            return;
        }

        // Consume first overlap evidence.
        this.overlapCountAccumulator.clear();
        for (int i = 0; i < firstElement.f2.length; i++) {
            this.overlapCountAccumulator.put(firstElement.f2[i], firstElement.f3[i]);
        }
        int baseCount = firstElement.f1;

        // Consume all other overlap evidences.
        while (inputIterator.hasNext()) {
            Tuple4<Integer, Integer, int[], int[]> nextElement = inputIterator.next();
            baseCount += nextElement.f1;
            for (int i = 0; i < nextElement.f2.length; i++) {
                overlapCountAccumulator.addTo(nextElement.f2[i], nextElement.f3[i]);
            }
        }

        // Produce the aggregated overlap evidence.
        this.outputTuple.f0 = firstElement.f0;
        this.outputTuple.f1 = baseCount;
        this.outputTuple.f2 = new int[overlapCountAccumulator.size()];
        this.outputTuple.f3 = new int[overlapCountAccumulator.size()];
        ObjectIterator<Int2IntMap.Entry> accuIterator = overlapCountAccumulator.int2IntEntrySet().fastIterator();
        int i = 0;
        while (accuIterator.hasNext()) {
            Int2IntMap.Entry accumulatorEntry = accuIterator.next();
            this.outputTuple.f2[i] = accumulatorEntry.getIntKey();
            this.outputTuple.f3[i] = accumulatorEntry.getIntValue();
            i++;
        }
        collector.collect(this.outputTuple);
    }

    @Override
    public void combine(Iterable<Tuple4<Integer, Integer, int[], int[]>> values, Collector<Tuple4<Integer, Integer, int[], int[]>> out) throws Exception {
        super.combine(values, out);
    }
}
