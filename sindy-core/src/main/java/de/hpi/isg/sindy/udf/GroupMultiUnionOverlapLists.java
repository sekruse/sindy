package de.hpi.isg.sindy.udf;

import de.hpi.isg.sindy.util.OverlapAccumulator;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Merges overlap lists via multi-union. Note, that this function does not provide sorted overlapping attributes but
 * also does not expect them.
 *
 * Created by basti on 8/7/15.
 */
@FunctionAnnotation.ForwardedFields("0")
public class GroupMultiUnionOverlapLists
        extends RichGroupReduceFunction<Tuple4<Integer, Integer, int[], int[]>, Tuple4<Integer, Integer, int[], int[]>>
        implements GroupCombineFunction<Tuple4<Integer, Integer, int[], int[]>, Tuple4<Integer, Integer, int[], int[]>> {

    /**
     * Aggregates the counts of each encountered attribute.
     */
    private Int2IntOpenHashMap overlapCountAccumulator = new Int2IntOpenHashMap();
    {
        this.overlapCountAccumulator.defaultReturnValue(0);
    }

    private Tuple4<Integer, Integer, int[], int[]> outputTuple = new Tuple4<>();

    /**
     * Keeps track of the relative overlaps (for the records).
     */
    private OverlapAccumulator overlapAccumulator;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.overlapAccumulator = new OverlapAccumulator();
        this.getRuntimeContext().addAccumulator(OverlapAccumulator.DEFAULT_KEY, this.overlapAccumulator);
    }

    @Override
    public void combine(Iterable<Tuple4<Integer, Integer, int[], int[]>> inputAttributeLists,
                        Collector<Tuple4<Integer, Integer, int[], int[]>> collector) throws Exception {
        this.merge(inputAttributeLists, collector, false);
    }

    @Override
    public void reduce(Iterable<Tuple4<Integer, Integer, int[], int[]>> inputAttributeLists,
                       Collector<Tuple4<Integer, Integer, int[], int[]>> collector)
            throws Exception {
        this.merge(inputAttributeLists, collector, true);
    }

    private void merge(Iterable<Tuple4<Integer, Integer, int[], int[]>> inputAttributeLists,
                       Collector<Tuple4<Integer, Integer, int[], int[]>> collector,
                       boolean isCollectStatistics)
            throws Exception {

        Iterator<Tuple4<Integer, Integer, int[], int[]>> inputIterator = inputAttributeLists.iterator();
        Tuple4<Integer, Integer, int[], int[]> firstElement = inputIterator.next();

        // Short-cut if there is only a single element.
        if (!inputIterator.hasNext()) {
            collector.collect(firstElement);
            if (isCollectStatistics) {
                double numDistinctValues = firstElement.f0;
                for (int overlapSize : firstElement.f3) {
                    this.overlapAccumulator.add(numDistinctValues == 0 ? 1 : overlapSize / numDistinctValues);
                }
            }
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

        // Accumulate statistics.
        if (isCollectStatistics) {
            double numDistinctValues = this.outputTuple.f0;
            for (int overlapSize : this.outputTuple.f3) {
                this.overlapAccumulator.add(numDistinctValues == 0 ? 1 : overlapSize / numDistinctValues);
            }
        }
    }

}
