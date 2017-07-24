package de.hpi.isg.sindy.udf;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * This function aggregates inclusion dependencies with the same included attribute by intersecting their including
 * attributes.
 * <p/>
 * <ul>
 * <li><b>Input/Output:</b> <i>(included attribute, including attributes)</i></li>
 * </ul>
 * Expects the reduce key to be the included attribute.
 *
 * @author Sebastian Kruse
 */
@FunctionAnnotation.ForwardedFields("0")
@FunctionAnnotation.ReadFields("1")
public class GroupIntersectCandidates
        extends RichGroupReduceFunction<Tuple3<Integer, Integer, int[]>, Tuple3<Integer, Integer, int[]>>
        implements GroupCombineFunction<Tuple3<Integer, Integer, int[]>, Tuple3<Integer, Integer, int[]>> {

    private static final long serialVersionUID = -8495767609476386745L;

    private final Tuple3<Integer, Integer, int[]> outputTuple = new Tuple3<>();

    private IntOpenHashSet intersection = new IntOpenHashSet();
    private IntOpenHashSet intersection2 = new IntOpenHashSet();

    @Override
    public void combine(final Iterable<Tuple3<Integer, Integer, int[]>> iterable,
                        final Collector<Tuple3<Integer, Integer, int[]>> out) throws Exception {
        this.reduce(iterable, out);
    }

    @Override
    public void reduce(final Iterable<Tuple3<Integer, Integer, int[]>> iterable,
                       final Collector<Tuple3<Integer, Integer, int[]>> out) throws Exception {

        Iterator<Tuple3<Integer, Integer, int[]>> iterator = iterable.iterator();
        Tuple3<Integer, Integer, int[]> firstCandidates = iterator.next();
        this.outputTuple.f0 = firstCandidates.f0;
        if (!iterator.hasNext()) {
            this.outputTuple.f1 = firstCandidates.f1;
            this.outputTuple.f2 = firstCandidates.f2;
            out.collect(this.outputTuple);
            return;
        }


        // Merge/intersect candidates.
        int count = firstCandidates.f1;
        this.intersection.clear();
        for (int attribute : firstCandidates.f2) {
            this.intersection.add(attribute);
        }

        while (iterator.hasNext() && !this.intersection.isEmpty()) {
            Tuple3<Integer, Integer, int[]> nextCell = iterator.next();
            count += nextCell.f1;
            this.intersection2.clear();
            for (int i : nextCell.f2) {
                if (this.intersection.contains(i)) {
                    this.intersection2.add(i);
                }
            }
            IntOpenHashSet swap = this.intersection;
            this.intersection = this.intersection2;
            this.intersection2 = swap;
        }
        while (iterator.hasNext()) {
            count += iterator.next().f1;
        }

        this.outputTuple.f1 = count;
        this.outputTuple.f2 = this.intersection.toIntArray();
        out.collect(this.outputTuple);
    }

}
