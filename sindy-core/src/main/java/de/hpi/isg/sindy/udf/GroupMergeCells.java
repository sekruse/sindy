/***********************************************************************************************************************
 * Copyright (C) 2014 by Sebastian Kruse
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package de.hpi.isg.sindy.udf;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * This function merges two cells by unioning their attribute sets.
 *
 * @author Sebastian Kruse
 */
@FunctionAnnotation.ReadFields("1")
@FunctionAnnotation.ForwardedFields("0")
@SuppressWarnings("serial")
public class GroupMergeCells
        extends RichGroupReduceFunction<Tuple2<String, int[]>, Tuple2<String, int[]>>
        implements GroupCombineFunction<Tuple2<String, int[]>, Tuple2<String, int[]>> {

    private final IntOpenHashSet accumulator = new IntOpenHashSet();

    @Override
    public void combine(Iterable<Tuple2<String, int[]>> iterable,
                        Collector<Tuple2<String, int[]>> out) throws Exception {
        this.reduce(iterable, out);
    }

    @Override
    public void reduce(Iterable<Tuple2<String, int[]>> iterable,
                       Collector<Tuple2<String, int[]>> out) throws Exception {

        Iterator<Tuple2<String, int[]>> iterator = iterable.iterator();
        Tuple2<String, int[]> firstCell = iterator.next();
        if (!iterator.hasNext()) {
            out.collect(firstCell);
            return;
        }

        this.accumulator.clear();
        for (int attribute : firstCell.f1) {
            this.accumulator.add(attribute);
        }

        while (iterator.hasNext()) {
            Tuple2<String, int[]> nextCell = iterator.next();
            for (int attribute : nextCell.f1) {
                this.accumulator.add(attribute);
            }
        }


        firstCell.f1 = this.accumulator.toIntArray();
        out.collect(firstCell);
    }

}
