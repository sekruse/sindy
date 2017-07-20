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

import de.hpi.isg.sindy.util.DistinctValueCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 * This operator transforms an attribute group into a set of attribute occurrences, i.e., each attribute is attached the count 1.
 *
 * @author Sebastian Kruse
 */
@SuppressWarnings("serial")
public class ExtractAttributeGroupsFromCells extends RichMapFunction<Tuple2<String, int[]>, int[]> {

    private DistinctValueCounter distinctValueCounter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        this.distinctValueCounter = new DistinctValueCounter();
        this.getRuntimeContext().addAccumulator(DistinctValueCounter.DEFAULT_KEY, this.distinctValueCounter);
    }

    @Override
    public int[] map(Tuple2<String, int[]> cell) throws Exception {
        for (int columnId : cell.f1) {
            this.distinctValueCounter.add(columnId);
        }
        return cell.f1;
    }

}
