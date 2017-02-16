package de.hpi.isg.sindy.core.test;

import de.hpi.isg.sindy.core.AbstractSindy;
import de.hpi.isg.sindy.core.Sandy;
import de.hpi.isg.sindy.core.Sindy;
import de.hpi.isg.sindy.util.PartialIND;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Integration test for {@link Sindy}.
 */
public class SandyTest {

    public static String getUrl(String testFile) {
        return "file:" + Thread.currentThread().getContextClassLoader().getResource(testFile).getPath();
    }

    @Test
    public void testUnaryPartialIndDiscovery() {
        int numColumnBits = 16;
        Int2ObjectMap<String> indexedInputFiles = AbstractSindy.indexInputFiles(
                Arrays.asList(getUrl("persons.tsv"), getUrl("planets.tsv")),
                numColumnBits
        );
        int personsMinColumnId = 0;
        int planetsMinColumnId = 1 << 16;
        Set<PartialIND> inds = new HashSet<>();
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        Sandy sandy = new Sandy(
                indexedInputFiles,
                numColumnBits,
                executionEnvironment,
                inds::add
        );
        sandy.setDropNulls(false);
        sandy.setFieldSeparator('\t');
        sandy.setMaxArity(1);
        sandy.setMinRelativeOverlap(0.0);
        sandy.run();

        // NOTE: Void INDs (no actual value inclusion) are not considered.
        Set<PartialIND> expectedINDs = new HashSet<>();
        expectedINDs.add(new PartialIND(personsMinColumnId, planetsMinColumnId, 4, 1d));
        expectedINDs.add(new PartialIND(planetsMinColumnId, personsMinColumnId, 4, 1d));
        expectedINDs.add(new PartialIND(planetsMinColumnId + 1, personsMinColumnId + 2, 4, 1d));
        expectedINDs.add(new PartialIND(personsMinColumnId + 2, planetsMinColumnId + 1, 4, 1d));
        expectedINDs.add(new PartialIND(personsMinColumnId + 1, personsMinColumnId + 2, 3, 1d / 3d));

        for (PartialIND ind : expectedINDs) {
            Assert.assertTrue(
                    String.format("%s has not been discovered.", ind.toString(indexedInputFiles, numColumnBits)),
                    inds.contains(ind)
            );
        }
    }

}
