package de.hpi.isg.sindy.core.test;

import de.hpi.isg.sindy.core.AbstractSindy;
import de.hpi.isg.sindy.core.Sindy;
import de.hpi.isg.sindy.util.IND;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Integration test for {@link Sindy}.
 */
public class SindyTest {

    public static String getUrl(String testFile) {
        return "file:" + Thread.currentThread().getContextClassLoader().getResource(testFile).getPath();
    }

    @Test
    public void testUnaryIndDiscovery1() {
        int numColumnBits = 16;
        Int2ObjectMap<String> indexedInputFiles = AbstractSindy.indexInputFiles(
                Arrays.asList(getUrl("persons.tsv"), getUrl("planets.tsv")),
                numColumnBits
        );
        int personsMinColumnId = 0;
        int planetsMinColumnId = 1 << 16;
        Set<IND> inds = new HashSet<>();
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        Sindy sindy = new Sindy(
                indexedInputFiles,
                numColumnBits,
                executionEnvironment,
                inds::add
        );
        sindy.setDropNulls(true);
        sindy.setNullString("\\N");
        sindy.setFieldSeparator('\t');
        sindy.setMaxArity(1);
        sindy.run();

        // NOTE: Void INDs (no actual value inclusion) are not considered.
        Set<IND> expectedINDs = new HashSet<>();
        expectedINDs.add(new IND(personsMinColumnId, planetsMinColumnId));
        expectedINDs.add(new IND(planetsMinColumnId, personsMinColumnId));
        expectedINDs.add(new IND(planetsMinColumnId + 1, personsMinColumnId + 2));
        expectedINDs.add(new IND(personsMinColumnId + 2, planetsMinColumnId + 1));

        Assert.assertEquals(expectedINDs, inds);
    }

    @Test
    public void testUnaryIndDiscovery2() {
        int numColumnBits = 16;
        Int2ObjectMap<String> indexedInputFiles = AbstractSindy.indexInputFiles(
                Arrays.asList(getUrl("persons.tsv"), getUrl("planets.tsv")),
                numColumnBits
        );
        int personsMinColumnId = 0;
        int planetsMinColumnId = 1 << 16;
        Set<IND> inds = new HashSet<>();
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        Sindy sindy = new Sindy(
                indexedInputFiles,
                numColumnBits,
                executionEnvironment,
                inds::add
        );
        sindy.setDropNulls(false);
        sindy.setNullString("\\N");
        sindy.setFieldSeparator('\t');
        sindy.setMaxArity(1);
        sindy.run();

        Set<IND> expectedINDs = new HashSet<>();
        expectedINDs.add(new IND(personsMinColumnId, planetsMinColumnId));
        expectedINDs.add(new IND(planetsMinColumnId, personsMinColumnId));

        expectedINDs.add(new IND(planetsMinColumnId + 1, personsMinColumnId + 2));
        expectedINDs.add(new IND(personsMinColumnId + 2, planetsMinColumnId + 1));

        // All the \N column inclusions...
        for (int dep : Arrays.asList(personsMinColumnId + 3, personsMinColumnId + 4, planetsMinColumnId + 3, planetsMinColumnId + 4)) {
            for (int ref : Arrays.asList(
                    personsMinColumnId + 1, personsMinColumnId + 2, personsMinColumnId + 3, personsMinColumnId + 4,
                    planetsMinColumnId + 1, planetsMinColumnId + 3, planetsMinColumnId + 4)
                    ) {
                if (dep != ref) expectedINDs.add(new IND(dep, ref));
            }
        }

        for (IND ind : inds) {
            Assert.assertTrue(
                    String.format("%s is not expected.", ind.toString(indexedInputFiles, numColumnBits)),
                    expectedINDs.contains(ind)
            );
        }
        for (IND ind : expectedINDs) {
            Assert.assertTrue(
                    String.format("%s has not been discovered.", ind.toString(indexedInputFiles, numColumnBits)),
                    inds.contains(ind)
            );
        }
    }

    @Test
    public void testNaryDiscovery() {
        int numColumnBits = 16;
        Int2ObjectMap<String> indexedInputFiles = AbstractSindy.indexInputFiles(
                Arrays.asList(getUrl("letters.csv"), getUrl("letters-ext.csv")),
                numColumnBits
        );
        int lettersMinColumnId = 0;
        int lettersExtMinColumnId = 1 << 16;
        Set<IND> inds = new HashSet<>();
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        Sindy sindy = new Sindy(
                indexedInputFiles,
                numColumnBits,
                executionEnvironment,
                inds::add
        );
        sindy.setFieldSeparator(',');
        sindy.run();

        Set<IND> expectedINDs = new HashSet<>();
        expectedINDs.add(new IND(
                new int[]{lettersMinColumnId, lettersMinColumnId + 1, lettersMinColumnId + 2, lettersMinColumnId + 3},
                new int[]{lettersExtMinColumnId, lettersExtMinColumnId + 1, lettersExtMinColumnId + 2, lettersExtMinColumnId + 3}
        ));
        expectedINDs.add(new IND(
                new int[]{lettersExtMinColumnId + 1, lettersExtMinColumnId + 2, lettersExtMinColumnId + 3},
                new int[]{lettersMinColumnId + 1, lettersMinColumnId + 2, lettersMinColumnId + 3}
        ));

        Collection<IND> consolidatedINDs = new HashSet<>(sindy.getConsolidatedINDs());

        for (IND ind : consolidatedINDs) {
            Assert.assertTrue(
                    String.format("%s is not expected.", ind.toString(indexedInputFiles, numColumnBits)),
                    expectedINDs.contains(ind)
            );
        }
        for (IND ind : expectedINDs) {
            Assert.assertTrue(
                    String.format("%s has not been discovered.", ind.toString(indexedInputFiles, numColumnBits)),
                    consolidatedINDs.contains(ind)
            );
        }
    }


}
