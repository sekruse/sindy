package de.hpi.isg.sindy.core.test;

import de.hpi.isg.sindy.core.AbstractSindy;
import de.hpi.isg.sindy.core.Sindy;
import de.hpi.isg.sindy.searchspace.AprioriCandidateGenerator;
import de.hpi.isg.sindy.searchspace.TransversalCandidateGenerator;
import de.hpi.isg.sindy.util.IND;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.junit.*;

import java.util.*;

/**
 * Integration test for {@link Sindy}.
 */
public class SindyTest {

    private ExecutionEnvironment executionEnvironment;

    @Before
    public void setUp() {
        this.executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
    }

    @After
    public void tearDown() {
        this.executionEnvironment = null;
    }

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
        this.executionEnvironment.setParallelism(1);
        Sindy sindy = new Sindy(
                indexedInputFiles,
                numColumnBits,
                this.executionEnvironment,
                inds::add
        );
        sindy.setDropNulls(true);
        sindy.setNullString("\\N");
        sindy.setFieldSeparator('\t');
        sindy.setMaxArity(1);
        sindy.run();

        Set<IND> expectedINDs = new HashSet<>();
        expectedINDs.add(new IND(personsMinColumnId, planetsMinColumnId));
        expectedINDs.add(new IND(planetsMinColumnId, personsMinColumnId));
        expectedINDs.add(new IND(planetsMinColumnId + 1, personsMinColumnId + 2));
        expectedINDs.add(new IND(personsMinColumnId + 2, planetsMinColumnId + 1));

        // Add all the "void" INDs (those, where the dependent columns contain but null values).
        int[] voidColumns = {personsMinColumnId + 3, personsMinColumnId + 4, planetsMinColumnId + 3, planetsMinColumnId + 4};
        List<Integer> allColumns = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            allColumns.add(personsMinColumnId + i);
            allColumns.add(planetsMinColumnId + i);
        }
        for (int voidColumn : voidColumns) {
            for (int refColumn : allColumns) {
                if (voidColumn != refColumn) expectedINDs.add(new IND(voidColumn, refColumn));
            }
        }

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
        this.executionEnvironment.setParallelism(1);
        Sindy sindy = new Sindy(
                indexedInputFiles,
                numColumnBits,
                this.executionEnvironment,
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
    public void testPessimisticNaryDiscovery() {
        int numColumnBits = 16;
        Int2ObjectMap<String> indexedInputFiles = AbstractSindy.indexInputFiles(
                Arrays.asList(getUrl("letters.csv"), getUrl("letters-ext.csv")),
                numColumnBits
        );
        int lettersMinColumnId = 0;
        int lettersExtMinColumnId = 1 << 16;
        Set<IND> inds = new HashSet<>();
        this.executionEnvironment.setParallelism(1);
        Sindy sindy = new Sindy(
                indexedInputFiles,
                numColumnBits,
                this.executionEnvironment,
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

    @Test
    public void testPessimisticNaryDiscoveryWithChunking() {
        int numColumnBits = 16;
        Int2ObjectMap<String> indexedInputFiles = AbstractSindy.indexInputFiles(
                Arrays.asList(getUrl("letters.csv"), getUrl("letters-ext.csv")),
                numColumnBits
        );
        int lettersMinColumnId = 0;
        int lettersExtMinColumnId = 1 << 16;
        Set<IND> inds = new HashSet<>();
        this.executionEnvironment.setParallelism(1);
        Sindy sindy = new Sindy(
                indexedInputFiles,
                numColumnBits,
                this.executionEnvironment,
                inds::add
        );
        sindy.setCandidateChunkSize(1);
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

    @Ignore("Mixed IND traversal strategies are not yet implemented fully.")
    @Test
    public void testMixedNaryDiscoveryWithZigZag() {
        int numColumnBits = 16;
        Int2ObjectMap<String> indexedInputFiles = AbstractSindy.indexInputFiles(
                Arrays.asList(getUrl("letters.csv"), getUrl("letters-ext.csv")),
                numColumnBits
        );
        int lettersMinColumnId = 0;
        int lettersExtMinColumnId = 1 << 16;
        Set<IND> inds = new HashSet<>();
        this.executionEnvironment.setParallelism(1);
        Sindy sindy = new Sindy(
                indexedInputFiles,
                numColumnBits,
                this.executionEnvironment,
                inds::add
        );
        sindy.setFieldSeparator(',');
        sindy.setMaxArity(-1);
        sindy.setMaxBfsArity(2); // Only the first two levels/arities should be checked with a pessimistic approach.
        sindy.setOptimisticCandidateGenerator(new TransversalCandidateGenerator());
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
