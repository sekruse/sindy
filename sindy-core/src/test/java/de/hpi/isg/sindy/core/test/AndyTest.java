package de.hpi.isg.sindy.core.test;

import de.hpi.isg.sindy.core.AbstractSindy;
import de.hpi.isg.sindy.core.Andy;
import de.hpi.isg.sindy.core.Sindy;
import de.hpi.isg.sindy.searchspace.IndAugmentationRule;
import de.hpi.isg.sindy.util.IND;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

/**
 * Integration test for {@link Sindy}.
 */
public class AndyTest {

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
    public void testNaryDiscovery() {
        int numColumnBits = 16;
        Int2ObjectMap<String> indexedInputFiles = AbstractSindy.indexInputFiles(
                Arrays.asList(getUrl("letters.csv"), getUrl("letters-ext.csv")),
                numColumnBits
        );
        int lettersMinColumnId = 0;
        int lettersExtMinColumnId = 1 << 16;
        Set<IND> inds = new HashSet<>();
        Andy andy = new Andy(
                indexedInputFiles,
                numColumnBits,
                this.executionEnvironment,
                inds::add
        );
        andy.setFieldSeparator(',');
        andy.run();

        Set<IND> expectedINDs = new HashSet<>();
        expectedINDs.add(new IND(
                new int[]{lettersMinColumnId, lettersMinColumnId + 1, lettersMinColumnId + 3},
                new int[]{lettersExtMinColumnId, lettersExtMinColumnId + 1, lettersExtMinColumnId + 3}
        ));
        expectedINDs.add(new IND(
                new int[]{lettersMinColumnId, lettersMinColumnId + 2, lettersMinColumnId + 3},
                new int[]{lettersExtMinColumnId, lettersExtMinColumnId + 2, lettersExtMinColumnId + 3}
        ));
        expectedINDs.add(new IND(
                new int[]{lettersMinColumnId, lettersMinColumnId + 1, lettersMinColumnId + 2},
                new int[]{lettersExtMinColumnId, lettersExtMinColumnId + 1, lettersExtMinColumnId + 2}
        ));

        expectedINDs.add(new IND(
                new int[]{lettersExtMinColumnId + 1, lettersExtMinColumnId + 3},
                new int[]{lettersMinColumnId + 1, lettersMinColumnId + 3}
        ));
        expectedINDs.add(new IND(
                new int[]{lettersExtMinColumnId + 2, lettersExtMinColumnId + 3},
                new int[]{lettersMinColumnId + 2, lettersMinColumnId + 3}
        ));
        expectedINDs.add(new IND(
                new int[]{lettersExtMinColumnId + 1, lettersExtMinColumnId + 2},
                new int[]{lettersMinColumnId + 1, lettersMinColumnId + 2}
        ));

        Collection<IND> consolidatedINDs = new HashSet<>(andy.getConsolidatedINDs());

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

        Set<IndAugmentationRule> expectedIARs = new HashSet<>();
        expectedIARs.add(new IndAugmentationRule(
                new IND(
                        new int[]{lettersMinColumnId + 2, lettersMinColumnId + 3},
                        new int[]{lettersExtMinColumnId + 2, lettersExtMinColumnId + 3}
                ),
                new IND(lettersMinColumnId + 1, lettersExtMinColumnId + 1)
        ));
        expectedIARs.add(new IndAugmentationRule(
                new IND(
                        new int[]{lettersMinColumnId + 1, lettersMinColumnId + 3},
                        new int[]{lettersExtMinColumnId + 1, lettersExtMinColumnId + 3}
                ),
                new IND(lettersMinColumnId + 2, lettersExtMinColumnId + 2)
        ));
        expectedIARs.add(new IndAugmentationRule(
                new IND(
                        new int[]{lettersExtMinColumnId + 2, lettersExtMinColumnId + 3},
                        new int[]{lettersMinColumnId + 2, lettersMinColumnId + 3}
                ),
                new IND(lettersExtMinColumnId + 1, lettersMinColumnId + 1)
        ));
        expectedIARs.add(new IndAugmentationRule(
                new IND(
                        new int[]{lettersExtMinColumnId + 1, lettersExtMinColumnId + 3},
                        new int[]{lettersMinColumnId + 1, lettersMinColumnId + 3}
                ),
                new IND(lettersExtMinColumnId + 2, lettersMinColumnId + 2)
        ));

        Set<IndAugmentationRule> augmentationRules = new HashSet<>(andy.getAugmentationRules());
        for (IndAugmentationRule iar : augmentationRules) {
            Assert.assertTrue(
                    String.format("%s (%s) is not expected.", iar.toString(indexedInputFiles, numColumnBits), iar),
                    expectedIARs.contains(iar)
            );
        }
        for (IndAugmentationRule iar : expectedIARs) {
            Assert.assertTrue(
                    String.format("%s (%s) has not been discovered.", iar.toString(indexedInputFiles, numColumnBits), iar),
                    augmentationRules.contains(iar)
            );
        }
    }

    @Test
    public void testNaryDiscoveryWithVoidInds() {
        int numColumnBits = 16;
        Int2ObjectMap<String> indexedInputFiles = AbstractSindy.indexInputFiles(
                Arrays.asList(getUrl("persons.tsv"), getUrl("planets.tsv")),
                numColumnBits
        );
        int personsMinId = 0;
        int planetsMinId = 1 << 16;
        Set<IND> inds = new HashSet<>();
        Andy andy = new Andy(
                indexedInputFiles,
                numColumnBits,
                this.executionEnvironment,
                inds::add
        );
        andy.setDropNulls(true);
        andy.setNullString("\\N");
        andy.setFieldSeparator('\t');
        andy.setExcludeVoidIndsFromCandidateGeneration(false);
        andy.run();

        // The only non-void INDs are between first and last names. The last names determine the first names (partially).
        Set<IND> expectedINDs = new HashSet<>();
        expectedINDs.add(new IND(personsMinId, planetsMinId));
        expectedINDs.add(new IND(planetsMinId, personsMinId));
        expectedINDs.add(new IND(personsMinId + 2, planetsMinId + 1));
        expectedINDs.add(new IND(planetsMinId + 1, personsMinId + 2));

        // All the void columns form 0-ary IARs.
        Set<IndAugmentationRule> expectedIARs = new HashSet<>();
        int[] voidColumns = {personsMinId + 3, personsMinId + 4, planetsMinId + 3, planetsMinId + 4};
        List<Integer> allColumns = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            allColumns.add(personsMinId + i);
            allColumns.add(planetsMinId + i);
        }
        for (int voidColumn : voidColumns) {
            for (int refColumn : allColumns) {
                if (voidColumn != refColumn)
                    expectedIARs.add(new IndAugmentationRule(IND.emptyInstance, new IND(voidColumn, refColumn)));
            }
        }
        // Also, we have above mentioned IARs between first and last names.
        expectedIARs.add(new IndAugmentationRule(
                new IND(personsMinId + 2, planetsMinId + 1),
                new IND(personsMinId, planetsMinId)
        ));
        expectedIARs.add(new IndAugmentationRule(
                new IND(planetsMinId + 1, personsMinId + 2),
                new IND(planetsMinId, personsMinId)
        ));

        Collection<IND> consolidatedINDs = new HashSet<>(andy.getConsolidatedINDs());
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

        Set<IndAugmentationRule> augmentationRules = new HashSet<>(andy.getAugmentationRules());
        for (IndAugmentationRule iar : augmentationRules) {
            Assert.assertTrue(
                    String.format("%s (%s) is not expected.", iar.toString(indexedInputFiles, numColumnBits), iar),
                    expectedIARs.contains(iar)
            );
        }
        for (IndAugmentationRule iar : expectedIARs) {
            Assert.assertTrue(
                    String.format("%s (%s) has not been discovered.", iar.toString(indexedInputFiles, numColumnBits), iar),
                    augmentationRules.contains(iar)
            );
        }
    }

}
