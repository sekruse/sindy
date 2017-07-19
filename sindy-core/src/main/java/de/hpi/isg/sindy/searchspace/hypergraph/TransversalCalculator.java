package de.hpi.isg.sindy.searchspace.hypergraph;

import de.hpi.isg.sindy.searchspace.NaryIndRestrictions;
import de.hpi.isg.sindy.util.IND;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Calculates the minimal transversals for a set of minimal non-INDs.
 *
 * @author sebastian.kruse
 * @since 05.08.2015
 */
public class TransversalCalculator {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransversalCalculator.class);

    /**
     * A set of n-ary non-INDs corresponds to the edges of the conceptual hypergraph.
     */
    private List<NaryInd> edges = new ArrayList<>();

    /**
     * The set of IND corresponds to the vertices in the conceptual hypergraph.
     */
    private Collection<UnaryInd> vertices = new ArrayList<>();

    /**
     * Creates a new transversal calculator for a hypergraph with the negative IND border as edges and the unary
     * INDs as vertices.
     *
     * @param negativeBorder is the (stripped) negative IND border
     * @param unaryInds      are the existing unary INDs
     */
    public TransversalCalculator(Collection<IND> negativeBorder, Collection<IND> unaryInds) {
        for (IND nind : negativeBorder) {
            this.edges.add(new NaryInd(nind));
        }
        for (IND unaryInd : unaryInds) {
            this.vertices.add(new UnaryInd(unaryInd));
        }
    }

    /**
     * Creates a new transversal calculator for a hypergraph with the negative IND border as edges and the unary
     * INDs as vertices.
     *
     * @param negativeBorder is the (stripped) negative IND border
     * @param unaryInds      are the existing unary INDs
     * @param distinguish    will be ignored, only helps to distinguish the constructor signatures
     */
    public TransversalCalculator(Collection<NaryInd> negativeBorder, Collection<UnaryInd> unaryInds, Void distinguish) {
        for (NaryInd nind : negativeBorder) {
            this.edges.add(nind);
        }
        for (UnaryInd unaryInd : unaryInds) {
            this.vertices.add(unaryInd);
        }
    }

    /**
     * Tests if there is a single hyperedge that contains all vertices.
     *
     * @return the test result
     */
    public boolean isASingleClique() {
        if (this.edges.size() == 1) {
            NaryInd edge = this.edges.get(0);
            if (edge.getUnaryInds().size() == this.vertices.size() && edge.getUnaryInds().containsAll(this.vertices)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Calculates the minimal transversals for the given hypergraph.
     *
     * @return the transversals
     */
    public Collection<NaryInd> calculateTransversals() {
        LOGGER.debug("Calculating transversal for {} vertices and {} edges.", this.vertices.size(), this.edges.size());
        Collection<NaryInd> transversals = new HashSet<>();
        if (this.edges.isEmpty()) {
            transversals.add(new NaryInd(Collections.<UnaryInd>emptySet()));
            return transversals;
        }

        // Bring edges into an efficient order for transversal cacluation.
        Collections.sort(this.edges, NaryInd.ARITY_COMPARATOR);

        // Initialize transversal for first edge.
        Iterator<NaryInd> edgeIterator = this.edges.iterator();
        NaryInd firstEdge = edgeIterator.next();
        for (UnaryInd unaryInd : firstEdge.getUnaryInds()) {
            transversals.add(unaryInd.toNaryInd());
        }

        // Consider the other edges and update the transversals.
        int edgeCount = 0;
        while (edgeIterator.hasNext()) {
            NaryInd nextEdge = edgeIterator.next();
            LOGGER.trace("Processing edge {} with {} transversals so far.", edgeCount++, transversals.size());

            // Split up disjoint transversals and only retain overlapping transversals.
            Collection<NaryInd> disjointTransversals = new HashSet<>();
            Iterator<NaryInd> transversalIterator = transversals.iterator();
            while (transversalIterator.hasNext()) {
                NaryInd transversal = transversalIterator.next();
                if (transversal.isDisjointWith(nextEdge)) {
                    transversalIterator.remove();
                    disjointTransversals.add(transversal);
                }
            }

            // For the disjoint transversals, add any possible unary IND from the edge to get reestablish the transversal
            // property.
            for (NaryInd disjointTransversal : disjointTransversals) {
                for (UnaryInd newUnaryInd : nextEdge.getUnaryInds()) {
                    NaryInd candidateTransversal = new NaryInd(disjointTransversal, newUnaryInd);
                    // Check that the transversal candidate is not a superset of an existing transversal.
                    if (!transversals.contains(candidateTransversal)) {
                        boolean isSpecializingExistingTransversal = false;
                        for (NaryInd existingTransversal : transversals) {
                            if (candidateTransversal.isSpecializing(existingTransversal)) {
                                isSpecializingExistingTransversal = true;
                                break;
                            }
                        }
                        if (!isSpecializingExistingTransversal) {
                            transversals.add(candidateTransversal);
                        }
                    }
                }
            }
        }

        return transversals;
    }

    public Collection<Collection<UnaryInd>> partitionUnaryInds(Collection<UnaryInd> unaryInds, NaryIndRestrictions restrictions) {
        // Check if we need to partition at all.
        if (restrictions.isAllowInterRepetitions() && restrictions.isAllowIntraRepetitions()) {
            return Collections.singleton(unaryInds);
        } else if (!restrictions.isAllowInterRepetitions() && restrictions.isAllowIntraRepetitions()) {
            throw new RuntimeException("Forbidding inter-restrictions but not intra-restrictions is not implmented.");
        }

        // Create conflict groups.
        Map<ConflictGroupKey, List<UnaryInd>> conflictGroups = new HashMap<>();
        for (UnaryInd unaryInd : unaryInds) {
            if (restrictions.isAllowInterRepetitions()) {
                ConflictGroupKey conflictGroupKey = new ConflictGroupKey(ConflictGroupKey.DEP_CONFLICT, unaryInd.depId);
                conflictGroups.computeIfAbsent(conflictGroupKey, key -> new ArrayList<>()).add(unaryInd);
                conflictGroupKey = new ConflictGroupKey(ConflictGroupKey.REF_CONFLICT, unaryInd.refId);
                conflictGroups.computeIfAbsent(conflictGroupKey, key -> new ArrayList<>()).add(unaryInd);
            } else {
                ConflictGroupKey conflictGroupKey = new ConflictGroupKey(ConflictGroupKey.ANY_CONFLICT, unaryInd.depId);
                conflictGroups.computeIfAbsent(conflictGroupKey, key -> new ArrayList<>()).add(unaryInd);
                if (unaryInd.depId != unaryInd.refId) {
                    conflictGroupKey = new ConflictGroupKey(ConflictGroupKey.ANY_CONFLICT, unaryInd.refId);
                    conflictGroups.computeIfAbsent(conflictGroupKey, key -> new ArrayList<>()).add(unaryInd);
                }
            }
        }

        // Remove singleton conflict groups and invert the remaining conflict groups to expose the membership of INDs
        // in conflict groups.
        Map<UnaryInd, Set<ConflictGroupKey>> conflictGroupMembership = new HashMap<>();
        for (Iterator<Map.Entry<ConflictGroupKey, List<UnaryInd>>> iterator = conflictGroups.entrySet().iterator();
             iterator.hasNext(); ) {
            Map.Entry<ConflictGroupKey, List<UnaryInd>> entry = iterator.next();
            List<UnaryInd> inds = entry.getValue();
            if (inds.size() < 2) {
                iterator.remove();
            } else {
                ConflictGroupKey conflictGroupKey = entry.getKey();
                for (UnaryInd ind : inds) {
                    conflictGroupMembership.computeIfAbsent(ind, key -> new HashSet<>()).add(conflictGroupKey);
                }
            }
        }

        // Find non-conflicting INDs, i.e., INDs that are in no (non-singleton) conflict group.
        Collection<UnaryInd> conflictFreeInds = new LinkedList<>(unaryInds);
        conflictFreeInds.removeAll(conflictGroupMembership.keySet());

        // Important: we assume that conflict groups are not subsets of each other. This is because:
        // * each unary IND is unique
        // * singleton conflict groups are removed
        // * if two unary INDs are in two common conflict groups, they must have to different values in common
        // * it follows: a<b and b<a are these INDs and inter-repetitions are forbidden
        // * therefore, two conflict groups can have a maximum intersection of two (or one with inter-repetitions)

        // Iterate the conflict groups to harvest the result:
        // * keep track of already seen unary INDs and process them only for their first appearance
        // * if a partial solution (an n-ary IND) contains the current conflict group -> new solutions
        // * else
        //   * create all combinations with unseen unary INDs from the current conflict group -> new solutions
        //   * if no such possible combination, keep as is -> new solution
        Set<UnaryInd> seenUnaryInds = new HashSet<>();
        List<ConflictGroupSolution> conflictGroupSolutions = new ArrayList<>();
        conflictGroupSolutions.add(new ConflictGroupSolution());
        for (Map.Entry<ConflictGroupKey, List<UnaryInd>> entry : conflictGroups.entrySet()) {
            ConflictGroupKey conflictGroupKey = entry.getKey();
            List<UnaryInd> conflictingInds = entry.getValue();

            // Remove already seen INDs from the conflict group.
            for (Iterator<UnaryInd> iterator = conflictingInds.iterator(); iterator.hasNext(); ) {
                if (!seenUnaryInds.add(iterator.next())) {
                    iterator.remove();
                }
            }

            // If there are no unseen INDs in the current capture group, we can skip it.
            if (conflictingInds.isEmpty()) {
                continue;
            }

            // Separate results that do not contain the current conflict group.
            List<ConflictGroupSolution> updatableResults = new LinkedList<>();
            for (Iterator<ConflictGroupSolution> iterator = conflictGroupSolutions.iterator(); iterator.hasNext(); ) {
                ConflictGroupSolution solution = iterator.next();
                if (!solution.conflictGroupKeys.contains(conflictGroupKey)) {
                    updatableResults.add(solution);
                    iterator.remove();
                }
            }

            // Extend solutions with unseen INDs.
            if (updatableResults.isEmpty()) continue;
            for (UnaryInd conflictingInd : conflictingInds) {
                Set<ConflictGroupKey> newConflictGroupKeys = conflictGroupMembership.get(conflictingInd);
                SolutionLoop:
                for (ConflictGroupSolution updatableSolution : updatableResults) {

                    // Check that the existing solution and the conflicting IND have no overlapping conflict group.
                    for (ConflictGroupKey encountedConflictGroupKeys : updatableSolution.conflictGroupKeys) {
                        if (newConflictGroupKeys.contains(encountedConflictGroupKeys)) {
                            conflictGroupSolutions.add(updatableSolution);
                            continue SolutionLoop;
                        }
                    }

                    // Build a new solution from the existing solution and the conflicting IND.
                    conflictGroupSolutions.add(updatableSolution.extend(conflictingInd, newConflictGroupKeys));
                }
            }
        }

        // Union the solutions among the conflict groups with the conflict-free INDs to create the actual conflict-free
        // unary IND sets.
        Collection<Collection<UnaryInd>> conflictFreeUnaryIndSets = new ArrayList<>(conflictGroupSolutions.size());
        for (ConflictGroupSolution conflictGroupSolution : conflictGroupSolutions) {
            Collection<UnaryInd> conflictFreeUnaryIndSet =
                    new ArrayList<>(conflictFreeInds.size() + conflictGroupSolution.inds.size());
            conflictFreeUnaryIndSet.addAll(conflictFreeInds);
            conflictFreeUnaryIndSet.addAll(conflictGroupSolution.inds);
            conflictFreeUnaryIndSets.add(conflictFreeUnaryIndSet);
        }


        return conflictFreeUnaryIndSets;
    }


    /**
     * Computes the complement of an edge over the set of vertices.
     *
     * @param edge is the edge to be complemented
     * @return the complement of the edge
     */
    public NaryInd complement(NaryInd edge) {
        Set<UnaryInd> complementVertices = new HashSet<>();
        for (UnaryInd vertex : this.vertices) {
            if (!edge.getUnaryInds().contains(vertex)) {
                complementVertices.add(vertex);
            }
        }

        return new NaryInd(complementVertices);
    }

    /**
     * Computes the complement of all given edges over the set of vertices.
     *
     * @param edges are the edges to be complemented
     * @return the complement of all edges
     * @see #complement(NaryInd)
     */
    public List<NaryInd> complementAll(Collection<NaryInd> edges) {
        List<NaryInd> complements = new ArrayList<>(edges.size());
        for (NaryInd edge : edges) {
            complements.add(complement(edge));
        }
        return complements;
    }

    /**
     * Describes an (intermediate) solution for the maximum conflict-free group problem.
     */
    private static class ConflictGroupSolution {

        Set<UnaryInd> inds;
        Set<ConflictGroupKey> conflictGroupKeys;

        /**
         * Creates an empty solution (no INDs, no conflict groups).
         */
        public ConflictGroupSolution() {
            this.inds = new HashSet<>();
            this.conflictGroupKeys = new HashSet<>();
        }

        /**
         * Creates a new solution with given INDs and their conflict groups.
         *
         * @param inds              are the INDs in the solution
         * @param conflictGroupKeys are the conflict groups in which the IND should be exclusively
         */
        public ConflictGroupSolution(Set<UnaryInd> inds, Set<ConflictGroupKey> conflictGroupKeys) {
            this.inds = inds;
            this.conflictGroupKeys = conflictGroupKeys;
        }

        /**
         * Extends this solution with additional INDs.
         *
         * @param additionalInd               are the INDs to add
         * @param additionalConflictGroupKeys are the conflict groups of the new INDs
         * @return a new solution comprising the new INDs and conflict groups
         */
        ConflictGroupSolution extend(UnaryInd additionalInd, Collection<ConflictGroupKey> additionalConflictGroupKeys) {
            Set<UnaryInd> newInds = new HashSet<>(this.inds);
            newInds.add(additionalInd);
            Set<ConflictGroupKey> newConflictGroupKeys = new HashSet<>(this.conflictGroupKeys);
            newConflictGroupKeys.addAll(additionalConflictGroupKeys);
            return new ConflictGroupSolution(newInds, newConflictGroupKeys);
        }
    }

    public static class ConflictGroupKey {

        static final int DEP_CONFLICT = 0;

        static final int REF_CONFLICT = 1;

        static final int ANY_CONFLICT = 2;

        private final int conflictType;

        private final int attribute;

        ConflictGroupKey(int conflictType, int attribute) {
            this.conflictType = conflictType;
            this.attribute = attribute;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ConflictGroupKey that = (ConflictGroupKey) o;

            if (conflictType != that.conflictType) return false;
            return attribute == that.attribute;
        }

        @Override
        public int hashCode() {
            int result = conflictType;
            result = 31 * result + attribute;
            return result;
        }
    }

}
