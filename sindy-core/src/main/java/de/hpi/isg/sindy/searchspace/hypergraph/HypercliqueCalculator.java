package de.hpi.isg.sindy.searchspace.hypergraph;

import de.hpi.isg.sindy.util.IND;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.*;

/**
 * Finds hypercliques in a hypergraph.
 * <p>
 * Created by basti on 8/18/15.
 */
public class HypercliqueCalculator {

    private static final Logger LOGGER = LoggerFactory.getLogger(HypercliqueCalculator.class);

    /**
     * The arity of the edges/n-ary INDs in the hypergraph.
     */
    private final int curArity;

    /**
     * A set of n-ary INDs corresponds to the edges of the conceptual hypergraph.
     */
    private List<NaryInd> edges = new ArrayList<>();

    /**
     * The set of IND corresponds to the vertices in the conceptual hypergraph.
     */
    private Collection<UnaryInd> vertices = new ArrayList<>();

    /**
     * Creates a new hyperclique calculator for a hypergraph with the positive IND border as edges and the unary
     * INDs as vertices.
     *
     * @param positiveBorder is the positive IND border
     * @param curArity       is the arity of the largest (pessimistic) INDs
     */
    public HypercliqueCalculator(Collection<IND> positiveBorder, int curArity) {
        for (IND ind : positiveBorder) {
            if (ind.getArity() == curArity) {
                this.edges.add(new NaryInd(ind));
            }
        }
        Set<UnaryInd> vertexSet = new HashSet<>();
        collectVertices(this.edges, vertexSet);
        this.vertices.addAll(vertexSet);
        this.curArity = curArity;
    }

    /**
     * Collects all vertices in the provided edges.
     *
     * @param edges     that contain the vertices
     * @param collector collects the vertices
     */
    private void collectVertices(Collection<NaryInd> edges, Set<UnaryInd> collector) {
        for (NaryInd edge : edges) {
            for (UnaryInd unaryInd : edge.getUnaryInds()) {
                collector.add(unaryInd);
            }
        }
    }

    /**
     * Collects all edges in the subgraph induces by the given vertices.
     *
     * @param vertices that induce the subgraph
     * @param edges    of the supergraph
     * @return collects the edges
     */
    private Set<NaryInd> collectSubgraphEdges(Collection<UnaryInd> vertices, Collection<NaryInd> edges) {
        Set<NaryInd> subgraphEdges = new HashSet<>();
        for (NaryInd edge : edges) {
            if (vertices.containsAll(edge.getUnaryInds())) {
                subgraphEdges.add(edge);
            }
        }
        return subgraphEdges;
    }

    /**
     * Finds the hypercliques in the hypergraph.
     *
     * @return the hypercliques
     */
    public Collection<Set<UnaryInd>> findHypercliques() {
        Collection<Set<UnaryInd>> cliques = new LinkedList<>();
        Set<NaryInd> edges = new HashSet<>(this.edges);
        Set<UnaryInd> vertices = new HashSet<>();
        collectVertices(edges, vertices);
        findHypercliques(cliques, vertices, edges);
        return cliques;
    }

    private void findHypercliques(Collection<Set<UnaryInd>> collector, Set<UnaryInd> vertices, Set<NaryInd> edges) {
        // Check that we are not given an empty hypergraph.
        if (edges.isEmpty()) {
            return;
        }

        boolean isFoundClique;

        LOGGER.info("Looking for hypercliques");
        LOGGER.info("{} vertices: {}", vertices.size(), vertices);
        LOGGER.info("{} edges: {}", edges.size(), edges);
        do {
            NaryInd partitionEdge = null;
            Set<UnaryInd> partitionEdgeCliqueCandidate = null;
            isFoundClique = false;
            Set<NaryInd> multiCliqueEdges = new HashSet<>();
            for (NaryInd edge : edges) {
                Set<UnaryInd> cliqueCandidate = generateCliqueCandidate(edge, vertices, edges);
                boolean isClique = isClique(cliqueCandidate, edges);
                if (isClique) {
                    boolean isNewClique = true;
                    for (Set<UnaryInd> existingClique : collector) {
                        isNewClique = !existingClique.containsAll(cliqueCandidate);
                        if (!isNewClique) {
                            break;
                        }
                    }
                    if (isNewClique) {
                        collector.add(cliqueCandidate);
                        LOGGER.info("Found maximal clique {}", cliqueCandidate);
                    }
                    isFoundClique = true;
                } else {
                    // The edge is not in its heuristic, maximum clique and must be in multiple smaller cliques.
                    multiCliqueEdges.add(edge);
                }

                // Enhancement: we keep track of potential partition cliques and prefer non-clique edges
                if (cliqueCandidate.size() < vertices.size()) {
                    if (!isClique || partitionEdge == null) {
                        partitionEdge = edge;
                        partitionEdgeCliqueCandidate = cliqueCandidate;
                    }
                }
            }

            if (!isFoundClique) {
                if (partitionEdge == null) {
                    LOGGER.error("No partition edge for {} and {}.", vertices, edges);
                }
                Set<NaryInd> subgraph1Edges = collectSubgraphEdges(partitionEdgeCliqueCandidate, edges);
                findHypercliques(collector, partitionEdgeCliqueCandidate, subgraph1Edges);

                Set<NaryInd> subgraph2Edges = new HashSet<>(edges);
                subgraph2Edges.remove(partitionEdge);
                Set<UnaryInd> subgraph2Vertices = new HashSet<>();
                collectVertices(subgraph2Edges, subgraph2Vertices);
                findHypercliques(collector, subgraph2Vertices, subgraph2Edges);

            } else {
                edges = multiCliqueEdges;
                vertices.clear();
                collectVertices(edges, vertices);
            }


        } while (isFoundClique && !edges.isEmpty());

    }

    /**
     * Tells if the given set of nodes forms a clique within the hypergraph.
     *
     * @param cliqueCandidate are vertices that might form a clique
     * @return whether the vertices really do form a clique
     */
    private boolean isClique(Set<UnaryInd> cliqueCandidate, Collection<NaryInd> edges) {
        // Calculate the necessary vertex degree for a clique and determine the actual vertex degrees in the clique
        // candidate subgraph.
        int cliqueVertexDegree = calculateCliqueVertexDegree(cliqueCandidate.size());
        Object2IntMap<UnaryInd> vertexDegrees = determineVertexDegrees(cliqueCandidate, edges);

        // Compare the required and actual degree to tell if we have a clique.
        for (UnaryInd vertex : cliqueCandidate) {
            if (vertexDegrees.getInt(vertex) != cliqueVertexDegree) {
                return false;
            }
        }
        return true;
    }


    /**
     * Calculates the degree of vertices in a clique subgraph of a given size.
     *
     * @param cliqueSize is the size of the clique subgraph
     * @return the degree of the vertices in a clique subgraph
     */
    private int calculateCliqueVertexDegree(int cliqueSize) {
        // We need to calculate binomialCoefficient(this.curArity-1, cliqueSize-1).
        // This is (this.curArity-1)! / ((this.curArity-cliqueSize)! * (cliqueSize-1)!)
        if (this.curArity == cliqueSize) {
            return 1;
        }

        // Initialize parameters.
        int n = cliqueSize - 1;
        int k = this.curArity - 1;
        k = Math.max(k, n - k); // Optimization.

        // Calculate n * (n-1) * ... * (k+1), i.e., n!/k!.
        BigInteger accu = BigInteger.valueOf(n);
        for (int i = n - 1; i > k; i--) {
            accu = accu.multiply(BigInteger.valueOf(i));
        }

        // Calculate the .../(n-k)!
        for (int i = n - k; i > 1; i--) {
            accu = accu.divide(BigInteger.valueOf(i));
        }

        return accu.intValue();
    }

    /**
     * Generates clique candidates based on a given edge. This method uses a greedy criterion:
     * <ul>
     * <li>the smallest candidate are all vertices within the edge</li>
     * <li>any vertex that can be arbitrarily exchanged with the edge vertices and yields a valid edge also goes
     * into the candidate</li>
     * </ul>
     *
     * @param baseEdge the edge for that the clique candidate should be generated
     * @param vertices all the vertices in the hypergraph
     * @param edges    all the edges in the hypergraph
     * @return the clique candidate
     */
    private Set<UnaryInd> generateCliqueCandidate(NaryInd baseEdge, Set<UnaryInd> vertices, Set<NaryInd> edges) {
        // Start with the nodes of an edge as clique candidate.
        Set<UnaryInd> cliqueCandidate = new HashSet<>(baseEdge.getUnaryInds());

        NaryInd testEdge = new NaryInd(new HashSet<>(cliqueCandidate));

        for (UnaryInd uncontainedUind : vertices) {
            // Make sure that the unary IND is not contained in the clique candidate already.
            if (cliqueCandidate.contains(uncontainedUind)) {
                continue;
            }

            // Check if we can replace any vertex in the base edge with this new IND and come up with existing nodes.
            // Note that this does not confirm the clique property yet!
            boolean isTestEdgeInGraph = true;
            testEdge.getUnaryInds().add(uncontainedUind);
            for (UnaryInd baseEdgeVertex : baseEdge.getUnaryInds()) {
                testEdge.getUnaryInds().remove(baseEdgeVertex);
                isTestEdgeInGraph = edges.contains(testEdge);
                testEdge.getUnaryInds().add(baseEdgeVertex);

                if (!isTestEdgeInGraph) break;
            }
            testEdge.getUnaryInds().remove(uncontainedUind);

            // If the uncontained unary IND holds the candidate criterion, we add it to the clique.
            if (isTestEdgeInGraph) cliqueCandidate.add(uncontainedUind);
        }

        return cliqueCandidate;
    }

    /**
     * Determines the degrees of the vertices in the given subgraph. This excludes edges that are not contained by
     * the subgraph.
     *
     * @param subgraphVertices are the vertices that induce the subgraph
     * @param edges            are all edges of the graph
     * @return a mapping from the subgraph vertices to their degree within the subgraph
     */
    private Object2IntMap<UnaryInd> determineVertexDegrees(Set<UnaryInd> subgraphVertices, Collection<NaryInd> edges) {
        Object2IntOpenHashMap<UnaryInd> counter = new Object2IntOpenHashMap<>(subgraphVertices.size());
        for (NaryInd edge : edges) {
            if (subgraphVertices.containsAll(edge.getUnaryInds())) {
                for (UnaryInd unaryInd : edge.getUnaryInds()) {
                    counter.addTo(unaryInd, 1);
                }
            }
        }
        return counter;
    }

}
