
package ch.ethz.las.wikimining.functions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.log4j.Logger;

/**
 * Computes the graph coverage submodular function.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class GraphCoverage implements ObjectiveFunction {

  private final Logger logger;
  private final HashMap<Integer, ArrayList<Integer>> graph;
  private final int totalCount;

  /**
   * Creates an object used to compute the necessary graph coverage score.
   */
  public GraphCoverage(HashMap<Integer, ArrayList<Integer>> theGraph) {
    logger = Logger.getLogger(this.getClass());
    graph = theGraph;

    final HashSet<Integer> totalCoverage = new HashSet<>();
    for (List<Integer> current : graph.values()) {
      totalCoverage.addAll(current);
    }
    totalCount = totalCoverage.size();
  }

  @Override
  public double compute(Set<Integer> docIds) {
    assert docIds != null;
    if (docIds.isEmpty()) {
      return 0;
    }

    final HashSet<Integer> coverage = new HashSet<>();
    for (Integer docId : docIds) {
      coverage.add(docId);
      final List<Integer> current = graph.get(docId);
      if (current != null) {
        coverage.addAll(graph.get(docId));
      } else {
        logger.info("No inlinks for docid " + docId);
      }
    }

    return (double) coverage.size() / totalCount;
  }
}
