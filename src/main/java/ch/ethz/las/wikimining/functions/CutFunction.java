package ch.ethz.las.wikimining.functions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Computes the maximum cut value given the graph split.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class CutFunction implements ObjectiveFunction {
  private final ArrayList<ArrayList<Integer>> graph;

  public CutFunction(ArrayList<ArrayList<Integer>> theGraph) {
    this.graph = theGraph;
  }

  @Override
  public double compute(Set<Integer> set) {
    Set<Integer> antiSet = new HashSet<>();
    for (int i = 0; i < graph.size(); ++i) {
      if (!set.contains(i)) {
        antiSet.add(i);
      }
    }

    long score = 0;
    for (Integer s : set) {
      for (Integer t : antiSet) {
        if (graph.get(s).get(t) > 0 || graph.get(t).get(s) > 0) {
          score++;
        }
      }
    }

    return score;
  }
}
