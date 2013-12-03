/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ch.ethz.las.wikimining.sfo;

import ch.ethz.las.wikimining.functions.ObjectiveFunction;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Object that implements a brute-force (not-lazy) greedy submodular function
 * maximisation (SFO) algorithm.
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class SfoGreedyNonLazy extends AbstractSfoGreedy {

  public SfoGreedyNonLazy(ObjectiveFunction theFunction) {
    super(theFunction);
  }

  /**
   * Runs the SFO greedy algorithm non-lazily.
   * <p>
   * @param n the total number of elements
   * @param k the number of ids to be selected
   * <p>
   * @return the selected ids
   */
  @Override
  public Set<Integer> run(int n, int k) {
    final Set<Integer> selected = new LinkedHashSet<>();

    for (int i = 0; i < k; ++i) {
      double maxScore = 0;
      int maxId = -1;

      for (int id = 0; id < n; ++id) {
        if (selected.contains((id))) {
          continue;
        }

        selected.add(id);
        final double score = function.compute(selected);
        selected.remove(id);

        if (score > maxScore) {
          maxScore = score;
          maxId = id;
        }
      }

      selected.add(maxId);
    }

    return selected;
  }
}
