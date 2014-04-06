/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ch.ethz.las.wikimining.sfo;

import ch.ethz.las.wikimining.functions.ObjectiveFunction;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Object that implements a brute-force (non-lazy) greedy submodular function
 * maximisation (SFO) algorithm.
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class SfoGreedyNonLazy implements SfoGreedyAlgorithm {

  private final ObjectiveFunction function;
  private final Set<Integer> selected = new LinkedHashSet<>();
  private double maxScore;
  private int maxId;

  /**
   * Creates an object used to run the SFO algorithm non-lazily.
   *
   * @param theFunction a submodular objective function used to evaluate the
   *        subsets score
   */
  public SfoGreedyNonLazy(ObjectiveFunction theFunction) {
    function = theFunction;
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
    for (int i = 0; i < k; ++i) {
      maxScore = 0;
      maxId = -1;

      for (int id = 0; id < n; ++id) {
        scoreId(id);
      }

      selected.add(maxId);
    }

    return selected;
  }

  /**
   * Runs the SFO greedy algorithm non-lazily.
   * <p>
   * @param k the number of ids to be selected
   * <p>
   * @return the selected ids
   */
  @Override
  public Set<Integer> run(Collection<Integer> ids, int k) {
    for (int i = 0; i < k; ++i) {
      maxScore = 0;
      maxId = -1;

      for (int id : ids) {
        scoreId(id);
      }

      selected.add(maxId);
    }

    return selected;
  }

  private void scoreId(int id) {
    if (selected.contains((id))) {
      return;
    }

    selected.add(id);
    final double score = function.compute(selected);
    selected.remove(id);

    if (score > maxScore) {
      maxScore = score;
      maxId = id;
    }
  }
}
