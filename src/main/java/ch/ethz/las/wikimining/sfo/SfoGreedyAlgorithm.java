/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ch.ethz.las.wikimining.sfo;

import java.util.Collection;
import java.util.Set;

/**
 * Object that offers a greedy submodular function maximisation (SFO) procedure.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public interface SfoGreedyAlgorithm {
  /**
   * Runs a SFO greedy algorithm.
   *
   * @param n the total number of elements
   * @param k the number of ids to be selected
   *
   * @return the selected ids
   */
  public abstract Set<Integer> run(int n, int k);

  /**
   * Runs a SFO greedy algorithm.
   *
   * @param ids the elements from which to select a subset
   * @param k the number of ids to be selected
   *
   * @return the selected ids
   */
  public abstract Set<Integer> run(Collection<Integer> ids, int k);
}
