package ch.ethz.las.wikimining.functions;

import java.util.Set;

/**
 * Object that computes the objective function for a submodular function
 * maximisation (SFO) algorithm.
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public interface ObjectiveFunction {

  /**
   * Computes the objective function value, given a set of indices.
   * <p>
   * @param set the set of indices
   * <p>
   * @return the computed value
   */
  public abstract double compute(Set<Integer> set);
}
