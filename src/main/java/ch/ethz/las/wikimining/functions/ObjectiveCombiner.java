
package ch.ethz.las.wikimining.functions;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Linearly combines multiple objective functions.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class ObjectiveCombiner implements ObjectiveFunction {

  private final List<Double> weights;
  private final List<ObjectiveFunction> functions;

  public ObjectiveCombiner(
      List<Double> theWeights, ObjectiveFunction... theFunctions) {
    assert theWeights.size() == theFunctions.length;

    weights = theWeights;
    functions = Arrays.asList(theFunctions);
    System.out.println(functions.get(0) + " " + functions.get(1));
  }

  @Override
  public double compute(Set<Integer> set) {
    double sum = 0;
    Iterator<Double> weight = weights.iterator();
    for (ObjectiveFunction function : functions) {
      sum += weight.next() * function.compute(set);
    }

    return sum;
  }
}
