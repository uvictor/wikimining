package ch.ethz.las.wikimining.sfo;

import ch.ethz.las.wikimining.functions.ObjectiveFunction;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Object that implements a lazy greedy submodular function maximisation (SFO)
 * algorithm using multiple threads for the initial computations.
 * <p>
 * Note: this implementation uses a non-stable sorting mechanism - ie. a heap.
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class SfoGreedyLazyThreaded extends SfoGreedyLazy {

  public SfoGreedyLazyThreaded(ObjectiveFunction theFunction) {
    super(theFunction);
  }

  @Override
  protected void computeInitialBestIds(int n) {
    for (int id = 0; id < n; ++id) {
      final Set<Integer> selected = new LinkedHashSet<>();
      selected.add(id);
      final double score = function.compute(selected);
      selected.remove(id);

      bestIds.add(new ScoreId(score, id));

      if ((id + 1) % 1000 == 0) {
        logger.info("Indexed pages: " + id);
      }
    }
  }
}
