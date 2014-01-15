package ch.ethz.las.wikimining.sfo;

import ch.ethz.las.wikimining.functions.ObjectiveFunction;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Object that implements a lazy greedy submodular function maximisation (SFO)
 * algorithm.
 * <p>
 * Note: this implementation uses a non-stable sorting mechanism - ie. a heap.
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class SfoGreedyLazy extends AbstractSfoGreedy {

  private final PriorityBlockingQueue<ScoreId> bestIds;

  public SfoGreedyLazy(ObjectiveFunction theFunction) {
    super(theFunction);

    bestIds = new PriorityBlockingQueue<>();
  }

  @Override
  protected void computeInitialBestIds(int n) {
    for (int id = 0; id < n; ++id) {
      scoreId(id);
    }
  }

  @Override
  protected void computeInitialBestIds(List<Integer> ids) {
    for (int id : ids) {
      scoreId(id);
    }
  }

  private void scoreId(int id) {
    final Set<Integer> selected = new LinkedHashSet<>();
    selected.add(id);
    final double score = function.compute(selected);
    selected.remove(id);

    bestIds.add(new ScoreId(score, id));
  }

  @Override
  protected Set<Integer> retrieveBestIds(int k) {
    final Set<Integer> selected = new LinkedHashSet<>();
    final ScoreId firstDoc = bestIds.poll();
    selected.add(firstDoc.getId());
    double currentScore = firstDoc.getScore();

    for (int i = 1; i < k; ++i) {
      ScoreId currentBest;
      double score;
      int id;

      // Find the first (highest valued) document that remains at the peak after
      // we recompute its score.
      do {
        id = bestIds.poll().getId();

        selected.add(id);
        score = function.compute(selected) - currentScore;
        selected.remove(id);

        bestIds.add(new ScoreId(score, id));
        currentBest = bestIds.peek();
      } while (currentBest.getId() != id
          && currentBest.getScore() != score);

      final ScoreId bestId = bestIds.poll();
      selected.add(bestId.getId());
      currentScore += bestId.getScore();
    }

    return selected;
  }
}
