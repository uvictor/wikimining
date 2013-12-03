package ch.ethz.las.wikimining.sfo;

import ch.ethz.las.wikimining.functions.ObjectiveFunction;
import java.util.LinkedHashSet;
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

  public SfoGreedyLazy(ObjectiveFunction theFunction) {
    super(theFunction);
  }

  /**
   * Runs the SFO greedy algorithm lazily.
   * <p>
   * Note: this implementation uses a non-stable sorting mechanism - ie. a heap.
   * <p>
   * Important: if this function changes the index, docIds might be
   * inconsistent!
   * <p>
   * TODO(uvictor): make this method use multiple threads.
   * <p>
   * TODO(uvictor): make this method easier to understand.
   * <p>
   * TODO(uvictor): check the difference between removing the document from A
   * and not removing it. If this function will change the index. Change the API
   * to mention this to the caller.
   * <p>
   * @param n the total number of elements
   * @param k the number of ids to be selected
   * <p>
   * @return the selected ids
   */
  @Override
  public Set<Integer> run(int n, int k) {
    final Set<Integer> selected = new LinkedHashSet<>();
    final PriorityBlockingQueue<ScoreId> bestIds
        = new PriorityBlockingQueue<>();
    double currentScore;

    logger.info("Scoring " + n + " elements.");
    for (int id = 0; id < n; ++id) {
      selected.add(id);
      final double score = function.compute(selected);
      selected.remove(id);

      bestIds.add(new ScoreId(score, id));

      if ((id + 1) % 1000 == 0) {
        logger.info("Indexed pages: " + id);
      }
    }

    final ScoreId firstDoc = bestIds.poll();
    selected.add(firstDoc.getId());
    currentScore = firstDoc.getScore();

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
