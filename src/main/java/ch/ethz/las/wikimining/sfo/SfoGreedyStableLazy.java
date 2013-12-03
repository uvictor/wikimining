package ch.ethz.las.wikimining.sfo;

import ch.ethz.las.wikimining.functions.ObjectiveFunction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Object that implements a stable lazy greedy submodular function maximisation
 * (SFO) algorithm.
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class SfoGreedyStableLazy extends AbstractSfoGreedy {

  public SfoGreedyStableLazy(ObjectiveFunction theFunction) {
    super(theFunction);
  }

  /**
   * Runs the SFO greedy algorithm lazily with stable sort.
   *
   * @param n the total number of elements
   * @param k the number of ids to be selected
   *
   * @return the selected ids
   */
  @Override
  public Set<Integer> run(int n, int k) {
    final Set<Integer> selected = new LinkedHashSet<>();
    final ArrayList<ScoreId> bestIds = new ArrayList<>();
    double currentScore;

    for (int id = 0; id < n; ++id) {
        selected.add(id);
        final double score = function.compute(selected);
        selected.remove(id);

        bestIds.add(new ScoreId(score, id));
    }
    Collections.sort(bestIds, new Comparator<ScoreId>() {
      @Override
      public int compare(ScoreId o1, ScoreId o2) {
        return new Double(o1.getScore()).compareTo(o2.getScore());
      }
    });

    final ScoreId firstDoc = bestIds.remove(bestIds.size() - 1);
    selected.add(firstDoc.getId());
    currentScore = firstDoc.getScore();
    logger.info("First score: " + currentScore);

    for (int i = 1; i < k; ++i) {
      ScoreId currentBest;
      double score;
      int s;

      // Find the first (highest valued) document that remains at the peak after
      // we recompute its score.
      do {
        s = bestIds.remove(bestIds.size() - 1).getId();

        selected.add(s);
        score = function.compute(selected) - currentScore;
        selected.remove(s);

        bestIds.add(new ScoreId(score, s));
        Collections.sort(bestIds, new Comparator<ScoreId>() {
          @Override
          public int compare(ScoreId o1, ScoreId o2) {
            return new Double(o1.getScore()).compareTo(o2.getScore());
          }
        });

        currentBest = bestIds.get(bestIds.size() - 1);
      } while (currentBest.getId() != s && currentBest.getScore() != score);

      final ScoreId bestId = bestIds.remove(bestIds.size() - 1);
      selected.add(bestId.getId());
      currentScore += bestId.getScore();
    }
    logger.info("Final score: " + currentScore);

    return selected;
  }

}
