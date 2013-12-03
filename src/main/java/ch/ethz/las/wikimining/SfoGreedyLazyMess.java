package ch.ethz.las.wikimining;

import ch.ethz.las.wikimining.functions.ObjectiveFunction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import org.apache.log4j.Logger;

/**
 * Object that implements a greedy submodular function maximisation (SFO)
 * algorithm using lazy evaluations.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class SfoGreedyLazyMess {

  private class ScoreId implements Comparable<ScoreId> {
    private final double score;
    private final int docId;

    public ScoreId(double score, int docId) {
      this.score = score;
      this.docId = docId;
    }

    public double getScore() {
      return score;
    }

    public int getId() {
      return docId;
    }

    @Override
    public int compareTo(ScoreId o) {
      return new Double(o.score).compareTo(score);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof ScoreId)) {
        return false;
      }

      ScoreId o = (ScoreId) obj;
      return score == o.score;
    }

    @Override
    public int hashCode() {
      int hash = 3;
      hash = 97 * hash + (int)(Double.doubleToLongBits(this.score)
          ^ (Double.doubleToLongBits(this.score) >>> 32));
      return hash;
    }
  }

  private final Logger logger;
  private final ObjectiveFunction function;

  /**
   * Creates an object used to run the SFO algorithm.
   *
   * @param theFunction a submodular objective function used to evaluate the
   *        subsets score
   */
  public SfoGreedyLazyMess(ObjectiveFunction theFunction) {
    function = theFunction;

    logger = Logger.getLogger(this.getClass());
  }

  /**
   * Runs the SFO greedy algorithm lazily.
   *
   * Important: if this function changes the index, docIds might be
   * inconsistent!
   *
   * TODO(uvictor): make this method use multiple threads.
   * TODO(uvictor): make this method easier to understand.
   * TODO(uvictor): check the difference between removing the document from A
   * and not removing it. If this function will change the index. Change the API
   * to mention this to the caller.
   *
   * @param n the total number of elements
   * @param k the number of ids to be selected
   *
   * @return the selected ids
   */
  public Set<Integer> run(int n, int k) {
    final Set<Integer> selected = new LinkedHashSet<>();
    final PriorityBlockingQueue<ScoreId> bestIds =
        new PriorityBlockingQueue<>();
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

  /**
   * Runs the SFO greedy algorithm non-lazily.
   *
   * @param n the total number of elements
   * @param k the number of ids to be selected
   *
   * @return the selected ids
   */
  public Set<Integer> runNonLazily(int n, int k) {
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

  /**
   * Runs the SFO greedy algorithm with stable sort.
   *
   * @param n the total number of elements
   * @param k the number of ids to be selected
   *
   * @return the selected ids
   */
  public Set<Integer> runStable(int n, int k) {
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
        return new Double(o1.score).compareTo(o2.score);
      }
    });

    final ScoreId firstDoc = bestIds.remove(bestIds.size() - 1);
    selected.add(firstDoc.getId());
    currentScore = firstDoc.getScore();
    //logger.info("First score: " + currentScore);

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
            return new Double(o1.score).compareTo(o2.score);
          }
        });

        currentBest = bestIds.get(bestIds.size() - 1);
      } while (currentBest.getId() != s && currentBest.getScore() != score);

      final ScoreId bestId = bestIds.remove(bestIds.size() - 1);
      selected.add(bestId.getId());
      currentScore += bestId.getScore();
    }
    //logger.info("Final score: " + currentScore);

    return selected;
  }
}
