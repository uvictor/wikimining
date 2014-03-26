package ch.ethz.las.wikimining.sfo;

import ch.ethz.las.wikimining.functions.ObjectiveFunction;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import org.apache.hadoop.mapred.Reporter;

/**
 * Object that implements a lazy greedy submodular function maximisation (SFO)
 * algorithm.
 * <p>
 * Note: this implementation uses a non-stable sorting mechanism - ie. a heap.
 * <p>
 * TODO(uvictor): Reset bestIds in all SFO classes.
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class SfoGreedyLazy extends AbstractSfoGreedy {

  private static enum ProcessedArticles {
    INITIAL_CURRENT, INITIAL_TOTAL, BEST_CURRENT, BEST_TOTAL
  }

  private final PriorityBlockingQueue<ScoreId> bestIds;
  private final Reporter reporter;

  public SfoGreedyLazy(ObjectiveFunction theFunction) {
    this(theFunction, null);
  }

  public SfoGreedyLazy(ObjectiveFunction theFunction, Reporter theReporter) {
    super(theFunction);

    bestIds = new PriorityBlockingQueue<>();
    reporter = theReporter;
  }

  @Override
  protected void computeInitialBestIds(int n) {
    setValueCounter(ProcessedArticles.INITIAL_TOTAL, n);
    for (int id = 0; id < n; ++id) {
      scoreId(id);
    }
  }

  @Override
  protected void computeInitialBestIds(Collection<Integer> ids) {
    setValueCounter(ProcessedArticles.INITIAL_TOTAL, ids.size());
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
    incrementCounter(ProcessedArticles.INITIAL_CURRENT);
  }

  @Override
  protected Set<Integer> retrieveBestIds(int k) {
    setValueCounter(ProcessedArticles.BEST_TOTAL, k);

    final Set<Integer> selected = new LinkedHashSet<>();
    final ScoreId firstDoc = bestIds.poll();
    selected.add(firstDoc.getId());
    double currentScore = firstDoc.getScore();
    incrementCounter(ProcessedArticles.BEST_CURRENT);

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
      } while (currentBest.getId() != id && currentBest.getScore() != score);

      final ScoreId bestId = bestIds.poll();
      selected.add(bestId.getId());
      currentScore += bestId.getScore();
      incrementCounter(ProcessedArticles.BEST_CURRENT);
    }

    return selected;
  }

  private void incrementCounter(Enum<?> name) {
    if (reporter != null) {
      reporter.getCounter(name).increment(1);
    }
  }

  private void setValueCounter(Enum<?> name, long value) {
    if (reporter != null) {
      reporter.getCounter(name).setValue(value);
    }
  }
}
