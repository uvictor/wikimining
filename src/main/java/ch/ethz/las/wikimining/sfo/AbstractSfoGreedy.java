
package ch.ethz.las.wikimining.sfo;

import ch.ethz.las.wikimining.functions.ObjectiveFunction;
import java.util.List;
import java.util.Set;
import org.apache.log4j.Logger;

/**
 * Object that implements a greedy submodular function maximisation (SFO)
 * procedure.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public abstract class AbstractSfoGreedy implements SfoGreedyAlgorithm {
  protected static class ScoreId implements Comparable<ScoreId> {
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

  protected final Logger logger;
  protected final ObjectiveFunction function;

 /**
   * Creates an object used to run the SFO algorithm.
   *
   * @param theFunction a submodular objective function used to evaluate the
   *        subsets score
   */
  public AbstractSfoGreedy(ObjectiveFunction theFunction) {
    function = theFunction;

    logger = Logger.getLogger(this.getClass());
  }

  @Override
  public Set<Integer> run(int n, int k) {
    logger.info("Scoring " + n + " elements.");
    computeInitialBestIds(n);

    return retrieveBestIds(k);
  }

  @Override
  public Set<Integer> run(List<Integer> ids, int k) {
    logger.info("Scoring " + ids.size() + " elements.");
    computeInitialBestIds(ids);

    return retrieveBestIds(k);
  }

  protected abstract void computeInitialBestIds(int n);

  protected abstract void computeInitialBestIds(List<Integer> ids);

  protected abstract Set<Integer> retrieveBestIds(int k);
}
