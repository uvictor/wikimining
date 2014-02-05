package ch.ethz.las.wikimining.functions;

import java.util.Set;
import org.apache.log4j.Logger;

/**
 * Provides the structure for computing the word coverage as equation (1) from
 * the paper, given a way to retrieve the tf-idf scores.
 * <p>
 * @deprecated not used
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public abstract class AbstractWordCoverage implements ObjectiveFunction {

  protected static class Score {

    double wordWeight;
    double maxTfIdf;

    public Score(double tf, double idf) {
      this.wordWeight = tf;
      this.maxTfIdf = idf;
    }

    public double getWordWeight() {
      return wordWeight;
    }

    public double getMaxTfIdf() {
      return maxTfIdf;
    }
  }

  protected final Logger logger;

  /**
   * Creates an object used to compute the necessary word coverage score.
   */
  public AbstractWordCoverage() {
    logger = Logger.getLogger(this.getClass());
  }

  @Override
  public abstract double compute(Set<Integer> set);
}
