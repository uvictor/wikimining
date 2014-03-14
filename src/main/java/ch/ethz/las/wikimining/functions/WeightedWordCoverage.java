package ch.ethz.las.wikimining.functions;

import ch.ethz.las.wikimining.mr.base.DocumentWithVectorWritable;
import java.util.Iterator;
import java.util.Map;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

/**
 * Computes the word coverage as equation (1) from the paper, from a Mahout
 * index.
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class WeightedWordCoverage extends WordCoverageFromMahout {

  // TODO(uvictor): change this to an ArrayList
  private final Map<Integer, Long> wordCount;
  private final double totalCount;

  public WeightedWordCoverage(Map<Integer, Long> theWordCount,
      Iterator<DocumentWithVectorWritable> theDocuments) {
    super(theDocuments);

    wordCount = theWordCount;
    wordCount.remove(-1);

    double tempTotal = 0;
    for (final Long value: wordCount.values()) {
      tempTotal += concaveFunction(value);
    }
    totalCount = tempTotal;
  }

  @Override
  protected double computeScore(Vector maxScores) {
    double score = 0;

    for (final Element element : maxScores.nonZeroes()) {
      final double wordWeight = concaveFunction(wordCount.get(element.index()));
      score += wordWeight * element.get();
    }

    return score / totalCount;
  }

  private double concaveFunction(double value) {
    return Math.log10(value);
  }
}
