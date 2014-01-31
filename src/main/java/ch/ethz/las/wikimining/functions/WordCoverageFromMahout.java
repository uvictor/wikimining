package ch.ethz.las.wikimining.functions;

import ch.ethz.las.wikimining.mr.base.DocumentWithVectorWritable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

/**
 * Computes the word coverage as equation (1) from the paper, from a Mahout
 * index.
 * <p>
 * TODO(uvictor): use Vector.aggregate to keep the max?
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class WordCoverageFromMahout extends AbstractWordCoverage {

  private final List<Integer> allDocIds;
  protected final Map<Integer, Vector> documents;

  /**
   * Creates an object used to compute the necessary word coverage score.
   */
  public WordCoverageFromMahout(
      Iterable<DocumentWithVectorWritable> theDocuments) {
    this(theDocuments.iterator());
  }

  public WordCoverageFromMahout(
      Iterator<DocumentWithVectorWritable> theDocuments) {
    allDocIds = new ArrayList<>();
    documents = new HashMap<>();

    while (theDocuments.hasNext()) {
      DocumentWithVectorWritable document = theDocuments.next();
      final Integer id = Integer.parseInt(document.getId().toString());
      final Vector tfIdfs = document.getVector().get();

      allDocIds.add(id);
      documents.put(id, tfIdfs);
    }
  }

  /**
   * Returns the list of all encountered doc ids.
   *
   * @return the list of all doc ids
   */
  public List<Integer> getAllDocIds() {
    return allDocIds;
  }

  @Override
  public double compute(Set<Integer> docIds) {
    assert docIds != null;
    if (docIds.isEmpty()) {
      return 0;
    }

    // Max tf-idfs for all terms from the allDocIds documents.
    final Map<Integer, Score> maxScores = new HashMap<>();
    for (Integer docId : docIds) {
      final Iterable<Element> terms = getTermsForDoc(docId);
      if (terms != null) {
        computeMaxScoresForDoc(terms, -1, maxScores);
      }
    }

    double sum = 0;
    for (Score score : maxScores.values()) {
      sum += score.getWordWeight() * score.getMaxTfIdf();
    }

    return sum;
  }

  protected Iterable<Element> getTermsForDoc(int docId) {
    return documents.get(docId).nonZeroes();
  }

  protected void computeMaxScoresForDoc(
      Iterable<Element> terms, int date, Map<Integer, Score> maxScores) {
    for (Element term : terms) {
      final Integer word = term.index();
      final double tfIdf = term.get();

      final Score score = maxScores.get(word);
      if (score == null) {
        maxScores
            .put(word, new Score(computeWordWeight(term, date), tfIdf));
      } else if (tfIdf > score.getMaxTfIdf()) {
        maxScores
            .put(word, new Score(score.getWordWeight(), tfIdf));
      }
    }
  }

  // TODO(uvictor): consider using a non-constant word weight - all reducers
  // load all global term frequencies.
  protected double computeWordWeight(Element term, int date) {
    return 1;
  }
}
