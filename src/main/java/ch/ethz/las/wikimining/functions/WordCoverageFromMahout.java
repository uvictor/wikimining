package ch.ethz.las.wikimining.functions;

import ch.ethz.las.wikimining.mr.DocumentWithVectorWritable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

/**
 * Computes the word coverage as equation (1) from the paper, from a Mahout
 * index.
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class WordCoverageFromMahout extends AbstractWordCoverage {

  final List<Integer> allDocIds;
  final Map<Integer, Vector> documents;

  /**
   * Creates an object used to compute the necessary word coverage score.
   */
  public WordCoverageFromMahout(
      Iterable<DocumentWithVectorWritable> theDocuments) {
    allDocIds = new ArrayList<>();
    documents = new HashMap<>();

    for (DocumentWithVectorWritable document : theDocuments) {
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
        computeMaxScoresForDoc(terms, maxScores);
      }
    }

    double sum = 0;
    for (Score score : maxScores.values()) {
      sum += score.getWordWeight() * score.getMaxTfIdf();
    }

    return sum;
  }

  private Iterable<Element> getTermsForDoc(int docId) {
    return documents.get(docId).all();
  }

  private void computeMaxScoresForDoc(
      Iterable<Element> terms, Map<Integer, Score> maxScores) {
    for (Element term : terms) {
      final Integer word = term.index();
      final double tfIdf = term.get();

      final Score score = maxScores.get(word);
      if (score == null) {
        maxScores
            .put(word, new Score(computeWordWeight(term), tfIdf));
      } else if (tfIdf > score.getMaxTfIdf()) {
        maxScores
            .put(word, new Score(score.getWordWeight(), tfIdf));
      }
    }
  }

  // TODO(uvictor): consider using a non-constant word weight. All reducers load
  // all global term frequencies.
  private double computeWordWeight(Element term) {
    return 1;
  }
}
