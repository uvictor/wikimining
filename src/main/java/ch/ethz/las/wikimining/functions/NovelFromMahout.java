
package ch.ethz.las.wikimining.functions;

import ch.ethz.las.wikimining.mr.base.DocumentWithVectorWritable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.mahout.math.Vector;

/**
 * Computes the influence of a document as equation (5) from the paper,
 * from a novelty document index and a yearly word spread index
 * <p>
 * TODO(uvictor): use Vector.aggregate to keep the max?
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class NovelFromMahout extends WordCoverageFromMahout {

  private final HashMap<Integer, Integer> docDates;
  private final HashMap<Integer, Vector> wordSpread;

  public NovelFromMahout(Iterable<DocumentWithVectorWritable> theDocuments,
      HashMap<Integer, Integer> theDocDates,
      HashMap<Integer, Vector> theWordSpread) {
    super(theDocuments);
    docDates = theDocDates;
    wordSpread = theWordSpread;
  }

  public NovelFromMahout(Iterator<DocumentWithVectorWritable> theDocuments,
      HashMap<Integer, Integer> theDocDates,
      HashMap<Integer, Vector> theWordSpread) {
    super(theDocuments);
    docDates = theDocDates;
    wordSpread = theWordSpread;
  }

  @Override
  public double compute(Set<Integer> docIds) {
    assert docIds != null;
    if (docIds.isEmpty()) {
      return 0;
    }

    // TODO(uvictor): consider making the docIds Set a List
    final ArrayList<Integer> docList = sortDocsByDate(docIds);

    // Max tf-idfs of all terms from the allDocIds documents for each year.
    double sum = 0;
    final Map<Integer, Score> maxScores = new HashMap<>();
    for (Integer docId : docList) {
      final Iterable<Vector.Element> terms = getTermsForDoc(docId);
      if (terms != null) {
        computeMaxScoresForDoc(terms, docDates.get(docId), maxScores);
      }

      for (Score score : maxScores.values()) {
        sum += score.getWordWeight() * score.getMaxTfIdf();
      }
    }

    return sum;
  }

  /**
   * Retrieves the word spread.
   * @param term the word
   * <p>
   * @return the word spread score
   */
  @Override
  protected double computeWordWeight(Vector.Element term, int date) {
    return wordSpread.get(date).getQuick(term.index());
  }

  private ArrayList<Integer> sortDocsByDate(Set<Integer> docIds) {
    final ArrayList<Integer> docList = new ArrayList<>(docIds);
    Collections.sort(docList, new Comparator<Integer>() {
      @Override
      public int compare(Integer o1, Integer o2) {
        return docDates.get(o1) - docDates.get(o2);
      }
    });

    return docList;
  }
}
