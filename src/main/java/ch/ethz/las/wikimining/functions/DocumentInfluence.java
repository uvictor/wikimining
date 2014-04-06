
package ch.ethz.las.wikimining.functions;

import ch.ethz.las.wikimining.base.DocumentWithVectorWritable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.Functions;

/**
 * Computes the influence of a document as equation (5) from the paper,
 * from a novelty document index and a yearly word spread index.
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class DocumentInfluence extends WordCoverageFromMahout {

  private final HashMap<Integer, Integer> docDates;
  private final HashMap<Integer, Vector> wordSpread;

  private ArrayList<Integer> docList;
  private Vector maxScores;

  public DocumentInfluence(Iterable<DocumentWithVectorWritable> theDocuments,
      HashMap<Integer, Integer> theDocDates,
      HashMap<Integer, Vector> theWordSpread) {
    super(theDocuments);
    docDates = theDocDates;
    wordSpread = theWordSpread;
  }

  public DocumentInfluence(Iterator<DocumentWithVectorWritable> theDocuments,
      HashMap<Integer, Integer> theDocDates,
      HashMap<Integer, Vector> theWordSpread) {
    super(theDocuments);
    docDates = theDocDates;
    wordSpread = theWordSpread;

    assert !wordSpread.isEmpty();
  }

  @Override
  public double compute(Set<Integer> docIds) {
    assert docIds != null;
    if (docIds.isEmpty()) {
      return 0;
    }

    // TODO(uvictor): consider making the docIds Set a List
    sortDocsByDate(docIds);

    // Max tf-idfs for all terms from the allDocIds documents.
    final int cardinality = documents.get(allDocIds.get(0)).size();
    maxScores = new RandomAccessSparseVector(cardinality);

    //return computeOnlyForDocumentDates();
    return computeEquationFive();
  }

  private double computeEquationFive() {
    double sum = 0;
    final Iterator<Entry<Integer, Vector>> it =
        wordSpread.entrySet().iterator();
    int docIndex = 0;
    int last = -1;

    // All products are zero until the first document's date.
    while (it.hasNext()) {
      final Entry<Integer, Vector> word = it.next();

      if (updateMaxScores(word, docIndex)) {
        docIndex++;
        sum += word.getValue().dot(maxScores);

        break;
      }
    }

    while (it.hasNext() && docIndex < docList.size()) {
      final Entry<Integer, Vector> word = it.next();

      if (updateMaxScores(word, docIndex)) {
        docIndex++;
      }
      sum += word.getValue().dot(maxScores);
    }

    // The max scores remain the same after the last document.
    while (it.hasNext()) {
      final Entry<Integer, Vector> word = it.next();

      sum += word.getValue().dot(maxScores);
    }

    return sum;
  }

  private boolean updateMaxScores(
      Entry<Integer, Vector> word, int docIndex) {
    if (word.getKey() >= docDates.get(docList.get(docIndex))) {
      maxScores.assign(documents.get(docList.get(docIndex)), Functions.MAX);

      return true;
    }

    return false;
  }

  /**
   * Implementation differs from eq 5. It considers only the dates on which
   * papers have been published.
   */
  private double computeOnlyForDocumentDates() {
    double sum = 0;
    for (Integer docId : docList) {
      final int date = docDates.get(docId);
      maxScores.assign(documents.get(docId), Functions.MAX);
      sum += wordSpread.get(date).dot(maxScores);
    }

    return sum;
  }

  private void sortDocsByDate(Set<Integer> docIds) {
    docList = new ArrayList<>(docIds);
    Collections.sort(docList, new Comparator<Integer>() {
      @Override
      public int compare(Integer o1, Integer o2) {
        return docDates.get(o1) - docDates.get(o2);
      }
    });
  }
}
