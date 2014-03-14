package ch.ethz.las.wikimining.functions;

import ch.ethz.las.wikimining.mr.base.DocumentWithVectorWritable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

/**
 * Computes the word coverage as equation (1) from the paper with a constant
 * word weight of one, from a Mahout index.
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class WordCoverageFromMahout implements ObjectiveFunction {

  protected final List<Integer> allDocIds;
  protected final Map<Integer, Vector> documents;

  /**
   * Creates an object used to compute the necessary word coverage score.
   */
  public WordCoverageFromMahout(
      Iterable<DocumentWithVectorWritable> theDocuments) {
    // TODO(uvictor): make the wordCount non-null to use it with Hadoop 2.0
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

    return computeScore(getMaxScores(docIds));
  }

  /**
   * Get max tf-idfs for each term for all docIds documents.
   */
  protected Vector getMaxScores(Set<Integer> docIds) {
    final int cardinality = documents.get(allDocIds.get(0)).size();
    final Vector maxScores = new RandomAccessSparseVector(cardinality);
    for (final Integer docId : docIds) {
      for (final Element element : documents.get(docId).nonZeroes()) {
        if (element.get() > maxScores.get(element.index())) {
          maxScores.setQuick(element.index(), element.get());
        }
      }
    }

    return maxScores;
  }

  protected double computeScore(Vector maxScores) {
    return maxScores.zSum() / maxScores.size();
  }
}
