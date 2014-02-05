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
import org.apache.mahout.math.function.Functions;

/**
 * Computes the word coverage as equation (1) from the paper, from a Mahout
 * index.
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
    final int cardinality = documents.get(allDocIds.get(0)).size();
    final Vector maxScores = new RandomAccessSparseVector(cardinality);
    for (Integer docId : docIds) {
      maxScores.assign(documents.get(docId), Functions.MAX);
    }

    return maxScores.zSum();
  }
}
