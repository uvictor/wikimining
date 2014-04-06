package ch.ethz.las.wikimining.base;

import org.apache.mahout.math.Vector;

/**
 * Stores a document id along with its tf-idf vector.
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class DocumentWithVector {
  final private int id;
  final private Vector vector;

  public DocumentWithVector(int theId, Vector theVector) {
    id = theId;
    vector = theVector;
  }

  public int getId() {
    return id;
  }

  public Vector getVector() {
    return vector;
  }
}
