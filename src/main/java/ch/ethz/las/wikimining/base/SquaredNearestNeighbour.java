
package ch.ethz.las.wikimining.base;

import java.util.HashMap;

/**
 * Retrieves the true nearest neighbour in O(N).
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class SquaredNearestNeighbour extends AbstractNeighbour {

  public SquaredNearestNeighbour(HashMap<Integer, Integer> theDocDates) {
    super(theDocDates);
  }

  @Override
  public DocumentWithVector getNearestNeighbour(DocumentWithVector current) {
    double min = Double.MAX_VALUE;
    DocumentWithVector minVector = null;

    for (DocumentWithVector before : super.headSet(current)) {
      double cosine = before.getVector().dot(current.getVector());

      if (cosine < min) {
        min = cosine;
        minVector = before;
      }
    }

    return minVector;
  }
}
