
package ch.ethz.las.wikimining.mr.base;

import java.util.HashMap;

/**
 * Retrieves the document that is closest by date from the given document in
 * O(log N).
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class ClosestDateNeighbour extends AbstractNeighbour {

  public ClosestDateNeighbour(final HashMap<Integer, Integer> theDocDates) {
    super(theDocDates);
  }

  @Override
  public DocumentWithVector getNearestNeighbour(DocumentWithVector current) {
    return this.lower(current);
  }
}
