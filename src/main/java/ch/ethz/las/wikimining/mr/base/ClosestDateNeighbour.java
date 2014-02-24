
package ch.ethz.las.wikimining.mr.base;

import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeSet;

/**
 * Retrieves the document that is closest by date from the given document.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class ClosestDateNeighbour extends TreeSet<DocumentWithVector>
    implements NearestNeighbourCollection<DocumentWithVector> {

  public ClosestDateNeighbour(final HashMap<Integer, Integer> docDates) {
    super(new Comparator<DocumentWithVector>() {

      @Override
      public int compare(DocumentWithVector o1, DocumentWithVector o2) {
        return docDates.get(o1.getId()) - docDates.get(o2.getId());
      }
    });
  }

  @Override
  public DocumentWithVector getNearestNeighbour(DocumentWithVector current) {
    return this.lower(current);
  }
}
