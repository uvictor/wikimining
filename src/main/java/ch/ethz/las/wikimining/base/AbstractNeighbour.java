
package ch.ethz.las.wikimining.base;

import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeSet;

/**
 * Retrieves a document from among the nearest neighbours bucket.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public abstract class AbstractNeighbour extends TreeSet<DocumentWithVector>
    implements NearestNeighbourCollection<DocumentWithVector> {

  public AbstractNeighbour(final HashMap<Integer, Integer> docDates) {
    super(new Comparator<DocumentWithVector>() {

      @Override
      public int compare(DocumentWithVector o1, DocumentWithVector o2) {
        final int time = docDates.get(o1.getId()) - docDates.get(o2.getId());
        // We want to keep all documents, so we order documents with the same
        // date by id.
        if (time == 0) {
          return o1.getId() - o2.getId();
        }

        return time;
      }
    });
  }
}
