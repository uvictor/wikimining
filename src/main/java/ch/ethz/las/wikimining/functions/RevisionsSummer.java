
package ch.ethz.las.wikimining.functions;

import java.util.HashMap;
import java.util.Set;
import org.apache.log4j.Logger;

/**
 * Sums the revisions count for each selected document.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class RevisionsSummer implements ObjectiveFunction {

  private final Logger logger;
  private final HashMap<Integer, Integer> revisions;
  private final int totalCount;

  public RevisionsSummer(HashMap<Integer, Integer> theRevisions) {
    logger = Logger.getLogger(this.getClass());
    revisions = theRevisions;

    int tempCount = 0;
    for (Integer current : revisions.values()) {
      tempCount += current;
    }
    totalCount = tempCount;
  }

  @Override
  public double compute(Set<Integer> docIds) {
    assert docIds != null;
    if (docIds.isEmpty()) {
      return 0;
    }

    int count = 0;
    for (Integer docId : docIds) {
      final Integer current = revisions.get(docId);
      if (current != null) {
        count += current;
      } else {
        logger.info("No inlinks for docid " + docId);
      }
    }

    return (double) count / totalCount;
  }
}
