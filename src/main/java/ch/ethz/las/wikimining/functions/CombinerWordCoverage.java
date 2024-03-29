
package ch.ethz.las.wikimining.functions;

import ch.ethz.las.wikimining.mr.base.DocumentWithVectorWritable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

/**
 * Combines the word coverage as equation (1) from the paper with the inlinks
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class CombinerWordCoverage extends WeightedWordCoverage {

  final Logger logger;

  private HashMap<Integer, ArrayList<Integer>> graph;
  private HashMap<Integer, ArrayList<Integer>> revisions;
  private boolean useInlinks;
  private boolean useRevisionCount;
  private boolean useRevisionVolume;

  public CombinerWordCoverage(Map<Integer, Long> theWordCount,
      Iterator<DocumentWithVectorWritable> theDocuments, boolean theUseInlinks,
      boolean theUseRevisionsCount, boolean theUseRevisionVolume) {
    super(theWordCount, theDocuments);
    useInlinks = theUseInlinks;
    useRevisionCount = theUseRevisionsCount;
    useRevisionVolume = theUseRevisionVolume;

    logger = Logger.getLogger(this.getClass());
  }

  public void setGraph(HashMap<Integer, ArrayList<Integer>> theGraph) {
    graph = theGraph;
  }

  public void setRevisions(HashMap<Integer, ArrayList<Integer>> theRevisions) {
    revisions = theRevisions;
  }

  public void setUseInlinks(boolean theUseInlinks) {
    useInlinks = theUseInlinks;
  }

  public void setUseRevisionCount(boolean theUseRevisionCount) {
    useRevisionCount = theUseRevisionCount;
  }

  public void setUseRevisionVolume(boolean theUseRevisionVolume) {
    useRevisionVolume = theUseRevisionVolume;
  }

  /**
   * Get max tf-idfs for each term combined with inlinks count for all
   * docIds documents.
   */
  @Override
  protected Vector getMaxScores(Set<Integer> docIds) {
    final int cardinality = documents.get(allDocIds.get(0)).size();
    final Vector maxScores = new RandomAccessSparseVector(cardinality);
    for (final Integer docId : docIds) {
      final double inlinks;
      if (!useInlinks || graph.get(docId) == null) {
        inlinks = 1;
      } else {
        inlinks = 1 + graph.get(docId).size();
      }

      final double revisionsCount;
      final double revisionsVolume;
      if (revisions.get(docId) == null) {
        revisionsCount = 1;
        revisionsVolume = 1;
      } else {
        if (useRevisionCount) {
          revisionsCount = 1 + revisions.get(docId).get(0);
        } else {
          revisionsCount = 1;
        }
        if (useRevisionVolume) {
          revisionsVolume = 1 + Math.log(revisions.get(docId).get(1));
        } else {
          revisionsVolume = 1;
        }
      }
      final double revisionsValue = revisionsCount * revisionsVolume;

      for (final Vector.Element element : documents.get(docId).nonZeroes()) {
        final double value = element.get() * inlinks * revisionsValue;

        if (value > maxScores.get(element.index())) {
          maxScores.setQuick(element.index(), value);
        }
      }
    }

    return maxScores;
  }
}
