package ch.ethz.las.wikimining;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;

/**
 * Class that implements a greedy submodular function maximisation (SFO)
 * algorithm using lazy evaluations.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class SfoGreedyLazy {

  private class ScoreDocId implements Comparable<ScoreDocId> {
    private final double score;
    private final int docId;

    public ScoreDocId(double score, int docId) {
      this.score = score;
      this.docId = docId;
    }

    public double getScore() {
      return score;
    }

    public int getDocId() {
      return docId;
    }

    @Override
    public int compareTo(ScoreDocId o) {
      return new Double(o.score).compareTo(score);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof ScoreDocId)) {
        return false;
      }

      ScoreDocId o = (ScoreDocId) obj;
      return score == o.score;
    }

    @Override
    public int hashCode() {
      int hash = 3;
      hash = 97 * hash + (int)(Double.doubleToLongBits(this.score)
          ^ (Double.doubleToLongBits(this.score) >>> 32));
      return hash;
    }
  }

  private final Logger logger;
  private final IndexReader reader;
  private final ObjectiveFunction function;

  /**
   * Creates an object used to run the SFO algorithm.
   *
   * @param theReader used to get the documents' scores
   * @param theFieldName name of the indexed field
   *
   * @throws java.io.IOException
   */
  public SfoGreedyLazy(IndexReader theReader, String theFieldName)
      throws IOException {
    logger = Logger.getLogger(this.getClass());
    reader = theReader;
    function = new ObjectiveFunction(theReader, theFieldName);
  }

  /**
   * Runs the SFO greedy algorithm lazily.
   *
   * Important: if this function changes the index, docIds might be
   * inconsistent!
   *
   * TODO(uvictor): make this method easier to understand.
   *
   * TODO(uvictor): check the difference between removing the document from A
   * and not removing it. If this function will change the index. Change the API
   * to mention this to the caller.
   *
   * @param k the number of documents to be selected
   * @return the selected document ids
   *
   * @throws IOException
   */
  public Set<Integer> run(int k)
      throws IOException {
    final Set<Integer> selected = new LinkedHashSet<>();
    final PriorityQueue<ScoreDocId> bestDocs = new PriorityQueue<>();
    double currentScore;

    logger.info("Scoring " + reader.numDocs() + " documents.");
    for (int docId = 0; docId < reader.numDocs(); ++docId) {
        selected.add(docId);
        final double score = function.computeWordCoverage(selected);
        selected.remove(docId);

        bestDocs.add(new ScoreDocId(score, docId));

        if ((docId + 1) % 1000 == 0) {
          logger.info("Indexed pages: " + docId);
        }
    }

    final ScoreDocId firstDoc = bestDocs.poll();
    selected.add(firstDoc.getDocId());
    currentScore = firstDoc.getScore();

    for (int i = 1; i < k; ++i) {
      int bestDocId = -1;
      int docId;

      // Find the first (highest valued) document that remains at the peak after
      // we recompute its score.
      do {
        docId = bestDocs.poll().getDocId();

        if (selected.contains((docId))) {
          continue;
        }

        selected.add(docId);
        final double score =
            function.computeWordCoverage(selected) - currentScore;
        selected.remove(docId);

        bestDocs.add(new ScoreDocId(score, docId));
        bestDocId = bestDocs.peek().getDocId();
      } while (!selected.contains(docId) && bestDocId != docId);

      final ScoreDocId bestDoc = bestDocs.poll();
      selected.add(bestDoc.getDocId());
      currentScore += bestDoc.getScore();
    }

    return selected;
  }

  /**
   * Runs the SFO greedy algorithm non-lazily.
   * @deprecated use {@link run} instead
   *
   * See {@link run} for other details.
   */
  public Set<Integer> runNonLazily(int k)
      throws IOException {
    final Set<Integer> selected = new LinkedHashSet<>();

    for (int i = 0; i < k; ++i) {
      double maxScore = 0;
      int maxDocument = -1;

      for (int docId = 0; docId < reader.numDocs(); ++docId) {
        if (selected.contains((docId))) {
          continue;
        }

        selected.add(docId);
        final double score = function.computeWordCoverage(selected);
        selected.remove(docId);

        if (score > maxScore) {
          maxScore = score;
          maxDocument = docId;
        }
      }

      selected.add(maxDocument);
    }

    return selected;
  }
}
