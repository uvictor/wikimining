
package ch.ethz.las.wikimining.evaluate;

import ch.ethz.las.wikimining.functions.CombinerWordCoverage;
import ch.ethz.las.wikimining.mr.base.Fields;
import ch.ethz.las.wikimining.mr.coverage.h104.CoverageGreeDiReducer;
import ch.ethz.las.wikimining.mr.io.h104.IntArraySequenceFileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

/**
 * Scores the chosen Wikipedia pages using based on the different coverage
 * functions.
 *
 * @deprecated not used.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class WikiCoverageEvaluator {

  private final Logger logger;

  private CombinerWordCoverage wordCoverage;

  public WikiCoverageEvaluator() {
    logger = Logger.getLogger(this.getClass());
  }

  public void readFiles(JobConf config) {
    final HashMap<Integer, Long> wordCount =
        CoverageGreeDiReducer.readWordCount(config);
    logger.warn("Added word count.");
    final CombinerWordCoverage wordCoverage =
        new CombinerWordCoverage(wordCount, null, false, false, false);

    try {
      final FileSystem fs = FileSystem.get(config);

      {
        final Path path = new Path(config.get(Fields.GRAPH.get()));
        final IntArraySequenceFileReader graphReader =
            new IntArraySequenceFileReader(path, fs, config);
        // Get the whole graph
        graphReader.setDocIds(null);

        final HashMap<Integer, ArrayList<Integer>> graph =
            graphReader.processFile();
        wordCoverage.setGraph(graph);
        logger.warn("Added graph.");
      }

      {
        final Path path = new Path(config.get(Fields.REVISIONS.get()));
        final IntArraySequenceFileReader revisionsReader =
            new IntArraySequenceFileReader(path, fs, config);
        revisionsReader.setDocIds(null);

        final HashMap<Integer, ArrayList<Integer>> revisions =
            revisionsReader.processFile();
        wordCoverage.setRevisions(revisions);
        logger.info("Added revisions");
      }
    } catch (IOException e) {
      logger.fatal("Error retrieving the filesystem", e);
    }
  }
}
