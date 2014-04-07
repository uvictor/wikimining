
package ch.ethz.las.wikimining.mr.coverage.h104;

import ch.ethz.las.wikimining.functions.CombinerWordCoverage;
import ch.ethz.las.wikimining.base.Defaults;
import ch.ethz.las.wikimining.base.DocumentWithVectorWritable;
import ch.ethz.las.wikimining.base.Fields;
import ch.ethz.las.wikimining.base.HashBandWritable;
import ch.ethz.las.wikimining.mr.io.h104.IntArraySequenceFileReader;
import ch.ethz.las.wikimining.sfo.SfoGreedyAlgorithm;
import ch.ethz.las.wikimining.sfo.SfoGreedyLazy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

/**
 * Applies the SFO greedy algorithm for multiple submodular functions, part of
 * the GreeDi protocol.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class CombinerGreeDiReducer extends MapReduceBase implements Reducer<
    IntWritable, DocumentWithVectorWritable, NullWritable, IntWritable> {

  private static final Logger logger =
      Logger.getLogger(CombinerGreeDiReducer.class);

  private HashMap<HashBandWritable, HashSet<Integer>> buckets;
  private IntArraySequenceFileReader graphReader;
  private IntArraySequenceFileReader revisionsReader;
  private HashMap<Integer, Long> wordCount;
  private int selectCount;

  @Override
  public void configure(JobConf config) {
    wordCount = CoverageGreeDiReducer.readWordCount(config);
    FileSystem fs = null;
    try {
      fs = FileSystem.get(config);
    } catch (IOException e) {
      logger.fatal("Error retrieving the filesystem", e);
    }

    /*try {
      final FileSystem fs = FileSystem.get(config);
      final Path bucketsPath = new Path(config.get(Fields.BUCKETS.get()));
      final BucketsSequenceFileReader bucketsReader =
          new BucketsSequenceFileReader(bucketsPath, fs, config);
      buckets = bucketsReader.processFile();
    } catch (IOException e) {
      logger.fatal("Error loading buckets!", e);
    }*/

    {
      final Path path = new Path(config.get(Fields.GRAPH.get()));
      graphReader = new IntArraySequenceFileReader(path, fs, config);
    }

    {
      final Path path = new Path(config.get(Fields.REVISIONS.get()));
      revisionsReader = new IntArraySequenceFileReader(path, fs, config);
    }

    selectCount =
        config.getInt(Fields.SELECT_COUNT.get(), Defaults.SELECT_COUNT.get());
  }

  @Override
  public void reduce(IntWritable key,
      Iterator<DocumentWithVectorWritable> documents,
      OutputCollector<NullWritable, IntWritable> output, Reporter reporter)
      throws IOException {
    final CombinerWordCoverage wordCoverage =
        new CombinerWordCoverage(wordCount, documents);
    logger.info("Created WordCoverageFromMahout");

    /*final LshBuckets lshBuckets = new LshBuckets(buckets);
    logger.info("Created LshBuckets");*/

    final HashSet<Integer> docIds = new HashSet(wordCoverage.getAllDocIds());

    final HashMap<Integer, ArrayList<Integer>> graph;
    graphReader.setDocIds(docIds);
    graph = graphReader.processFile();
    wordCoverage.setGraph(graph);
    logger.info("Added graph");

    final HashMap<Integer, ArrayList<Integer>> revisions;
    revisionsReader.setDocIds(docIds);
    revisions = revisionsReader.processFile();
    wordCoverage.setRevisions(revisions);
    logger.info("Added revisions");

    /*final ObjectiveCombiner combiner = new ObjectiveCombiner(
        Arrays.asList(1D, 1D), wordCoverage, graphCoverage);*/
    //final RevisionsSummer revisionsSummer = new RevisionsSummer(revisions);

    final SfoGreedyAlgorithm sfo = new SfoGreedyLazy(wordCoverage, reporter);
    logger.info("Created SfoGreedyAlgorithm");

    Set<Integer> selected =
        sfo.run(wordCoverage.getAllDocIds(), selectCount);
    logger.info("Finished running SFO");

    for (Integer docId : selected) {
      IntWritable outValue = new IntWritable(docId);
      output.collect(NullWritable.get(), outValue);
    }
  }
}
