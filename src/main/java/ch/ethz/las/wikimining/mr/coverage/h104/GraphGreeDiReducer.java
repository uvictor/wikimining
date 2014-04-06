
package ch.ethz.las.wikimining.mr.coverage.h104;

import ch.ethz.las.wikimining.functions.GraphCoverage;
import ch.ethz.las.wikimining.base.Defaults;
import ch.ethz.las.wikimining.base.DocumentWithVectorWritable;
import ch.ethz.las.wikimining.base.Fields;
import ch.ethz.las.wikimining.mr.utils.h104.IntArraySequenceFileReader;
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
 * Applies the SFO greedy algorithm for Graph inlinks as a Reduce stage, part of
 * the GreeDi protocol.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class GraphGreeDiReducer extends MapReduceBase implements Reducer<
    IntWritable, DocumentWithVectorWritable, NullWritable, IntWritable> {

  private static final Logger logger =
      Logger.getLogger(GraphGreeDiReducer.class);

  private IntArraySequenceFileReader graphReader;
  private int selectCount;

  @Override
  public void configure(JobConf config) {
    try {
      final FileSystem fs = FileSystem.get(config);
      final Path bucketsPath = new Path(config.get(Fields.GRAPH.get()));
      graphReader = new IntArraySequenceFileReader(bucketsPath, fs, config);
    } catch (IOException e) {
      logger.fatal("Error loading graph!", e);
    }

    selectCount =
        config.getInt(Fields.SELECT_COUNT.get(), Defaults.SELECT_COUNT.get());
  }

  @Override
  public void reduce(IntWritable key,
      Iterator<DocumentWithVectorWritable> documents,
      OutputCollector<NullWritable, IntWritable> output, Reporter reporter)
      throws IOException {
    final HashSet<Integer> docIds = new HashSet<>();
    while (documents.hasNext()) {
      DocumentWithVectorWritable document = documents.next();
      final Integer id = Integer.parseInt(document.getId().toString());
      docIds.add(id);
    }

    final HashMap<Integer, ArrayList<Integer>> graph;
    graphReader.setDocIds(docIds);
    graph = graphReader.processFile();

    final GraphCoverage graphCoverage = new GraphCoverage(graph);
    logger.info("Created GraphCoverage");

    final SfoGreedyAlgorithm sfo = new SfoGreedyLazy(graphCoverage, reporter);
    logger.info("Created SfoGreedyAlgorithm");

    Set<Integer> selected = sfo.run(docIds, selectCount);
    logger.info("Finished running SFO");

    for (Integer docId : selected) {
      IntWritable outValue = new IntWritable(docId);
      output.collect(NullWritable.get(), outValue);
    }
  }
}
