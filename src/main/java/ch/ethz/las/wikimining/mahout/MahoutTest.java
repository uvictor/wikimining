package ch.ethz.las.wikimining.mahout;

import com.google.common.collect.Lists;
import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.commons.cli2.util.HelpFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.clustering.streaming.mapreduce.CentroidWritable;
import org.apache.mahout.clustering.streaming.mapreduce.StreamingKMeansDriver;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirValueIterable;
import org.apache.mahout.math.Centroid;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.neighborhood.ProjectionSearch;
import org.apache.mahout.math.random.WeightedThing;
import org.apache.mahout.math.stats.OnlineSummarizer;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class MahoutTest {
  private String outputFile;

  private PrintWriter fileOut;

  private String trainFile;
  private String testFile;
  private String centroidFile;
  private String centroidCompareFile;
  private boolean mahoutKMeansFormat;
  private boolean mahoutKMeansFormatCompare;

  private String input;

  private DistanceMeasure distanceMeasure = new SquaredEuclideanDistanceMeasure();

  private String inputFile;

  private String initialCentroidsFile;

  public void printSummaries(List<OnlineSummarizer> summarizers, String type) {
    printSummaries(summarizers, type, fileOut);
  }

  public static void printSummaries(List<OnlineSummarizer> summarizers, String type, PrintWriter fileOut) {
    double maxDistance = 0;
    for (int i = 0; i < summarizers.size(); ++i) {
      OnlineSummarizer summarizer = summarizers.get(i);
      if (summarizer.getCount() == 0) {
        System.out.printf("Cluster %d is empty\n", i);
        continue;
      }
      maxDistance = Math.max(maxDistance, summarizer.getMax());
      System.out.printf("Average distance in cluster %d [%d]: %f\n", i, summarizer.getCount(), summarizer.getMean());
      // If there is just one point in the cluster, quartiles cannot be estimated. We'll just assume all the quartiles
      // equal the only value.
      boolean moreThanOne = summarizer.getCount() > 1;
      if (fileOut != null) {
        fileOut.printf("%d,%f,%f,%f,%f,%f,%f,%f,%d,%s\n", i, summarizer.getMean(),
            summarizer.getSD(),
            summarizer.getQuartile(0),
            moreThanOne ? summarizer.getQuartile(1) : summarizer.getQuartile(0),
            moreThanOne ? summarizer.getQuartile(2) : summarizer.getQuartile(0),
            moreThanOne ? summarizer.getQuartile(3) : summarizer.getQuartile(0),
            summarizer.getQuartile(4), summarizer.getCount(), type);
      }
    }
    System.out.printf("Num clusters: %d; maxDistance: %f\n", summarizers.size(), maxDistance);
  }

  public Iterable<Centroid> runKMeans(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
    Path initialCentroidsPath = RandomSeedGenerator.buildRandom(conf, new Path(inputFile), new Path(initialCentroidsFile), 100, new CosineDistanceMeasure());

    KMeansDriver.buildClusters(conf, new Path(inputFile), initialCentroidsPath, new Path(outputFile),
        new CosineDistanceMeasure(), 20, "0.001", true);

    return IOUtils.getCentroidsFromClusterWritableIterable(
        new SequenceFileDirValueIterable<ClusterWritable>(
            new Path(outputFile), PathType.GLOB, conf));
  }

  public Iterable<Centroid> runStreamingKMeans(Configuration conf) throws InterruptedException, ClassNotFoundException, ExecutionException, IOException {
    StreamingKMeansDriver.configureOptionsForWorkers(conf,
        20,
        100,
        0.001f,
        10,
        0.9f,
        false,
        false,
        0.2f,
        1,
        CosineDistanceMeasure.class.getCanonicalName(),
        ProjectionSearch.class.getCanonicalName(),
        5,
        3,
        "sequential",
        true);

    StreamingKMeansDriver.run(conf, new Path(inputFile), new Path(outputFile));

    return IOUtils.getCentroidsFromCentroidWritableIterable(
        new SequenceFileDirValueIterable<CentroidWritable>(
            new Path(outputFile), PathType.GLOB, conf));
  }

  public void run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, ExecutionException {
    if (!parseArgs(args)) {
      return;
    }

    Configuration conf = new Configuration();

    System.out.println(inputFile);

    SequenceFileDirValueIterable<VectorWritable> clusterIterable =
        new SequenceFileDirValueIterable<>(new Path(inputFile), PathType.GLOB, conf);

    List<RandomAccessSparseVector> wikiPages = Lists.newArrayList();
    for (VectorWritable writable : clusterIterable) {
      wikiPages.add(new RandomAccessSparseVector(writable.get()));
    }
    System.out.printf("Num Wiki pages: %d\n", wikiPages.size());

    List<Centroid> centroids = Lists.newArrayList(runStreamingKMeans(conf));

    // Centroid weights.
    ProjectionSearch searcher = new ProjectionSearch(new CosineDistanceMeasure(), 3, 5);
    searcher.addAll(centroids);

    for (Vector wikiPage : wikiPages) {
      WeightedThing<Vector> result = searcher.searchFirst(wikiPage, false);
      Centroid centroid = (Centroid) result.getValue();
      centroid.addWeight(1);
    }

    // Best vector for centroid.
    searcher.clear();
    searcher.addAll(wikiPages);

    int numCentroid = 0;
    for (Centroid centroid : centroids) {
      WeightedThing<Vector> result = searcher.searchFirst(centroid, false);
      System.out.printf("Best vector for %d => %s\n", numCentroid++, result.getValue());
    }

    /*

    Configuration conf = new Configuration();
    try {
      Configuration.dumpConfiguration(conf, new OutputStreamWriter(System.out));

      fileOut = new PrintWriter(new FileOutputStream(outputFile));
      fileOut.printf("cluster,distance.mean,distance.sd,distance.q0,distance.q1,distance.q2,distance.q3,"
          + "distance.q4,count,is.train\n");

      // Reading in the centroids (both pairs, if they exist).
      List<Centroid> centroids;
      List<Centroid> centroidsCompare = null;
      if (mahoutKMeansFormat) {
        SequenceFileDirValueIterable<ClusterWritable> clusterIterable =
            new SequenceFileDirValueIterable<ClusterWritable>(new Path(centroidFile), PathType.GLOB, conf);
        centroids = Lists.newArrayList(IOUtils.getCentroidsFromClusterWritableIterable(clusterIterable));
      } else {
        SequenceFileDirValueIterable<CentroidWritable> centroidIterable =
            new SequenceFileDirValueIterable<CentroidWritable>(new Path(centroidFile), PathType.GLOB, conf);
        centroids = Lists.newArrayList(IOUtils.getCentroidsFromCentroidWritableIterable(centroidIterable));
      }

      if (centroidCompareFile != null) {
        if (mahoutKMeansFormatCompare) {
          SequenceFileDirValueIterable<ClusterWritable> clusterCompareIterable =
              new SequenceFileDirValueIterable<ClusterWritable>(new Path(centroidCompareFile), PathType.GLOB, conf);
          centroidsCompare = Lists.newArrayList(
              IOUtils.getCentroidsFromClusterWritableIterable(clusterCompareIterable));
        } else {
          SequenceFileDirValueIterable<CentroidWritable> centroidCompareIterable =
              new SequenceFileDirValueIterable<CentroidWritable>(new Path(centroidCompareFile), PathType.GLOB, conf);
          centroidsCompare = Lists.newArrayList(
              IOUtils.getCentroidsFromCentroidWritableIterable(centroidCompareIterable));
        }
      }

      // Reading in the "training" set.
      SequenceFileDirValueIterable<VectorWritable> trainIterable =
          new SequenceFileDirValueIterable<VectorWritable>(new Path(trainFile), PathType.GLOB, conf);
      Iterable<Vector> trainDatapoints = IOUtils.getVectorsFromVectorWritableIterable(trainIterable);
      Iterable<Vector> datapoints = trainDatapoints;

      printSummaries(ClusteringUtils.summarizeClusterDistances(trainDatapoints, centroids,
          new SquaredEuclideanDistanceMeasure()), "train");

      // Also adding in the "test" set.
      if (testFile != null) {
        SequenceFileDirValueIterable<VectorWritable> testIterable =
            new SequenceFileDirValueIterable<VectorWritable>(new Path(testFile), PathType.GLOB, conf);
        Iterable<Vector> testDatapoints = IOUtils.getVectorsFromVectorWritableIterable(testIterable);

        printSummaries(ClusteringUtils.summarizeClusterDistances(testDatapoints, centroids,
            new SquaredEuclideanDistanceMeasure()), "test");

        datapoints = Iterables.concat(trainDatapoints, testDatapoints);
      }

      // At this point, all train/test CSVs have been written. We now compute quality metrics.
      List<OnlineSummarizer> summaries =
          ClusteringUtils.summarizeClusterDistances(datapoints, centroids, distanceMeasure);
      List<OnlineSummarizer> compareSummaries = null;
      if (centroidsCompare != null) {
        compareSummaries =
            ClusteringUtils.summarizeClusterDistances(datapoints, centroidsCompare, distanceMeasure);
      }
      System.out.printf("[Dunn Index] First: %f", ClusteringUtils.dunnIndex(centroids, distanceMeasure, summaries));
      if (compareSummaries != null) {
        System.out.printf(" Second: %f\n",
            ClusteringUtils.dunnIndex(centroidsCompare, distanceMeasure, compareSummaries));
      } else {
        System.out.printf("\n");
      }
      System.out.printf("[Davies-Bouldin Index] First: %f",
          ClusteringUtils.daviesBouldinIndex(centroids, distanceMeasure, summaries));
      if (compareSummaries != null) {
        System.out.printf(" Second: %f\n",
            ClusteringUtils.daviesBouldinIndex(centroidsCompare, distanceMeasure, compareSummaries));
      } else {
        System.out.printf("\n");
      }

      if (outputFile != null) {
        fileOut.close();
      }
    } catch (IOException e) {
      System.out.println(e.getMessage());
    }
    */
  }

  private boolean parseArgs(String[] args) {
    DefaultOptionBuilder builder = new DefaultOptionBuilder();

    Option help = builder.withLongName("help").withDescription("print this list").create();

    ArgumentBuilder argumentBuilder = new ArgumentBuilder();
    Option inputFileOption = builder.withLongName("input")
        .withShortName("i")
        .withRequired(true)
        .withArgument(argumentBuilder.withName("input").withMaximum(1).create())
        .withDescription("where to get seq files with the vectors (training set)")
        .create();

    Option testInputFileOption = builder.withLongName("testInput")
        .withShortName("itest")
        .withArgument(argumentBuilder.withName("testInput").withMaximum(1).create())
        .withDescription("where to get seq files with the vectors (test set)")
        .create();

    Option initialCentroidsFileOption = builder.withLongName("initialCentroids")
        .withShortName("c")
        .withArgument(argumentBuilder.withName("initialCentroids").withMaximum(1).create())
        .withDescription("where to get seq files with the centroids (from Mahout KMeans or StreamingKMeansDriver)")
        .withRequired(true)
        .create();

    Option centroidsCompareFileOption = builder.withLongName("centroidsCompare")
        .withShortName("cc")
        .withArgument(argumentBuilder.withName("centroidsCompare").withMaximum(1).create())
        .withDescription("where to get seq files with the second set of centroids (from Mahout KMeans or " +
            "StreamingKMeansDriver)")
        .create();

    Option outputFileOption = builder.withLongName("output")
        .withShortName("o")
        .withArgument(argumentBuilder.withName("output").withMaximum(1).create())
        .withDescription("where to dump the CSV file with the results")
        .create();

    Option mahoutKMeansFormatOption = builder.withLongName("mahoutkmeansformat")
        .withShortName("mkm")
        .withDescription("if set, read files as (IntWritable, ClusterWritable) pairs")
        .withArgument(argumentBuilder.withName("numpoints").withMaximum(1).create())
        .create();

    Option mahoutKMeansCompareFormatOption = builder.withLongName("mahoutkmeansformatCompare")
        .withShortName("mkmc")
        .withDescription("if set, read files as (IntWritable, ClusterWritable) pairs")
        .withArgument(argumentBuilder.withName("numpoints").withMaximum(1).create())
        .create();

    Group normalArgs = new GroupBuilder()
        .withOption(help)
        .withOption(inputFileOption)
        .withOption(testInputFileOption)
        .withOption(outputFileOption)
        .withOption(initialCentroidsFileOption)
        .withOption(centroidsCompareFileOption)
        .withOption(mahoutKMeansFormatOption)
        .withOption(mahoutKMeansCompareFormatOption)
        .create();

    Parser parser = new Parser();
    parser.setHelpOption(help);
    parser.setHelpTrigger("--help");
    parser.setGroup(normalArgs);
    parser.setHelpFormatter(new HelpFormatter(" ", "", " ", 150));

    CommandLine cmdLine = parser.parseAndHelp(args);
    if (cmdLine == null) {
      return false;
    }

    inputFile = (String) cmdLine.getValue(inputFileOption);
    if (cmdLine.hasOption(testInputFileOption)) {
      inputFile = (String) cmdLine.getValue(testInputFileOption);
    }
    initialCentroidsFile = (String) cmdLine.getValue(initialCentroidsFileOption);
    if (cmdLine.hasOption(centroidsCompareFileOption)) {
      centroidCompareFile = (String) cmdLine.getValue(centroidsCompareFileOption);
    }
    outputFile = (String) cmdLine.getValue(outputFileOption);
    return true;
  }

  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, ExecutionException {
    Logger.getRootLogger().setLevel(Level.ALL);
    new MahoutTest().run(args);
  }
}
