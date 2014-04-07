
package ch.ethz.las.wikimining.evaluate;

import ch.ethz.las.wikimining.evaluate.NipsCiteCounter;
import ch.ethz.las.wikimining.mr.io.h104.TextVectorSequenceFileReader;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

/**
 * Checks the distances between this implementation's and the paper's
 * python implementation's vectors.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class VectorComparator {
  private static final int SIZE = 1800;
  private static final int DIMENSIONS = 3524;

  private final Logger logger;

  private final String namePath;
  private final String selfPath;
  private final String otherPath;
  private HashMap<Integer, Vector> self;
  private HashMap<Integer, Vector> other;

  public VectorComparator(
      String theNamePath, String theSelfPath, String theOtherPath) {
    logger = Logger.getLogger(this.getClass());

    namePath = theNamePath;
    selfPath = theSelfPath;
    otherPath = theOtherPath;
  }

  private void compare() {
    readSelf();
    readOther();

    double totalError = 0;
    for (Map.Entry<Integer, Vector> o : other.entrySet()) {
      final Vector s = self.get(o.getKey());
      if (s == null) {
        logger.error("Cannot find " + o.getKey());
        continue;
      }
      final double error = s.getDistanceSquared(o.getValue());
      if (error > 1e-2) {
        System.out.println(o.getKey() + ": " + error);
        printDiff(s, o.getValue());
      }
      totalError += error;
    }
    System.out.println("TOTAL ERROR: " + totalError);
  }

  private void readSelf() {
    try {
      JobConf config = new JobConf();
      final FileSystem fs = FileSystem.get(config);
      final Path vectorsPath = new Path(selfPath);
      final TextVectorSequenceFileReader vectorsReader =
          new TextVectorSequenceFileReader(vectorsPath, fs, config);
      self = vectorsReader.processFile();
    } catch (IOException e) {
      logger.fatal("Error loading self vectors", e);
    }
    logger.warn("Read self vectors: " + self.size());
  }

  private void readOther() {
    try (final Scanner s = new Scanner(new File(otherPath));
        BufferedReader nameReader =
            new BufferedReader(new FileReader(namePath))) {
      other = new LinkedHashMap<>(SIZE);
      for (int i = 0; i < SIZE; i++) {
        final String nameLine = nameReader.readLine();
        final Scanner line = new Scanner(s.nextLine());
        final Vector vector = new RandomAccessSparseVector(DIMENSIONS);
        for (int j = 0; j < DIMENSIONS; j++) {
          final double value = line.nextDouble();
          if (Math.abs(value) > 1e-5) {
            vector.set(j, value);
          }
        }
        other.put(NipsCiteCounter.computeKeyBenyah(nameLine), vector);
      }
    } catch (IOException e) {
      logger.fatal("Error loading other vectors", e);
    }
    logger.warn("Read other vectors: " + other.size());
  }

  private void printDiff(Vector s, Vector o) {
    System.out.println(o.minus(s));
  }

  public static void main(String[] args) {
    final VectorComparator comparator =
        new VectorComparator(args[0], args[1], args[2]);
    comparator.compare();
  }
}
