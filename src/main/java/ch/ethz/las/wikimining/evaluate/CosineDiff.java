
package ch.ethz.las.wikimining.evaluate;

import ch.ethz.las.wikimining.mr.io.h104.TextVectorSequenceFileReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.apache.mahout.math.Vector;

/**
 * Checks the cosine distances between this implementation and the paper's
 * python implementation.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class CosineDiff {
  private static final int SIZE = 1800;

  private final Logger logger;

  private final String vectorPath;
  private final String cosinePath;
  private HashMap<Integer, Vector> tfidfs;
  private ArrayList<ArrayList<Double>> cosines;

  public CosineDiff(String theVectorPath, String theCosinePath) {
    logger = Logger.getLogger(this.getClass());

    vectorPath = theVectorPath;
    cosinePath = theCosinePath;
  }

  public void compare() {
    readVectors();
    readCosines();

    int i = 0;
    double totalError = 0;
    for (Vector x : tfidfs.values()) {
      int j = 0;
      for (Vector y : tfidfs.values()) {
        double error;
        if (i == j) {
          error = x.dot(y) - cosines.get(i).get(j);
        } else {
          error = 1.0D - x.dot(y) - cosines.get(i).get(j);
        }
        error = Math.abs(error);
        if (!Double.isNaN(error)) {
          totalError += error;
        }
        if (error > 1e-2) {
          System.out.println(i + ", " + j + ": " + error + " - "
              + x.dot(y) + " vs. " + cosines.get(i).get(j) + " | " + x.zSum());
        }

        j++;
      }

      i++;
    }
    System.out.println("TOTAL ERROR: " + totalError);
  }

  public void printVectors() {
    readVectors();

    for (Vector x : tfidfs.values()) {
      for (Vector y : tfidfs.values()) {
        System.out.print(1.0D - x.dot(y) + " ");
      }
      System.out.println();
    }
  }

  private void readVectors() {
    try {
      JobConf config = new JobConf();
      final FileSystem fs = FileSystem.get(config);
      final Path vectorsPath = new Path(vectorPath);
      final TextVectorSequenceFileReader vectorsReader =
          new TextVectorSequenceFileReader(vectorsPath, fs, config);
      tfidfs = vectorsReader.processFile();
    } catch (IOException e) {
      logger.fatal("Error loading tf-idf vectors", e);
    }
    logger.warn("Read tfidfs: " + tfidfs.size());
  }

  private void readCosines() {
    try (final Scanner s = new Scanner(new File(cosinePath))) {
      cosines = new ArrayList<>(SIZE);
      for (int i = 0; i < SIZE; i++) {
        final Scanner line = new Scanner(s.nextLine());
        final ArrayList<Double> aux = new ArrayList<>(SIZE);
        for (int j = 0; j < SIZE; j++) {
          aux.add(line.nextDouble());
        }
        cosines.add(aux);
      }
    } catch (FileNotFoundException e) {
      logger.fatal("Error loading cosine distances", e);
    }
    logger.warn("Read cosines: " + cosines.size());
  }

  public static void main(String[] args) {
    final CosineDiff diff = new CosineDiff(args[0], args[1]);
    //diff.printVectors();
    diff.compare();
  }
}
