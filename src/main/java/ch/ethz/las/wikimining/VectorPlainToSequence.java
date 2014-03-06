
package ch.ethz.las.wikimining;

import ch.ethz.las.wikimining.mr.utils.h104.TextVectorSequenceFileWriter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Scanner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

/**
 * Converts a set of vectors from plain text to sequence files.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class VectorPlainToSequence {
  private static final int SIZE = 1800;
  private static final int DIMENSIONS = 3524;

  private final Logger logger;

  private final String plainPath;
  private final String sequencePath;
  private final String namePath;
  private HashMap<Integer, Vector> tfidfs;

  public VectorPlainToSequence(
      String theNamePath, String thePlainPath, String theSequencePath) {
    logger = Logger.getLogger(this.getClass());

    namePath = theNamePath;
    plainPath = thePlainPath;
    sequencePath = theSequencePath;
  }

  public void convert() {
    readVectors();
    writeVectors();
  }

  private void readVectors() {
    tfidfs = new LinkedHashMap<>();

    try (final Scanner in = new Scanner(new File(plainPath));
        BufferedReader nameReader =
        new BufferedReader(new FileReader(namePath))) {
      for (int i = 1; i <= SIZE; i++) {
        final String nameLine = nameReader.readLine();
        final Scanner line = new Scanner(in.nextLine());
        final Vector vector = new RandomAccessSparseVector(DIMENSIONS);

        for (int j = 0; j < DIMENSIONS; j++) {
          final double value = line.nextDouble();
          if (Math.abs(value) > 1e-5) {
            vector.set(j, value);
          }
        }

        tfidfs.put(NipsCiteCounter.computeKeyBenyah(nameLine), vector);
      }
    } catch (IOException e) {
      logger.fatal("Cannot open plain file.", e);
    }
  }

  private void writeVectors() {
    try {
      JobConf config = new JobConf();
      final FileSystem fs = FileSystem.get(config);
      final Path sequencesPath = new Path(sequencePath);
      final TextVectorSequenceFileWriter vectorsWriter =
          new TextVectorSequenceFileWriter(tfidfs, sequencesPath, fs, config);
      tfidfs = vectorsWriter.processFile();
    } catch (IOException e) {
      logger.fatal("Cannot write to sequence file", e);
    }
  }

  public static void main(String[] args) {
    final VectorPlainToSequence converter =
        new VectorPlainToSequence(args[0], args[1], args[2]);
    converter.convert();
  }
}
