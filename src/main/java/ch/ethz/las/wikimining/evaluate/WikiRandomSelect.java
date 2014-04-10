
package ch.ethz.las.wikimining.evaluate;

import ch.ethz.las.wikimining.mr.io.h104.SequenceFileProcessor;
import ch.ethz.las.wikimining.mr.io.h104.TextVectorKeySequenceFileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

/**
 * Randomly selects Wiki pages, given the document dates file
 * (for retrieving the ids).
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class WikiRandomSelect {

  private final Logger logger;

  private final String inputPath;
  private final String outputPath;
  private final int selectCount;
  private ArrayList<Integer> docIds;

  public WikiRandomSelect(
      String theInputPath, String theOutputPath, int theSelectCount) {
    logger = Logger.getLogger(this.getClass());

    inputPath = theInputPath;
    outputPath = theOutputPath + "/part-0000";
    selectCount = theSelectCount;
  }

  public void readDocIds() {
    try {
      JobConf config = new JobConf();
      Path tfidfsPath = new Path(inputPath);
      logger.info("Loading doc ids: " + inputPath);

      FileSystem fs = FileSystem.get(config);
      final SequenceFileProcessor keyReader =
          new TextVectorKeySequenceFileReader(tfidfsPath, fs, config);
      final HashMap<Integer, Integer> tfidfs = keyReader.processFile();

      docIds = new ArrayList<>(tfidfs.keySet());
    } catch (IOException e) {
      logger.fatal("Error loading doc ids!", e);
    }
    logger.info("Loaded " + docIds.size() + " doc ids.");
  }

  public void select(int count) {
    Collections.shuffle(docIds);

    try (PrintWriter out = new PrintWriter(outputPath + count)) {
      for (int i = 0; i < selectCount; i++) {
        out.println(docIds.get(i));
      }
    } catch (IOException e) {
      logger.fatal("Could not write to file.", e);
    }
  }

  public void select10() {
    readDocIds();
    for (int i = 0; i < 10; i++) {
      select(i);
    }
  }

  public static void main(String[] args) {
    WikiRandomSelect selector =
        new WikiRandomSelect(args[0], args[1], Integer.parseInt(args[2]));
    selector.select10();
  }
}
