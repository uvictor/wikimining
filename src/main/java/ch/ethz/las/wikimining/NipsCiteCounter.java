
package ch.ethz.las.wikimining;

import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Scanner;
import org.apache.log4j.Logger;

/**
 * Counts the citations of all selected document.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class NipsCiteCounter {
  private static final String SELECTED_FILE = "part-00000";
  private static final String DOC_NAMES = "nips.docnames";
  private static final String CITE = "nips.cite";

  private final String selectedPath;
  private final String statsPath;

  // Map from Key to cite count.
  private HashMap<Integer, Integer> docCites;

  private final Logger logger;

  public NipsCiteCounter(String theSelectedPath, String theStatsPath) {
    selectedPath = theSelectedPath + SELECTED_FILE;
    statsPath = theStatsPath;

    logger = Logger.getLogger(this.getClass());
  }

  public int run() {
    readDocCites();

    return computeTotalCites();
  }

  private void readDocCites() {
    final TreeMultiset<Integer> temp = TreeMultiset.create();

    try (BufferedReader nameReader =
        new BufferedReader(new FileReader(statsPath + DOC_NAMES));
        BufferedReader citesReader =
        new BufferedReader(new FileReader(statsPath + CITE))) {
      docCites = new HashMap<>();

      String nameLine = nameReader.readLine();
      String citesLine = citesReader.readLine();
      int c = 0;
      while (nameLine != null && citesLine != null) {
        c++;
        docCites.put(computeKey(nameLine), computeCites(citesLine));
        temp.add(computeCites(citesLine));

        nameLine = nameReader.readLine();
        citesLine = citesReader.readLine();
      }
      logger.error("Elements: " + c);
    } catch (IOException e) {
      logger.fatal("Problem reading name/cites file.", e);
    }

    int tempCount = 0;
    logger.error("SIZE: " + temp.size());
    for (int i = 0; i < 100;) {
      final Multiset.Entry<Integer> entry = temp.pollLastEntry();
      tempCount += Math.min(100 - i, entry.getCount()) * entry.getElement();
      i += entry.getCount();
    }
    System.out.println("Bound: " + tempCount);

    while (!temp.isEmpty()) {
      final Multiset.Entry<Integer> entry = temp.pollLastEntry();
      tempCount += entry.getCount() * entry.getElement();
    }
    System.out.println("All: " + tempCount);
  }

  private int computeTotalCites() {
    int count = 0;

    try (Scanner scanner = new Scanner(new File(selectedPath))) {
      while (scanner.hasNextInt()) {
        count += docCites.get(scanner.nextInt());
      }
    } catch (FileNotFoundException e) {
      logger.fatal("Problem reading selected file.", e);
    }

    return count;
  }

  private int computeKey(String nameLine) {
    return computeKeyBenyah(nameLine);
    //return computeKeyKnowceans(nameLine);
  }

  private int computeCites(String nameLine) {
    return computeCitesBenyah(nameLine);
    //return computeCitesKnowceans(nameLine);
  }

  public static int computeKeyBenyah(String nameLine) {
    return computeKeyKnowceans(
        nameLine.split("file is ../nipsdata/txt/", 2)[1]);
  }

  private int computeCitesBenyah(String citesLine) {
    final Scanner scanner = new Scanner(citesLine);
    scanner.nextInt();

    return scanner.nextInt();
  }

  public static int computeKeyKnowceans(String nameLine) {
    final int year = Integer.parseInt(nameLine.substring(4, 6));
    final int name = Integer.parseInt(nameLine.substring(7, 11));

    return year * 10_000 + name;
  }

  private int computeCitesKnowceans(String citesLine) {
    int count = 0;

    final Scanner scanner = new Scanner(citesLine);
    while (scanner.hasNext()) {
      count++;
      scanner.next();
    }

    return count;
  }

  public static void main(String[] args) {
    final NipsCiteCounter counter = new NipsCiteCounter((args[0]), args[1]);
    System.out.println(counter.run());
  }
}
