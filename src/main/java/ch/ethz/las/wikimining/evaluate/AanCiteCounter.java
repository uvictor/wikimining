
package ch.ethz.las.wikimining.evaluate;

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
public class AanCiteCounter {

  private final Logger logger;

  private final String selectedPath;
  private final String citesPath;

  // Map from Key to cite count.
  private HashMap<Integer, Integer> docCites;

  public AanCiteCounter(String theSelectedPath, String theCitesPath) {
    selectedPath = theSelectedPath;
    citesPath = theCitesPath;

    logger = Logger.getLogger(this.getClass());
  }

  public int run() {
    readDocCites();

    return computeTotalCites();
  }

  private void readDocCites() {
    final TreeMultiset<Integer> temp = TreeMultiset.create();

    try (BufferedReader citeReader =
        new BufferedReader(new FileReader(citesPath))) {
      docCites = new HashMap<>();

      int c = 0;
      for(String citesLine = citeReader.readLine();
          citesLine != null;
          citesLine = citeReader.readLine()) {
        c++;
        final String[] citesTerms = citesLine.split("\t", 3);
        if (citesTerms.length < 2 || "".equals(citesTerms[1])) {
          continue;
        }

        docCites.put(computeKey(citesTerms[1]), computeCites(citesTerms[0]));
        temp.add(computeCites(citesTerms[0]));
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

  private int computeKey(String name) {
    final int conference = ((int) name.charAt(0)) % 100;
    final String year = name.substring(1, 3);
    final String id = name.substring(4, 8);

    return Integer.parseInt(conference + year + id);
  }

  private int computeCites(String cites) {
    return Integer.parseInt(cites);
  }

  public static void main(String[] args) {
    final AanCiteCounter counter = new AanCiteCounter((args[0]), args[1]);
    System.out.println(counter.run());
  }
}
