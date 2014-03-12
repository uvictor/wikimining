
package ch.ethz.las.wikimining.evaluate;

import de.tudarmstadt.ukp.wikipedia.api.Wikipedia;
import de.tudarmstadt.ukp.wikipedia.api.exception.WikiApiException;
import de.tudarmstadt.ukp.wikipedia.api.exception.WikiInitializationException;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import org.apache.log4j.Logger;

/**
 * Counts the in-links for the chosen Wikipedia pages using the JWPL SQL
 * database.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class CountWikiInlinks {

  private final Logger logger;
  private final String selectedPath;
  private final Wikipedia wiki;

  public CountWikiInlinks(String theSelectedPath)
      throws WikiInitializationException {
    logger = Logger.getLogger(this.getClass());

    selectedPath = theSelectedPath;

    final WikiDatabase database = new WikiDatabase();
    database.initialiseWikiDatabase();
    wiki = database.getWiki();
  }

  public int computeTotalCites() {
    int count = 0;

    try (Scanner scanner = new Scanner(new File(selectedPath))) {
      while (scanner.hasNextInt()) {
        count += wiki.getPage(scanner.nextInt()).getNumberOfInlinks();
      }
    } catch (FileNotFoundException e) {
      logger.fatal("Problem reading selected file.", e);
    } catch (WikiApiException e) {
      logger.error("Could not get Wiki page.", e);
    }

    return count;
  }

  public static void main(String[] args) throws WikiInitializationException {
    final CountWikiInlinks counter = new CountWikiInlinks((args[0]));
    System.out.println(counter.computeTotalCites());
  }
}
