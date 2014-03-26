
package ch.ethz.las.wikimining.evaluate;

import de.tudarmstadt.ukp.wikipedia.api.Page;
import de.tudarmstadt.ukp.wikipedia.api.Wikipedia;
import de.tudarmstadt.ukp.wikipedia.api.exception.WikiApiException;
import de.tudarmstadt.ukp.wikipedia.api.exception.WikiInitializationException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Scanner;
import org.apache.log4j.Logger;

/**
 * Counts the in-links for the chosen Wikipedia pages using the JWPL SQL
 * database.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class WikiCountInlinks {

  private final Logger logger;
  private final String selectedPath;
  private final String outputPath;
  private final Wikipedia wiki;

  public WikiCountInlinks(String theSelectedPath, String theOutputPath)
      throws WikiInitializationException {
    logger = Logger.getLogger(this.getClass());

    selectedPath = theSelectedPath;
    outputPath = theOutputPath;

    final WikiDatabase database = new WikiDatabase();
    database.initialiseWikiDatabase();
    wiki = database.getWiki();
  }

  public int computeTotalCites() {
    int count = 0;

    try (final Scanner scanner = new Scanner(new File(selectedPath));
        final PrintWriter out = new PrintWriter(outputPath);) {
      while (scanner.hasNextInt()) {
        final Page page = wiki.getPage(scanner.nextInt());
        final String title = page.getTitle().getWikiStyleTitle();
        final int inlinks = page.getNumberOfInlinks();
        out.println(title + "\t" + inlinks);

        count += page.getNumberOfInlinks();
      }

      out.println(count);
    } catch (FileNotFoundException e) {
      logger.fatal("Problem reading selected file.", e);
    } catch (WikiApiException e) {
      logger.error("Could not get Wiki page.", e);
    }

    return count;
  }

  public static void main(String[] args) throws WikiInitializationException {
    final WikiCountInlinks counter = new WikiCountInlinks((args[0]), args[1]);
    counter.computeTotalCites();
  }
}
