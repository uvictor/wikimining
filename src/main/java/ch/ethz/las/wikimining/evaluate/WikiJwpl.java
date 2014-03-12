
package ch.ethz.las.wikimining.evaluate;

import de.tudarmstadt.ukp.wikipedia.api.Wikipedia;
import de.tudarmstadt.ukp.wikipedia.api.exception.WikiApiException;
import de.tudarmstadt.ukp.wikipedia.api.exception.WikiInitializationException;

/**
 * Access information from JWPL SQL database of Wikipedia.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class WikiJwpl {
  public static void main(String[] args)
      throws WikiInitializationException, WikiApiException {
    final WikiDatabase database = new WikiDatabase();
    database.initialiseWikiDatabase();
    final Wikipedia wiki = database.getWiki();

    System.out.println(
        "AI: " + wiki.getPage("Artificial_intelligence").getPageId());
    System.out.println(
        "ML: " + wiki.getPage("Machine_learning").getPageId());
  }
}
