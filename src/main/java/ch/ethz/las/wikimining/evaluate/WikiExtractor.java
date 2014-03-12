
package ch.ethz.las.wikimining.evaluate;

import de.tudarmstadt.ukp.wikipedia.api.Category;
import de.tudarmstadt.ukp.wikipedia.api.Wikipedia;
import de.tudarmstadt.ukp.wikipedia.api.exception.WikiApiException;
import de.tudarmstadt.ukp.wikipedia.api.exception.WikiInitializationException;
import org.apache.log4j.Logger;

/**
 * Extract a subset of Wikipedia from the JWPL SQL database.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class WikiExtractor {

  private final Logger logger;
  private final Wikipedia wiki;

  public WikiExtractor() throws WikiInitializationException {
    logger = Logger.getLogger(this.getClass());

    final WikiDatabase database = new WikiDatabase();
    database.initialiseWikiDatabase();
    wiki = database.getWiki();
  }

  public void extractCategoryRecursively(String title) {
    int count = 0;

    try {
      final Category mainCategory = wiki.getCategory(title);
      count += extractSingleCategory(mainCategory);
      for (final Category category : mainCategory.getDescendants()) {
        count += extractSingleCategory(category);
      }
    } catch (WikiApiException e) {
      logger.fatal("Could not find category.", e);
    }

    logger.warn("Extracted articles: " + count);
  }

  public int extractSingleCategory(Category category) throws WikiApiException {
    final int count = category.getNumberOfPages();
    logger.warn("Found pages: " + count);
    return count;
  }

  public static void main(String[] args) throws WikiInitializationException {
    final WikiExtractor extractor = new WikiExtractor();
    extractor.extractCategoryRecursively("Artificial intelligence");
  }
}
