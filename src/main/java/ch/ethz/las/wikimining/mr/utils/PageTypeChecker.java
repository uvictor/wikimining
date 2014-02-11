
package ch.ethz.las.wikimining.mr.utils;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Checks the type of an Wikipedia page.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class PageTypeChecker {
  private static enum PageTypes {
    TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB, NON_ARTICLE, OTHER,
    LIST
  };

  /**
   * Checks if the page is an article and updates the MapReduce counters.
   *
   * @param doc the Wikipedia page
   * @param context the MapReduce context
   *
   * @return true if the Wiki page is an article
   */
  public static boolean isArticle(WikipediaPage doc, Mapper.Context context) {
    context.getCounter(PageTypes.TOTAL).increment(1);
    if (doc.isRedirect()) {
      context.getCounter(PageTypes.REDIRECT).increment(1);
      return false;
    }
    if (doc.isEmpty()) {
      context.getCounter(PageTypes.EMPTY).increment(1);
      return false;
    }
    if (doc.isDisambiguation() || doc.getTitle().endsWith("(disambiguation)")) {
      context.getCounter(PageTypes.DISAMBIGUATION).increment(1);
      return false;
    }

    if (doc.isStub()) {
      context.getCounter(PageTypes.STUB).increment(1);
      return false;
    }

    if (doc.isArticle()) {
      // heuristic: potentially template or stub article
      if (doc.getTitle().length() > 0.03 * doc.getContent().length()) {
        context.getCounter(PageTypes.OTHER).increment(1);
        return false;
      }
      if (doc.getTitle().startsWith("List of")
          || doc.getTitle().startsWith("List_of")) {
        context.getCounter(PageTypes.LIST).increment(1);
        return false;
      }

      context.getCounter(PageTypes.ARTICLE).increment(1);
      return true;
    }

    context.getCounter(PageTypes.NON_ARTICLE).increment(1);
    return false;
  }
}
