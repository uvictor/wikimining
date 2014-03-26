
package ch.ethz.las.wikimining.evaluate;

import de.tudarmstadt.ukp.wikipedia.api.Category;
import de.tudarmstadt.ukp.wikipedia.api.Page;
import de.tudarmstadt.ukp.wikipedia.api.Wikipedia;
import de.tudarmstadt.ukp.wikipedia.api.exception.WikiApiException;
import de.tudarmstadt.ukp.wikipedia.api.exception.WikiInitializationException;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

/**
 * Extract a subset of Wikipedia from the JWPL SQL database.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class WikiExtractor {

  private final Logger logger;
  private final String outputPath;
  private final SequenceFile.Writer writer;
  private final Wikipedia wiki;

  public WikiExtractor(String theOutputPath)
      throws WikiInitializationException, IOException {
    logger = Logger.getLogger(this.getClass());

    outputPath = theOutputPath;

    final WikiDatabase database = new WikiDatabase();
    database.initialiseWikiDatabase();
    wiki = database.getWiki();

    JobConf config = new JobConf();
    final Path path = new Path(outputPath);
    final FileSystem fs = FileSystem.get(config);
    writer = new SequenceFile.Writer(fs, config, path, Text.class, Text.class);
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
      logger.error("Could not find category.", e);
    }

    logger.warn("Extracted articles: " + count);
  }

  public int extractSingleCategory(Category category) {
    try {
      final int count = category.getNumberOfPages();
      logger.warn("Found pages: " + count);

      for (final Page page : category.getArticles()) {
        extractText(page);
      }

      return count;
    } catch (WikiApiException e) {
      logger.error("Could not get articles", e);
    }

    return 0;
  }

  public void extractText(Page page) {
    try {
      writer.append(new Text(String.valueOf(page.getPageId())),
          new Text(page.getPlainText()));
    } catch (IOException e) {
      logger.error("Could not write to file", e);
    } catch (WikiApiException e) {
      logger.error("Could not get article plain text.", e);
    }
  }

  public void close() throws IOException {
    writer.close();
  }

  public static void main(String[] args)
      throws WikiInitializationException, IOException {
    final WikiExtractor extractor = new WikiExtractor(args[0]);
    /*extractor.extractCategoryRecursively("Machine learning");
    extractor.extractCategoryRecursively("Game_artificial_intelligence");
    extractor.extractCategoryRecursively("Vectors");
    extractor.extractCategoryRecursively("Classical_composers");*/
    for (int i = 1; i < args.length; i++) {
      extractor.extractCategoryRecursively(args[i]);
    }
    extractor.close();
  }
}
