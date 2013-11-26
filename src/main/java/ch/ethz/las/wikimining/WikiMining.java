package ch.ethz.las.wikimining;

import de.tudarmstadt.ukp.wikipedia.api.exception.WikiApiException;
import de.tudarmstadt.ukp.wikipedia.api.exception.WikiInitializationException;
import java.io.IOException;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;

public class WikiMining {

  public static void main(String[] args)
      throws WikiInitializationException, WikiApiException, IOException {
    // Import Wikipedia
    ImportWiki wiki = new ImportWiki();
    final long start = System.currentTimeMillis();
    wiki.initialiseForReading();
    final long time = (System.currentTimeMillis() - start) / 1000;
    System.out.println("Finished importing after " + time + " seconds.");

    // Initialize Lucene reading
    try (final IndexReader reader = DirectoryReader.open(wiki.getIndexDir())) {
      final SfoGreedyLazy sfo =
          new SfoGreedyLazy(reader, ImportWiki.FieldNames.TEXT.toString());
      final Set<Integer> selected = sfo.run(1);

      printSelected(reader, selected);
    }
  }

  private static void printSelected(IndexReader reader, Set<Integer> selected)
      throws IOException {
    for (Integer docId : selected) {
      final Document topDoc = reader.document(docId);
      final String topTitle =
          topDoc.getField(ImportWiki.FieldNames.TITLE.toString()).stringValue();
      System.out.println(docId + ": " + topTitle);
    }
  }
}
