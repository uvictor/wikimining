package ch.ethz.las.wikimining.evaluate;

import ch.ethz.las.wikimining.functions.ObjectiveFunction;
import ch.ethz.las.wikimining.functions.WordCoverageFromLucene;
import ch.ethz.las.wikimining.sfo.SfoGreedyAlgorithm;
import ch.ethz.las.wikimining.sfo.SfoGreedyLazy;
import de.tudarmstadt.ukp.wikipedia.api.exception.WikiApiException;
import de.tudarmstadt.ukp.wikipedia.api.exception.WikiInitializationException;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Set;

public class WikiMining {

  public static final int THREAD_COUNT = 48;

  public static void main(String[] args)
      throws WikiInitializationException, WikiApiException, IOException {
    // Import Wikipedia
    WikiDatabase wiki = new WikiDatabase();
    final long start = System.currentTimeMillis();
    wiki.initialiseForReading();
    final long time = (System.currentTimeMillis() - start) / 1000;
    System.out.println("Finished importing after " + time + " seconds.");


    System.out.println(wiki.getWiki().getPage("US").getTitle());

    // Initialize Lucene reading
    try (final IndexReader reader = DirectoryReader.open(wiki.getIndexDir())) {
      final ObjectiveFunction objectiveFunction = new WordCoverageFromLucene(
              reader, WikiDatabase.FieldNames.TEXT.toString());
      final SfoGreedyAlgorithm sfo = new SfoGreedyLazy(objectiveFunction);
      final ArrayList<ArrayList<Integer>> G = readGraph();

      final long greedyStart = System.currentTimeMillis();
      //final Set<Integer> selected = sfo.run(reader.numDocs(), 3);
      final Set<Integer> selected = sfo.run(100, 3);
      final double greedyTime =
          (System.currentTimeMillis() - greedyStart) / 1000.0;
      System.out.println("Finished greedy after " + greedyTime + " seconds.");

      printSelected(reader, selected);
    }
  }

  private static void printSelected(IndexReader reader, Set<Integer> selected)
      throws IOException {
    for (Integer docId : selected) {
      final Document topDoc = reader.document(docId);
      final String topTitle =
          topDoc.getField(WikiDatabase.FieldNames.TITLE.toString()).stringValue();
      System.out.println(docId + ": " + topTitle);
    }
  }

  private static ArrayList<ArrayList<Integer>> createGraph() {
    final int[][] GRAPH = {
      /*{1, 1, 0},
      {1, 0, 1},
      {0, 1, 1},*/
      {0, 1, 0, 0, 0},
      {1, 0, 1, 1, 0},
      {0, 1, 0, 0, 1},
      {0, 1, 0, 0, 1},
      {0, 0, 1, 1, 0},
    };
    final int N = GRAPH.length;

    final ArrayList<ArrayList<Integer>> G = new ArrayList<>(N);
    for (int i = 0; i < N; ++i) {
      ArrayList<Integer> line = new ArrayList<>(N);
      for (int j = 0; j < N; ++j) {
        line.add(GRAPH[i][j]);
      }
      G.add(line);
    }

    return G;
  }

  private static ArrayList<ArrayList<Integer>> readGraph()
      throws FileNotFoundException {
    final File file = new File("facebook_combined.txt");
    final int N = 4039;

    final ArrayList<ArrayList<Integer>> G = new ArrayList<>(N);
    for (int i = 0; i < N; ++i) {
      ArrayList<Integer> line = new ArrayList<>(N);
      for (int j = 0; j < N; ++j) {
        line.add(0);
      }
      G.add(line);
    }

    final Scanner s = new Scanner(file);

    while (s.hasNextLine()) {
      String line = s.nextLine();
      if (line.startsWith("#")) {
        continue;
      }

      final Scanner l = new Scanner(line);
      int x = l.nextInt();
      int y = l.nextInt();
      G.get(x).set(y, 1);
      G.get(y).set(x, 1);
    }

    System.out.println("Finished reading graph.");
    return G;
  }
}
