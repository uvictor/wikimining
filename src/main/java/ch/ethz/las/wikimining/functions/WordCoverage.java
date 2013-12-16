/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ch.ethz.las.wikimining.functions;

import ch.ethz.las.wikimining.ImportWiki;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.similarities.DefaultSimilarity;

/**
 * Computes the word coverage as equation (1) from the paper.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class WordCoverage implements ObjectiveFunction {

  protected static class Score {
    double wordWeight;
    double maxTfIdf;

    public Score(double tf, double idf) {
      this.wordWeight = tf;
      this.maxTfIdf = idf;
    }

    public double getWordWeight() {
      return wordWeight;
    }

    public double getMaxTfIdf() {
      return maxTfIdf;
    }
  }

  protected final Logger logger;
  protected final String fieldName;

  private final IndexReader reader;
  private final DefaultSimilarity similarity;

  /**
   * Creates an object used to compute the necessary word coverage score.
   *
   * @param theReader used to get the documents' scores
   * @param theFieldName name of the indexed field
   */
  public WordCoverage(IndexReader theReader, String theFieldName) {
    reader = theReader;
    fieldName = theFieldName;

    logger = Logger.getLogger(this.getClass());
    similarity = new DefaultSimilarity();
  }

  /**
   * Computes the word coverage as equation (1) from the paper.
   *
   * TODO(uvictor): reuse termsEnum & docsEnum while keeping this function
   * thread-safe.
   *
   * @param docIds the set of documents for which to compute the word
   * coverage score.
   *
   * @return the word coverage score
   */
  @Override
  public double compute(Set<Integer> docIds) {
    assert docIds != null;
    if (docIds.isEmpty()) {
      return 0;
    }

    // Max tf-idfs for all terms from the docIds documents.
    final Map<String, Score> maxScores = new HashMap<>();
    for (Integer docId : docIds) {
      try {
        final Terms terms = getTermsForDoc(docId);
        if (terms != null) {
          computeMaxScoresForDoc(terms, maxScores);
        }
      } catch (IOException ex) {
        logger.warn("Could not read the Lucene index.", ex);
      }
    }

    double sum = 0;
    for (Score score : maxScores.values()) {
      sum += score.getWordWeight() * score.getMaxTfIdf();
    }

    return sum;
  }

  /**
   * Returns the word weight as the tf score.
   *
   * @param term the word
   *
   * @return the word weight
   *
   * @throws java.io.IOException
   */
  protected double computeWordWeight(Term term) throws IOException {
    long totalTermFreq = reader.totalTermFreq(term);

    return similarity.tf(totalTermFreq);
  }

  /**
   * Returns the tf-idf score for a given (term frequency within a) document
   * and word.
   *
   * @param termFreq number of times the word appears in the current document
   * @param term the word
   * @return the tf-idf score
   */
  protected double computeTfIdf(int termFreq, Term term) throws IOException {
    double tf = similarity.tf(termFreq);
    double idf = similarity.idf(reader.docFreq(term), reader.numDocs());

    return tf * idf;
  }

  private Terms getTermsForDoc(int docId) throws IOException {
    final Terms terms = reader.getTermVector(docId, fieldName);
    if (terms == null) {
      final String title =
          reader.document(docId).get(ImportWiki.FieldNames.TITLE.toString());
      logger.warn("Document " + title + "(" + docId + ")" + " has an empty"
          + " term vector");
    }

    return terms;
  }

  private void computeMaxScoresForDoc(
      Terms terms, Map<String, Score> maxScores) throws IOException {
    TermsEnum termsEnum = terms.iterator(null);

    while (termsEnum.next() != null) {
      final Term term = new Term(fieldName, termsEnum.term());
      final String word = term.text();
      DocsEnum docsEnum = termsEnum.docs(null, null);
      docsEnum.nextDoc();
      final double tfIdf = computeTfIdf(docsEnum.freq(), term);

      final Score score = maxScores.get(word);
      if (score == null) {
        maxScores
            .put(word, new Score(computeWordWeight(term), tfIdf));
      } else if (tfIdf > score.getMaxTfIdf()) {
        maxScores
            .put(word, new Score(score.getWordWeight(), tfIdf));
      }

      assert docsEnum.nextDoc() == DocIdSetIterator.NO_MORE_DOCS;
    }
  }
}
