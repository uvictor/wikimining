package ch.ethz.las.wikimining.mr.base;

/**
 * List of field names for MapReduce.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public enum Fields {

  BANDS("bands"),
  BASIS("basis"),
  BUCKETS("buckets"),
  COMPRESSION("compression"),
  DIMENSIONS("dimensions"),
  DOC_DATES("dates"),
  DOCS_SUBSET("docs"),
  GRAPH("graph"),
  IGNORE("ignore"),
  INFO("info"),
  INPUT("input"),
  LANGUAGE("language"),
  OUTPUT("output"),
  OUTPUT_BUCKETS("outputBuckets"),
  PARTITION_COUNT("partitions"),
  TYPE("type"),
  REVISIONS("revisions"),
  ROWS("rows"),
  SECOND_STAT("secondStat"),
  SELECT_COUNT("select"),
  WORD_SPREAD("spread"),
  WORD_COUNT("wordCount"),
  WORD_COUNT_TYPE("wordCountType"),

  VALUE_INLINKS("inlinks"),
  VALUE_REVISIONS_COUNT("revcount"),
  VALUE_REVISIONS_VOLUME("revvolume"),
  ;

  private final String value;

  private Fields(String theValue) {
    value = theValue;
  }

  public String get() {
    return value;
  }

  @Override
  public String toString() {
    return value;
  }
}
