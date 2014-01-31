package ch.ethz.las.wikimining.mr.base;

/**
 * List of defaults for MapReduce.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public enum Defaults {

  DIMENSIONS(-1),
  BANDS(9),
  PARTITION_COUNT(10),
  ROWS(13),
  SELECT_COUNT(4),
  ;

  private final int value;

  private Defaults(int theValue) {
    value = theValue;
  }

  public int get() {
    return value;
  }
}
