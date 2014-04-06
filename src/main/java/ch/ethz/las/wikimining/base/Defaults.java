package ch.ethz.las.wikimining.base;

/**
 * List of defaults for MapReduce.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public enum Defaults {

  BANDS(9),
  BLOCK_SIZE(1_000_000),
  DIMENSIONS(-1),
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
