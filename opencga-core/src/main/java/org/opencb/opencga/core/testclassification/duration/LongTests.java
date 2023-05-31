package org.opencb.opencga.core.testclassification.duration;
/**
 * Tagging a test as 'long' means that the test class has the following characteristics:
 * <ul>
 *  <li>ideally, the whole large test-suite/class, no matter how many or how few test methods it
 *  has, will run in last less than three minutes</li>
 *  <li>No large test should take longer than ten minutes</li>
 *  <li>it can use any services (mongodb, solr, hadoop, ...)</li>
 * </ul>
 *
 * @see ShortTests
 * @see MediumTests
 */
public interface LongTests {
}
