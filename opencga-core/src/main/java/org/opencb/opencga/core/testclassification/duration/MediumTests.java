package org.opencb.opencga.core.testclassification.duration;

/**
 * Tagging a test as 'medium' means that the test class has the following characteristics:
 * <ul>
 *  <li>ideally, the WHOLE implementing test-suite/class, no matter how many or how few test
 *  methods it has, should take less than 50 seconds to complete</li>
 *  <li>it does not use heavy services (solr, hadoop, ...), only mongodb allowed</li>
 * </ul>
 *
 * @see ShortTests
 * @see LongTests
 */
public interface MediumTests {
}
