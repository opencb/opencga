package org.opencb.opencga.core.testclassification.duration;

/**
 * Tagging a test as 'short' means that the test class has the following characteristics:
 * <ul>
 *  <li>it can be run simultaneously with other short tests all in the same JVM</li>
 *  <li>ideally, the WHOLE implementing test-suite/class, no matter how many or how few test
 *  methods it has, should take less than 15 seconds to complete</li>
 *  <li>it does not use a services (mongodb, solr, hadoop, ...)</li>
 * </ul>
 *
 * @see MediumTests
 * @see LongTests
 */
public interface ShortTests {
}
