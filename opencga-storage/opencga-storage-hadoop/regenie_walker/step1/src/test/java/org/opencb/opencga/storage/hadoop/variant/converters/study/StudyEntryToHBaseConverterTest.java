package org.opencb.opencga.storage.hadoop.variant.converters.study;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * Created on 10/10/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(ShortTests.class)
public class StudyEntryToHBaseConverterTest {

    @Test
    public void trimLeadingNullValues() {
        assertEquals(0, StudyEntryToHBaseConverter.trimLeadingNullValues(Arrays.asList(null, null), 0).size());
        assertEquals(1, StudyEntryToHBaseConverter.trimLeadingNullValues(Arrays.asList(null, null), 1).size());

        assertEquals(0, StudyEntryToHBaseConverter.trimLeadingNullValues(Arrays.asList("", ""), 0).size());
        assertEquals(1, StudyEntryToHBaseConverter.trimLeadingNullValues(Arrays.asList("", ""), 1).size());

        assertEquals(2, StudyEntryToHBaseConverter.trimLeadingNullValues(Arrays.asList("a", "b"), 0).size());
        assertEquals(2, StudyEntryToHBaseConverter.trimLeadingNullValues(Arrays.asList("a", "b"), 1).size());
        assertEquals(2, StudyEntryToHBaseConverter.trimLeadingNullValues(Arrays.asList("a", "b"), 2).size());

        assertEquals(2, StudyEntryToHBaseConverter.trimLeadingNullValues(Arrays.asList("a", "b", null, null, null), 1).size());
        assertEquals(2, StudyEntryToHBaseConverter.trimLeadingNullValues(Arrays.asList("a", "b", null, null, null), 2).size());
        assertEquals(3, StudyEntryToHBaseConverter.trimLeadingNullValues(Arrays.asList("a", "b", null, null, null), 3).size());
        assertEquals(4, StudyEntryToHBaseConverter.trimLeadingNullValues(Arrays.asList("a", "b", null, null, null), 4).size());
        assertEquals(5, StudyEntryToHBaseConverter.trimLeadingNullValues(Arrays.asList("a", "b", null, null, null), 5).size());
        assertEquals(5, StudyEntryToHBaseConverter.trimLeadingNullValues(Arrays.asList("a", "b", null, null, null), 9).size());

        assertEquals(6, StudyEntryToHBaseConverter.trimLeadingNullValues(Arrays.asList("a", "b", null, null, null, "a"), 1).size());
    }
}