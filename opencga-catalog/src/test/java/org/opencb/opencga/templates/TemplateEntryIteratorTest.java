package org.opencb.opencga.templates;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.catalog.templates.TemplateEntryIterator;
import org.opencb.opencga.core.models.individual.IndividualUpdateParams;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

@Category(ShortTests.class)
public class TemplateEntryIteratorTest {

    @Test
    public void testIndividualIterator() throws URISyntaxException {
        Path path = Paths.get(this.getClass().getClassLoader().getResource("templates").toURI());
        try(TemplateEntryIterator<IndividualUpdateParams> iterator =
                new TemplateEntryIterator<>(path, "individuals", IndividualUpdateParams.class)) {
            while (iterator.hasNext()) {
                IndividualUpdateParams next = iterator.next();
                assertNotNull(next.getId());
                assertFalse(next.getSamples().isEmpty());
            }
        }
    }

    @Test
    public void testSampleIterator() throws URISyntaxException {
        Path path = Paths.get(this.getClass().getClassLoader().getResource("templates").toURI());
        try(TemplateEntryIterator<SampleUpdateParams> iterator =
                    new TemplateEntryIterator<>(path, "samples", SampleUpdateParams.class)) {
            while (iterator.hasNext()) {
                SampleUpdateParams next = iterator.next();
                assertNotNull(next.getId());
//                assertFalse(next.getSamples().isEmpty());
            }
        }
    }

}