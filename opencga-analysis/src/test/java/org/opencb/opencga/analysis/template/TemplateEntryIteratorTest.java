package org.opencb.opencga.analysis.template;

import org.junit.Test;
import org.opencb.opencga.analysis.template.manager.TemplateEntryIterator;
import org.opencb.opencga.core.models.individual.IndividualUpdateParams;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class TemplateEntryIteratorTest {

    @Test
    public void testIterator() throws URISyntaxException {
        Path path = Paths.get(this.getClass().getClassLoader().getResource("template").toURI());
        try(TemplateEntryIterator<IndividualUpdateParams> iterator =
                new TemplateEntryIterator<>(path, "individuals", IndividualUpdateParams.class)) {
            while (iterator.hasNext()) {
                IndividualUpdateParams next = iterator.next();
                assertNotNull(next.getId());
                assertFalse(next.getSamples().isEmpty());
            }
        }
    }

}