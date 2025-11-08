package org.opencb.opencga.core.models.externalTool;

import junit.framework.TestCase;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.models.job.JobType;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

@Category(ShortTests.class)
public class ExternalToolTypeTest extends TestCase {

    // Test to ensure the all enum values are defined in JobType
    @Test
    public void testEnumValues() {
        for (ExternalToolType toolType : ExternalToolType.values()) {
            JobType.valueOf(toolType.name());
        }
        for (JobType value : JobType.values()) {
            if (value != JobType.NATIVE) {
                ExternalToolType.valueOf(value.name());
            }
        }
    }


}