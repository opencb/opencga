package org.opencb.opencga.catalog.managers;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.config.Email;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

@Category(MediumTests.class)
public class MailManagerTest extends AbstractManagerTest {

    @Test
    public void sendErrorMessage() {
        Email emailConfig = new Email();
        emailConfig.setHost("");
    }

}
