package org.opencb.opencga.catalog.managers;

import org.junit.Test;
import org.opencb.opencga.core.config.Email;

public class MailManagerTest extends AbstractManagerTest {

    @Test
    public void sendErrorMessage() {
        Email emailConfig = new Email();
        emailConfig.setHost("");
    }

}
