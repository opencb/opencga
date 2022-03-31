package org.opencb.opencga.server.generator.writers;

import org.junit.Test;
import org.opencb.opencga.server.generator.models.RestCategory;
import org.opencb.opencga.server.generator.models.RestEndpoint;

import static org.junit.Assert.*;

public class ParentClientRestApiWriterTest {

    @Test
    public void getCommandName() {
        String commandName = ParentClientRestApiWriter.getCommandName(
                new RestCategory().setPath("/{apiVersion}/operation"),
                new RestEndpoint().setPath("/{apiVersion}/operation/variant/secondaryIndex"));
        assertEquals("secondaryIndex-variant", commandName);
        assertEquals("variant-secondary-index", ParentClientRestApiWriter.reverseCommandName(commandName));
    }
}