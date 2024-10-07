package org.opencb.opencga.catalog.utils;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import static org.junit.Assert.*;

@Category(ShortTests.class)
public class FqnUtilsTest {

    @Test
    public void testFqnFull() {
        FqnUtils.FQN fqn = new FqnUtils.FQN("org@project:study");
        assertEquals("org", fqn.getOrganization());
        assertEquals("project", fqn.getProject());
        assertEquals("org@project", fqn.getProjectFqn());
        assertEquals("study", fqn.getStudy());
        assertEquals("org@project:study", fqn.toString());
    }

    @Test
    public void testFqnPartial() {
        FqnUtils.FQN fqn = new FqnUtils.FQN("org@project");
        assertEquals("org", fqn.getOrganization());
        assertEquals("project", fqn.getProject());
        assertEquals("org@project", fqn.getProjectFqn());
        assertEquals(null, fqn.getStudy());
        assertEquals("org@project", fqn.toString());
    }
}