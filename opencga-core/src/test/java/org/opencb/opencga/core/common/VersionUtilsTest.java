package org.opencb.opencga.core.common;

import org.junit.Assert;
import org.junit.Test;

public class VersionUtilsTest {
    public static String getComparation(String minVersion, String version) {
        int c = new VersionUtils.Version(minVersion).compareTo(new VersionUtils.Version(version));
        String comp;
        if (c == 0) {
            comp = "=";
        } else if (c < 0) {
            comp = "<";
        } else {
            comp = ">";
        }
        System.out.println(minVersion + "\t" + comp + "\t" + version);
        return comp;
    }

    @Test
    public void testOrder() {
        Assert.assertEquals("<", getComparation("5.2.7", "5.2.8"));
        Assert.assertEquals("=", getComparation("5.2.7", "5.2.7"));
        Assert.assertEquals(">", getComparation("5.2.7.1", "5.2.7.1-alpha"));
        Assert.assertEquals(">", getComparation("5.2.7", "5.2.7-SNAPSHOT"));
        Assert.assertEquals("<", getComparation("5.2.7-alpha", "5.2.7"));
        Assert.assertEquals("<", getComparation("5.2.7-alpha", "5.2.7-beta"));
        Assert.assertEquals(">", getComparation("5.2.7", "5.2.6"));
        Assert.assertEquals("=", getComparation("5.2.7", "5.2.7.0"));
    }
}