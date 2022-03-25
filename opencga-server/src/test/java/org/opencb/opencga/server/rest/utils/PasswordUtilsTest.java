package org.opencb.opencga.server.rest.utils;

import org.junit.Assert;
import org.junit.Test;
import org.opencb.opencga.core.common.PasswordUtils;

public class PasswordUtilsTest {

    @Test
    public void validateStrongRandomPassword() {
        Assert.assertTrue(PasswordUtils.isStrongPassword(PasswordUtils.getStrongRandomPassword()));
    }

    @Test
    public void validateInvalidPassword() {
        Assert.assertFalse(PasswordUtils.isStrongPassword("admin"));
    }

    @Test
    public void validateValidPassword() {
        Assert.assertTrue(PasswordUtils.isStrongPassword("adminOpencga2021."));
    }

    @Test
    public void validateRandomPassword() {
        Assert.assertNotEquals(PasswordUtils.getStrongRandomPassword(), PasswordUtils.getStrongRandomPassword());
    }
}