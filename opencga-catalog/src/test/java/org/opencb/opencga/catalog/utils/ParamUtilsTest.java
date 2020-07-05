package org.opencb.opencga.catalog.utils;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ParamUtilsTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void checkGetEnum() {
        Assert.assertEquals(ParamUtils.UpdateAction.ADD, ParamUtils.getEnum("ADD", ParamUtils.UpdateAction.class, null));
        Assert.assertEquals(ParamUtils.CompleteUpdateAction.ADD, ParamUtils.getEnum("ADD", ParamUtils.CompleteUpdateAction.class, null));
        Assert.assertEquals(ParamUtils.CompleteUpdateAction.ADD, ParamUtils.getEnum(ParamUtils.CompleteUpdateAction.ADD, ParamUtils.CompleteUpdateAction.class, null));
        Assert.assertEquals(ParamUtils.CompleteUpdateAction.ADD, ParamUtils.getEnum(ParamUtils.UpdateAction.ADD, ParamUtils.CompleteUpdateAction.class, null));
    }

    @Test()
    public void checkGetEnumFail() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Unknown value 'REPLACE'. Accepted values are: [ADD, SET, REMOVE]");
        ParamUtils.getEnum(ParamUtils.CompleteUpdateAction.REPLACE, ParamUtils.UpdateAction.class, null);
    }

}