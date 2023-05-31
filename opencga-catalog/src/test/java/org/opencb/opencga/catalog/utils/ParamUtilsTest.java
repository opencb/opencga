package org.opencb.opencga.catalog.utils;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

@Category(ShortTests.class)
public class ParamUtilsTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void checkGetEnum() {
        Assert.assertEquals(ParamUtils.BasicUpdateAction.ADD, ParamUtils.getEnum("ADD", ParamUtils.BasicUpdateAction.class, null));
        Assert.assertEquals(ParamUtils.CompleteUpdateAction.ADD, ParamUtils.getEnum("ADD", ParamUtils.CompleteUpdateAction.class, null));
        Assert.assertEquals(ParamUtils.CompleteUpdateAction.ADD, ParamUtils.getEnum(ParamUtils.CompleteUpdateAction.ADD, ParamUtils.CompleteUpdateAction.class, null));
        Assert.assertEquals(ParamUtils.CompleteUpdateAction.ADD, ParamUtils.getEnum(ParamUtils.BasicUpdateAction.ADD, ParamUtils.CompleteUpdateAction.class, null));
    }

    @Test()
    public void checkGetEnumFail() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Unknown value 'REPLACE'. Accepted values are: [ADD, SET, REMOVE]");
        ParamUtils.getEnum(ParamUtils.CompleteUpdateAction.REPLACE, ParamUtils.BasicUpdateAction.class, null);
    }

}