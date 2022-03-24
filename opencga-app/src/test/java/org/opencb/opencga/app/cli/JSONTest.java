package org.opencb.opencga.app.cli;

import org.junit.Test;
import org.opencb.commons.utils.DataModelsUtils;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;

public class JSONTest {


    @Test
    public void testJSONBean() throws Exception {
        SampleUpdateParams sup = new SampleUpdateParams();
        String json = DataModelsUtils.dataModelToJsonString(sup.getClass());
        System.out.println(json);
    }

}
