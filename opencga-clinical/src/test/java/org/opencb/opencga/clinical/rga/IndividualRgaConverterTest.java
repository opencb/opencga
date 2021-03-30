package org.opencb.opencga.clinical.rga;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Before;
import org.junit.Test;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByIndividual;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.clinical.rga.RgaUtilsTest.createKnockoutByIndividual;

public class IndividualRgaConverterTest {

    private IndividualRgaConverter converter;

    @Before
    public void setUp() throws Exception {
        converter = new IndividualRgaConverter();
    }

    @Test
    public void convertToDataModelType() throws JsonProcessingException {
        List<KnockoutByIndividual> knockoutByIndividualList = new ArrayList<>(2);
        knockoutByIndividualList.add(createKnockoutByIndividual(1));
        knockoutByIndividualList.add(createKnockoutByIndividual(2));

        List<RgaDataModel> rgaDataModels = converter.convertToStorageType(knockoutByIndividualList);
        List<KnockoutByIndividual> knockoutByIndividuals = converter.convertToDataModelType(rgaDataModels);

        assertEquals(knockoutByIndividualList.size(), knockoutByIndividuals.size());
        for (int i = 0; i < knockoutByIndividualList.size(); i++) {
            assertEquals(JacksonUtils.getDefaultObjectMapper().writeValueAsString(knockoutByIndividualList.get(i)),
                    JacksonUtils.getDefaultObjectMapper().writeValueAsString(knockoutByIndividuals.get(i)));
        }
    }

}