package org.opencb.opencga.storage.hadoop.variant.transform;

import org.junit.Test;
import org.opencb.biodata.models.variant.VariantBuilder;

import static org.junit.Assert.*;

/**
 * Created on 31/01/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantToVcfSliceConverterTaskTest {

    @Test
    public void isHomRefTest() throws Exception {
        assertTrue(VariantToVcfSliceConverterTask.isHomRef("0/0"));
        assertTrue(VariantToVcfSliceConverterTask.isHomRef("0|0"));
        assertTrue(VariantToVcfSliceConverterTask.isHomRef("0"));

        assertFalse(VariantToVcfSliceConverterTask.isHomRef("1/2"));
        assertFalse(VariantToVcfSliceConverterTask.isHomRef("0/2"));
    }

    @Test
    public void isRefVariantTest() throws Exception {
        assertTrue(VariantToVcfSliceConverterTask.isRefVariant(new VariantBuilder("1:100:A:C").setSampleDataKeys("GT").addSample("S1", "0/0").build()));
        assertTrue(VariantToVcfSliceConverterTask.isRefVariant(new VariantBuilder("1:100:A:C").setSampleDataKeys("GT").addSample("S1", "0/0").addSample("S2", "0").build()));

        assertFalse(VariantToVcfSliceConverterTask.isRefVariant(new VariantBuilder("1:100:A:C").setSampleDataKeys("GT").addSample("S1", "0/0").addSample("S2", "0/1").build()));
        assertFalse(VariantToVcfSliceConverterTask.isRefVariant(new VariantBuilder("1:100:A:C").setSampleDataKeys("GT").addSample("S1", "0/1").addSample("S2", "0/0").build()));
        assertFalse(VariantToVcfSliceConverterTask.isRefVariant(new VariantBuilder("1:100:A:C").setSampleDataKeys("GT").addSample("S1", "0/1").addSample("S2", "0/1").build()));
        assertFalse(VariantToVcfSliceConverterTask.isRefVariant(new VariantBuilder("1:100:A:C").setSampleDataKeys("GT").addSample("S1", "./.").build()));
    }
}