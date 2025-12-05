package org.opencb.opencga.storage.core.variant.annotation.converters;

import junit.framework.TestCase;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.avro.Xref;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

@Category(ShortTests.class)
public class VariantAnnotationModelUtilsTest extends TestCase {

    public void testXrefsHgvs() throws Exception {
        VariantAnnotation variantAnnotation = new VariantAnnotation();
        variantAnnotation.setId("id");
        variantAnnotation.setXrefs(Collections.singletonList(new Xref("xref1", "source")));
        variantAnnotation.setHgvs(Arrays.asList(
                "ENST00000680783.1(ENSG00000135744):c.776T>C",
                "ENSP00000451720.1:p.Asn134Lys"
        ));
        ConsequenceType ct = new ConsequenceType();
        ct.setGeneName("GENE");
        ct.setHgvs(variantAnnotation.getHgvs());
        ct.setGeneId(null);
        variantAnnotation.setConsequenceTypes(Arrays.asList(ct, new ConsequenceType()));
        Set<String> xrefs = VariantAnnotationModelUtils.extractXRefs(variantAnnotation);

        assertEquals(7, xrefs.size());
        // Default fields
        assertTrue(xrefs.contains("id"));
        assertTrue(xrefs.contains("xref1"));
        assertTrue(xrefs.contains("GENE"));

        // Untouched hgvs, not starting with ENST or NM_
        assertTrue(xrefs.contains("ENSP00000451720.1:p.Asn134Lys"));

        assertTrue(xrefs.contains("ENST00000680783.1(ENSG00000135744):c.776T>C"));
        assertTrue(xrefs.contains("ENST00000680783.1:c.776T>C"));
        assertTrue(xrefs.contains("GENE:c.776T>C"));

    }
}