package org.opencb.opencga.analysis.clinical;

import org.junit.Test;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.biodata.models.clinical.interpretation.GenomicFeature;

import java.io.IOException;

import static org.junit.Assert.*;

public class ClinicalUtilsTest {

    @Test
    public void testMatchEvidence() throws IOException {
        // true
        ClinicalVariantEvidence ev1 = new ClinicalVariantEvidence();
        GenomicFeature gf1 = new GenomicFeature();
        gf1.setType("GENE");
        ev1.setGenomicFeature(gf1);
        ClinicalVariantEvidence ev2 = new ClinicalVariantEvidence();
        GenomicFeature gf2 = new GenomicFeature();
        gf2.setType("GENE");
        ev2.setGenomicFeature(gf2);
        assertTrue(ClinicalUtils.matchEvidence(ev1, ev2));

        // false
        gf2.setType("REGION");
        ev2.setGenomicFeature(gf2);
        assertFalse(ClinicalUtils.matchEvidence(ev1, ev2));

        // true
        gf1.setType("");
        gf1.setId("1");
        gf1.setTranscriptId("a");
        ev1.setGenomicFeature(gf1);
        gf2.setType("");
        gf2.setId("1");
        gf2.setTranscriptId("a");
        ev2.setGenomicFeature(gf2);
        assertTrue(ClinicalUtils.matchEvidence(ev1, ev2));

        // false
        gf2.setTranscriptId("b");
        ev2.setGenomicFeature(gf2);
        assertFalse(ClinicalUtils.matchEvidence(ev1, ev2));

        gf2.setTranscriptId("a");

        // true
        ev1.setPanelId("b");
        ev2.setPanelId("b");
        assertTrue(ClinicalUtils.matchEvidence(ev1, ev2));

        // false
        ev1.setPanelId("c");
        ev2.setPanelId("b");
        assertFalse(ClinicalUtils.matchEvidence(ev1, ev2));
    }


}