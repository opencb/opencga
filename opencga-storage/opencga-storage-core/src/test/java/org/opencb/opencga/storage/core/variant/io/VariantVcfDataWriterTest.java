/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.core.variant.io;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by mh719 on 06/01/2017.
 */
public class VariantVcfDataWriterTest {

    private Variant createVariantSecAlt(String varString, String secAlt) {
        Variant secAltVar = new Variant(secAlt);
        Variant variant = new Variant(varString);
        StudyEntry se = new StudyEntry("1");
        AlternateCoordinate ac = new AlternateCoordinate(
                secAltVar.getChromosome(), secAltVar.getStart(), secAltVar.getEnd(),
                secAltVar.getReference(), secAltVar.getAlternate(), secAltVar.getType());
        se.setSecondaryAlternates(Collections.singletonList(ac));
        variant.setStudies(Collections.singletonList(se));
        return variant;
    }

    @Test
    public void adjustedVariantStart_SecAlt_MNV() throws Exception {
        StudyConfiguration sc = new StudyConfiguration(1, "1");
        VariantVcfDataWriter dw = new VariantVcfDataWriter(sc, null, null, null, null);
        Integer adjustStart = dw.adjustedVariantStart(createVariantSecAlt("1:123:A:C", "1:122:GG:CC")).getLeft();
        assertEquals("Adjusted start position wrong", Integer.valueOf(122), adjustStart);
    }

    @Test
    public void adjustedVariantStart_SecAlt_INDEL() throws Exception {
        StudyConfiguration sc = new StudyConfiguration(1, "1");
        VariantVcfDataWriter dw = new VariantVcfDataWriter(sc, null, null, null, null);
        Integer adjustStart = dw.adjustedVariantStart(createVariantSecAlt("1:123:A:C", "1:122:GG:-")).getLeft();
        assertEquals("Adjusted start position wrong", Integer.valueOf(121), adjustStart);
    }

    @Test
    public void buildAlleles_SecAlt_INDEL() throws Exception {
        StudyConfiguration sc = new StudyConfiguration(1, "1");
        VariantVcfDataWriter dw = new VariantVcfDataWriter(sc, null, null, null, null);

        Variant variant = createVariantSecAlt("1:123:A:C", "1:122:GGT:-");
        List<String> alles = dw.buildAlleles(variant, new ImmutablePair<>(121, 124));
        assertEquals("Missing alleles", 3, alles.size());
        assertEquals("Ref allele not correctly adjusted", "NNAN", alles.get(0));
        assertEquals("Ref allele not correctly adjusted", "NNCN", alles.get(1));
        assertEquals("Ref allele not correctly adjusted", "N", alles.get(2));
    }

    @Test
    public void buildAlleles_SecAlt_MNV() throws Exception {
        StudyConfiguration sc = new StudyConfiguration(1, "1");
        VariantVcfDataWriter dw = new VariantVcfDataWriter(sc, null, null, null, null);

        Variant variant = createVariantSecAlt("1:123:A:C", "1:122:GG:TT");
        List<String> alles = dw.buildAlleles(variant, new ImmutablePair<>(122, 123));
        assertEquals("Missing alleles", 3, alles.size());
        assertEquals("Ref allele not correctly adjusted", "NA", alles.get(0));
        assertEquals("Ref allele not correctly adjusted", "NC", alles.get(1));
        assertEquals("Ref allele not correctly adjusted", "TT", alles.get(2));
    }

}