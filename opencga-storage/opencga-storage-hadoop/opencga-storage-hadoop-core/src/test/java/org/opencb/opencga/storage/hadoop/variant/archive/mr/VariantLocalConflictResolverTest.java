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

package org.opencb.opencga.storage.hadoop.variant.archive.mr;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.OriginalCall;
import org.opencb.biodata.tools.variant.VariantNormalizer;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantType;

import java.util.*;
import java.util.stream.Collectors;

import static htsjdk.variant.vcf.VCFConstants.GENOTYPE_FILTER_KEY;
import static htsjdk.variant.vcf.VCFConstants.GENOTYPE_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.opencb.biodata.models.variant.StudyEntry.FILTER;
import static org.opencb.biodata.models.variant.StudyEntry.QUAL;
import static org.opencb.biodata.models.variant.avro.VariantType.INDEL;
import static org.opencb.biodata.models.variant.avro.VariantType.NO_VARIATION;

/**
 * Created by mh719 on 15/06/2016.
 */
public class VariantLocalConflictResolverTest {
    private VariantNormalizer normalizer;

//    public void testFindConflictAlternates() {
//        Variant a1 = getVariantFilter("2:10048156:T:T", "PASS");
//        Variant a2 = getVariantFilter("2:10048156:T:T", "PASS");
//        NavigableSet<Variant> b;
//        Map<Variant, Variant> c;
//        Map<Variant, List<Variant>> d;
//        VariantLocalConflictResolver.findConflictAlternates(a1, b, c, d);
//    }

    @Before
    public void setUp() throws Exception {
        normalizer = new VariantNormalizer(false)
                .setGenerateReferenceBlocks(true);
    }

    @Test
    public void resolveConflicts() throws Exception {
        Variant a = getVariant("2:10048150:C:C", "PASS", ".", "0/1");
        Variant b = getVariant("2:10048155:TCTTTTTTTT:AC", "SiteConflict", "220", "0/1");
        Variant c = getVariant("2:10048156:T:T", "PASS", ".", "0/1");
        Variant d = getVariant("2:10048157:C:C", "PASS", ".", "0/1");
        Variant e = getVariant("2:10048187:C:C", "PASS", ".", "0/1");
        List<Variant> resolved = new VariantLocalConflictResolver().resolveConflicts(Arrays.asList(a, b, c, d, e));
        assertEquals(6, resolved.size());
//        assertEquals(a, resolved.get(0));
    }


    @Test
    public void resolveRefRegionWithIndel() throws Exception {
        Variant a = getVariant("1:10230:AC:A", "TruthSensitivityTranche99.90to100.00", "1065.12", "0/1");
        Variant b = getVariant("2:10231:C:.", "PASS", "87.71", "0");
//        b.setType(VariantType.NO_VARIATION);
        VariantType ta = a.getType();
        VariantType tb = b.getType();
        Collection<Variant> resolved = new VariantLocalConflictResolver().resolveConflicts(Arrays.asList(a, b));
        assertEquals(2, resolved.size());
        assertEquals(true, resolved.contains(a));
    }

    @Test
    public void resolveRefRegion() throws Exception {
        Variant a = addAttribute(getVariantFilter("2:10048155:TCTTTTTTTT:AC", "PASS"), QUAL, "220");
        Variant b = addAttribute(getVariantFilter("2:10048156:T", "SiteConflict"), QUAL, ".");
        Variant c = addAttribute(getVariantFilter("2:10048157:C", "PASS"), QUAL, ".");
        List<Variant> resolved = new ArrayList<>(new VariantLocalConflictResolver().resolveConflicts(Arrays.asList(a, b)));
        assertEquals(1, resolved.size());
        assertEquals(a, resolved.get(0));
    }

    @Test
    public void resolveRefRegion2() throws Exception {
        Variant a = getVariant("2:10048155:TCTTTTTTTT:AC", "SiteConflict", "220", "0/1");
        Variant b = getVariant("2:10048156:T:T", "PASS", ".", "0/1");
        Variant c = getVariant("2:10048157:C:C", "PASS", ".", "0/1");
        Collection<Variant> resolved = new VariantLocalConflictResolver().resolveConflicts(Arrays.asList(a, b, c));
        assertEquals(4, resolved.size());
//        assertEquals(a, resolved.get(0));
    }

    @Test
    public void resolveSameVariantWithSecAlt() throws Exception {
        Variant a = getVariant("2:10048155:TCTTTTTTTT:AC", "PASS", "220", "1/2");
        Variant b = getVariant("2:10048155:TCTTTTTTTT:-", "PASS", "220", "2/1");
        a.getStudies().get(0).getSecondaryAlternates().add(new AlternateCoordinate("2", b.getStart(), b.getEnd(), b.getReference(), b.getAlternate(), INDEL));
        a.getStudies().get(0).getFiles().get(0).setCall(new OriginalCall("2:10048155:TTCTTTTTTTT:TAC,T", 0));
        b.getStudies().get(0).getSecondaryAlternates().add(new AlternateCoordinate("2", a.getStart(), a.getEnd(), a.getReference(), a.getAlternate(), INDEL));
        b.getStudies().get(0).getFiles().get(0).setCall(new OriginalCall("2:10048155:TTCTTTTTTTT:TAC,T", 1));

        Collection<Variant> resolved = new VariantLocalConflictResolver().resolveConflicts(Arrays.asList(a, b));
        assertEquals(1, resolved.size());
    }

    @Test
    public void resolveSameVariantWithSecAltInsertion() throws Exception {
        Variant a = getVariant("2:10048155:-:AT", "PASS", "220", "1/2");
        Variant b = getVariant("2:10048155:ATATATATATAT:-", "PASS", "220", "2/1");
        a.getStudies().get(0).getSecondaryAlternates().add(new AlternateCoordinate("2", b.getStart(), b.getEnd(), b.getReference(), b.getAlternate(), INDEL));
        b.getStudies().get(0).getSecondaryAlternates().add(new AlternateCoordinate("2", a.getStart(), a.getEnd(), a.getReference(), a.getAlternate(), INDEL));
        List<Variant> resolved = new ArrayList<>(new VariantLocalConflictResolver().resolveConflicts(Arrays.asList(a, b)));
        System.out.println("a.toString() = " + a.toString());
        System.out.println("b.getStudies().get(0).getSecondaryAlternates().get(0).toString() = " + b.getStudies().get(0).getSecondaryAlternates().get(0).toString());
        assertEquals(1, resolved.size());
        assertEquals(1, resolved.get(0).getStudies().get(0).getSecondaryAlternates().size());
        assertEquals("1/2", resolved.get(0).getStudies().get(0).getSampleData(0).get(0));
    }

    @Test
    public void resolveRefOverlap() throws Exception {
        Variant a = getVariant("2:10048155-10048156:", "PASS", "220", "0/0");
        a.setType(NO_VARIATION);
        Variant b = getVariant("2:10048155:AAA:-", "PASS", "220", "0/1");
        System.out.println("a.toString() = " + a.toJson());
        System.out.println("b.toString() = " + b.toJson());
        Collection<Variant> resolved = new VariantLocalConflictResolver().resolveConflicts(Arrays.asList(a, b));
        resolved.forEach(res -> System.out.println("res.toJson() = " + res.toJson()));
        assertEquals(1, resolved.size());
    }

    @Test
    public void resolveConflictInserions() throws Exception {
        Variant a = getVariant("1:10048155:-:AT", "PASS", "220", "0/1");
        Variant b = getVariant("1:10048155:-:ATT", "PASS", "220", "0/1");
        Variant c = getVariant("1:10048155:-:ATTT", "PASS", "220", "0/1");
        Collection<Variant> resolved = new VariantLocalConflictResolver().resolveConflicts(Arrays.asList(a, b, c));
        assertEquals(1, resolved.size());
    }

    @Test
    public void resolveConflictIndelCase1() throws Exception {
        // 1:328:CTTCTT:CCTT,CTTCTTTC
        // 1:328:CTTCTT:CCTT ~ 1:328:CTT:C
        // 1:328:CTTCTT:CTTCTTTC  ~? 1:331:CTT:CTTTC ~ 1:334:-:TC
        Variant v1 = new Variant("1:328:CTT:C");
        StudyEntry se = new StudyEntry("1");
        se.setFiles(Collections.singletonList(new FileEntry("1", null, new HashMap<>())));
        v1.setStudies(Collections.singletonList(se));
        se.setSampleDataKeys(Arrays.asList(GENOTYPE_KEY, GENOTYPE_FILTER_KEY));
        se.setSamplesPosition(asMap("S1", 0));
        se.addSampleData("S1", Arrays.asList("1/2", "LowGQXHetDel"));
        se.getSecondaryAlternates().add(new AlternateCoordinate(null, null, 328, "CTT", "CTTTC", INDEL));
        addAttribute(v1, FILTER, "LowGQXHetDel");


        Variant v2 = new Variant("1:331:N:TCN");
        se = new StudyEntry("1");
        se.setFiles(Collections.singletonList(new FileEntry("1", null, new HashMap<>())));
        v2.setStudies(Collections.singletonList(se));
        se.setSamplesPosition(asMap("S1", 0));
        se.setSampleDataKeys(Arrays.asList(GENOTYPE_KEY, GENOTYPE_FILTER_KEY));
        se.addSampleData("S1", Arrays.asList("0/1", "PASS"));
        addAttribute(v2, FILTER, "PASS");

        System.out.println("v1.toJson() = " + v1.toJson());
        System.out.println("v2.toJson() = " + v2.toJson());
        System.out.println();
        List<Variant> variants = new VariantNormalizer().normalize(Arrays.asList(v1, v2), false);
        variants.forEach(norm -> System.out.println("norm.toJson() = " + norm.toJson()));
        System.out.println();
        Collection<Variant> resolved = new VariantLocalConflictResolver().resolveConflicts(variants);
        resolved.forEach(res -> System.out.println("res = " + res.toString() + " call: " + res.getStudies().get(0).getFiles().get(0).getCall()));

        List<Variant> resVar = resolved.stream()
                .filter(v -> !v.getType().equals(NO_VARIATION))
                .collect(Collectors.toList());

        assertEquals(1, resVar.size());
    }

    private Map<String, Integer> asMap(String s1, int i) {
        Map<String, Integer> map = new HashMap<>();
        map.put(s1, i);
        return map;
    }

    @Test
    public void resolve_SNP_REF() throws Exception {
        Variant a = getVariant("1:100:A:T", "PASS", "731", "0/1");
        Variant b = getVariant("1:100-103:AAAA", "PASS", "390", "0/0");
        b.setType(NO_VARIATION);
        Collection<Variant> resolved = new VariantLocalConflictResolver().resolveConflicts(Arrays.asList(a, b));
        assertEquals(2, resolved.size());
    }

    @Test
    public void resolve_INS_REF_Split() throws Exception {
        Variant a = addGTAndFilter(addAttribute(getVariantFilter("1:102::TTT", "PASS"), QUAL, "731"), "0/1", "PASS");
        Variant b = addGTAndFilter(addAttribute(getVariantFilter("1:100-103:AAAA", "PASS"), QUAL, "390"), "0/0", "PASS");
        b.setType(NO_VARIATION);
        List<Variant> resolved = new ArrayList<>(new VariantLocalConflictResolver().resolveConflicts(Arrays.asList(a, b)));
        assertEquals(3, resolved.size());
        assertEquals("SiteConflict", resolved.get(2).getStudies().get(0).getSampleData("1", GENOTYPE_FILTER_KEY));
    }

    @Test
    public void resolve_INS_REF() throws Exception {
        Variant a = addAttribute(getVariantFilter("1:100:-:GGTTG", "PASS"), QUAL, "731");
        Variant b = addAttribute(getVariantFilter("1:100-103:AAAA", "PASS"), QUAL, "390");
        b.setType(NO_VARIATION);
        Collection<Variant> resolved = new VariantLocalConflictResolver().resolveConflicts(Arrays.asList(a, b));
        assertEquals(2, resolved.size());
    }

    @Test
    public void resolve_INS_SNP() throws Exception {
        Variant a = getVariant("1:100:-:GGTTG", "PASS", "390", "0/1");
        Variant b = getVariant("1:100:A:T", "PASS", "390", "0/1");
        Variant c = getVariant("1:99:G:C", "PASS", "390", "0/1");
        b.setType(NO_VARIATION);
        Collection<Variant> resolved = new VariantLocalConflictResolver().resolveConflicts(Arrays.asList(a, b, c));
        assertEquals(3, resolved.size());
    }

    @Test
    public void resolve_DEL_DEL_SEC_ALT_normalized() throws Exception {
        List<Variant> normalized;
        normalizer.setGenerateReferenceBlocks(false);

        Variant a = getVariant("1:99:N:NGG,NGA", "lowqual", "392", "1/2");
        normalized = normalizer.apply(Collections.singletonList(a));
        Variant b = getVariant("1:99:N:NGG,NGT", "PASS", "391", "1/2");
        normalized.addAll(normalizer.apply(Collections.singletonList(b)));

        List<Variant> resolved = new VariantLocalConflictResolver().resolveConflicts(normalized);
        System.out.println("resolved = " + resolved);
        assertEquals(1, resolved.size());
        assertSame(normalized.get(0), resolved.get(0));
        System.out.println("resolved.get(0).toJson(); = " + resolved.get(0).toJson());
    }

    @Test
    public void resolve_DEL_DEL_SEC_ALT() throws Exception {
        Variant a = getVariant("1:100:-:GG", "PASS", "390", "1/2");
        Variant b = getVariant("1:100:-:GG", "PASS", "390", "1/2");
        Variant c = getVariant("1:100:-:GA", "PASS", "390", "1/2"); // mix with a
        Variant d = getVariant("1:100:-:GT", "PASS", "390", "1/2"); // mix with b

        a.getStudies().get(0).getSecondaryAlternates().add(
                new AlternateCoordinate(c.getChromosome(), c.getStart(), c.getEnd(), c.getReference(), c.getAlternate(),
                        INDEL));
        c.getStudies().get(0).getSecondaryAlternates().add(
                new AlternateCoordinate(a.getChromosome(), a.getStart(), a.getEnd(), a.getReference(), a.getAlternate(),
                        INDEL));

        b.getStudies().get(0).getSecondaryAlternates().add(
                new AlternateCoordinate(d.getChromosome(), d.getStart(), d.getEnd(), d.getReference(), d.getAlternate(),
                        INDEL));
        d.getStudies().get(0).getSecondaryAlternates().add(
                new AlternateCoordinate(b.getChromosome(), b.getStart(), b.getEnd(), b.getReference(), b.getAlternate(),
                        INDEL));
        Collection<Variant> resolved = new VariantLocalConflictResolver().resolveConflicts(Arrays.asList(a, b, c, d));
        System.out.println("resolved = " + resolved);
        assertEquals(1, resolved.size());
    }


    @Test
    public void resolve_DEL_REF() throws Exception {
        Variant a = getVariant("1:997:AT:-", "PASS", "390", "0/1");
        Variant b = getVariant("1:997:AT:-", "PASS", "390", "0/0");
        b.setEnd(1010);
        b.setType(NO_VARIATION);
        List<Variant> resolved = new VariantLocalConflictResolver().resolveConflicts(Arrays.asList(a, b));
        assertEquals(2, resolved.size());
        System.out.println("resolved = " + resolved);
    }

    @Test
    public void resolve_INS_SNP_SEC_ALT_normalized() throws Exception {
        // Original variant: 1:100:TT:GGTTGTT,TTAGGA
        // Normalized :
        //  1:100:-:GGTTG
        //  1:100:G:-
        //  1:102:-:AGGA
        Variant v = getVariant("1:100:TT:GGTTGTT,TTAGGA", "PASS", "390", "1/2");
        List<Variant> apply = normalizer.apply(Collections.singletonList(v));

        Collection<Variant> resolved = new VariantLocalConflictResolver().resolveConflicts(apply);
        assertEquals(2, resolved.size());
        System.out.println("resolved = " + resolved);
    }

    @Test
    public void resolve_INS_SNP_SEC_ALT() throws Exception {
        Variant a = getVariant("1:100:-:GGTTG", "PASS", "390", "1/2");
        Variant b = getVariant("1:100:G:-", "PASS", "390", "0/0");
        Variant c = getVariant("1:102:-:AGGA", "PASS", "390", "0/1");
        b.setType(NO_VARIATION);
        a.getStudies().get(0).getSecondaryAlternates().add(
                new AlternateCoordinate(c.getChromosome(), c.getStart(), c.getEnd(), c.getReference(), c.getAlternate(),
                        INDEL));
        a.getStudies().get(0).getFiles().get(0).setCall(new OriginalCall("1:100:TT:GGTTGTT,TTAGGA", 0));
        b.getStudies().get(0).getFiles().get(0).setCall(new OriginalCall("1:100:TT:GGTTGTT,TTAGGA", 0));
        c.getStudies().get(0).getFiles().get(0).setCall(new OriginalCall("1:100:TT:GGTTGTT,TTAGGA", 1));
        Collection<Variant> resolved = new VariantLocalConflictResolver().resolveConflicts(Arrays.asList(a, b, c));
        assertEquals(2, resolved.size());
    }

    @Test
    public void resolvePass() throws Exception {
        Variant a = addAttribute(getVariantFilter("1:5731287:C:-", "PASS"), QUAL, "731");
        Variant b = addAttribute(getVariantFilter("1:5731287:C:G", "SiteConflict"), QUAL, "390");
        List<Variant> resolved = new ArrayList<>(new VariantLocalConflictResolver().resolveConflicts(Arrays.asList(a, b)));
        assertEquals(1, resolved.size());
        assertEquals(a, resolved.get(0));
    }

    @Test
    public void resolveStrangeSet() throws Exception {
        Variant b = addAttribute(getVariantFilter("1:5731287:C:G", "SiteConflict"), QUAL, "390");
        Collection<Variant> resolved = new VariantLocalConflictResolver().resolveConflicts(Arrays.asList(b));
        assertEquals(1, resolved.size());
    }

    @Test
    public void resolveQUality() throws Exception {
        Variant a = addAttribute(getVariantFilter("1:5731287:C:-", "PASS"), QUAL, "731");
        Variant b = addAttribute(getVariantFilter("1:5731287:C:G", "PASS"), QUAL, "1390");
        List<Variant> resolved = new ArrayList<>(new VariantLocalConflictResolver().resolveConflicts(Arrays.asList(a, b)));
        assertEquals(1, resolved.size());
        assertEquals(b, resolved.get(0));
    }

    @Test
    public void getMissingRegionsBeforeOutside() throws Exception {
        Variant a = getVariant("1:1000:A:T");
        Variant b = getVariant("1:1002:A:T");
        Variant indel = getVariant("1:999:-:T");
        List<Pair<Integer, Integer>> missingRegions = VariantLocalConflictResolver.getMissingRegions(Arrays.asList
                (new Variant[]{a, b}), indel);
        System.out.println("missingRegions = " + missingRegions);
        assertEquals(0, missingRegions.size());
    }

    @Test
    public void getMissingRegionsAfterOutside() throws Exception {
        Variant a = getVariant("1:1000:A:T");
        a.setType(NO_VARIATION);
        Variant b = getVariant("1:1002:A:T");
        b.setType(NO_VARIATION);
        Variant snp = getVariant("1:1003:A:T");
        System.out.println("a = " + a.toJson());
        System.out.println("b = " + b.toJson());
        System.out.println("snp = " + snp.toJson());
        List<Pair<Integer, Integer>> missingRegions = VariantLocalConflictResolver.getMissingRegions(Arrays.asList
                (new Variant[]{a, b}), snp);
        System.out.println("missingRegions = " + missingRegions);
        assertEquals(1, missingRegions.size());
    }

    @Test
    public void getMissingRegionsIndel() throws Exception {
        Variant a = getVariant("1:1000:A:T");
        Variant b = getVariant("1:1002:A:T");
        Variant indel = getVariant("1:1000:ATA:-");
        List<Pair<Integer, Integer>> missingRegions =
                VariantLocalConflictResolver.getMissingRegions(Arrays.asList(new Variant[]{a, b}), indel);
        assertEquals(1, missingRegions.size());
        assertEquals(new ImmutablePair<Integer, Integer>(1001, 1001), missingRegions.get(0));
    }

    @Test
    public void getMissingRegionsStart() throws Exception {
        Variant a = getVariant("1:1000:A:T");
        Variant b = getVariant("1:1002:A:T");
        Variant indel = getVariant("1:999:ATA:-");
        List<Pair<Integer, Integer>> missingRegions =
                VariantLocalConflictResolver.getMissingRegions(Arrays.asList(new Variant[]{a, b}), indel);
        assertEquals(2, missingRegions.size());
        assertEquals(new ImmutablePair<Integer, Integer>(999, 999), missingRegions.get(0));
        assertEquals(new ImmutablePair<Integer, Integer>(1001, 1001), missingRegions.get(1));
    }

    @Test
    public void getMissingRegionsEnd() throws Exception {
        Variant a = getVariant("1:1000:A:T");
        Variant b = getVariant("1:1002:A:T");
        Variant indel = getVariant("1:1001:ATA:-");
        List<Pair<Integer, Integer>> missingRegions =
                VariantLocalConflictResolver.getMissingRegions(Arrays.asList(new Variant[]{a, b}), indel);
        assertEquals(2, missingRegions.size());
        assertEquals(new ImmutablePair<Integer, Integer>(1001, 1001), missingRegions.get(0));
        assertEquals(new ImmutablePair<Integer, Integer>(1003, 1003), missingRegions.get(1));
    }

    @Test
    public void hasConflictOverlap_SNP_SNP() throws Exception {
        String[] vars = {"1:1000:A:T", "1:1002:A:T"};
        conflictOverlap(vars, "1:999:A:T", false);
        conflictOverlap(vars, "1:1000:A:T", true);
        conflictOverlap(vars, "1:1001:A:T", false);
        conflictOverlap(vars, "1:1002:A:T", true);
        conflictOverlap(vars, "1:1003:A:T", false);
    }

    @Test
    public void hasConflictOverlap_SNP_DEL() throws Exception {
        String[] vars = {"1:1000:A:T", "1:1002:A:T"};
        conflictOverlap(vars, "1:998:AT:-", false);
        conflictOverlap(vars, "1:999:AT:-", true);
        conflictOverlap(vars, "1:1000:AT:-", true);
        conflictOverlap(vars, "1:1001:AT:-", true);
        conflictOverlap(vars, "1:1002:AT:-", true);
        conflictOverlap(vars, "1:1003:AT:-", false);
    }

    @Test
    public void hasConflictOverlap_DEL_SNP() throws Exception {
        conflictOverlap("1:1000:AT:-", "1:999:A:T", false);
        conflictOverlap("1:1000:AT:-", "1:1000:A:T", true);
        conflictOverlap("1:1000:AT:-", "1:1001:A:T", true);
        conflictOverlap("1:1000:AT:-", "1:1002:A:T", false);
    }

    @Test
    public void hasConflictOverlap_DEL_DEL() throws Exception {
        conflictOverlap("1:1000:AT:-", "1:998:AT:-", false);
        conflictOverlap("1:1000:AT:-", "1:999:AT:-", true);
        conflictOverlap("1:1000:AT:-", "1:1000:AT:-", true);
        conflictOverlap("1:1000:AT:-", "1:1001:AT:-", true);
        conflictOverlap("1:1000:AT:-", "1:1002:AT:-", false);
    }

    @Test
    public void hasConflictOverlap_SNP_INS() throws Exception {
        conflictOverlap("1:1000:A:T", "1:999::AT", false);
        conflictOverlap("1:1000:A:T", "1:1000::AT", false);
        conflictOverlap("1:1000:A:T", "1:1001::AT", false);
        conflictOverlap("1:1000:A:T", "1:1002::AT", false);
    }

    @Test
    public void hasConflictOverlap_DEL_INS() throws Exception {
        conflictOverlap("1:1000:AT:-", "1:999::AT", false);
        conflictOverlap("1:1000:AT:-", "1:1000::AT", false);
        conflictOverlap("1:1000:AT:-", "1:1001::AT", true);
        conflictOverlap("1:1000:AT:-", "1:1002::AT", false);
        conflictOverlap("1:1000:AT:-", "1:1003::AT", false);
    }

    @Test
    public void hasConflictOverlap_INS_DEL() throws Exception {
        conflictOverlap("1:1000::AT", "1:997:AT:-", false);
        conflictOverlap("1:1000::AT", "1:998:AT:-", false);
        conflictOverlap("1:1000::AT", "1:999:AT:-", true);
        conflictOverlap("1:1000::AT", "1:1000:AT:-", false);

        conflictOverlap("1:1000::AT", "1:998:ATA:-", true);
        conflictOverlap("1:1000::AT", "1:999:ATA:-", true);
        conflictOverlap("1:1000::AT", "1:1000:ATA:-", false);
    }


    @Test
    public void hasConflictOverlap_INS_INS() throws Exception {
        conflictOverlap("1:1000::AT", "1:999::AT", false);
        conflictOverlap("1:1000::AT", "1:1000::AT", true);
        conflictOverlap("1:1000::AT", "1:1000::ATTTATAATAT", true);
        conflictOverlap("1:1000::AT", "1:1001::AT", false);
    }

    public void conflictOverlap(String varA, String varB, boolean hasOverlap) {
        conflictOverlap(new String[]{varA}, varB, hasOverlap);
    }

    public void conflictOverlap(String[] varA, String varB, boolean hasOverlap) {
        Variant b = new Variant(varB);
        Map<Variant, List<VariantLocalConflictResolver.Alternate>> map = Arrays.stream(varA)
                .map(Variant::new)
                .flatMap(v -> VariantLocalConflictResolver.expandToVariants(v)
                        .stream()
                        .map(e -> new ImmutablePair<>(e, v)))
                .collect(Collectors.groupingBy(Pair::getRight, Collectors.mapping(Pair::getLeft, Collectors.toList())));
        map.put(b, VariantLocalConflictResolver.expandToVariants(b));

        List<Variant> collect = Arrays.stream(varA).map(Variant::new).collect(Collectors.toList());
        assertEquals(hasOverlap,
                VariantLocalConflictResolver.hasAnyConflictOverlapInclSecAlt(collect, b, map));
    }

    @Test
    public void min() throws Exception {
        Variant[] var = new Variant[]{new Variant("1:1000:A:T"), new Variant("1:1002:A:T")};
        int min = VariantLocalConflictResolver.min(Arrays.asList(var));
        assertEquals(1000, min);
    }

    @Test
    public void max() throws Exception {
        Variant[] var = new Variant[]{new Variant("1:1000:ATTTTT:T"), new Variant("1:1002:A:T")};
        int max = VariantLocalConflictResolver.max(Arrays.asList(var));
        assertEquals(1005, max);
    }

    @Test
    public void checkSiteConflict() throws Exception {
        Variant a = getVariantFilter("1:1000:A:T", "OtherRandom;SiteConflict;Stuff;");
        Variant b = getVariantFilter("1:1000:A:T", "PASS");
        assertEquals(0, VariantLocalConflictResolver.checkSiteConflict(a, a));
        assertEquals(0, VariantLocalConflictResolver.checkSiteConflict(b, b));
        assertEquals(-1, VariantLocalConflictResolver.checkSiteConflict(a, b));
        assertEquals(1, VariantLocalConflictResolver.checkSiteConflict(b, a));
    }

    public static Variant getVariantFilter(String var, String filter) {
        return getVariantAttribute(var, FILTER, filter);
    }

    protected Variant getVariantQuality(String var, String filter) {
        return getVariantAttribute(var, QUAL, filter);
    }

    public static Variant getVariant(String variantStr, String filter, String qual, String gt) {
        return getVariant(variantStr, filter, qual, null, gt);
    }
    public static Variant getVariant(String variantStr, String filter, String qual, Map<String, String> info, String gt) {
        Variant v = addGT(addAttribute(getVariantFilter(variantStr, filter), QUAL, qual), gt);
        if (info != null) {
            for (Map.Entry<String, String> entry : info.entrySet()) {
                addAttribute(v, entry.getKey(), entry.getValue());
            }
        }
        return v;
    }

    public static Variant getVariant(String var) {
        Variant v = new Variant(var);
        StudyEntry sb = new StudyEntry("1", "1");
        String call = v.getStart() + ":" + v.getReference() + ":" + v.getAlternate() + ":" + 0;
        sb.setFiles(Collections.singletonList(new FileEntry("1", null, new HashMap<>())));
        v.setStudies(Collections.singletonList(sb));
        if (v.getAlternate().contains(",")) {
            String[] alternates = v.getAlternate().split(",");
            v.setAlternate(alternates[0]);
            for (int i = 1; i < alternates.length; i++) {
                v.getStudies().get(0).getSecondaryAlternates().add(new AlternateCoordinate(null, null, null, null, alternates[i], null));
            }
        }
        return v;
    }

    public static Variant getVariantAttribute(String var, String key, String value) {
        return addAttribute(getVariant(var), key, value);
    }

    public static Variant addAttribute(Variant var, String key, String value) {
        var.getStudy("1").getFile("1").getData().put(key, value);
        return var;
    }

    public static Variant addGT(Variant var, String gt) {
        StudyEntry se = var.getStudy("1");
        se.addSampleDataKey("GT");
        se.getSampleDataKeyPositions();
        se.setSamplesPosition(Collections.singletonMap("1", 0));
//        se.setFormat(Collections.singletonList("GT"));
        se.addSampleData("1", Collections.singletonList(gt));
        return var;
    }

    public static Variant addGTAndFilter(Variant var, String gt, String filter) {
        StudyEntry se = var.getStudy("1");
        se.setSamplesPosition(Collections.singletonMap("1", 0));
        se.setSampleDataKeys(Arrays.asList(GENOTYPE_KEY, GENOTYPE_FILTER_KEY));
        se.addSampleData("1", Arrays.asList(gt, filter));
        return var;
    }

    @Test
    public void checkPassConflict() throws Exception {
        Variant a = getVariantFilter("1:1000:A:T", "PASS");
        Variant b = getVariantFilter("1:1000:A:T", "OtherRandom;SiteConflict;Stuff;");
        assertEquals(0, VariantLocalConflictResolver.checkPassConflict(a, a));
        assertEquals(0, VariantLocalConflictResolver.checkPassConflict(b, b));
        assertEquals(-1, VariantLocalConflictResolver.checkPassConflict(a, b));
        assertEquals(1, VariantLocalConflictResolver.checkPassConflict(b, a));
    }

    @Test
    public void checkQualityScore() throws Exception {
        Variant a = getVariantQuality("1:1000:A:T", "100.1123");
        Variant b = getVariantQuality("1:1000:A:T", "10");
        Variant c = getVariantQuality("1:1000:A:T", ".");
        assertEquals(0, VariantLocalConflictResolver.checkQualityScore(a, a));
        assertEquals(0, VariantLocalConflictResolver.checkQualityScore(b, b));
        assertEquals(0, VariantLocalConflictResolver.checkQualityScore(c, c));

        assertEquals(1, VariantLocalConflictResolver.checkQualityScore(a, b));
        assertEquals(1, VariantLocalConflictResolver.checkQualityScore(b, c));
        assertEquals(1, VariantLocalConflictResolver.checkQualityScore(a, c));

        assertEquals(-1, VariantLocalConflictResolver.checkQualityScore(b, a));
        assertEquals(-1, VariantLocalConflictResolver.checkQualityScore(c, a));
        assertEquals(-1, VariantLocalConflictResolver.checkQualityScore(c, b));
    }

}