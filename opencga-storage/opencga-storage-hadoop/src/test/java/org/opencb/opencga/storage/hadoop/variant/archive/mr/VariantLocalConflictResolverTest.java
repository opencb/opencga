package org.opencb.opencga.storage.hadoop.variant.archive.mr;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.opencb.biodata.models.variant.VariantVcfFactory.FILTER;
import static org.opencb.biodata.models.variant.VariantVcfFactory.QUAL;

/**
 * Created by mh719 on 15/06/2016.
 */
public class VariantLocalConflictResolverTest {

    @Test
    public void resolveConflicts() throws Exception {
        Variant a = addGT(addAttribute(getVariantFilter("2:10048155:TCTTTTTTTT:AC", "SiteConflict"),QUAL,"220"), "0/1");
        Variant b = addGT(addAttribute(getVariantFilter("2:10048156:T:T", "PASS"),QUAL,"."), "0/1");
        Variant c = addGT(addAttribute(getVariantFilter("2:10048157:C:C", "PASS"),QUAL,"."), "0/1");
        Variant d = addGT(addAttribute(getVariantFilter("2:10048150:C:C", "PASS"),QUAL,"."), "0/1");
        Variant e = addGT(addAttribute(getVariantFilter("2:10048187:C:C", "PASS"),QUAL,"."), "0/1");
        List<Variant> resolved = new VariantLocalConflictResolver().resolveConflicts(Arrays.asList(d, a, b, c, e));
        assertEquals(6,resolved.size());
//        assertEquals(a, resolved.get(0));
    }


    @Test
    public void resolveRefRegionWithIndel() throws Exception {
        Variant a = addGT(addAttribute(getVariantFilter("1:10230:AC:A", "TruthSensitivityTranche99.90to100.00"),QUAL,"1065.12"), "0/1");
        Variant b = addGT(addAttribute(getVariantFilter("2:10231:C:.", "PASS"),QUAL,"87.71"), "0");
//        b.setType(VariantType.NO_VARIATION);
        VariantType ta = a.getType();
        VariantType tb = b.getType();
        List<Variant> resolved = new VariantLocalConflictResolver().resolve(Arrays.asList(a, b));
        assertEquals(2,resolved.size());
        assertEquals(true, resolved.contains(a));
    }

    @Test
    public void resolveRefRegion() throws Exception {
        Variant a = addAttribute(getVariantFilter("2:10048155:TCTTTTTTTT:AC", "PASS"),QUAL,"220");
        Variant b = addAttribute(getVariantFilter("2:10048156:T", "SiteConflict"),QUAL,".");
        Variant c = addAttribute(getVariantFilter("2:10048157:C", "PASS"),QUAL,".");
        List<Variant> resolved = new VariantLocalConflictResolver().resolve(Arrays.asList(a, b));
        assertEquals(1,resolved.size());
        assertEquals(a, resolved.get(0));
    }

    @Test
    public void resolveRefRegion2() throws Exception {
        Variant a = addGT(addAttribute(getVariantFilter("2:10048155:TCTTTTTTTT:AC", "SiteConflict"),QUAL,"220"), "0/1");
        Variant b = addGT(addAttribute(getVariantFilter("2:10048156:T:T", "PASS"),QUAL,"."), "0/1");
        Variant c = addGT(addAttribute(getVariantFilter("2:10048157:C:C", "PASS"),QUAL,"."), "0/1");
        List<Variant> resolved = new VariantLocalConflictResolver().resolve(Arrays.asList(a, b, c));
        assertEquals(4,resolved.size());
//        assertEquals(a, resolved.get(0));
    }

    @Test
    public void resolvePass() throws Exception {
        Variant a = addAttribute(getVariantFilter("1:5731287:C:-", "PASS"),QUAL,"731");
        Variant b = addAttribute(getVariantFilter("1:5731287:C:G", "SiteConflict"),QUAL,"390");
        List<Variant> resolved = new VariantLocalConflictResolver().resolve(Arrays.asList(a, b));
        assertEquals(1,resolved.size());
        assertEquals(a, resolved.get(0));
    }

    @Test
    public void resolveQUality() throws Exception {
        Variant a = addAttribute(getVariantFilter("1:5731287:C:-", "PASS"),QUAL,"731");
        Variant b = addAttribute(getVariantFilter("1:5731287:C:G", "PASS"),QUAL,"1390");
        List<Variant> resolved = new VariantLocalConflictResolver().resolve(Arrays.asList(a, b));
        assertEquals(1,resolved.size());
        assertEquals(b, resolved.get(0));
    }

    @Test(expected = IllegalStateException.class)
    public void getMissingRegionsBeforeOutside() throws Exception {
        Variant a = getVariant("1:1000:A:T");
        Variant b = getVariant("1:1002:A:T");
        Variant indel = getVariant("1:999:A:T");
        VariantLocalConflictResolver.getMissingRegions(Arrays.asList(new Variant[]{a, b}), indel);
    }
    @Test(expected = IllegalStateException.class)
    public void getMissingRegionsAfterOutside() throws Exception {
        Variant a = getVariant("1:1000:A:T");
        Variant b = getVariant("1:1002:A:T");
        Variant indel = getVariant("1:1003:A:T");
        VariantLocalConflictResolver.getMissingRegions(Arrays.asList(new Variant[]{a, b}), indel);
    }
    @Test
    public void getMissingRegionsIndel() throws Exception {
        Variant a = getVariant("1:1000:A:T");
        Variant b = getVariant("1:1002:A:T");
        Variant indel = getVariant("1:1000:ATA:-");
        List<Pair<Integer, Integer>> missingRegions =
                VariantLocalConflictResolver.getMissingRegions(Arrays.asList(new Variant[]{a, b}), indel);
        assertEquals(1, missingRegions.size());
        assertEquals(new ImmutablePair<Integer, Integer>(1001,1001), missingRegions.get(0));
    }
    @Test
    public void getMissingRegionsStart() throws Exception {
        Variant a = getVariant("1:1000:A:T");
        Variant b = getVariant("1:1002:A:T");
        Variant indel = getVariant("1:999:ATA:-");
        List<Pair<Integer, Integer>> missingRegions =
                VariantLocalConflictResolver.getMissingRegions(Arrays.asList(new Variant[]{a, b}), indel);
        assertEquals(2, missingRegions.size());
        assertEquals(new ImmutablePair<Integer, Integer>(999,999), missingRegions.get(0));
        assertEquals(new ImmutablePair<Integer, Integer>(1001,1001), missingRegions.get(1));
    }
    @Test
    public void getMissingRegionsEnd() throws Exception {
        Variant a = getVariant("1:1000:A:T");
        Variant b = getVariant("1:1002:A:T");
        Variant indel = getVariant("1:1001:ATA:-");
        List<Pair<Integer, Integer>> missingRegions =
                VariantLocalConflictResolver.getMissingRegions(Arrays.asList(new Variant[]{a, b}), indel);
        assertEquals(2, missingRegions.size());
        assertEquals(new ImmutablePair<Integer, Integer>(1001,1001), missingRegions.get(0));
        assertEquals(new ImmutablePair<Integer, Integer>(1003,1003), missingRegions.get(1));
    }

    @Test
    public void hasOverlap() throws Exception {
        Variant regA = new Variant("1:1002:A:T");
        Variant[] var = new Variant[]{new Variant("1:1000:A:T"), regA};
        Variant qa = new Variant("1:1003:A:T");
        Variant qb = new Variant("1:1001:A:T");
        Variant qc = new Variant("1:1001:AT:-");

        assertFalse(VariantLocalConflictResolver.hasOverlap(Arrays.asList(var), qa));
        assertTrue(VariantLocalConflictResolver.hasOverlap(Arrays.asList(var), regA));
        assertFalse(VariantLocalConflictResolver.hasOverlap(Arrays.asList(var), qb));
        assertTrue(VariantLocalConflictResolver.hasOverlap(Arrays.asList(var), qc));
    }

    @Test
    public void min() throws Exception {
        Variant[] var = new Variant[]{new Variant("1:1000:A:T"), new Variant("1:1002:A:T")};
        int min = VariantLocalConflictResolver.min(Arrays.asList(var));
        assertEquals(1000,min);
    }

    @Test
    public void max() throws Exception {
        Variant[] var = new Variant[]{new Variant("1:1000:ATTTTT:T"), new Variant("1:1002:A:T")};
        int max = VariantLocalConflictResolver.max(Arrays.asList(var));
        assertEquals(1005,max);
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

    protected Variant getVariantFilter(String var, String filter) {
        return getVariantAttribute(var, FILTER, filter);
    }
    protected Variant getVariantQuality(String var, String filter) {
        return getVariantAttribute(var, QUAL, filter);
    }

    protected  Variant getVariant(String var) {
        Variant b = new Variant(var);
        StudyEntry sb = new StudyEntry("1", "1");
        sb.setFiles(Collections.singletonList(new FileEntry("1", "1", new HashedMap())));
        b.setStudies(Collections.singletonList(sb));
        return b;
    }
    protected Variant getVariantAttribute(String var, String key, String value) {
        return addAttribute(getVariant(var), key, value);
    }
    protected  Variant addAttribute(Variant var, String key, String value){
        var.getStudy("1").getFile("1").getAttributes().put(key, value);
        return var;
    }
    protected Variant addGT(Variant var, String gt){
        StudyEntry se = var.getStudy("1");
        se.setSamplesPosition(Collections.singletonMap("1",0));
        se.setFormat(Collections.singletonList("GT"));
        se.setSamplesData(Collections.singletonList(Collections.singletonList(gt)));
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