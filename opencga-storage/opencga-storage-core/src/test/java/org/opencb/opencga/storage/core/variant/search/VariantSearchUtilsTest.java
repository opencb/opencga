package org.opencb.opencga.storage.core.variant.search;

import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.util.Collection;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.SEARCH_PROTEIN_SUBSTITUTION_SCORES_COMPLETE;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;

public class VariantSearchUtilsTest {

    // ---- isQueryCovered ----

    @Test
    public void testIsQueryCoveredEmptyQuery() {
        assertTrue(VariantSearchUtils.isQueryCovered(new Query(), defaultIndexMetadata()));
    }

    @Test
    public void testIsQueryCoveredWithCoveredParams() {
        assertTrue(VariantSearchUtils.isQueryCovered(new Query(ANNOT_BIOTYPE.key(), "protein_coding"), defaultIndexMetadata()));
        assertTrue(VariantSearchUtils.isQueryCovered(new Query(REGION.key(), "1:1000-2000"), defaultIndexMetadata()));
        assertTrue(VariantSearchUtils.isQueryCovered(new Query(TYPE.key(), "SNV"), defaultIndexMetadata()));
        assertTrue(VariantSearchUtils.isQueryCovered(new Query(ANNOT_CONSEQUENCE_TYPE.key(), "missense_variant"), defaultIndexMetadata()));
    }

    @Test
    public void testIsQueryCoveredWithUnsupportedParams() {
        assertFalse(VariantSearchUtils.isQueryCovered(new Query(FILE.key(), "file1"), defaultIndexMetadata()));
        assertFalse(VariantSearchUtils.isQueryCovered(new Query(SAMPLE.key(), "sample1"), defaultIndexMetadata()));
        assertFalse(VariantSearchUtils.isQueryCovered(new Query(GENOTYPE.key(), "0/1"), defaultIndexMetadata()));
        assertFalse(VariantSearchUtils.isQueryCovered(new Query(ANNOT_DRUG.key(), "aspirin"), defaultIndexMetadata()));
        assertFalse(VariantSearchUtils.isQueryCovered(new Query(FILTER.key(), "PASS"), defaultIndexMetadata()));
        assertFalse(VariantSearchUtils.isQueryCovered(new Query(QUAL.key(), ">30"), defaultIndexMetadata()));
    }

    @Test
    public void testIsQueryCoveredMixedParams() {
        // One unsupported param makes the whole query uncovered
        assertFalse(VariantSearchUtils.isQueryCovered(new Query()
                .append(REGION.key(), "1:1000-2000")
                .append(FILE.key(), "file1"), defaultIndexMetadata()));
    }

    // ---- isQueryCovered with SearchIndexMetadata (protein substitution) ----

    @Test
    public void testIsQueryCoveredSafeProteinSubstitution() {
        // polyphen > X is safe (Solr stores max, max > X means at least one CT > X)
        Query query = new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "polyphen>0.5");
        assertTrue(VariantSearchUtils.isQueryCovered(query, defaultIndexMetadata()));
    }

    @Test
    public void testIsQueryCoveredUnsafeProteinSubstitution() {
        // polyphen < X is unsafe (Solr stores max, max < X doesn't mean any CT < X)
        Query query = new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "polyphen<0.5");
        assertFalse(VariantSearchUtils.isQueryCovered(query, defaultIndexMetadata()));
    }

    @Test
    public void testIsQueryCoveredUnsafeSift() {
        // sift > X is unsafe (Solr stores min, min > X doesn't mean any CT > X)
        Query query = new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift>0.5");
        assertFalse(VariantSearchUtils.isQueryCovered(query, defaultIndexMetadata()));
    }

    @Test
    public void testIsQueryCoveredSafeSift() {
        // sift < X is safe (Solr stores min, min < X means at least one CT < X)
        Query query = new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift<0.5");
        assertTrue(VariantSearchUtils.isQueryCovered(query, defaultIndexMetadata()));
    }

    @Test
    public void testIsQueryCoveredProteinSubstitutionScoresComplete() {
        // When scores are complete (per-CT), all operators are safe
        SearchIndexMetadata indexMetadata = buildIndexMetadata(
                new ObjectMap(SEARCH_PROTEIN_SUBSTITUTION_SCORES_COMPLETE.key(), true));
        Query query = new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "polyphen<0.5");
        assertTrue(VariantSearchUtils.isQueryCovered(query, indexMetadata));
    }

    @Test
    public void testIsQueryCoveredProteinSubstitutionNullMetadata() {
        // Null metadata defaults to scores NOT complete — unsafe operators trigger refinement
        Query query = new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "polyphen<0.5");
        assertFalse(VariantSearchUtils.isQueryCovered(query, null));
    }

    @Test
    public void testIsQueryCoveredProteinSubstitutionDescription() {
        // Description filters (no numeric operator) are always safe
        Query query = new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "polyphen=probably damaging");
        assertTrue(VariantSearchUtils.isQueryCovered(query, defaultIndexMetadata()));
    }

    // ---- isTranscriptFlagCovered ----

    @Test
    public void testTranscriptFlagCoveredNoFlag() {
        assertTrue(VariantSearchUtils.isTranscriptFlagCovered(new Query()));
    }

    @Test
    public void testTranscriptFlagCoveredImportantFlags() {
        assertTrue(VariantSearchUtils.isTranscriptFlagCovered(new Query(ANNOT_TRANSCRIPT_FLAG.key(), "basic")));
        assertTrue(VariantSearchUtils.isTranscriptFlagCovered(new Query(ANNOT_TRANSCRIPT_FLAG.key(), "CCDS")));
        assertTrue(VariantSearchUtils.isTranscriptFlagCovered(new Query(ANNOT_TRANSCRIPT_FLAG.key(), "canonical")));
        assertTrue(VariantSearchUtils.isTranscriptFlagCovered(new Query(ANNOT_TRANSCRIPT_FLAG.key(), "MANE Select")));
        assertTrue(VariantSearchUtils.isTranscriptFlagCovered(new Query(ANNOT_TRANSCRIPT_FLAG.key(), "basic,CCDS")));
    }

    @Test
    public void testTranscriptFlagCoveredNonImportantFlags() {
        assertFalse(VariantSearchUtils.isTranscriptFlagCovered(new Query(ANNOT_TRANSCRIPT_FLAG.key(), "mRNA_start_NF")));
        assertFalse(VariantSearchUtils.isTranscriptFlagCovered(new Query(ANNOT_TRANSCRIPT_FLAG.key(), "cds_end_NF")));
    }

    @Test
    public void testTranscriptFlagCoveredMixed() {
        // One non-important flag makes the whole param uncovered
        assertFalse(VariantSearchUtils.isTranscriptFlagCovered(
                new Query(ANNOT_TRANSCRIPT_FLAG.key(), "basic,mRNA_start_NF")));
    }

    @Test
    public void testTranscriptFlagCoveredNegated() {
        assertTrue(VariantSearchUtils.isTranscriptFlagCovered(new Query(ANNOT_TRANSCRIPT_FLAG.key(), "!basic")));
        assertFalse(VariantSearchUtils.isTranscriptFlagCovered(new Query(ANNOT_TRANSCRIPT_FLAG.key(), "!mRNA_start_NF")));
    }

    @Test
    public void testIsQueryCoveredNonImportantFlag() {
        // Non-important flag makes isQueryCovered return false
        assertFalse(VariantSearchUtils.isQueryCovered(new Query(ANNOT_TRANSCRIPT_FLAG.key(), "mRNA_start_NF"), defaultIndexMetadata()));
    }

    // ---- isProteinSubstitutionOperatorSafe ----

    @Test
    public void testPolyphenOperatorSafe() {
        assertTrue(VariantSearchUtils.isProteinSubstitutionOperatorSafe("polyphen", ">"));
        assertTrue(VariantSearchUtils.isProteinSubstitutionOperatorSafe("polyphen", ">="));
        assertTrue(VariantSearchUtils.isProteinSubstitutionOperatorSafe("polyphen", "="));
        assertTrue(VariantSearchUtils.isProteinSubstitutionOperatorSafe("polyphen", "!="));
        // "!<" contains "!" so it's safe (negated less-than)
        assertTrue(VariantSearchUtils.isProteinSubstitutionOperatorSafe("polyphen", "!<"));
    }

    @Test
    public void testPolyphenOperatorUnsafe() {
        assertFalse(VariantSearchUtils.isProteinSubstitutionOperatorSafe("polyphen", "<"));
        assertFalse(VariantSearchUtils.isProteinSubstitutionOperatorSafe("polyphen", "<="));
    }

    @Test
    public void testSiftOperatorSafe() {
        assertTrue(VariantSearchUtils.isProteinSubstitutionOperatorSafe("sift", "<"));
        assertTrue(VariantSearchUtils.isProteinSubstitutionOperatorSafe("sift", "<="));
        assertTrue(VariantSearchUtils.isProteinSubstitutionOperatorSafe("sift", "="));
        assertTrue(VariantSearchUtils.isProteinSubstitutionOperatorSafe("sift", "!="));
    }

    @Test
    public void testSiftOperatorUnsafe() {
        assertFalse(VariantSearchUtils.isProteinSubstitutionOperatorSafe("sift", ">"));
        assertFalse(VariantSearchUtils.isProteinSubstitutionOperatorSafe("sift", ">="));
    }

    @Test
    public void testOtherScoreAlwaysSafe() {
        assertTrue(VariantSearchUtils.isProteinSubstitutionOperatorSafe("cadd", "<"));
        assertTrue(VariantSearchUtils.isProteinSubstitutionOperatorSafe("cadd", ">"));
        assertTrue(VariantSearchUtils.isProteinSubstitutionOperatorSafe("unknown", "<="));
    }

    // ---- needsProteinSubstitutionRefinement ----

    @Test
    public void testNeedsRefinementNoParam() {
        assertFalse(VariantSearchUtils.needsProteinSubstitutionRefinement(new Query(), null));
    }

    @Test
    public void testNeedsRefinementSafeOperators() {
        assertFalse(VariantSearchUtils.needsProteinSubstitutionRefinement(
                new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "polyphen>0.5"), defaultIndexMetadata()));
        assertFalse(VariantSearchUtils.needsProteinSubstitutionRefinement(
                new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift<0.05"), defaultIndexMetadata()));
    }

    @Test
    public void testNeedsRefinementUnsafeOperators() {
        assertTrue(VariantSearchUtils.needsProteinSubstitutionRefinement(
                new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "polyphen<0.5"), defaultIndexMetadata()));
        assertTrue(VariantSearchUtils.needsProteinSubstitutionRefinement(
                new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift>0.05"), defaultIndexMetadata()));
    }

    @Test
    public void testNeedsRefinementMixedOperators() {
        // One unsafe operator is enough to need refinement
        assertTrue(VariantSearchUtils.needsProteinSubstitutionRefinement(
                new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "polyphen>0.5,sift>0.05"), defaultIndexMetadata()));
    }

    @Test
    public void testNeedsRefinementScoresComplete() {
        SearchIndexMetadata complete = buildIndexMetadata(
                new ObjectMap(SEARCH_PROTEIN_SUBSTITUTION_SCORES_COMPLETE.key(), true));
        assertFalse(VariantSearchUtils.needsProteinSubstitutionRefinement(
                new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "polyphen<0.5"), complete));
    }

    // ---- coveredParams / uncoveredParams ----

    @Test
    public void testCoveredParamsAllCovered() {
        Query query = new Query()
                .append(REGION.key(), "1:1000-2000")
                .append(ANNOT_BIOTYPE.key(), "protein_coding");
        Collection<VariantQueryParam> covered = VariantSearchUtils.coveredParams(query);
        assertTrue(covered.contains(REGION));
        assertTrue(covered.contains(ANNOT_BIOTYPE));
    }

    @Test
    public void testUncoveredParamsWithUnsupported() {
        Query query = new Query()
                .append(REGION.key(), "1:1000-2000")
                .append(FILE.key(), "file1")
                .append(SAMPLE.key(), "sample1");
        Collection<VariantQueryParam> uncovered = VariantSearchUtils.uncoveredParams(query);
        assertTrue(uncovered.contains(FILE));
        assertTrue(uncovered.contains(SAMPLE));
        assertFalse(uncovered.contains(REGION));
    }

    @Test
    public void testUncoveredParamsNonImportantFlag() {
        Query query = new Query(ANNOT_TRANSCRIPT_FLAG.key(), "mRNA_start_NF");
        Collection<VariantQueryParam> uncovered = VariantSearchUtils.uncoveredParams(query);
        assertTrue(uncovered.contains(ANNOT_TRANSCRIPT_FLAG));
    }

    @Test
    public void testCoveredParamsNonImportantFlagRemoved() {
        Query query = new Query(ANNOT_TRANSCRIPT_FLAG.key(), "mRNA_start_NF");
        Collection<VariantQueryParam> covered = VariantSearchUtils.coveredParams(query);
        assertFalse(covered.contains(ANNOT_TRANSCRIPT_FLAG));
    }

    @Test
    public void testUncoveredParamsImportantFlag() {
        Query query = new Query(ANNOT_TRANSCRIPT_FLAG.key(), "basic");
        Collection<VariantQueryParam> uncovered = VariantSearchUtils.uncoveredParams(query);
        assertFalse(uncovered.contains(ANNOT_TRANSCRIPT_FLAG));
    }

    // ---- getSearchEngineQuery ----

    @Test
    public void testGetSearchEngineQueryRemovesUncovered() {
        Query query = new Query()
                .append(REGION.key(), "1:1000-2000")
                .append(FILE.key(), "file1")
                .append(ANNOT_BIOTYPE.key(), "protein_coding");
        Query searchQuery = VariantSearchUtils.getSearchEngineQuery(query);
        assertTrue(searchQuery.containsKey(REGION.key()));
        assertTrue(searchQuery.containsKey(ANNOT_BIOTYPE.key()));
        assertFalse(searchQuery.containsKey(FILE.key()));
    }

    @Test
    public void testGetSearchEngineQueryRemovesNonImportantFlag() {
        Query query = new Query(ANNOT_TRANSCRIPT_FLAG.key(), "mRNA_start_NF");
        Query searchQuery = VariantSearchUtils.getSearchEngineQuery(query);
        assertFalse(searchQuery.containsKey(ANNOT_TRANSCRIPT_FLAG.key()));
    }

    @Test
    public void testGetSearchEngineQueryKeepsImportantFlag() {
        Query query = new Query(ANNOT_TRANSCRIPT_FLAG.key(), "basic");
        Query searchQuery = VariantSearchUtils.getSearchEngineQuery(query);
        assertTrue(searchQuery.containsKey(ANNOT_TRANSCRIPT_FLAG.key()));
    }

    @Test
    public void testGetSearchEngineQueryKeepsProteinSubstitution() {
        // Protein substitution stays in Solr query even when unsafe (Solr parser handles partial coverage internally)
        Query query = new Query(ANNOT_PROTEIN_SUBSTITUTION.key(), "polyphen<0.5");
        Query searchQuery = VariantSearchUtils.getSearchEngineQuery(query);
        assertTrue(searchQuery.containsKey(ANNOT_PROTEIN_SUBSTITUTION.key()));
    }

    // ---- helper ----

    private static SearchIndexMetadata defaultIndexMetadata() {
        return buildIndexMetadata(new ObjectMap(SEARCH_PROTEIN_SUBSTITUTION_SCORES_COMPLETE.key(), false));
    }

    private static SearchIndexMetadata buildIndexMetadata(ObjectMap attributes) {
        return new SearchIndexMetadata(0, null, null,
                SearchIndexMetadata.Status.ACTIVE, SearchIndexMetadata.DataStatus.READY,
                null, null, attributes);
    }
}
