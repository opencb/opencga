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

package org.opencb.opencga.storage.core.variant.adaptors;

import htsjdk.variant.variantcontext.VariantContext;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.models.metadata.SampleSetType;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.tools.variant.VariantNormalizer;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.sample.VariantSampleData;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.*;

/**
 * Tests that all the VariantDBAdaptor filters and methods work correctly with more than one study loaded.
 *
 * Do not check that all the values are loaded correctly
 * Do not check that variant annotation is correct
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Ignore
public abstract class VariantDBAdaptorLargeTest extends VariantStorageBaseTest {

    protected static final String fileName1 = "1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz";
    protected static final String fileName2 = "501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz";
    protected static final String fileName3 = "1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz";
    protected static final String fileName4 = "1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz";
    protected static final String fileName5 = "2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz";
    protected static Integer file1;
    protected static Integer file2;
    protected static Integer file3;
    protected static Integer file4;
    protected static Integer file5;
    protected static StudyMetadata studyMetadata1;
    protected static StudyMetadata studyMetadata2;
    protected static StudyMetadata studyMetadata3;
    protected static VariantDBAdaptor dbAdaptor;
    protected static int NUM_VARIANTS = 9751;
    protected static long numVariants;
    protected QueryResult<Variant> queryResult;
    protected QueryOptions options;
    protected Query query;
    private static QueryResult<Variant> allVariants;
    private VariantStorageMetadataManager metadataManager;

    protected int skippedVariants() {
        return 0;
    }

    @Before
    public void before() throws Exception {
        options = new QueryOptions();
        query = new Query();
        metadataManager = getVariantStorageEngine().getMetadataManager();
        if (studyMetadata1 == null) {
            clearDB(DB_NAME);
            studyMetadata1 = metadataManager.createStudy("Study1");
            studyMetadata2 = metadataManager.createStudy("Study2");
            studyMetadata3 = metadataManager.createStudy("Study3");

            ObjectMap options = getExtraOptions()
                    .append(VariantStorageEngine.Options.STUDY_TYPE.key(), SampleSetType.CONTROL_SET)
                    .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), true)
                    .append(VariantStorageEngine.Options.ANNOTATE.key(), false);
            //Study1
            URI file1Uri = getResourceUri("1000g_batches/" + fileName1);
            runDefaultETL(file1Uri, variantStorageEngine, studyMetadata1, options);
            assertEquals(500, metadataManager.getCohortMetadata(studyMetadata1.getId(), StudyEntry.DEFAULT_COHORT).getSamples().size());
            file1 = metadataManager.getFileId(studyMetadata1.getId(), file1Uri);

            URI file2Uri = getResourceUri("1000g_batches/" + fileName2);
            runDefaultETL(file2Uri, variantStorageEngine, studyMetadata1, options);
            assertEquals(1000, metadataManager.getCohortMetadata(studyMetadata1.getId(), StudyEntry.DEFAULT_COHORT).getSamples().size());
            file2 = metadataManager.getFileId(studyMetadata1.getId(), file2Uri);

            //Study2
            URI file3Uri = getResourceUri("1000g_batches/" + fileName3);
            runDefaultETL(file3Uri, variantStorageEngine, studyMetadata2, options);
            assertEquals(500, metadataManager.getCohortMetadata(studyMetadata2.getId(), StudyEntry.DEFAULT_COHORT).getSamples().size());
            file3 = metadataManager.getFileId(studyMetadata2.getId(), file3Uri);

            URI file4Uri = getResourceUri("1000g_batches/" + fileName4);
            runDefaultETL(file4Uri, variantStorageEngine, studyMetadata2, options);
            assertEquals(1000, metadataManager.getCohortMetadata(studyMetadata2.getId(), StudyEntry.DEFAULT_COHORT).getSamples().size());
            file4 = metadataManager.getFileId(studyMetadata2.getId(), file4Uri);

            //Study3
            URI file5Uri = getResourceUri("1000g_batches/" + fileName5);
            runDefaultETL(file5Uri, variantStorageEngine, studyMetadata3, options);
            assertEquals(504, metadataManager.getCohortMetadata(studyMetadata3.getId(), StudyEntry.DEFAULT_COHORT).getSamples().size());
            file5 = metadataManager.getFileId(studyMetadata3.getId(), file5Uri);


            dbAdaptor = variantStorageEngine.getDBAdaptor();

            NUM_VARIANTS -= skippedVariants();
            allVariants = dbAdaptor.get(new Query(), new QueryOptions());
            numVariants = allVariants.getNumResults();
        }
        dbAdaptor = getVariantStorageEngine().getDBAdaptor();
    }

    public ObjectMap getExtraOptions() {
        return new ObjectMap();
    }


    @Test
    public void testGetAllVariants_returnedStudies1() {
        query.append(INCLUDE_STUDY.key(), studyMetadata1.getId());
        queryResult = dbAdaptor.get(query, options);

        assertEquals(numVariants, queryResult.getNumResults());
        assertEquals(numVariants, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            for (StudyEntry sourceEntry : variant.getStudies()) {
                assertEquals(studyMetadata1.getName(), sourceEntry.getStudyId());
            }
        }
    }

    @Test
    public void testGetVariantsByType() {
        Set<Variant> snv = new HashSet<>(dbAdaptor.get(new Query(VariantQueryParam.TYPE.key(), VariantType.SNV), new QueryOptions()).getResult());
        System.out.println("SNV = " + snv.size());
        assertEquals(9515, snv.size());
        snv.forEach(variant -> assertThat(EnumSet.of(VariantType.SNV, VariantType.SNP), hasItem(variant.getType())));

        Set<Variant> not_snv = new HashSet<>(dbAdaptor.get(new Query(VariantQueryParam.TYPE.key(), "!" + VariantType.SNV), new QueryOptions()).getResult());
        System.out.println("!SNV = " + not_snv.size());
        not_snv.forEach(variant -> assertFalse(EnumSet.of(VariantType.SNV, VariantType.SNP).contains(variant.getType())));

        Set<Variant> snv_snp = new HashSet<>(dbAdaptor.get(new Query(VariantQueryParam.TYPE.key(), VariantType.SNV + "," + VariantContext.Type.SNP), new QueryOptions()).getResult());
        System.out.println("SNV_SNP = " + snv_snp.size());
        assertEquals(snv_snp, snv);

        Set<Variant> snp = new HashSet<>(dbAdaptor.get(new Query(VariantQueryParam.TYPE.key(), VariantType.SNP), new QueryOptions()).getResult());
        snp.forEach(variant -> assertEquals(VariantType.SNP, variant.getType()));
        snp.forEach(variant -> assertThat(snv, hasItem(variant)));
        System.out.println("SNP = " + snp.size());

        Set<Variant> indels = new HashSet<>(dbAdaptor.get(new Query(VariantQueryParam.TYPE.key(), VariantType.INDEL), new QueryOptions()).getResult());
        indels.forEach(variant -> assertEquals(VariantType.INDEL, variant.getType()));
        System.out.println("INDEL = " + indels.size());

        Set<Variant> indels_snp = new HashSet<>(dbAdaptor.get(new Query(VariantQueryParam.TYPE.key(), VariantType.INDEL + "," + VariantType.SNP), new QueryOptions()).getResult());
        indels_snp.forEach(variant -> assertThat(EnumSet.of(VariantType.INDEL, VariantType.SNP), hasItem(variant.getType())));
        indels_snp.forEach(variant -> assertTrue(indels.contains(variant) || snp.contains(variant)));
        System.out.println("INDEL_SNP = " + indels_snp.size());

        Set<Variant> indels_snv = new HashSet<>(dbAdaptor.get(new Query(VariantQueryParam.TYPE.key(), VariantType.INDEL + "," + VariantType.SNV), new QueryOptions()).getResult());
        indels_snv.forEach(variant -> assertThat(EnumSet.of(VariantType.INDEL, VariantType.SNP, VariantType.SNV), hasItem(variant.getType())));
        indels_snv.forEach(variant -> assertTrue(indels.contains(variant) || snv.contains(variant)));
        System.out.println("INDEL_SNV = " + indels_snv.size());
    }

    @Test
    public void testGetAllVariants_returnedStudies3() {
        String studyId = Integer.toString(studyMetadata3.getId());
        query.put(INCLUDE_STUDY.key(), studyId);
        queryResult = dbAdaptor.get(query, options);

        assertEquals(numVariants, queryResult.getNumResults());
        assertEquals(numVariants, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            for (StudyEntry sourceEntry : variant.getStudies()) {
                assertEquals(studyMetadata3.getName(), sourceEntry.getStudyId());
            }
        }

        query.put(INCLUDE_STUDY.key(), studyMetadata3.getName());
        queryResult = dbAdaptor.get(query, options);

        assertEquals(numVariants, queryResult.getNumResults());
        assertEquals(numVariants, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            for (StudyEntry sourceEntry : variant.getStudies()) {
                assertEquals(studyMetadata3.getName(), sourceEntry.getStudyId());
            }
        }
    }

    @Test
    public void testGetAllVariants_returnedStudies2_3() {
        List<String> studyIds = Arrays.asList(
                studyMetadata2.getName(),
                studyMetadata3.getName());
        query.append(INCLUDE_STUDY.key(), studyIds);
        queryResult = dbAdaptor.get(query, options);

        assertEquals(numVariants, queryResult.getNumResults());
        assertEquals(numVariants, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            for (StudyEntry sourceEntry : variant.getStudies()) {
                assertThat(studyIds, hasItem(sourceEntry.getStudyId()));
            }
        }
    }

    @Test
    public void testGetAllVariants_returnedStudiesAll() {
        List<String> studyIds = Arrays.asList(
                studyMetadata1.getName(),
                studyMetadata2.getName(),
                studyMetadata3.getName());
        query.append(INCLUDE_STUDY.key(), studyIds);
        queryResult = dbAdaptor.get(query, options);

        assertEquals(numVariants, queryResult.getNumResults());
        assertEquals(numVariants, queryResult.getNumTotalResults());

        Set<Set<String>> studySets = new HashSet<>();
        for (Variant variant : queryResult.getResult()) {
            Set<String> readStudies = variant.getStudies().stream().map(StudyEntry::getStudyId).collect(Collectors.toSet());
            studyIds.containsAll(readStudies);
            studySets.add(readStudies);
        }
        assertThat(studySets, hasItem(new HashSet<>(studyIds)));
    }

    @Test
    public void testGetAllVariants_returnedStudiesEmpty() {
        query.append(INCLUDE_STUDY.key(), NONE);

//        thrown.expect(VariantQueryException.class); //StudyNotFound exception
        queryResult = dbAdaptor.get(query, options);

        assertEquals(numVariants, queryResult.getNumResults());
        assertEquals(numVariants, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            assertEquals(Collections.emptyList(), variant.getStudies());
        }
    }

    @Test
    public void testGetAllVariants_returnedStudies_wrong() {
        query.append(INCLUDE_STUDY.key(), -1);

        thrown.expect(VariantQueryException.class); //StudyNotFound exception
        queryResult = dbAdaptor.get(query, options);

    }

    @Test
    public void testGetAllVariants_filterStudy_returnedStudies_wrong() {
        query.append(STUDY.key(), -1);

        thrown.expect(VariantQueryException.class); //StudyNotFound exception
        queryResult = dbAdaptor.get(query, options);
    }

    @Test
    public void testGetAllVariants_filterStudy_unknownStudy() {
        query.append(GENOTYPE.key(), "HG00258:1/1");

        thrown.expect(IllegalArgumentException.class); //Unspecified study exception
        queryResult = dbAdaptor.get(query, options);
    }

    @Test
    public void testGetAllVariants_genotypes() {
        List<Integer> indexedFiles = new ArrayList<>(metadataManager.getIndexedFiles(studyMetadata1.getId()));
        // Sample from
        List<Integer> file1Samples = new ArrayList<>(metadataManager.getFileMetadata(studyMetadata1.getId(), indexedFiles.get(0)).getSamples());
        List<Integer> file2Samples = new ArrayList<>(metadataManager.getFileMetadata(studyMetadata1.getId(), indexedFiles.get(1)).getSamples());
        String f1_s1 = metadataManager.getSampleName(studyMetadata1.getId(), file1Samples.get(0));
        String f1_s2 = metadataManager.getSampleName(studyMetadata1.getId(), file1Samples.get(1));
        String f2_s1 = metadataManager.getSampleName(studyMetadata1.getId(), file2Samples.get(0));
        String f2_s2 = metadataManager.getSampleName(studyMetadata1.getId(), file2Samples.get(1));
        String study = studyMetadata1.getName();
        Query query = new Query(STUDY.key(), study)
                .append(INCLUDE_STUDY.key(), ALL)
                .append(UNKNOWN_GENOTYPE.key(), "./.")
                .append(INCLUDE_SAMPLE.key(), ALL)
                .append(INCLUDE_FILE.key(), ALL);
        QueryResult<Variant> allVariants = dbAdaptor.get(new Query(UNKNOWN_GENOTYPE.key(), "./."), new QueryOptions());

        query.put(GENOTYPE.key(), f1_s1 + IS + "1|1" + AND + f2_s1 + IS + "0|1");
        queryResult = dbAdaptor.get(query, new QueryOptions());
        assertThat(queryResult, everyResult(allVariants, withStudy(study, allOf(
                withSampleData(f1_s1, "GT", is("1|1")),
                withSampleData(f2_s1, "GT", is("0|1"))))));

        query.put(GENOTYPE.key(), f1_s1 + IS + "1|1" + AND + f1_s2 + IS + "0|1");
        queryResult = dbAdaptor.get(query, new QueryOptions());
        assertThat(queryResult, everyResult(allVariants, withStudy(study, allOf(
                withSampleData(f1_s1, "GT", is("1|1")),
                withSampleData(f1_s2, "GT", is("0|1"))))));

        query.put(GENOTYPE.key(), f1_s1 + IS + "1|1" + AND + f1_s2 + IS + "0/0" + OR + "0|0");
        queryResult = dbAdaptor.get(query, new QueryOptions());
        assertThat(queryResult, everyResult(allVariants, withStudy(study, allOf(
                withSampleData(f1_s1, "GT", is("1|1")),
                withSampleData(f1_s2, "GT", anyOf(is("0|0"), is("0/0")))))));

        query.put(GENOTYPE.key(), f1_s1 + IS + "1|1" + AND + f2_s1 + IS + "0/0" + OR + "0|0");
        queryResult = dbAdaptor.get(query, new QueryOptions());
        assertThat(queryResult, everyResult(allVariants, withStudy(study, allOf(
                withSampleData(f1_s1, "GT", is("1|1")),
                withSampleData(f2_s1, "GT", anyOf(is("0|0"), is("0/0")))))));

        query.put(GENOTYPE.key(), f2_s1 + IS + "0/0" + OR + "0|0");
        queryResult = dbAdaptor.get(query, new QueryOptions());
        assertThat(queryResult, everyResult(allVariants, withStudy(study,
                withSampleData(f2_s1, "GT", anyOf(is("0|0"), is("0/0"))))));

        query.put(GENOTYPE.key(), f1_s1 + IS + "0/0" + OR + "0|0" + OR + f2_s1 + IS + "0/0" + OR + "0|0");
        queryResult = dbAdaptor.get(query, new QueryOptions());
        assertThat(queryResult, everyResult(allVariants, withStudy(study, anyOf(
                withSampleData(f1_s1, "GT", anyOf(is("0|0"), is("0/0"))),
                withSampleData(f2_s1, "GT", anyOf(is("0|0"), is("0/0")))
        ))));

        query.put(GENOTYPE.key(), f1_s1 + IS + "0/0" + OR + "0|0" + AND + f2_s1 + IS + "0/0" + OR + "0|0");
        queryResult = dbAdaptor.get(query, new QueryOptions());
        assertThat(queryResult, everyResult(allVariants, withStudy(study, allOf(
                withSampleData(f1_s1, "GT", anyOf(is("0|0"), is("0/0"))),
                withSampleData(f2_s1, "GT", anyOf(is("0|0"), is("0/0")))
        ))));
    }

    @Test
    public void testGetAllVariants_negatedGenotypes() {
        Iterator<SampleMetadata> it = metadataManager.sampleMetadataIterator(studyMetadata1.getId());
        String s1 = it.next().getName();
        String s2 = it.next().getName();
        String study = studyMetadata1.getName();
        Query query = new Query(STUDY.key(), study)
                .append(INCLUDE_STUDY.key(), ALL)
                .append(UNKNOWN_GENOTYPE.key(), "./.")
                .append(INCLUDE_SAMPLE.key(), ALL)
                .append(INCLUDE_FILE.key(), ALL);
        QueryResult<Variant> allVariants = dbAdaptor.get(new Query(UNKNOWN_GENOTYPE.key(), "./."), new QueryOptions());

        //Get all variants with not 1|1 for s1
        query.put(GENOTYPE.key(), s1 + ":!1|1");
        queryResult = dbAdaptor.get(query, new QueryOptions());
        assertThat(queryResult, everyResult(allVariants, withStudy(study, withSampleData(s1, "GT", not(is("1|1"))))));

        //Get all variants with not 0/0 for s1
        query.put(GENOTYPE.key(), s1 + ":!0/0");
        queryResult = dbAdaptor.get(query, new QueryOptions());
        assertThat(queryResult, everyResult(allVariants, withStudy(study, withSampleData(s1, "GT", not(is("0/0"))))));

        //Get all variants with not 0/0 or 0|1 for s1
        query.put(GENOTYPE.key(), s1 + ":!0/0,!0|1");
        queryResult = dbAdaptor.get(query, new QueryOptions());
        assertThat(queryResult, everyResult(allVariants, withStudy(study, withSampleData(s1, "GT", allOf(not(is("0/0")), not(is("0|1")))))));

        //Get all variants with 1|1 for s1 and 0|0 or 1|0 for s2
        query.put(GENOTYPE.key(), s1 + ":1|1" + ';' + s2 + ":!0|0,!1|0");
        queryResult = dbAdaptor.get(query, new QueryOptions());
        assertThat(queryResult, everyResult(allVariants, withStudy(study, allOf(
                withSampleData(s1, "GT", is("1|1")),
                withSampleData(s2, "GT", allOf(not(is("0/0")), not(is("1|0"))))))));

    }

    @Test
    public void testGetAllVariants_filterStudy_returnedStudies2_3() {
        List<String> studyIds = Arrays.asList(
                studyMetadata2.getName(),
                studyMetadata3.getName());
        query.append(INCLUDE_STUDY.key(), studyIds)
                .append(STUDY.key(), studyIds);
        queryResult = dbAdaptor.get(query, options);

        int expectedVariants = 0;
        for (Variant variant : allVariants.getResult()) {
            for (StudyEntry studyEntry : variant.getStudies()) {
                if (studyIds.contains(studyEntry.getStudyId())) {
                    expectedVariants++;
                    break;
                }
            }
        }

        assertTrue(expectedVariants > 0);
        assertEquals(expectedVariants, queryResult.getNumResults());
        assertEquals(expectedVariants, queryResult.getNumTotalResults());

        Set<String> actualStudyIds = new HashSet<>();
        Set<Set<String>> actualStudyIdSets = new HashSet<>();
        for (Variant variant : queryResult.getResult()) {
            Set<String> set = new HashSet<>();
            for (StudyEntry entry : variant.getStudies()) {
                actualStudyIds.add(entry.getStudyId());
                set.add(entry.getStudyId());
                assertThat(studyIds, hasItem(entry.getStudyId()));
            }
            actualStudyIdSets.add(set);
        }

        assertEquals(new HashSet<>(studyIds), actualStudyIds);
        assertThat(actualStudyIdSets, hasItems(new HashSet<>(studyIds)));
    }

    @Test
    public void testGetAllVariants_filterStudies2_OR_3() {
        List<String> studyIds = Arrays.asList(
                studyMetadata2.getName(),
                studyMetadata3.getName());
        query.append(STUDY.key(), studyIds);
        queryResult = dbAdaptor.get(query, options);

        int expectedVariants = 0;
        for (Variant variant : allVariants.getResult()) {
            for (StudyEntry studyEntry : variant.getStudies()) {
                if (studyIds.contains(studyEntry.getStudyId())) {
                    expectedVariants++;
                    break;
                }
            }
        }
        assertTrue(expectedVariants > 0);
        assertEquals(expectedVariants, queryResult.getNumResults());
        assertEquals(expectedVariants, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            List<String> returnedStudyIds = variant.getStudies().stream().map(StudyEntry::getStudyId).collect(Collectors.toList());
            assertThat(returnedStudyIds, anyOf(hasItem(studyMetadata2.getName()), hasItem(studyMetadata3.getName())));
        }
    }

    @Test
    public void testGetAllVariants_filterStudies2_AND_3() {
        String studyIds = studyMetadata2.getName() + ';' + studyMetadata3.getName();
        Query query = new Query(STUDY.key(), studyIds)
                .append(INCLUDE_STUDY.key(), ALL);
        QueryResult<Variant> queryResult = dbAdaptor.get(query, options);

        assertThat(queryResult, everyResult(allVariants,
                allOf(withStudy(studyMetadata2.getName()), withStudy(studyMetadata3.getName()))));


        query = new Query(STUDY.key(), studyIds).append(FILE.key(), Arrays.asList(file3, file4, file5))
                .append(INCLUDE_STUDY.key(), ALL)
                .append(INCLUDE_FILE.key(), ALL)
                .append(INCLUDE_SAMPLE.key(), ALL);
        queryResult = dbAdaptor.get(query, options);

        assertThat(queryResult, everyResult(allVariants,
                allOf(withStudy(studyMetadata2.getName()), withStudy(studyMetadata3.getName()))));

    }

    @Test
    public void testGetAllVariants_filterStudies2_not_3() {
        String studyIds = studyMetadata2.getName() + ";!" + studyMetadata3.getName();
        query = new Query(STUDY.key(), studyIds)
                .append(INCLUDE_STUDY.key(), ALL);
        queryResult = dbAdaptor.get(query, options);

        assertThat(queryResult, everyResult(allVariants,
                allOf(withStudy(studyMetadata2.getName()), withStudy(studyMetadata3.getName(), nullValue()))));


        query = new Query(STUDY.key(), studyIds).append(FILE.key(), Arrays.asList(file3, file4))
                .append(INCLUDE_STUDY.key(), ALL)
                .append(INCLUDE_FILE.key(), ALL)
                .append(INCLUDE_SAMPLE.key(), ALL);
        queryResult = dbAdaptor.get(query, options);

        assertThat(queryResult, everyResult(allVariants,
                allOf(withStudy(studyMetadata2.getName()), withStudy(studyMetadata3.getName(), nullValue()))));
    }

    @Test
    public void testGetAllVariants_filterFiles1_2() {
        long count = dbAdaptor.count(new Query(STUDY.key(), studyMetadata1.getName())).first();

        Query query = new Query().append(FILE.key(), Arrays.asList(fileName1, fileName2))
                .append(INCLUDE_FILE.key(), ALL)
                .append(INCLUDE_SAMPLE.key(), ALL);
        QueryResult<Variant> queryResult = dbAdaptor.get(query, options);

        assertThat(queryResult, everyResult(allVariants,
                withStudy(studyMetadata1.getName(), withFileId(anyOf(hasItem(fileName1), hasItem(fileName2))))));
        assertEquals(count, queryResult.getNumResults());

        query = new Query().append(FILE.key(), Arrays.asList(fileName1, fileName2)).append(STUDY.key(), studyMetadata1.getName())
                .append(INCLUDE_STUDY.key(), ALL)
                .append(INCLUDE_FILE.key(), ALL)
                .append(INCLUDE_SAMPLE.key(), ALL);
        queryResult = dbAdaptor.get(query, options);

        assertThat(queryResult, everyResult(allVariants,
                withStudy(studyMetadata1.getName(), withFileId(anyOf(hasItem(fileName1), hasItem(fileName2))))));
        assertEquals(count, queryResult.getNumResults());

        query = new Query().append(FILE.key(), Arrays.asList(fileName1, fileName2)).append(STUDY.key(), studyMetadata1.getName())
                .append(INCLUDE_STUDY.key(), ALL)
                .append(INCLUDE_FILE.key(), ALL);
        queryResult = dbAdaptor.get(query, options);

        List<Integer> expectedSamplesIds = new ArrayList<>(metadataManager.getFileMetadata(studyMetadata1.getId(), file1).getSamples());
        expectedSamplesIds.addAll(metadataManager.getFileMetadata(studyMetadata1.getId(), file2).getSamples());
        List<String> expectedSamples = expectedSamplesIds.stream()
                .map(s -> metadataManager.getSampleName(studyMetadata1.getId(), s))
                .collect(Collectors.toList());

        assertThat(queryResult, everyResult(withStudy(studyMetadata1.getName(),
                allOf(
                        withFileId(anyOf(hasItem(fileName1), hasItem(fileName2))),
                        withSamples(expectedSamples)
                )
        )));
        assertEquals(count, queryResult.getNumResults());
    }

    @Test()
    public void testGetAllVariants_filterFiles_not_1() {
        String unknownGenotype = "./.";
        query.append(FILE.key(), "!" + file1)
                .append(STUDY.key(), studyMetadata1.getName())
                .append(UNKNOWN_GENOTYPE.key(), unknownGenotype)
                .append(INCLUDE_STUDY.key(), studyMetadata1.getName());
        queryResult = dbAdaptor.get(query, options);

        FileMetadata fileMetadata = metadataManager.getFileMetadata(studyMetadata1.getId(), file1);
        for (Variant variant : queryResult.getResult()) {
            Set<String> returnedFileIds = variant.getStudies()
                    .stream()
                    .map(StudyEntry::getFiles)
                    .flatMap(Collection::stream)
                    .filter(fileEntry -> sameVariant(variant, fileEntry.getCall()))
                    .map(FileEntry::getFileId)
                    .collect(Collectors.toSet());
            assertEquals(Collections.singleton(fileName2), returnedFileIds);
            Set<String> returnedStudiesIds = variant.getStudiesMap().keySet();
            assertTrue("Returned studies :" + returnedStudiesIds.toString(), returnedStudiesIds.contains(studyMetadata1.getName()));
            StudyEntry sourceEntry = variant.getStudy(studyMetadata1.getName());
            for (String sampleName : sourceEntry.getOrderedSamplesName()) {
                String genotype = sourceEntry.getSampleData(sampleName, "GT");
                if (fileMetadata.getSamples().contains(metadataManager.getSampleId(studyMetadata1.getId(), sampleName))
                        && sourceEntry.getFile(fileName1) == null) {
                    assertEquals(unknownGenotype, genotype);
                } else {
                    assertNotEquals(unknownGenotype, genotype);
                }
            }
        }
    }

    @Test
    public void testGetAllVariants_filter() {
        // FILTER
        Query query = new Query(FILTER.key(), "PASS");
        long numResults = dbAdaptor.count(query).first();
        assertEquals(allVariants.getNumResults(), numResults);

        query.append(FILTER.key(), "NO_PASS");
        assertEquals(0, dbAdaptor.count(query).first().longValue());

        // FILTER+FILE1,FILE2
        query = new Query(FILE.key(), file1 + "," + file2).append(FILTER.key(), "PASS")
                .append(INCLUDE_STUDY.key(), ALL)
                .append(INCLUDE_FILE.key(), ALL)
                .append(INCLUDE_SAMPLE.key(), ALL);
        queryResult = dbAdaptor.get(query, null);
        assertThat(queryResult, everyResult(allVariants,
                withStudy(studyMetadata1.getName(), withFileId(anyOf(hasItem(fileName1), hasItem(fileName2))))));

        query.append(FILTER.key(), "NO_PASS");
        assertEquals(0, dbAdaptor.count(query).first().longValue());

        // FILTER+FILE1;!FILE2
        query = new Query(FILE.key(), file1 + ";!" + file2).append(FILTER.key(), "PASS")
                .append(INCLUDE_STUDY.key(), ALL)
                .append(INCLUDE_FILE.key(), ALL)
                .append(INCLUDE_SAMPLE.key(), ALL);
        queryResult = dbAdaptor.get(query, null);
        assertThat(queryResult, everyResult(allVariants,
                withStudy(studyMetadata1.getName(), withFileId(allOf(hasItem(fileName1), not(hasItem(fileName2)))))));

        query.append(FILTER.key(), "NO_PASS");
        assertEquals(0, dbAdaptor.count(query).first().longValue());

        // FILTER+STUDY
        query = new Query(STUDY.key(), studyMetadata1.getId()).append(FILTER.key(), "PASS")
                .append(INCLUDE_STUDY.key(), ALL)
                .append(INCLUDE_FILE.key(), ALL)
                .append(INCLUDE_SAMPLE.key(), ALL);
        queryResult = dbAdaptor.get(query, null);
        assertThat(queryResult, everyResult(allVariants, withStudy(studyMetadata1.getName())));

        query.append(FILTER.key(), "NO_PASS");
        assertEquals(0, dbAdaptor.count(query).first().longValue());

        // FILTER+FILE+STUDY
        query = new Query(FILE.key(), file1 + "," + file2)
                .append(INCLUDE_FILE.key(), ALL)
                .append(STUDY.key(), studyMetadata1.getId())
                .append(INCLUDE_STUDY.key(), ALL)
                .append(INCLUDE_SAMPLE.key(), ALL)
                .append(FILTER.key(), "PASS");
        queryResult = dbAdaptor.get(query, null);
        assertThat(queryResult, everyResult(allVariants,
                withStudy(studyMetadata1.getName(), withFileId(anyOf(hasItem(fileName1), hasItem(fileName2))))));

        query.append(FILTER.key(), "NO_PASS");
        assertEquals(0, dbAdaptor.count(query).first().longValue());
    }

    @Test
    public void testGetAllVariants_returnedFiles1() {
        testGetAllVariants_returnedFiles1(false);
    }

    @Test
    public void testGetAllVariants_returnedFiles1_implicit() {
        testGetAllVariants_returnedFiles1(true);
    }

    public void testGetAllVariants_returnedFiles1(boolean implicitReturnedFields) {

        query.append(FILE.key(), file1)
                .append(STUDY.key(), studyMetadata1.getName());
        if (!implicitReturnedFields) {
            query.append(INCLUDE_FILE.key(), file1)
                    .append(INCLUDE_STUDY.key(), studyMetadata1.getName());
        }

        queryResult = dbAdaptor.get(query, options);

        query = new Query(FILE.key(), file1);
        QueryResult<Long> queryResultFile = dbAdaptor.count(query);

        int expectedCount = 0;
        Set<String> expectedVariants = new HashSet<>();
        for (Variant variant : allVariants.getResult()) {
            if (variant.getStudies()
                    .stream()
                    .map(StudyEntry::getFiles)
                    .flatMap(Collection::stream)
                    .filter(fileEntry -> sameVariant(variant, fileEntry.getCall()))
                    .map(FileEntry::getFileId)
                    .anyMatch(fileName1::equals)) {
                expectedCount++;
                expectedVariants.add(variant.toString());
            }
        }
        Set<String> readVariants = queryResult.getResult().stream().map(Variant::toString).collect(Collectors.toSet());
        for (String expectedVariant : expectedVariants) {
            if (!readVariants.contains(expectedVariant)) {
                System.out.println("missing expected variant : " + expectedVariant);
            }
        }
        for (String readVariant : readVariants) {
            if (!expectedVariants.contains(readVariant)) {
                System.out.println("extra variant : " + readVariant);
            }
        }
        assertTrue(expectedCount > 0);
        assertEquals(queryResultFile.first().intValue(), queryResult.getNumResults());
        assertEquals(expectedCount, queryResult.getNumResults());
        assertEquals(expectedCount, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            Set<String> returnedFileIds = variant.getStudies()
                    .stream()
                    .map(StudyEntry::getFiles)
                    .flatMap(Collection::stream)
                    .map(FileEntry::getFileId)
                    .collect(Collectors.toSet());
            Set<String> returnedStudiesIds = variant.getStudies().stream().map(StudyEntry::getStudyId).collect(Collectors.toSet());

            assertEquals("Returned files :" + returnedFileIds.toString(), Collections.singleton(fileName1), returnedFileIds);
            assertEquals("Returned studies :" + returnedStudiesIds.toString(), Collections.singleton(studyMetadata1.getName()),
                    returnedStudiesIds);
        }
    }


    @Test
    public void testGetAllVariants_samples() {

        List<String> samples = metadataManager.getFileMetadata(studyMetadata1.getId(), file1).getSamples()
                .stream()
                .map(s -> metadataManager.getSampleName(studyMetadata1.getId(), s))
                .limit(5)
                .collect(Collectors.toList());

        query = new Query(STUDY.key(), studyMetadata1.getId())
                .append(SAMPLE.key(), String.join(AND, samples));
        queryResult = dbAdaptor.get(query, options);

        assertThat(queryResult, everyResult(withStudy(studyMetadata1.getName(), allOf(
                withSampleData(samples.get(0), "GT", containsString("1")),
                withSampleData(samples.get(1), "GT", containsString("1")),
                withSampleData(samples.get(2), "GT", containsString("1")),
                withSampleData(samples.get(3), "GT", containsString("1")),
                withSampleData(samples.get(4), "GT", containsString("1")),
                withSamples(samples),
                withFileId(is(Collections.singletonList(fileName1)))
        ))));

    }

    @Test
    public void testGetAllVariants_returnedSamples() {

        int i = 0;
        Set<String> sampleSet = new HashSet<>();
        Iterator<SampleMetadata> iterator = metadataManager.sampleMetadataIterator(studyMetadata1.getId());
        while (i++ < 5 && iterator.hasNext()) {
            sampleSet.add(iterator.next().getName());
        }

        query.append(INCLUDE_SAMPLE.key(), new ArrayList<>(sampleSet));
        queryResult = dbAdaptor.get(query, options);

        assertEquals(numVariants, queryResult.getNumResults());
        assertEquals(numVariants, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            for (StudyEntry sourceEntry : variant.getStudies()) {
                if (sourceEntry.getStudyId().equals(studyMetadata1.getName())) {
                    assertEquals("StudyId:" + sourceEntry.getStudyId() + ", SampleNames " + sourceEntry.getSamplesName(), sampleSet,
                            sourceEntry.getSamplesName());
                } else {
                    assertEquals("StudyId:" + sourceEntry.getStudyId() + ", SampleNames " + sourceEntry.getSamplesName(), Collections
                            .<String>emptySet(), sourceEntry.getSamplesName());
                }
            }
        }
    }


    private boolean sameVariant(Variant variant, String call) {
        if (StringUtils.isEmpty(call)) {
            return true;
        }
        String[] split = call.split(":", -1);
        List<VariantNormalizer.VariantKeyFields> normalized;
        if (variant.isSymbolic()) {
            normalized = new VariantNormalizer()
                    .normalizeSymbolic(Integer.parseInt(split[0]), variant.getEnd(), split[1], Arrays.asList(split[2].split(",")));
        } else {
            normalized = new VariantNormalizer()
                    .normalize(variant.getChromosome(), Integer.parseInt(split[0]), split[1], Arrays.asList(split[2].split(",")));
        }
        for (VariantNormalizer.VariantKeyFields variantKeyFields : normalized) {
            if (variantKeyFields.getStart() == variant.getStart()
                    && variantKeyFields.getReference().equals(variant.getReference())
                    && variantKeyFields.getAlternate().equals(variant.getAlternate())) {
                return true;
            }
        }
        return false;
    }

    @Test
    public void testSampleData() throws Exception {
        Set<String> validGts = new HashSet<>(Arrays.asList("0/1", "0|1", "1|0", "1/1", "1|1"));
        VariantDBIterator iterator = dbAdaptor.iterator(new Query(VariantQueryParam.STUDY.key(), studyMetadata1.getName()), null);
        for (int i = 0; i < 20; i++) {
            Variant variant = iterator.next();
            int expectedNumSamples = (int) variant.getStudies().get(0).getSamplesData().stream().filter(data -> validGts.contains(data.get(0))).count();
            int actualNumSamples = 0;
            Set<String> sampleNames = new HashSet<>(); // look for repeated samples
            int queries = 0;
            for (int skip = 0; skip < 1000; skip++) {
                QueryResult<VariantSampleData> queryResult = variantStorageEngine.getSampleData(variant.toString(), studyMetadata1.getName(),
                        new QueryOptions(QueryOptions.LIMIT, 10)
                                .append(QueryOptions.SKIP, skip * 10)
                );
                queries++;

                VariantSampleData sampleData = queryResult.first();
                int numSamples = sampleData.getSamples().values().stream().mapToInt(List::size).sum();
                if (numSamples == 0) {
                    break;
                }
                sampleData.getSamples().values().stream().flatMap(List::stream).forEach(sample -> sampleNames.add(sample.getId()));
                actualNumSamples += numSamples;
//                System.out.println(JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(queryResult));
            }
            assertEquals(variant.toString(), expectedNumSamples, actualNumSamples);
            assertEquals(variant.toString(), expectedNumSamples, sampleNames.size());
            System.out.println("variant = " + variant + ", samples: " + actualNumSamples + ", queries: " + queries);

        }
    }

}
