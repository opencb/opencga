/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.analysis.variant;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjectionParser;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;

import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.opencb.biodata.models.clinical.interpretation.DiseasePanel.GenePanel;
import static org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;

/**
 * Created on 12/12/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantCatalogQueryUtilsTest {

    @ClassRule
    public static CatalogManagerExternalResource catalogManagerExternalResource = new CatalogManagerExternalResource();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static CatalogManager catalog;
    private static String sessionId;
    private static VariantCatalogQueryUtils queryUtils;
    private static List<Sample> samples = new ArrayList<>();
    private static List<Individual> individuals = new ArrayList<>();
    private static File file1;
    private static File file2;
    private static File file3;
    private static File file4;
    private static File file5;

    @BeforeClass
    public static void setUp() throws Exception {
        catalog = catalogManagerExternalResource.getCatalogManager();

        User user = catalog.getUserManager().create("user", "user", "my@email.org", "1234", "ACME", 1000L, Account.AccountType.FULL, null).first();

        sessionId = catalog.getUserManager().login("user", "1234").getToken();
        catalog.getProjectManager().create("p1", "p1", "", "hsapiens", "Homo Sapiens", "GRCh38", null, sessionId);
        catalog.getStudyManager().create("p1", "s1", "s1", "s1", null, null, null, null, null, null, sessionId);
        catalog.getStudyManager().create("p1", "s2", "s2", "s2", null, null, null, null, null, null, sessionId);
        catalog.getStudyManager().create("p1", "s3", "s3", "s3", null, null, null, null, null, null, sessionId);
        file1 = createFile("data/file1.vcf");
        file2 = createFile("data/file2.vcf");


        Phenotype phenotype = new Phenotype("phenotype", "phenotype", "");
        Disorder disorder = new Disorder("disorder", "disorder", "", "", Collections.singletonList(phenotype), Collections.emptyMap());
        individuals.add(catalog.getIndividualManager().create("s1", new Individual("individual1", "individual1", IndividualProperty.Sex.MALE, null, null, 0, Collections.emptyList(), Collections.emptyMap()), null, sessionId).first());
        individuals.add(catalog.getIndividualManager().create("s1", new Individual("individual2", "individual2", IndividualProperty.Sex.FEMALE, null, null, 0, Collections.emptyList(), Collections.emptyMap()), null, sessionId).first());
        individuals.add(catalog.getIndividualManager().create("s1", new Individual("individual3", "individual3", IndividualProperty.Sex.MALE, null, null, 0, Collections.emptyList(), Collections.emptyMap()).setFather(individuals.get(0)).setMother(individuals.get(1)).setDisorders(Collections.singletonList(disorder)), null, sessionId).first());
        individuals.add(catalog.getIndividualManager().create("s1", new Individual("individual4", "individual4", IndividualProperty.Sex.FEMALE, null, null, 0, Collections.emptyList(), Collections.emptyMap()).setFather(individuals.get(0)).setMother(individuals.get(1)), null, sessionId).first());
        catalog.getFamilyManager().create(
                "s1",
                new Family("f1", "f1", Collections.singletonList(phenotype), Collections.singletonList(disorder), null, null, 3, null, null),
                individuals.stream().map(Individual::getId).collect(Collectors.toList()), new QueryOptions(),
                sessionId);


        createSample("sample1", "individual1");
        createSample("sample2", "individual2");
        catalog.getCohortManager().create("s1", new Cohort().setId("c1").setSamples(Collections.emptyList()), null, sessionId);

        catalog.getProjectManager().incrementRelease("p1", sessionId);
        file3 = createFile("data/file3.vcf");
        file4 = createFile("data/file4.vcf");
        file5 = createFile("data/file5.vcf", false);
        createSample("sample3", "individual3");
        createSample("sample4", "individual4");
        catalog.getCohortManager().create("s1", new Cohort().setId("c2").setSamples(Collections.emptyList()), null, sessionId);
        catalog.getCohortManager().create("s1", new Cohort().setId(StudyEntry.DEFAULT_COHORT).setSamples(samples), null, sessionId);

        catalog.getCohortManager().create("s2", new Cohort().setId(StudyEntry.DEFAULT_COHORT).setSamples(Collections.emptyList()), null, sessionId);

        catalog.getProjectManager().create("p2", "p2", "", "hsapiens", "Homo Sapiens", "GRCh38", null, sessionId);
        catalog.getStudyManager().create("p2", "p2s2", "p2s2", "p2s2", null, null, null, null, null, null, sessionId);

        Panel panel = new Panel("MyPanel", "MyPanel", 1);
        panel.setGenes(
                Arrays.asList(
                        new GenePanel().setName("BRCA2"),
                        new GenePanel().setName("CADM1"),
                        new GenePanel().setName("CTBP2P1"),
                        new GenePanel().setName("ADSL")
                ));

        catalog.getPanelManager().create("s1", panel, null, sessionId);


        queryUtils = new VariantCatalogQueryUtils(catalog);
    }

    public static void createSample(String name, String individualName) throws CatalogException {
        samples.add(catalog.getSampleManager().create("s1", new Sample().setId(name).setIndividualId(individualName), null, sessionId).first());
    }

    public static File createFile(String path) throws CatalogException {
        return createFile(path, true);
    }

    public static File createFile(String path, boolean indexed) throws CatalogException {
        File file = catalog.getFileManager().create("s1", File.Type.FILE, File.Format.VCF, File.Bioformat.VARIANT, path, null, 10, null, null, true, "", null, sessionId).first();
        if (indexed) {
            int release = catalog.getProjectManager().get("p1", null, sessionId).first().getCurrentRelease();
            catalog.getFileManager().updateFileIndexStatus(file, Status.READY, "", release, sessionId);
        }
        return file;
    }

    @Test
    public void queriesWithRelease() throws Exception {
        queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(VariantQueryParam.SAMPLE.key(), "sample2").append(VariantQueryParam.RELEASE.key(), 2), sessionId);
        queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(VariantQueryParam.SAMPLE.key(), "sample2").append(VariantQueryParam.RELEASE.key(), 1), sessionId);
        queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(VariantQueryParam.INCLUDE_SAMPLE.key(), "sample2").append(VariantQueryParam.RELEASE.key(), 1), sessionId);
        queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(VariantQueryParam.FILE.key(), "file1.vcf").append(VariantQueryParam.RELEASE.key(), 1), sessionId);
        queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(VariantQueryParam.STATS_MAF.key(), "c1>0.1").append(VariantQueryParam.RELEASE.key(), 1), sessionId);
        queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(VariantQueryParam.GENOTYPE.key(), "sample1:HOM_ALT,sample2:HET_REF").append(VariantQueryParam.RELEASE.key(), 1), sessionId);
    }

    @Test
    public void sampleWrongRelease() throws Exception {
        VariantQueryException e = VariantCatalogQueryUtils.wrongReleaseException(VariantQueryParam.SAMPLE, "sample3", 1);
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(VariantQueryParam.SAMPLE.key(), "sample3")
                .append(VariantQueryParam.STUDY.key(), "s1")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void sampleNotFound() throws Exception {
        thrown.expectMessage("not found");
        thrown.expect(CatalogException.class);
        queryUtils.parseQuery(new Query(VariantQueryParam.SAMPLE.key(), "sample_not_exists")
                .append(VariantQueryParam.STUDY.key(), "s1")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void includeSampleWrongRelease() throws Exception {
        VariantQueryException e = VariantCatalogQueryUtils.wrongReleaseException(VariantQueryParam.INCLUDE_SAMPLE, "sample3", 1);
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(VariantQueryParam.INCLUDE_SAMPLE.key(), "sample3")
                .append(VariantQueryParam.STUDY.key(), "s1")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void fileWrongRelease() throws Exception {
        VariantQueryException e = VariantCatalogQueryUtils.wrongReleaseException(VariantQueryParam.FILE, "file3.vcf", 1);
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(VariantQueryParam.FILE.key(), "file3.vcf")
                .append(VariantQueryParam.STUDY.key(), "s1")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void fileNotFound() throws Exception {
        thrown.expectMessage("not found");
        thrown.expect(CatalogException.class);
        queryUtils.parseQuery(new Query(VariantQueryParam.FILE.key(), "non_existing_file.vcf")
                .append(VariantQueryParam.STUDY.key(), "s1")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void fileNotIndexed() throws Exception {
        thrown.expectMessage("not indexed");
        thrown.expect(VariantQueryException.class);
        queryUtils.parseQuery(new Query(VariantQueryParam.FILE.key(), "file5.vcf")
                .append(VariantQueryParam.STUDY.key(), "s1")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void fileWrongNameWithRelease() throws Exception {
        thrown.expectMessage("not found");
        thrown.expect(CatalogException.class);
        queryUtils.parseQuery(new Query(VariantQueryParam.FILE.key(), "non_existing_file.vcf")
                .append(VariantQueryParam.STUDY.key(), "s1"), sessionId).toJson();
    }

    @Test
    public void includeFileWrongRelease() throws Exception {
        VariantQueryException e = VariantCatalogQueryUtils.wrongReleaseException(VariantQueryParam.INCLUDE_FILE, "file3.vcf", 1);
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(VariantQueryParam.INCLUDE_FILE.key(), "file3.vcf")
                .append(VariantQueryParam.STUDY.key(), "s1")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void cohortWrongRelease() throws Exception {
        VariantQueryException e = VariantCatalogQueryUtils.wrongReleaseException(VariantQueryParam.COHORT, "c2", 1);
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(VariantQueryParam.COHORT.key(), "c2")
                .append(VariantQueryParam.STUDY.key(), "s1")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void cohortStatsWrongRelease() throws Exception {
        VariantQueryException e = VariantCatalogQueryUtils.wrongReleaseException(VariantQueryParam.STATS_MAF, "c2", 1);
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(VariantQueryParam.STATS_MAF.key(), "c2>0.2")
                .append(VariantQueryParam.STUDY.key(), "s1")
                .append(VariantQueryParam.RELEASE.key(), 1), sessionId).toJson();
    }

    @Test
    public void parseQuery() throws Exception {
        assertEquals("user@p1:s1", parseValue(STUDY, "s1"));
        assertEquals("user@p1:s1,user@p1:s2", parseValue(STUDY, "s1,s2"));
        assertEquals("!user@p1:s1,user@p1:s2", parseValue(STUDY, "!s1,s2"));
        assertEquals("user@p1:s2;!user@p1:s1;user@p1:s3", parseValue(STUDY, "user@p1:s2;!s1;p1:s3"));

        assertEquals(file1.getName(), parseValue("s1", FILE, file1.getName()));
        assertEquals(file1.getName(), parseValue("s1", FILE, file1.getId()));
        assertEquals(file1.getName(), parseValue("s1", FILE, file1.getPath()));
        assertEquals(file1.getName() + "," + file2.getName(), parseValue("s1", FILE, file1.getPath() + "," + file2.getPath()));
        assertEquals(file1.getName() + ";" + file2.getName(), parseValue("s1", FILE, file1.getPath() + ";" + file2.getPath()));
//        assertEquals("file1.vcf", parseValue("s1", FILE, String.valueOf(file1.getUid())));

        assertEquals("sample1:HOM_ALT;sample2:HET_REF", parseValue("s1", GENOTYPE, "sample1:HOM_ALT;sample2:HET_REF"));
        assertEquals("sample1:HOM_ALT,sample2:HET_REF", parseValue("s1", GENOTYPE, "sample1:HOM_ALT,sample2:HET_REF"));
        assertEquals("sample2:HOM_ALT,sample1:HET_REF", parseValue("s1", GENOTYPE, "sample2:HOM_ALT,sample1:HET_REF"));


        assertEquals("c1;c2", parseValue("s1", COHORT, "c1;c2"));
        assertEquals("c1>0.1;c2>0.1", parseValue("s1", STATS_MAF, "c1>0.1;c2>0.1"));

        assertEquals("c1", parseValue("s1", COHORT, "s1:c1"));
        assertEquals("c1>0.1", parseValue("s1", STATS_MAF, "s1:c1>0.1"));

        assertEquals("c1", parseValue("s1", COHORT, "user@p1:s1:c1"));
        assertEquals("c1>0.1", parseValue("s1", STATS_MAF, "user@p1:s1:c1>0.1"));

        assertEquals("user@p1:s1:ALL;user@p1:s2:ALL", parseValue("s1,s2", COHORT, "s1:ALL;s2:ALL"));
        assertEquals("user@p1:s1:ALL>0.1;user@p1:s2:ALL>0.1", parseValue("s1,s2", STATS_MAF, "s1:ALL>0.1;s2:ALL>0.1"));
    }

    @Test
    public void queryBySavedFilter() throws Exception {
        String userId = catalog.getUserManager().getUserId(sessionId);
        catalog.getUserManager().addFilter(userId, "myFilter", "", Enums.Resource.VARIANT,
                new Query("key1", "value1").append("key2", "value2"), new QueryOptions(), sessionId);

        Query query = queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(SAVED_FILTER.key(), "myFilter"), sessionId);
        assertEquals(new Query().append(STUDY.key(), "user@p1:s1").append(SAVED_FILTER.key(), "myFilter").append("key1", "value1").append("key2", "value2"), query);

        query = queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(SAVED_FILTER.key(), "myFilter").append("key1", "otherValue"), sessionId);
        assertEquals(new Query().append(STUDY.key(), "user@p1:s1").append(SAVED_FILTER.key(), "myFilter").append("key1", "otherValue").append("key2", "value2"), query);
    }

    @Test
    public void queryByFamily() throws Exception {
        Query query = queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(FAMILY.key(), "f1"), sessionId);
        assertEquals(Arrays.asList("sample1", "sample2", "sample3", "sample4"), query.getAsStringList(SAMPLE.key()));
        assertFalse(VariantQueryUtils.isValidParam(query, GENOTYPE));
    }

    @Test
    public void queryByFamilyMembers() throws Exception {
        Query query = queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(FAMILY.key(), "f1").append(FAMILY_MEMBERS.key(), "individual1,individual4"), sessionId);
        assertEquals(Arrays.asList("sample1", "sample4"), query.getAsStringList(SAMPLE.key()));
    }

    @Test
    public void queryByFamilySegregation() throws Exception {
        Query query = queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(FAMILY.key(), "f1").append(FAMILY_SEGREGATION.key(), "BIALLELIC"), sessionId);
        assertEquals("sample1:0/1,0|1,1|0;sample2:0/1,0|1,1|0;sample3:1/1,1|1;sample4:0/0,0/1,0|0,0|1,1|0", query.getString(GENOTYPE.key()));
        assertFalse(VariantQueryUtils.isValidParam(query, SAMPLE));
    }

    @Test
    public void queryBySampleSegregation() throws Exception {
        Query query = queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(SAMPLE.key(), "sample3:biallelic"), sessionId);
        assertEquals("sample1:0/1,0|1,1|0;sample2:0/1,0|1,1|0;sample3:1/1,1|1", query.getString(GENOTYPE.key()));
        assertFalse(VariantQueryUtils.isValidParam(query, SAMPLE));
    }

    @Test
    public void queryBySampleSegregationDeNovo() throws Exception {
        Query query = queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(SAMPLE.key(), "sample3:denovo"), sessionId);
        assertEquals("sample3", query.getString(SAMPLE_DE_NOVO.key()));
        assertFalse(VariantQueryUtils.isValidParam(query, GENOTYPE));
        assertFalse(VariantQueryUtils.isValidParam(query, SAMPLE));
    }

    @Test
    public void queryBySampleSegregationMendelianError() throws Exception {
        Query query = queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(SAMPLE.key(), "sample3:mendelianerror"), sessionId);
        assertEquals("sample3", query.getString(SAMPLE_MENDELIAN_ERROR.key()));
        assertFalse(VariantQueryUtils.isValidParam(query, GENOTYPE));
        assertFalse(VariantQueryUtils.isValidParam(query, SAMPLE));
    }

    @Test
    public void queryByFamilySegregationMendelianError() throws Exception {
        Query query = queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(FAMILY.key(), "f1").append(FAMILY_SEGREGATION.key(), "mendelianError"), sessionId);
        assertEquals("sample3,sample4", query.getString(VariantQueryUtils.SAMPLE_MENDELIAN_ERROR.key()));
        assertFalse(VariantQueryUtils.isValidParam(query, SAMPLE));
        assertFalse(VariantQueryUtils.isValidParam(query, VariantQueryUtils.SAMPLE_DE_NOVO));
        assertTrue(VariantQueryProjectionParser.isIncludeSamplesDefined(query, Collections.singleton(VariantField.STUDIES_SAMPLES)));
    }

    @Test
    public void queryByFamilySegregationDeNovo() throws Exception {
        Query query = queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(FAMILY.key(), "f1").append(FAMILY_SEGREGATION.key(), "deNovo"), sessionId);
        assertEquals("sample3,sample4", query.getString(VariantQueryUtils.SAMPLE_DE_NOVO.key()));
        assertFalse(VariantQueryUtils.isValidParam(query, SAMPLE));
        assertFalse(VariantQueryUtils.isValidParam(query, VariantQueryUtils.SAMPLE_MENDELIAN_ERROR));
        assertTrue(VariantQueryProjectionParser.isIncludeSamplesDefined(query, Collections.singleton(VariantField.STUDIES_SAMPLES)));
    }

    @Test
    public void queryByFamilyWithoutStudy() throws CatalogException {
        VariantQueryException e = VariantQueryException.missingStudyFor("family", "f1", Collections.emptyList());
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(FAMILY.key(), "f1"), sessionId);
    }

    @Test
    public void queryByFamilyNotFound() throws CatalogException {
        CatalogException e = new CatalogException("Family asdf not found");
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(FAMILY.key(), "asdf").append(FAMILY_DISORDER.key(), "asdf"), sessionId);
    }

    @Test
    public void queryByFamilyMissingModeOfInheritance() throws CatalogException {
        VariantQueryException e = VariantQueryException.malformedParam(FAMILY_DISORDER, "asdf", "Require parameter \""
                + FAMILY.key() + "\" and \"" + FAMILY_SEGREGATION.key() + "\" to use \""+FAMILY_DISORDER.key()+"\".");
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(FAMILY.key(), "f1").append(FAMILY_DISORDER.key(), "asdf"), sessionId);
    }

    @Test
    public void queryByFamilyUnknownDisorder() throws CatalogException {
        VariantQueryException e = VariantQueryException.malformedParam(FAMILY_DISORDER, "asdf", "Available disorders: [disorder]");
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(STUDY.key(), "s1")
                .append(FAMILY.key(), "f1")
                .append(FAMILY_SEGREGATION.key(), "autosomal_dominant")
                .append(FAMILY_DISORDER.key(), "asdf"), sessionId);
    }

    @Test
    public void queryByPanel() throws Exception {
        Query query = queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(PANEL.key(), "MyPanel"), sessionId);
        assertEquals(new HashSet<>(Arrays.asList("BRCA2", "CADM1", "CTBP2P1", "ADSL")), new HashSet<>(query.getAsList(GENE.key())));
        query = queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(PANEL.key(), "MyPanel").append(GENE.key(), "ASDF"), sessionId);
        assertEquals(new HashSet<>(Arrays.asList("BRCA2", "CADM1", "CTBP2P1", "ADSL", "ASDF")), new HashSet<>(query.getAsList(GENE.key())));
        assertEquals(true, query.getBoolean(SKIP_MISSING_GENES, false));
    }

    @Test
    public void queryByPanelNotFound() throws Exception {
        CatalogException e = new CatalogException("Panel 'MyPanel_wrong' not found");
        thrown.expectMessage(e.getMessage());
        thrown.expect(e.getClass());
        queryUtils.parseQuery(new Query(STUDY.key(), "s1").append(PANEL.key(), "MyPanel_wrong"), sessionId);
    }

    @Test
    public void getAnyStudy() throws Exception {
        assertEquals("user@p1:s1", queryUtils.getAnyStudy(new Query(PROJECT.key(), "p1"), sessionId));
        assertEquals("user@p2:p2s2", queryUtils.getAnyStudy(new Query(PROJECT.key(), "p2"), sessionId));
        assertEquals("user@p2:p2s2", queryUtils.getAnyStudy(new Query(STUDY.key(), "p2s2"), sessionId));
        assertEquals("user@p2:p2s2", queryUtils.getAnyStudy(new Query(INCLUDE_STUDY.key(), "p2s2"), sessionId));
        assertEquals("user@p2:p2s2", queryUtils.getAnyStudy(new Query(STUDY.key(), "p2s2").append(INCLUDE_STUDY.key(), "all"), sessionId));
        thrown.expect(CatalogException.class);
        thrown.expectMessage("Multiple projects");
        queryUtils.getAnyStudy(new Query(), sessionId);
    }

    @Test
    public void getTriosFromFamily() throws Exception {

        Family f1 = catalog.getFamilyManager().get("s1", "f1", null, sessionId).first();
        VariantStorageMetadataManager metadataManager = new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory());
        StudyMetadata s1 = metadataManager.createStudy("s1");
        List<Integer> sampleIds = metadataManager.registerSamples(s1.getId(), Arrays.asList(
                "sample1",
                "sample2",
                "sample3",
                "sample4"
        ));

        for (Integer sampleId : sampleIds) {
            metadataManager.updateSampleMetadata(s1.getId(), sampleId,
                    sampleMetadata -> sampleMetadata.setIndexStatus(TaskMetadata.Status.READY));
        }

        List<List<String>> trios = queryUtils.getTriosFromFamily("s1", f1, metadataManager, true, sessionId);
//        System.out.println("trios = " + trios);
        assertEquals(Arrays.asList(Arrays.asList("sample1", "sample2", "sample3"), Arrays.asList("sample1", "sample2", "sample4")), trios);

    }

    @Test
    public void testQueryParams() throws IllegalAccessException {
        Class<VariantCatalogQueryUtils> clazz = VariantCatalogQueryUtils.class;
        Field[] declaredFields = clazz.getDeclaredFields();
        Set<QueryParam> params = new HashSet<>();
        for (Field field : declaredFields) {
            if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                if (field.getAnnotation(Deprecated.class)==null) {
                    if (QueryParam.class.isAssignableFrom(field.getType())) {
                        params.add((QueryParam) field.get(null));
                    }
                }
            }
        }
        assertEquals(new HashSet<>(VariantCatalogQueryUtils.VARIANT_CATALOG_QUERY_PARAMS), params);
    }

    protected String parseValue(VariantQueryParam param, String value) throws CatalogException {
        return queryUtils.parseQuery(new Query(param.key(), value), sessionId).getString(param.key());
    }

    protected String parseValue(String study, VariantQueryParam param, String value) throws CatalogException {
        return queryUtils.parseQuery(new Query(STUDY.key(), study).append(param.key(), value), sessionId).getString(param.key());
    }

}
