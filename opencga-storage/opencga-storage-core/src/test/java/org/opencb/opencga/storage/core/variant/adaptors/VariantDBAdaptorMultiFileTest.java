package org.opencb.opencga.storage.core.variant.adaptors;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.AND;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.NOT;

/**
 * Created on 24/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Ignore
public abstract class VariantDBAdaptorMultiFileTest extends VariantStorageBaseTest {

    protected static boolean loaded = false;
    private VariantDBAdaptor dbAdaptor;
    private Query query;
    private QueryOptions options = new QueryOptions();
    private VariantQueryResult<Variant> queryResult;

    @Before
    public void before() throws Exception {
        dbAdaptor = variantStorageEngine.getDBAdaptor();
        if (!loaded) {
            super.before();

            VariantStorageEngine storageEngine = getVariantStorageEngine();
            ObjectMap options = getOptions();

            int studyId = 1;
            List<URI> inputFiles = new ArrayList<>();
            StudyConfiguration studyConfiguration = new StudyConfiguration(studyId, "S_" + studyId);
            for (int fileId = 12877; fileId <= 12893; fileId++) {
                String fileName = "1K.end.platinum-genomes-vcf-NA" + fileId + "_S1.genome.vcf.gz";
                URI inputFile = getResourceUri("platinum/" + fileName);
                inputFiles.add(inputFile);
                studyConfiguration.getFileIds().put(fileName, fileId);
                if (inputFiles.size() == 4) {
                    dbAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, null);
                    options.put(VariantStorageEngine.Options.STUDY_ID.key(), studyId);
                    storageEngine.getOptions().putAll(options);
                    storageEngine.index(inputFiles, outputUri, true, true, true);

                    studyId++;
                    studyConfiguration = new StudyConfiguration(studyId, "S_" + studyId);
                    inputFiles.clear();
                }
            }
            loaded = true;
        }
    }

    protected ObjectMap getOptions() {
        return new ObjectMap();
    }

    @Test
    public void testGetByFileName() throws Exception {
        query = new Query()
                .append(VariantQueryParam.STUDIES.key(), "S_1")
                .append(VariantQueryParam.FILES.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz");
        queryResult = dbAdaptor.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.RETURNED_STUDIES.key(), "S_1")
                .append(VariantQueryParam.RETURNED_SAMPLES.key(), "NA12877")
                .append(VariantQueryParam.RETURNED_FILES.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz"), options);
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", withFileId("12877"))));

    }

    @Test
    public  void testGetByFileNamesOr() {
        query = new Query()
                .append(VariantQueryParam.STUDIES.key(), "S_1")
                .append(VariantQueryParam.FILES.key(),
                        "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz"
                                + VariantQueryUtils.OR +
                        "1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz");
        queryResult = dbAdaptor.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.RETURNED_STUDIES.key(), "S_1")
                .append(VariantQueryParam.RETURNED_SAMPLES.key(), "NA12877,NA12878")
                .append(VariantQueryParam.RETURNED_FILES.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz,1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz"), options);
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", anyOf(withFileId("12877"), withFileId("12878")))));
    }

    @Test
    public void testGetByFileNamesAnd() {
        query = new Query()
                .append(VariantQueryParam.STUDIES.key(), "S_1")
                .append(VariantQueryParam.FILES.key(),
                        "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz"
                                + AND +
                        "1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz");
        queryResult = dbAdaptor.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.RETURNED_STUDIES.key(), "S_1")
                .append(VariantQueryParam.RETURNED_SAMPLES.key(), "NA12877,NA12878")
                .append(VariantQueryParam.RETURNED_FILES.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz,1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz"), options);
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", allOf(withFileId("12877"), withFileId("12878")))));
    }

    @Test
    public void testGetByFileNamesAndNegated() {
        query = new Query()
                .append(VariantQueryParam.STUDIES.key(), "S_1")
                .append(VariantQueryParam.FILES.key(),
                        "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz"
                                + AND + NOT +
                                "1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz")
                .append(VariantQueryParam.RETURNED_SAMPLES.key(), "NA12877,NA12878")
                .append(VariantQueryParam.RETURNED_FILES.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz,1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz");        queryResult = dbAdaptor.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.RETURNED_STUDIES.key(), "S_1")
                .append(VariantQueryParam.RETURNED_SAMPLES.key(), "NA12877,NA12878")
                .append(VariantQueryParam.RETURNED_FILES.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz,1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz"), options);
        assertThat(queryResult, everyResult(allVariants, withStudy("S_1", allOf(withFileId("12877"), not(withFileId("12878"))))));
    }

    @Test
    public void testGetByFileNamesMultiStudiesAnd() {
        query = new Query()
                .append(VariantQueryParam.STUDIES.key(), "S_1,S_2")
                .append(VariantQueryParam.FILES.key(),
                        "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz"
                                + AND +
                        "1K.end.platinum-genomes-vcf-NA12882_S1.genome.vcf.gz");
        queryResult = dbAdaptor.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.RETURNED_STUDIES.key(), "S_1,S_2")
                .append(VariantQueryParam.RETURNED_SAMPLES.key(), "NA12877,NA12882")
                .append(VariantQueryParam.RETURNED_FILES.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz,1K.end.platinum-genomes-vcf-NA12882_S1.genome.vcf.gz"), options);
        assertThat(queryResult, everyResult(allVariants, allOf(withStudy("S_1", withFileId("12877")), withStudy("S_2", withFileId("12882")))));
    }

    @Test
    public void testGetByFileNamesMultiStudiesOr() {
        query = new Query()
                .append(VariantQueryParam.STUDIES.key(), "S_1,S_2")
                .append(VariantQueryParam.FILES.key(),
                        "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz"
                                + VariantQueryUtils.OR +
                        "1K.end.platinum-genomes-vcf-NA12882_S1.genome.vcf.gz");
        queryResult = dbAdaptor.get(query, options);
        Variant variant = queryResult.getResult().get(0);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.RETURNED_STUDIES.key(), "S_1,S_2")
                .append(VariantQueryParam.RETURNED_SAMPLES.key(), "NA12877,NA12882")
                .append(VariantQueryParam.RETURNED_FILES.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz,1K.end.platinum-genomes-vcf-NA12882_S1.genome.vcf.gz"), options);
        System.out.println(variant);
        System.out.println("from result       "+variant.toJson());
        System.out.println("from all variants "+allVariants.getResult().stream().filter(v -> v.toString().equals(variant.toString())).findFirst().get().toJson());
        assertThat(queryResult, everyResult(allVariants, anyOf(withStudy("S_1", withFileId("12877")), withStudy("S_2", withFileId("12882")))));
    }

    @Test
    public void testGetByFileNamesMultiStudiesImplicitAnd() {
        query = new Query()
                .append(VariantQueryParam.FILES.key(),
                        "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz"
                                + AND +
                                "1K.end.platinum-genomes-vcf-NA12882_S1.genome.vcf.gz");
        queryResult = dbAdaptor.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.RETURNED_SAMPLES.key(), "NA12877,NA12882")
                .append(VariantQueryParam.RETURNED_FILES.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz,1K.end.platinum-genomes-vcf-NA12882_S1.genome.vcf.gz"), options);
        assertThat(queryResult, everyResult(allVariants, allOf(withStudy("S_1", withFileId("12877")), withStudy("S_2", withFileId("12882")))));
    }

    @Test
    public void testGetByFileNamesMultiStudiesImplicitOr() {
        query = new Query()
                .append(VariantQueryParam.FILES.key(),
                        "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz"
                                + VariantQueryUtils.OR +
                                "1K.end.platinum-genomes-vcf-NA12882_S1.genome.vcf.gz");
        queryResult = dbAdaptor.get(query, options);
        VariantQueryResult<Variant> allVariants = dbAdaptor.get(new Query()
                .append(VariantQueryParam.RETURNED_SAMPLES.key(), "NA12877,NA12882")
                .append(VariantQueryParam.RETURNED_FILES.key(), "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz,1K.end.platinum-genomes-vcf-NA12882_S1.genome.vcf.gz"), options);
        assertThat(queryResult, everyResult(allVariants, anyOf(withStudy("S_1", withFileId("12877")), withStudy("S_2", withFileId("12882")))));
    }

}
