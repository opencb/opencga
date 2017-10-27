package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils.printVariants;

/**
 * Created on 27/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FillGapsTaskTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

    public static void fillGaps(HadoopVariantStorageEngine variantStorageEngine, StudyConfiguration studyConfiguration,
                                Collection<Integer> sampleIds)
            throws StorageEngineException, IOException {
        VariantHadoopDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();
        FillGapsTask fillGapsTask = new FillGapsTask(dbAdaptor.getHBaseManager(),
                variantStorageEngine.getVariantTableName(),
                variantStorageEngine.getArchiveTableName(studyConfiguration.getStudyId()),
                studyConfiguration, dbAdaptor.getGenomeHelper(), sampleIds);
        fillGapsTask.pre();

        ProgressLogger progressLogger = new ProgressLogger("Fill gaps:", dbAdaptor.count(new Query()).first(), 10);
        for (Variant variant : dbAdaptor) {
            progressLogger.increment(1, variant::toString);
            fillGapsTask.fillGapsAndPut(variant);
        }

        fillGapsTask.post();
    }


    @Test
    public void testFillGapsPlatinumFiles() throws Exception {
        StudyConfiguration studyConfiguration = loadPlatinum(new ObjectMap()
                        .append(VariantStorageEngine.Options.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC), 4);

        HadoopVariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        VariantHadoopDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();
        List<Integer> sampleIds = new ArrayList<>(studyConfiguration.getSampleIds().values());
        sampleIds.sort(Integer::compareTo);

        List<Integer> subSamples = sampleIds.subList(0, sampleIds.size() / 2);
        System.out.println("subSamples = " + subSamples);
        fillGaps(variantStorageEngine, studyConfiguration, subSamples);
        printVariants(studyConfiguration, dbAdaptor, newOutputUri());
        checkMissing(studyConfiguration, dbAdaptor, subSamples);

        subSamples = sampleIds.subList(sampleIds.size() / 2, sampleIds.size());
        System.out.println("subSamples = " + subSamples);
        fillGaps(variantStorageEngine, studyConfiguration, subSamples);
        printVariants(studyConfiguration, dbAdaptor, newOutputUri());
        checkMissing(studyConfiguration, dbAdaptor, subSamples);

        subSamples = sampleIds;
        System.out.println("subSamples = " + subSamples);
        fillGaps(variantStorageEngine, studyConfiguration, subSamples);
        printVariants(studyConfiguration, dbAdaptor, newOutputUri());
        checkMissing(studyConfiguration, dbAdaptor, subSamples);
    }

    private StudyConfiguration loadPlatinum(ObjectMap extraParams, int max) throws Exception {

        StudyConfiguration studyConfiguration = VariantStorageBaseTest.newStudyConfiguration();
        HadoopVariantStorageEngine variantStorageManager = getVariantStorageEngine();
        VariantHadoopDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor();

        List<URI> inputFiles = new LinkedList<>();

        for (int fileId = 12877; fileId <= 12893; fileId++) {
            String fileName = "platinum/1K.end.platinum-genomes-vcf-NA" + fileId + "_S1.genome.vcf.gz";
            inputFiles.add(getResourceUri(fileName));
            max--;
            if (max == 0) {
                break;
            }
        }

        ObjectMap options = variantStorageManager.getConfiguration().getStorageEngine(variantStorageManager.getStorageEngineId()).getVariant().getOptions();
        options.put(VariantStorageEngine.Options.STUDY_ID.key(), studyConfiguration.getStudyId());
        options.put(VariantStorageEngine.Options.STUDY_NAME.key(), studyConfiguration.getStudyName());
        options.put(HadoopVariantStorageEngine.VARIANT_TABLE_INDEXES_SKIP, true);
        options.putAll(extraParams);
        List<StoragePipelineResult> index = variantStorageManager.index(inputFiles, outputUri, true, true, true);

        for (StoragePipelineResult storagePipelineResult : index) {
            System.out.println(storagePipelineResult);
        }

        URI outputUri = newOutputUri(1);
        studyConfiguration = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyConfiguration.getStudyId(), null).first();
        printVariants(studyConfiguration, dbAdaptor, outputUri);

        return studyConfiguration;
    }

    protected void checkMissing(StudyConfiguration studyConfiguration, VariantHadoopDBAdaptor dbAdaptor, List<Integer> sampleIds) {
        for (Variant variant : dbAdaptor) {
            boolean anyUnknown = false;
            boolean allUnknown = true;
            for (Integer sampleId : sampleIds) {
                boolean unknown = variant.getStudies().get(0).getSampleData(studyConfiguration.getSampleIds().inverse().get(sampleId), "GT").equals("?/?");
                anyUnknown |= unknown;
                allUnknown &= unknown;
            }
            // Fail if any, but not all samples are unknown
            Assert.assertFalse(anyUnknown && !allUnknown);
//            Assert.assertTrue(allUnknown || !anyUnknown);
        }
    }
}