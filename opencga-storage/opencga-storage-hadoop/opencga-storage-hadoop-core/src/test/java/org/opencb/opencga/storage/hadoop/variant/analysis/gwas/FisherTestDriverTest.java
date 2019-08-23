package org.opencb.opencga.storage.hadoop.variant.analysis.gwas;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.models.metadata.SampleSetType;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.annotators.CellBaseRestVariantAnnotator;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;

import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class FisherTestDriverTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    @Rule
    public ExternalResource externalResource = new HadoopExternalResource();

    @Test
    public void testFisher() throws Exception {

        StudyMetadata studyMetadata = newStudyMetadata();

        ObjectMap params = new ObjectMap(VariantStorageEngine.Options.STUDY_TYPE.key(), SampleSetType.FAMILY)
                .append(VariantStorageEngine.Options.ANNOTATE.key(), true)
                .append(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), "DS,GL")
                .append(VariantAnnotationManager.VARIANT_ANNOTATOR_CLASSNAME, CellBaseRestVariantAnnotator.class.getName())
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false);

        HadoopVariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        runDefaultETL(smallInputUri, variantStorageEngine, studyMetadata, params);
        VariantHadoopDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();

        URI localOut = newOutputUri();
        FileSystem fs = FileSystem.get(configuration.get());

        ObjectMap objectMap = new ObjectMap()
                .append(FisherTestDriver.CONTROL_COHORT, "1,2")
                .append(FisherTestDriver.CASE_COHORT, "3,4")
                .append(FisherTestDriver.OUTDIR, "fisher_result");
        new TestMRExecutor().run(FisherTestDriver.class, FisherTestDriver.buildArgs(
                dbAdaptor.getArchiveTableName(1),
                dbAdaptor.getVariantTable(),
                1,
                Collections.emptySet(), objectMap), objectMap);

        fs.copyToLocalFile(new Path("fisher_result"), new Path(localOut));

        objectMap.append(FisherTestDriver.OUTDIR, "fisher_result2")
                .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), "lof,missense_variant")
                .append(VariantQueryParam.ANNOT_BIOTYPE.key(), "protein_coding");
        new TestMRExecutor().run(FisherTestDriver.class, FisherTestDriver.buildArgs(
                dbAdaptor.getArchiveTableName(1),
                dbAdaptor.getVariantTable(),
                1,
                Collections.emptySet(), objectMap), objectMap);

        fs.copyToLocalFile(new Path("fisher_result2"), new Path(localOut));
        System.out.println("localOut = " + localOut);

        Set<String> lines1 = new HashSet<>();
        try (FSDataInputStream is = fs.open(new Path("fisher_result/part-r-00000"))) {
            String x = is.readLine();
            while (x != null) {
                System.out.println(x);
                if (!x.startsWith("#")) {
                    lines1.add(x);
                }
                x = is.readLine();
            }
        }
        try (FSDataInputStream is = fs.open(new Path("fisher_result2/part-r-00000"))) {
            String x = is.readLine();
            while (x != null) {
                System.out.println(x);
                if (!x.startsWith("#")) {
                    Assert.assertTrue(lines1.contains(x));
                }
                x = is.readLine();
            }
        }

    }

}