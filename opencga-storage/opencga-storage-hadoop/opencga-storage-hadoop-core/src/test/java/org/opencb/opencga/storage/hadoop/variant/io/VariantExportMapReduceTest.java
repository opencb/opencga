package org.opencb.opencga.storage.hadoop.variant.io;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;

import java.net.URI;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.GENOTYPE;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.STUDY;

/**
 * Created on 11/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantExportMapReduceTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    private static StudyConfiguration studyConfiguration;

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();
    private static HadoopVariantStorageEngine variantStorageEngine;

    @BeforeClass
    public static void beforeClass() throws Exception {
        variantStorageEngine = externalResource.getVariantStorageEngine();
        externalResource.clearDB(variantStorageEngine.getVariantTableName());
        externalResource.clearDB(variantStorageEngine.getArchiveTableName(STUDY_ID));

//        URI inputUri = VariantStorageBaseTest.getResourceUri("sample1.genome.vcf");
        URI inputUri = VariantStorageBaseTest.getResourceUri("variant-test-file.vcf.gz");

        studyConfiguration = VariantStorageBaseTest.newStudyConfiguration();
        VariantStorageBaseTest.runDefaultETL(inputUri, variantStorageEngine, studyConfiguration,
                new ObjectMap(VariantStorageEngine.Options.ANNOTATE.key(), true)
                        .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
        );

        VariantHbaseTestUtils.printVariants(variantStorageEngine.getDBAdaptor(), newOutputUri());

    }

    @Before
    public void before() throws Exception {
        // Do not clean database!
    }

    @Test
    public void exportAvro() throws Exception {
        URI uri = URI.create("hdfs:///variants.avro");
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.AVRO, new Query(STUDY.key(), STUDY_NAME), new QueryOptions());

        FileSystem.get(externalResource.getConf()).copyToLocalFile(new Path(uri), new Path(outputUri.resolve("variants.avro")));
    }


}
