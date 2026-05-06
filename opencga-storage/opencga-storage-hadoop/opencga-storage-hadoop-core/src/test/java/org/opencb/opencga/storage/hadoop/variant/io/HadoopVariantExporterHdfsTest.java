package org.opencb.opencga.storage.hadoop.variant.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.io.VariantExporter;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonReader;
import org.opencb.opencga.storage.hadoop.utils.MapReduceOutputFile;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;

import java.net.URI;

import static org.junit.Assert.assertNotNull;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;

/**
 * HDFS-specific export tests. Verifies that export to hdfs:/// paths works correctly.
 */
@Category(MediumTests.class)
public class HadoopVariantExporterHdfsTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();

    private static HadoopVariantStorageEngine variantStorageEngine;
    private static final String study1 = "st1";
    private static final String study2 = "st2";

    @BeforeClass
    public static void beforeClass() throws Exception {
        variantStorageEngine = externalResource.getVariantStorageEngine();

        URI inputUri = VariantStorageBaseTest.getResourceUri("platinum/1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz");
        VariantStorageBaseTest.runDefaultETL(inputUri, variantStorageEngine, new StudyMetadata(0, study1),
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false)
                        .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
        );

        inputUri = VariantStorageBaseTest.getResourceUri("platinum/1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz");
        VariantStorageBaseTest.runDefaultETL(inputUri, variantStorageEngine, new StudyMetadata(0, study1),
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false)
                        .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
        );

        inputUri = VariantStorageBaseTest.getResourceUri("platinum/1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz");
        VariantStorageBaseTest.runDefaultETL(inputUri, variantStorageEngine, new StudyMetadata(0, study2),
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false)
                        .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
        );

        variantStorageEngine.annotate(newOutputUri(), new ObjectMap());

        VariantHbaseTestUtils.printVariants(variantStorageEngine.getDBAdaptor(), newOutputUri());
    }

    @Before
    public void before() throws Exception {
        // Do not clean database!
    }

    @Test
    public void exportVcfHdfs() throws Exception {
        String fileName = "variants.vcf";
        URI uri = URI.create("hdfs:///" + fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.VCF,
                null, new Query(STUDY.key(), study1), new QueryOptions());
        copyToLocal(fileName, uri);
    }

    @Test
    public void exportJsonHdfs() throws Exception {
        String fileName = "variants.json";
        URI uri = URI.create("hdfs:///" + fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.JSON,
                null, new VariantQuery().study(study1).includeSampleAll(), new QueryOptions());
        copyToLocal(fileName, uri);
    }

    @Test
    public void exportAvroHdfs() throws Exception {
        String fileName = "variants.avro";
        URI uri = URI.create("hdfs:///" + fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.AVRO,
                null, new Query(STUDY.key(), study1), new QueryOptions());
        copyToLocal(fileName, uri);
    }

    @Test
    public void exportJsonSparseHdfs() throws Exception {
        String fileName = "variants.sparse.json";
        URI hdfsUri = URI.create("hdfs:///" + fileName);
        variantStorageEngine.exportData(hdfsUri, VariantWriterFactory.VariantOutputFormat.JSON_SPARSE, null,
                new VariantQuery().study(study1).includeSampleAll(),
                new QueryOptions(HadoopVariantExporter.SKIP_SMALL_QUERY, true));

        URI localUri = copyToLocal(fileName, hdfsUri);

        new VariantJsonReader(null, localUri.getPath()).forEach(variant -> {
            assertNotNull(variant.getStudies());
            assertNotNull(variant.getStudies().get(0).getSamples());
            for (SampleEntry sample : variant.getStudies().get(0).getSamples()) {
                assertNotNull(sample.getSampleId());
                assertNotNull(sample.getFileIndex());
                assertNotNull(variant.getStudies().get(0).getFile(sample.getFileIndex()));
                assertNotNull(sample.getData());
                assertNotNull(sample.getData().get(0));
            }
        });
    }

    private URI copyToLocal(String fileName, URI uri) throws Exception {
        URI localOutdir = newOutputUri();
        Configuration conf = externalResource.getVariantStorageEngine().getConf();
        URI target = localOutdir.resolve(fileName);
        URI metaUri;
        URI metaUriTarget;

        System.out.println("Copy file " + uri + " to " + target);

        if (fileName.endsWith(VariantExporter.TPED_FILE_EXTENSION)) {
            metaUri = new URI(uri.toString().replace(VariantExporter.TPED_FILE_EXTENSION, VariantExporter.TFAM_FILE_EXTENSION));
            metaUriTarget = localOutdir.resolve(
                    fileName.replace(VariantExporter.TPED_FILE_EXTENSION, VariantExporter.METADATA_FILE_EXTENSION));
        } else {
            metaUri = new URI(uri.toString() + VariantExporter.METADATA_FILE_EXTENSION);
            metaUriTarget = localOutdir.resolve(fileName + VariantExporter.METADATA_FILE_EXTENSION);
        }
        new MapReduceOutputFile(new Path(uri), new Path(target), conf).postExecute(true);
        FileSystem.get(externalResource.getConf()).copyToLocalFile(false,
                new Path(metaUri),
                new Path(metaUriTarget));
        return target.toURL().toURI();
    }
}
