package org.opencb.opencga.storage.hadoop.variant.io;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.io.VariantExporter;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;

/**
 * Created on 11/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@RunWith(Parameterized.class)
public class HadoopVariantExporterTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    private final boolean exportToLocal;

    @Parameterized.Parameters(name="{0}")
    public static Object[][] parameters() {
        return new Object[][]{{"Export to local", true}, {"Export to HDFS", false}};
    }

    public HadoopVariantExporterTest(String name, Boolean exportToLocal) {
        this.exportToLocal = exportToLocal;
        System.out.println(name);
    }

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();
    private static HadoopVariantStorageEngine variantStorageEngine;
    private static final String study1 = "st1";
    private static final String study2 = "st2";

    @BeforeClass
    public static void beforeClass() throws Exception {
        variantStorageEngine = externalResource.getVariantStorageEngine();

//        URI inputUri = VariantStorageBaseTest.getResourceUri("sample1.genome.vcf");
        URI inputUri = VariantStorageBaseTest.getResourceUri("platinum/1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz");

        VariantStorageBaseTest.runDefaultETL(inputUri, variantStorageEngine, new StudyMetadata(0, study1),
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), true)
                        .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
        );

        inputUri = VariantStorageBaseTest.getResourceUri("platinum/1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz");
        VariantStorageBaseTest.runDefaultETL(inputUri, variantStorageEngine, new StudyMetadata(0, study1),
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), true)
                        .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
        );

        inputUri = VariantStorageBaseTest.getResourceUri("platinum/1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz");
        VariantStorageBaseTest.runDefaultETL(inputUri, variantStorageEngine, new StudyMetadata(0, study2),
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), true)
                        .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
        );

        VariantHbaseTestUtils.printVariants(variantStorageEngine.getDBAdaptor(), newOutputUri());

    }

    @Before
    public void before() throws Exception {
        // Do not clean database!
    }

    @Test
    public void exportAvro() throws Exception {
        String fileName = "variants.avro";
        URI uri = getOutputUri(fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.AVRO, null, new Query(STUDY.key(), study1), new QueryOptions());

        copyToLocal(fileName, uri);
    }

    @Test
    public void exportMultiStudy() throws Exception {
        String fileName = "multi.variants.avro";
        URI uri = getOutputUri(fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.AVRO, null, new Query(STUDY.key(), study1 + "," + study2), new QueryOptions());

        copyToLocal(fileName, uri);
    }

    @Test
    public void exportMultiRegion() throws Exception {
        String fileName = "multi.region.avro";
        URI uri = getOutputUri(fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.AVRO, null, new Query(REGION.key(), "1,2"), new QueryOptions());

        copyToLocal(fileName, uri);
    }

    @Test
    public void exportAvroGz() throws Exception {
        String fileName = "variants.avro_gz";
        URI uri = getOutputUri(fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.AVRO_GZ, null, new Query(STUDY.key(), study1), new QueryOptions());

        copyToLocal(fileName, uri);
    }

    @Test
    public void exportVcf() throws Exception {
        String fileName = "variants.vcf";
        URI uri = getOutputUri(fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.VCF, null, new Query(STUDY.key(), study1), new QueryOptions());

        copyToLocal(fileName, uri);
    }

    @Test
    public void exportVcfGz() throws Exception {
        String fileName = "variants.vcf.gz";
        URI uri = getOutputUri(fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.VCF_GZ, null, new Query(STUDY.key(), study1), new QueryOptions());

        copyToLocal(fileName, uri);
    }

    @Test
    public void exportTped() throws Exception {
        String fileName = "variants";
        URI uri = getOutputUri(fileName);
        uri = variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.TPED, null, new Query(STUDY.key(), study1), new QueryOptions());

        copyToLocal(Paths.get(uri).getFileName().toString(), uri);
    }

    @Test
    public void exportJson() throws Exception {
        String fileName = "variants.json";
        URI uri = getOutputUri(fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.JSON, null, new Query(STUDY.key(), study1), new QueryOptions());

        copyToLocal(fileName, uri);
    }

    @Test
    public void exportParquet() throws Exception {
        String fileName = "variants.parquet";
        URI uri = getOutputUri(fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.PARQUET_GZ, null, new Query(STUDY.key(), study1), new QueryOptions());

        copyToLocal(fileName, uri);
    }

    @Test
    public void exportIndexSmallQuery() throws Exception {
        String fileName = "some_variants.sample_index.avro";
        URI uri = getOutputUri(fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.AVRO,
                null, new Query(STUDY.key(), study1).append(SAMPLE.key(), "NA12877:0/1;NA12878:1/1"),
                new QueryOptions());

        copyToLocal(fileName, uri);
    }

    @Test
    public void exportIndex() throws Exception {
        String fileName = "some_variants.sample_index.avro";
        URI uri = getOutputUri(fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.AVRO,
                null, new Query(STUDY.key(), study1).append(GENOTYPE.key(), "NA12877:0/1;NA12878:1/1"),
                new QueryOptions("skipSmallQuery", true));

        copyToLocal(fileName, uri);
    }

    @Test
    public void exportUncompleteIndex() throws Exception {
        String fileName = "some_variants.phoenix.avro";
        URI uri = getOutputUri(fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.AVRO,
                null, new Query(STUDY.key(), study1).append(GENOTYPE.key(), "NA12877:0/1;NA12878:1/1")
                        .append(SAMPLE_DATA.key(), "NA12877:DP>3;NA12878:DP>3"), new QueryOptions());

        copyToLocal(fileName, uri);
    }

    public URI getOutputUri(String fileName) throws IOException {
        if (exportToLocal) {
            return newOutputUri(1).resolve(fileName);
        } else {
            return URI.create("hdfs:///" + fileName);
        }
    }

    @Test
    public void exportIndexMultiRegion() throws Exception {
        String fileName = "some_variants.sample_index.multiregion.json";
        URI uri = getOutputUri(fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.JSON,
                null, new Query(STUDY.key(), study1).append(GENOTYPE.key(), "NA12877:0/1;NA12878:1/1")
                .append(REGION.key(), "1,2"), new QueryOptions());
    }

    @Test
    public void exportFromPhoenix() throws Exception {
        String fileName = "sift_variants.vcf";
        URI uri = getOutputUri(fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.VCF,
                null, new Query(STUDY.key(), study1).append(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift<0.2"), new QueryOptions());

        copyToLocal(fileName, uri);
    }

    @Test
    public void exportWithGenes() throws Exception {
        String fileName = "brca2.vcf";
        URI uri = getOutputUri(fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.VCF,
                null, new Query(STUDY.key(), study1).append(GENE.key(), "BRCA2"), new QueryOptions());

        copyToLocal(fileName, uri);
    }

    protected void copyToLocal(String fileName, URI uri) throws IOException {
        if (!exportToLocal) {
            FileSystem.get(externalResource.getConf()).copyToLocalFile(true,
                    new Path(uri),
                    new Path(outputUri.resolve(fileName)));

            if (fileName.endsWith(VariantExporter.TPED_FILE_EXTENSION)) {
                FileSystem.get(externalResource.getConf()).copyToLocalFile(true,
                        new Path(uri.toString().replace(VariantExporter.TPED_FILE_EXTENSION, VariantExporter.TFAM_FILE_EXTENSION)),
                        new Path(outputUri.resolve(fileName.replace(VariantExporter.TPED_FILE_EXTENSION, VariantExporter.TFAM_FILE_EXTENSION))));
            } else {
                FileSystem.get(externalResource.getConf()).copyToLocalFile(true,
                        new Path(uri.toString() + VariantExporter.METADATA_FILE_EXTENSION),
                        new Path(outputUri.resolve(fileName + VariantExporter.METADATA_FILE_EXTENSION)));
            }
        }
    }

}
