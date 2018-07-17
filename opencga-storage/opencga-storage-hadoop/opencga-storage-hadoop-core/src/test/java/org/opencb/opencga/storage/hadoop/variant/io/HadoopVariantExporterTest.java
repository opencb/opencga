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
import org.opencb.opencga.storage.core.variant.io.VariantExporter;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;

import java.io.IOException;
import java.net.URI;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;

/**
 * Created on 11/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantExporterTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

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

        VariantStorageBaseTest.runDefaultETL(inputUri, variantStorageEngine, new StudyConfiguration(0, study1),
                new ObjectMap(VariantStorageEngine.Options.ANNOTATE.key(), true)
                        .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
        );

        inputUri = VariantStorageBaseTest.getResourceUri("platinum/1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz");
        VariantStorageBaseTest.runDefaultETL(inputUri, variantStorageEngine, new StudyConfiguration(0, study1),
                new ObjectMap(VariantStorageEngine.Options.ANNOTATE.key(), true)
                        .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
        );

        inputUri = VariantStorageBaseTest.getResourceUri("platinum/1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz");
        VariantStorageBaseTest.runDefaultETL(inputUri, variantStorageEngine, new StudyConfiguration(0, study2),
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
        String fileName = "variants.avro";
        URI uri = URI.create("hdfs:///" + fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.AVRO, new Query(STUDY.key(), study1), new QueryOptions());

        copyToLocal(fileName, uri);
    }

    @Test
    public void exportMultiStudy() throws Exception {
        String fileName = "multi.variants.avro";
        URI uri = URI.create("hdfs:///" + fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.AVRO, new Query(STUDY.key(), study1 + "," + study2), new QueryOptions());

        copyToLocal(fileName, uri);
    }

    @Test
    public void exportMultiRegion() throws Exception {
        String fileName = "multi.region.avro";
        URI uri = URI.create("hdfs:///" + fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.AVRO, new Query(REGION.key(), "1,2"), new QueryOptions());

        copyToLocal(fileName, uri);
    }

    @Test
    public void exportAvroGz() throws Exception {
        String fileName = "variants.avro_gz";
        URI uri = URI.create("hdfs:///" + fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.AVRO_GZ, new Query(STUDY.key(), study1), new QueryOptions());

        copyToLocal(fileName, uri);
    }

    @Test
    public void exportVcf() throws Exception {
        String fileName = "variants.vcf";
        URI uri = URI.create("hdfs:///" + fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.VCF, new Query(STUDY.key(), study1), new QueryOptions());

        copyToLocal(fileName, uri);
    }

    @Test
    public void exportJson() throws Exception {
        String fileName = "variants.json";
        URI uri = URI.create("hdfs:///" + fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.JSON, new Query(STUDY.key(), study1), new QueryOptions());

        copyToLocal(fileName, uri);
    }

    @Test
    public void exportParquet() throws Exception {
        String fileName = "variants.parquet";
        URI uri = URI.create("hdfs:///" + fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.PARQUET_GZ, new Query(STUDY.key(), study1), new QueryOptions());

        copyToLocal(fileName, uri);
    }

    @Test
    public void exportIndex() throws Exception {
        String fileName = "some_variants.avro";
        URI uri = URI.create("hdfs:///" + fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.AVRO,
                new Query(STUDY.key(), study1).append(GENOTYPE.key(), "NA12877:0|1,1|0;NA12878:1|1"), new QueryOptions());

        copyToLocal(fileName, uri);
    }

    @Test
    public void exportIndexMultiRegion() throws Exception {
        String fileName = "some_variants.multiregion.avro";
        URI uri = URI.create("hdfs:///" + fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.AVRO,
                new Query(STUDY.key(), study1).append(GENOTYPE.key(), "NA12877:0|1,1|0;NA12878:1|1")
                .append(REGION.key(), "1,2"), new QueryOptions());

        copyToLocal(fileName, uri);
    }

    @Test
    public void exportFromPhoenix() throws Exception {
        String fileName = "sift_variants.vcf";
        URI uri = URI.create("hdfs:///" + fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.VCF,
                new Query(STUDY.key(), study1).append(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift<0.2"), new QueryOptions());

        copyToLocal(fileName, uri);
    }

    protected void copyToLocal(String fileName, URI uri) throws IOException {
        FileSystem.get(externalResource.getConf()).copyToLocalFile(true,
                new Path(uri),
                new Path(outputUri.resolve(fileName)));
        FileSystem.get(externalResource.getConf()).copyToLocalFile(true,
                new Path(uri.toString() + VariantExporter.METADATA_FILE_EXTENSION),
                new Path(outputUri.resolve(fileName + VariantExporter.METADATA_FILE_EXTENSION)));
    }

}
