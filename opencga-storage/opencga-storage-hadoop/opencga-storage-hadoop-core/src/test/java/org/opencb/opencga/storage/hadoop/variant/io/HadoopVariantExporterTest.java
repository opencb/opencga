package org.opencb.opencga.storage.hadoop.variant.io;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.opencb.biodata.models.metadata.Individual;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.io.VariantExporter;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.core.variant.io.avro.VariantAvroReader;
import org.opencb.opencga.storage.core.variant.solr.VariantSolrExternalResource;
import org.opencb.opencga.storage.hadoop.HBaseCompat;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;

/**
 * Created on 11/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@RunWith(Parameterized.class)
@Category(MediumTests.class)
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

    public static VariantSolrExternalResource solr = new VariantSolrExternalResource();

    private static HadoopVariantStorageEngine variantStorageEngine;
    private static final String study1 = "st1";
    private static final String study2 = "st2";
    private static final String study3 = "st3";

    @BeforeClass
    public static void beforeClass() throws Exception {
        variantStorageEngine = externalResource.getVariantStorageEngine();
        if (HBaseCompat.getInstance().isSolrTestingAvailable()) {
            solr.before();
            solr.configure(variantStorageEngine);
        } else {
            System.out.println("Skip embedded solr tests");
        }

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

        inputUri = VariantStorageBaseTest.getResourceUri("variant-test-unusual-contigs.vcf");
        VariantStorageBaseTest.runDefaultETL(inputUri, variantStorageEngine, new StudyMetadata(0, study3),
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), true)
                        .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
        );

        if (HBaseCompat.getInstance().isSolrTestingAvailable()) {
            variantStorageEngine.secondaryIndex();
        }

        VariantHbaseTestUtils.printVariants(variantStorageEngine.getDBAdaptor(), newOutputUri());

    }

    @AfterClass
    public static void afterClass() {
        if (HBaseCompat.getInstance().isSolrTestingAvailable()) {
            solr.after();
        }
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
    public void exportUnusualContigs() throws Exception {
        String fileName = "unusual_contigs.vcf";
        URI uri = getOutputUri(fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.VCF, null, new Query(STUDY.key(), study3),
                new QueryOptions("skipSmallQuery", true));

        copyToLocal(fileName, uri);
    }

    @Test
    public void exportAvroGz() throws Exception {
        String fileName = "variants.avro_gz";
        URI uri = getOutputUri(fileName);
        List<URI> uris = variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.AVRO_GZ, null, new Query(STUDY.key(), study1), new QueryOptions());

        URI outputUri = copyToLocal(uris.get(0));
        if (exportToLocal) {
            URI metaUri = copyToLocal(uris.get(1));

            ObjectMapper objectMapper = new ObjectMapper().configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
            VariantMetadata metadata;
            try (InputStream is = ioConnectorProvider.newInputStream(metaUri)) {
                metadata = objectMapper.readValue(is, VariantMetadata.class);
            }

            Map<String, LinkedHashMap<String, Integer>> samplesPositions = new HashMap<>();
            for (VariantStudyMetadata study : metadata.getStudies()) {
                LinkedHashMap<String, Integer> samples = samplesPositions.put(study.getId(), new LinkedHashMap<>());
                for (Individual individual : study.getIndividuals()) {
                    samples.put(individual.getId(), samples.size());
                }
            }
            List<Variant> variants = new VariantAvroReader(Paths.get(outputUri).toFile(), samplesPositions).stream().collect(Collectors.toList());
            System.out.println("variants.size() = " + variants.size());
        }
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
        System.out.println("variantStorageEngine.getMRExecutor() = " + variantStorageEngine.getMRExecutor());
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.VCF_GZ, null, new Query(STUDY.key(), study1), new QueryOptions());

        copyToLocal(fileName, uri);
    }

    @Test
    public void exportTped() throws Exception {
        String fileName = "variants";
        URI uri = getOutputUri(fileName);
        uri = variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.TPED, null, new Query(STUDY.key(), study1), new QueryOptions()).get(0);

        copyToLocal(uri);
    }

    @Test
    public void exportJson() throws Exception {
        String fileName = "variants.json";
        URI uri = getOutputUri(fileName);
        variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.JSON, null, new VariantQuery().study(study1).includeSampleAll(), new QueryOptions());

        copyToLocal(fileName, uri);
    }

    @Test
    public void exportParquet() throws Exception {
        String fileName = "variants.parquet";
        URI uri = getOutputUri(fileName);
        uri = variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.PARQUET_GZ, null, new Query(STUDY.key(), study1), new QueryOptions()).get(0);

        copyToLocal(uri);
    }

    @Test
    public void exportParquetSmallQuery() throws Exception {
        String fileName = "variants.small.parquet";
        URI uri = getOutputUri(fileName);
        uri = variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.PARQUET_GZ, null, new VariantQuery()
                .study(study1).sample("NA12877"), new QueryOptions()).get(0);

        copyToLocal(uri);
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

    @Test
    public void exportFromSearchIndex() throws Exception {
        String fileName = "searchIndex";
        URI uri = getOutputUri(fileName);
        uri = variantStorageEngine.exportData(uri, VariantWriterFactory.VariantOutputFormat.AVRO,
                null, new Query(STUDY.key(), study1).append(GENOTYPE.key(), "NA12877:0/0,0/1;NA12878:0/0,1/1")
                        .append(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), ParamConstants.POP_FREQ_GNOMAD_GENOMES+":ALL>0.3"), new QueryOptions()).get(0);

        copyToLocal(fileName, uri);
    }

    public URI getOutputUri(String fileName) throws IOException {
        if (exportToLocal) {
            return newOutputUri().resolve(fileName);
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

    protected URI copyToLocal(URI uri) throws IOException {
        return copyToLocal(Paths.get(uri.getPath()).getFileName().toString(), uri);
    }

    protected URI copyToLocal(String fileName, URI uri) throws IOException {
        if (!exportToLocal) {
            System.out.println("Copy file " + uri);
            FileSystem.get(externalResource.getConf()).copyToLocalFile(true,
                    new Path(uri),
                    new Path(outputUri.resolve(fileName)));

            if (fileName.endsWith(VariantExporter.TPED_FILE_EXTENSION)) {
                Path dst = new Path(outputUri.resolve(fileName.replace(VariantExporter.TPED_FILE_EXTENSION, VariantExporter.TFAM_FILE_EXTENSION)));
                FileSystem.get(externalResource.getConf()).copyToLocalFile(true,
                        new Path(uri.toString().replace(VariantExporter.TPED_FILE_EXTENSION, VariantExporter.TFAM_FILE_EXTENSION)),
                        dst);
                return dst.toUri();
            } else {
                Path dst = new Path(outputUri.resolve(fileName + VariantExporter.METADATA_FILE_EXTENSION));
                FileSystem.get(externalResource.getConf()).copyToLocalFile(true,
                        new Path(uri.toString() + VariantExporter.METADATA_FILE_EXTENSION),
                        dst);
                return dst.toUri();
            }
        } else {
            return uri;
        }
    }

}
