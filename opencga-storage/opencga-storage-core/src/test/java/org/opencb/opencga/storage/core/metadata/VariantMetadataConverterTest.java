package org.opencb.opencga.storage.core.metadata;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.opencga.storage.core.io.managers.IOConnectorProvider;
import org.opencb.opencga.storage.core.io.managers.LocalIOConnector;
import org.opencb.opencga.storage.core.metadata.models.*;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;

/**
 * Created on 09/08/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMetadataConverterTest {


//    private StudyConfiguration studyConfiguration;
    private VariantMetadataConverter variantMetadataConverter;
    private ObjectWriter objectWriter;
    private ProjectMetadata projectMetadata;
    private VariantStorageMetadataManager metadataManager;
    private StudyMetadata studyMetadata;
    private VariantReaderUtils variantReaderUtils;

    @Before
    public void setUp() throws Exception {
        metadataManager = new VariantStorageMetadataManager(new DummyVariantStorageMetadataDBAdaptorFactory());

        URI uri = VariantStorageBaseTest.getResourceUri("platinum/1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz");
        variantReaderUtils = new VariantReaderUtils(new IOConnectorProvider(LocalIOConnector.class));
        VariantFileMetadata fileMetadata = variantReaderUtils.readVariantFileMetadata(Paths.get(uri), null);
        studyMetadata = new StudyMetadata(1, "study").addVariantFileHeader(fileMetadata.getHeader(), null);

        metadataManager.unsecureUpdateStudyMetadata(studyMetadata);
        metadataManager.unsecureUpdateSampleMetadata(1, new SampleMetadata(1, 1, "s1"));
        metadataManager.unsecureUpdateSampleMetadata(1, new SampleMetadata(1, 2, "s2"));
        metadataManager.unsecureUpdateSampleMetadata(1, new SampleMetadata(1, 3, "s3"));
        metadataManager.unsecureUpdateSampleMetadata(1, new SampleMetadata(1, 4, "s4"));
        metadataManager.unsecureUpdateSampleMetadata(1, new SampleMetadata(1, 5, "s5"));
        metadataManager.unsecureUpdateSampleMetadata(1, new SampleMetadata(1, 6, "s6"));
        metadataManager.unsecureUpdateSampleMetadata(1, new SampleMetadata(1, 7, "s7"));

        metadataManager.unsecureUpdateFileMetadata(1, new FileMetadata(1, 10, "file1.vcf")
                .setSamples(new LinkedHashSet<>(Arrays.asList(1, 2, 3, 4)))
                .setIndexStatus(TaskMetadata.Status.READY)
        );
        metadataManager.unsecureUpdateFileMetadata(1, new FileMetadata(1, 11, "file2.vcf")
                .setSamples(new LinkedHashSet<>(Arrays.asList(4, 5, 6)))
                .setIndexStatus(TaskMetadata.Status.READY)
        );
        metadataManager.unsecureUpdateCohortMetadata(1, new CohortMetadata(1, 20, "ALL", Arrays.asList(1, 2, 3, 4, 5, 6), Arrays.asList(10, 11))
                .setStatsStatus(TaskMetadata.Status.READY));

        variantMetadataConverter = new VariantMetadataConverter(metadataManager);
        objectWriter = new ObjectMapper()
                .configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true)
                .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
                .writerWithDefaultPrettyPrinter();
        projectMetadata = new ProjectMetadata("hsapiens", "37", 1);
    }

    @Test
    public void toVariantMetadataTest() throws IOException {
        VariantMetadata variantMetadata = variantMetadataConverter.toVariantMetadata(new VariantQueryProjection(studyMetadata, Collections.emptyList(), Collections.emptyList()));
        System.out.println("variantMetadata = " + objectWriter.writeValueAsString(variantMetadata));

    }

}