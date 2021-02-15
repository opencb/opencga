package org.opencb.opencga.analysis.file;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManagerTest;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.managers.SampleManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.file.FileStatus;
import org.opencb.opencga.core.models.file.PostLinkToolParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class PostLinkSampleAssociationTest extends AbstractManagerTest {

    private FileManager fileManager;

    @ClassRule
    public static OpenCGATestExternalResource opencga = new OpenCGATestExternalResource();

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();

    @Before
    public void setUp() throws IOException, CatalogException {
        super.setUp();
        fileManager = catalogManager.getFileManager();
    }

    @Test
    public void linkTest() throws Exception {
        fileManager.setFileSampleLinkThreshold(1);

        String vcfFile = getClass().getResource("/biofiles/variant-test-file.vcf.gz").getFile();
        OpenCGAResult<File> link = fileManager.link(studyFqn, new FileLinkParams(vcfFile, "", "", null, null, null), false, token);

        assertEquals(0, link.first().getSampleIds().size());
        assertEquals(FileStatus.MISSING_SAMPLES, link.first().getInternal().getStatus().getName());
        assertFalse(link.first().getInternal().getMissingSamples().getNonExisting().isEmpty());

        Query query = new Query(SampleDBAdaptor.QueryParams.FILE_IDS.key(), link.first().getId());
        OpenCGAResult<Sample> search = catalogManager.getSampleManager().search(studyFqn, query, SampleManager.INCLUDE_SAMPLE_IDS, token);
        assertEquals(0, search.getNumResults());

        query = new Query(SampleDBAdaptor.QueryParams.ID.key(), Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685"));
        search = catalogManager.getSampleManager().search(studyFqn, query, SampleManager.INCLUDE_SAMPLE_IDS, token);
        assertEquals(0, search.getNumResults());

        Path outDir = Paths.get(opencga.createTmpOutdir("_postlink"));

        PostLinkSampleAssociation postLinkSampleAssociation = new PostLinkSampleAssociation();
        postLinkSampleAssociation.setUp(opencga.getOpencgaHome().toString(), null, outDir, token);
        postLinkSampleAssociation.setStudy(studyFqn);
        postLinkSampleAssociation.start();
        System.out.println(link);

        File file = fileManager.get(studyFqn, link.first().getId(), QueryOptions.empty(), token).first();

        assertEquals(4, file.getSampleIds().size());
        assertTrue(file.getSampleIds().containsAll(Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685")));
        assertEquals(FileStatus.READY, file.getInternal().getStatus().getName());
        assertTrue(file.getInternal().getMissingSamples().getNonExisting().isEmpty());
        assertTrue(file.getInternal().getMissingSamples().getExisting().isEmpty());

        query = new Query(SampleDBAdaptor.QueryParams.FILE_IDS.key(), file.getId());
        search = catalogManager.getSampleManager().search(studyFqn, query, SampleManager.INCLUDE_SAMPLE_IDS, token);
        assertEquals(4, search.getNumResults());
        assertTrue(search.getResults().stream().map(Sample::getId).collect(Collectors.toSet())
                .containsAll(Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685")));

        query = new Query(SampleDBAdaptor.QueryParams.FILE_IDS.key(), file.getUuid());
        search = catalogManager.getSampleManager().search(studyFqn, query, SampleManager.INCLUDE_SAMPLE_IDS, token);
        assertEquals(4, search.getNumResults());
        assertTrue(search.getResults().stream().map(Sample::getId).collect(Collectors.toSet())
                .containsAll(Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685")));

        query = new Query(SampleDBAdaptor.QueryParams.FILE_IDS.key(), file.getPath());
        search = catalogManager.getSampleManager().search(studyFqn, query, SampleManager.INCLUDE_SAMPLE_IDS, token);
        assertEquals(4, search.getNumResults());
        assertTrue(search.getResults().stream().map(Sample::getId).collect(Collectors.toSet())
                .containsAll(Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685")));
    }

    @Test
    public void linkTestFile() throws Exception {
        fileManager.setFileSampleLinkThreshold(1);

        String vcfFile = getClass().getResource("/biofiles/variant-test-file.vcf.gz").getFile();
        OpenCGAResult<File> link = fileManager.link(studyFqn, new FileLinkParams(vcfFile, "", "", null, null, null), false, token);

        assertEquals(0, link.first().getSampleIds().size());
        assertEquals(FileStatus.MISSING_SAMPLES, link.first().getInternal().getStatus().getName());
        assertFalse(link.first().getInternal().getMissingSamples().getNonExisting().isEmpty());

        Query query = new Query(SampleDBAdaptor.QueryParams.FILE_IDS.key(), link.first().getId());
        OpenCGAResult<Sample> search = catalogManager.getSampleManager().search(studyFqn, query, SampleManager.INCLUDE_SAMPLE_IDS, token);
        assertEquals(0, search.getNumResults());

        query = new Query(SampleDBAdaptor.QueryParams.ID.key(), Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685"));
        search = catalogManager.getSampleManager().search(studyFqn, query, SampleManager.INCLUDE_SAMPLE_IDS, token);
        assertEquals(0, search.getNumResults());

        Path outDir = Paths.get(opencga.createTmpOutdir("_postlink"));

        PostLinkSampleAssociation postLinkSampleAssociation = new PostLinkSampleAssociation();
        postLinkSampleAssociation.setUp(opencga.getOpencgaHome().toString(),
                new ObjectMap("files", Collections.singletonList("variant-test-file.vcf.gz")), outDir, token);
        postLinkSampleAssociation.setStudy(studyFqn);
        postLinkSampleAssociation.start();
        System.out.println(link);

        File file = fileManager.get(studyFqn, link.first().getId(), QueryOptions.empty(), token).first();

        assertEquals(4, file.getSampleIds().size());
        assertTrue(file.getSampleIds().containsAll(Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685")));
        assertEquals(FileStatus.READY, file.getInternal().getStatus().getName());
        assertTrue(file.getInternal().getMissingSamples().getNonExisting().isEmpty());
        assertTrue(file.getInternal().getMissingSamples().getExisting().isEmpty());

        query = new Query(SampleDBAdaptor.QueryParams.FILE_IDS.key(), file.getId());
        search = catalogManager.getSampleManager().search(studyFqn, query, SampleManager.INCLUDE_SAMPLE_IDS, token);
        assertEquals(4, search.getNumResults());
        assertTrue(search.getResults().stream().map(Sample::getId).collect(Collectors.toSet())
                .containsAll(Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685")));
    }

}
