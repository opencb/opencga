package org.opencb.opencga.analysis.file;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.managers.AbstractManagerTest;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.managers.SampleManager;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.file.FileStatus;
import org.opencb.opencga.core.models.file.PostLinkToolParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@Category(MediumTests.class)
public class PostLinkSampleAssociationTest extends AbstractManagerTest {

    private FileManager fileManager;

    @ClassRule
    public static OpenCGATestExternalResource opencga = new OpenCGATestExternalResource();

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        fileManager = catalogManager.getFileManager();
    }

    @Test
    public void linkTest() throws Exception {
        fileManager.setFileSampleLinkThreshold(1);

        String vcfFile = opencga.getResourceUri("biofiles/variant-test-file.vcf.gz").toString();
        OpenCGAResult<File> link = fileManager.link(studyFqn, new FileLinkParams(vcfFile, "", "", "", null, null, null, null, null), false, token);

        assertEquals(0, link.first().getSampleIds().size());
        assertEquals(FileStatus.MISSING_SAMPLES, link.first().getInternal().getStatus().getId());
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
        assertEquals(FileStatus.READY, file.getInternal().getStatus().getId());
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
    public void postLinkPassingVirtualFile() throws Exception {
        fileManager.setFileSampleLinkThreshold(1);

        String vcfFile = opencga.getResourceUri("biofiles/variant-test-file.vcf.gz").toString();
        OpenCGAResult<File> link = fileManager.link(studyFqn, new FileLinkParams(vcfFile, "", "", "", null, "biofiles/virtual_file.vcf", null, null, null), false, token);

        OpenCGAResult<File> virtualResult = fileManager.get(studyFqn, "virtual_file.vcf", QueryOptions.empty(), token);

        assertEquals(0, link.first().getSampleIds().size());
        assertEquals(FileStatus.READY, link.first().getInternal().getStatus().getId());
        assertTrue(link.first().getInternal().getMissingSamples().getNonExisting().isEmpty());

        assertEquals(0, virtualResult.first().getSampleIds().size());
        assertEquals(FileStatus.MISSING_SAMPLES, virtualResult.first().getInternal().getStatus().getId());
        assertFalse(virtualResult.first().getInternal().getMissingSamples().getNonExisting().isEmpty());

        Query query = new Query(SampleDBAdaptor.QueryParams.FILE_IDS.key(), link.first().getId());
        OpenCGAResult<Sample> search = catalogManager.getSampleManager().search(studyFqn, query, SampleManager.INCLUDE_SAMPLE_IDS, token);
        assertEquals(0, search.getNumResults());

        query = new Query(SampleDBAdaptor.QueryParams.ID.key(), Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685"));
        search = catalogManager.getSampleManager().search(studyFqn, query, SampleManager.INCLUDE_SAMPLE_IDS, token);
        assertEquals(0, search.getNumResults());

        Path outDir = Paths.get(opencga.createTmpOutdir("_postlink"));

        PostLinkSampleAssociation postLinkSampleAssociation = new PostLinkSampleAssociation();
        postLinkSampleAssociation.setUp(opencga.getOpencgaHome().toString(),
                new ObjectMap(new PostLinkToolParams().setFiles(Collections.singletonList(virtualResult.first().getId())).toParams()),
                outDir, token);
        postLinkSampleAssociation.setStudy(studyFqn);
        postLinkSampleAssociation.start();
        System.out.println(link);

        File file = fileManager.get(studyFqn, virtualResult.first().getId(), QueryOptions.empty(), token).first();

        assertEquals(4, file.getSampleIds().size());
        assertTrue(file.getSampleIds().containsAll(Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685")));
        assertEquals(FileStatus.READY, file.getInternal().getStatus().getId());
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
    public void postLinkPassingMultipartFile() throws Exception {
        fileManager.setFileSampleLinkThreshold(1);

        String vcfFile = opencga.getResourceUri("biofiles/variant-test-file.vcf.gz").toString();
        OpenCGAResult<File> link = fileManager.link(studyFqn, new FileLinkParams(vcfFile, "", "", "", null, "biofiles/virtual_file.vcf", null, null, null), false, token);

        OpenCGAResult<File> virtualResult = fileManager.get(studyFqn, "virtual_file.vcf", QueryOptions.empty(), token);

        assertEquals(0, link.first().getSampleIds().size());
        assertEquals(FileStatus.READY, link.first().getInternal().getStatus().getId());
        assertTrue(link.first().getInternal().getMissingSamples().getNonExisting().isEmpty());

        assertEquals(0, virtualResult.first().getSampleIds().size());
        assertEquals(FileStatus.MISSING_SAMPLES, virtualResult.first().getInternal().getStatus().getId());
        assertFalse(virtualResult.first().getInternal().getMissingSamples().getNonExisting().isEmpty());

        Query query = new Query(SampleDBAdaptor.QueryParams.FILE_IDS.key(), link.first().getId());
        OpenCGAResult<Sample> search = catalogManager.getSampleManager().search(studyFqn, query, SampleManager.INCLUDE_SAMPLE_IDS, token);
        assertEquals(0, search.getNumResults());

        query = new Query(SampleDBAdaptor.QueryParams.ID.key(), Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685"));
        search = catalogManager.getSampleManager().search(studyFqn, query, SampleManager.INCLUDE_SAMPLE_IDS, token);
        assertEquals(0, search.getNumResults());

        Path outDir = Paths.get(opencga.createTmpOutdir("_postlink"));

        PostLinkSampleAssociation postLinkSampleAssociation = new PostLinkSampleAssociation();
        postLinkSampleAssociation.setUp(opencga.getOpencgaHome().toString(),
                new ObjectMap(new PostLinkToolParams().setFiles(Collections.singletonList(link.first().getId())).toParams()),
                outDir, token);
        postLinkSampleAssociation.setStudy(studyFqn);
        postLinkSampleAssociation.start();
        System.out.println(link);

        File file = fileManager.get(studyFqn, virtualResult.first().getId(), QueryOptions.empty(), token).first();

        assertEquals(4, file.getSampleIds().size());
        assertTrue(file.getSampleIds().containsAll(Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685")));
        assertEquals(FileStatus.READY, file.getInternal().getStatus().getId());
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

        String vcfFile = opencga.getResourceUri("biofiles/variant-test-file.vcf.gz").toString();
        OpenCGAResult<File> link = fileManager.link(studyFqn, new FileLinkParams(vcfFile, "", "", "", null, null, null, null, null), false, token);

        assertEquals(0, link.first().getSampleIds().size());
        assertEquals(FileStatus.MISSING_SAMPLES, link.first().getInternal().getStatus().getId());
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
        assertEquals(FileStatus.READY, file.getInternal().getStatus().getId());
        assertTrue(file.getInternal().getMissingSamples().getNonExisting().isEmpty());
        assertTrue(file.getInternal().getMissingSamples().getExisting().isEmpty());

        query = new Query(SampleDBAdaptor.QueryParams.FILE_IDS.key(), file.getId());
        search = catalogManager.getSampleManager().search(studyFqn, query, SampleManager.INCLUDE_SAMPLE_IDS, token);
        assertEquals(4, search.getNumResults());
        assertTrue(search.getResults().stream().map(Sample::getId).collect(Collectors.toSet())
                .containsAll(Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685")));
    }

}
