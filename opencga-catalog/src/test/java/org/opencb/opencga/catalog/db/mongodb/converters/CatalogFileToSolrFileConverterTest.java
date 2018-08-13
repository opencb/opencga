package org.opencb.opencga.catalog.db.mongodb.converters;

import org.junit.Test;
import org.opencb.opencga.catalog.stats.solr.FileSolrModel;
import org.opencb.opencga.catalog.stats.solr.converters.CatalogFileToSolrFileConverter;
import org.opencb.opencga.core.models.Experiment;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Sample;
import org.opencb.opencga.core.models.Software;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Created by wasim on 13/08/18.
 */
public class CatalogFileToSolrFileConverterTest {

    @Test
    public void FileToSolrTest() {
        File file = new File("name", File.Type.FILE, File.Format.BAM, File.Bioformat.MICROARRAY_EXPRESSION_ONECHANNEL_AGILENT,
                "test/base", "convertorTest", new File.FileStatus("READY"), 1111L, 2);
        file.setUid(111).setSamples(Arrays.asList(new Sample().setId("1"), new Sample().setId("2")))
                .setExperiment(new Experiment().setName("Experiment")).setSoftware(new Software().setName("Software"));

        CatalogFileToSolrFileConverter converter = new CatalogFileToSolrFileConverter();
        FileSolrModel fileSolrModel = converter.convertToStorageType(file);

        assert (fileSolrModel.getUid() == file.getUid());
        assert (fileSolrModel.getName() == file.getName());
        assert (fileSolrModel.getType() == file.getType().name());
        assert (fileSolrModel.getFormat() == file.getFormat().name());
        assert (fileSolrModel.getBioformat() == file.getBioformat().name());
        assert (fileSolrModel.getRelease() == file.getRelease());
        assert (fileSolrModel.getCreationDate() == file.getCreationDate());
        assert (fileSolrModel.getStatus() == file.getStatus().getName());
        assert (fileSolrModel.isExternal() == file.isExternal());
        assert (fileSolrModel.getSize() == file.getSize());
        assert (fileSolrModel.getSoftware() == file.getSoftware().getName());
        assert (fileSolrModel.getExperiment() == file.getExperiment().getName());
        assert (fileSolrModel.getSamples() == file.getSamples().size());

    }

}
