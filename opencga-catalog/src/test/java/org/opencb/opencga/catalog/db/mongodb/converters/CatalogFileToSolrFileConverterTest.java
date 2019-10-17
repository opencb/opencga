package org.opencb.opencga.catalog.db.mongodb.converters;

import org.junit.Test;
import org.opencb.biodata.models.commons.Software;
import org.opencb.opencga.catalog.stats.solr.FileSolrModel;
import org.opencb.opencga.catalog.stats.solr.converters.CatalogFileToSolrFileConverter;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Sample;
import org.opencb.opencga.core.models.Study;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by wasim on 13/08/18.
 */
public class CatalogFileToSolrFileConverterTest {

    @Test
    public void FileToSolrTest() {
        Study study = new Study().setFqn("user@project:study").setAttributes(new HashMap<>());
        File file = new File("name", File.Type.FILE, File.Format.BAM, File.Bioformat.MICROARRAY_EXPRESSION_ONECHANNEL_AGILENT,
                "test/base", null, "convertorTest", new File.FileStatus("READY"), 1111L, 2);
        file.setUid(111).setSamples(Arrays.asList(new Sample().setId("1"), new Sample().setId("2")))
                .setSoftware(new Software().setName("Software"));
        file.setAnnotationSets(AnnotationHelper.createAnnotation());

        FileSolrModel fileSolrModel = new CatalogFileToSolrFileConverter(study).convertToStorageType(file);

        assert (fileSolrModel.getUid() == file.getUid());
        assert (fileSolrModel.getName().equals(file.getName()));
        assert (fileSolrModel.getType().equals(file.getType().name()));
        assert (fileSolrModel.getFormat().equals(file.getFormat().name()));
        assert (fileSolrModel.getBioformat().equals(file.getBioformat().name()));
        assert (fileSolrModel.getRelease() == file.getRelease());

        Date date = TimeUtils.toDate(file.getCreationDate());
        LocalDate localDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

        assertEquals(localDate.getYear(), fileSolrModel.getCreationYear());
        assertEquals(localDate.getMonth().toString(), fileSolrModel.getCreationMonth());
        assertEquals(localDate.getDayOfMonth(), fileSolrModel.getCreationDay());
        assertEquals(localDate.getDayOfMonth(), fileSolrModel.getCreationDay());
        assertEquals(localDate.getDayOfWeek().toString(), fileSolrModel.getCreationDayOfWeek());

        assert (fileSolrModel.getStatus().equals(file.getStatus().getName()));
        assert (fileSolrModel.isExternal() == file.isExternal());
        assert (fileSolrModel.getSize() == file.getSize());
        assert (fileSolrModel.getSoftware().equals(file.getSoftware().getName()));
        assert (fileSolrModel.getNumSamples() == file.getSamples().size());

        assertEquals(fileSolrModel.getAnnotations().get("annotations__o__annotName.vsId.a.ab2.ab2c1.ab2c1d1"), Arrays.asList(1, 2, 3, 4, 11, 12, 13, 14, 21));
        assertEquals(fileSolrModel.getAnnotations().get("annotations__o__annotName.vsId.a.ab1.ab1c1"), Arrays.asList(true, false, false));
        assertEquals(fileSolrModel.getAnnotations().get("annotations__s__annotName.vsId.a.ab1.ab1c2"), "hello world");
        assertEquals(fileSolrModel.getAnnotations().get("annotations__o__annotName.vsId.a.ab2.ab2c1.ab2c1d2"), Arrays.asList("hello ab2c1d2 1", "hello ab2c1d2 2"));
        assertEquals(fileSolrModel.getAnnotations().get("annotations__o__annotName.vsId.a.ab3.ab3c1.ab3c1d1"), Arrays.asList(Arrays.asList("hello"), Arrays.asList("hello2", "bye2"), Arrays.asList("byeee2", "hellooo2")));
        assertEquals(fileSolrModel.getAnnotations().get("annotations__o__annotName.vsId.a.ab3.ab3c1.ab3c1d2"), Arrays.asList(2.0, 4.0, 24.0));
        assertNull(fileSolrModel.getAnnotations().get("nothing"));
        assertEquals(fileSolrModel.getAnnotations().keySet().size(), 6);

    }

}
