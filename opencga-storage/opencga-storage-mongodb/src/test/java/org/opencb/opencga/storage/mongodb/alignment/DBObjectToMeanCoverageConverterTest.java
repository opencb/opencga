package org.opencb.opencga.storage.mongodb.alignment;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.junit.Test;
import org.opencb.biodata.models.alignment.stats.MeanCoverage;
import org.opencb.biodata.models.feature.Region;
import org.opencb.commons.test.GenericTest;

import java.util.Arrays;
import java.util.LinkedList;

import static org.junit.Assert.*;

public class DBObjectToMeanCoverageConverterTest extends GenericTest {

    @Test
    public void testConvertToDataModelType() throws Exception {
        DBObjectToMeanCoverageConverter converter = new DBObjectToMeanCoverageConverter();

        testConvert(converter, new MeanCoverage(1000, "1k", new Region("3",14001,15000), (float) 0.123454));
        testConvert(converter, new MeanCoverage(1000000, "1m", new Region("3",14000001,15000000), (float) 9.8765));
        testConvert(converter, new MeanCoverage(10, "10", new Region("3",14001,14010), (float) 0.12345));
        testConvert(converter, new MeanCoverage(2000, "2k", new Region("3",14001,16000), (float) 9.8765));
    }

    private void testConvert(DBObjectToMeanCoverageConverter converter, MeanCoverage meancoverage) {
        DBObject object = converter.convertToStorageType(meancoverage);
        DBObject id = converter.getIdObject(meancoverage);
        BasicDBList filesList = new BasicDBList(); filesList.add(object);

        DBObject objectInFile = new BasicDBObject(CoverageMongoWriter.FILES_FIELD, filesList); objectInFile.putAll(id);
        DBObject objectWithId = new BasicDBObject();    objectWithId.putAll(id); objectWithId.putAll(object);

        MeanCoverage mc1 = converter.convertToDataModelType(objectInFile);
        MeanCoverage mc2 = converter.convertToDataModelType(objectWithId);

        assertEquals(meancoverage, mc1);
        assertEquals(meancoverage, mc2);
    }
}