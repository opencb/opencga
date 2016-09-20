package org.opencb.opencga.catalog.db.mongodb;

import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Created by pfurio on 3/3/16.
 */
public class CatalogMongoDBUtilsTest extends CatalogMongoDBAdaptorTest {
    /////////// Other tests
    @Test
    public void replaceDots() {

        Document original = new Document("o.o", 4).append("4.4", Arrays.asList(1, 3, 4, new Document("933.44", "df.sdf")))
                .append("key", new Document("key....k", "value...2.2.2"));
        Document o = new Document("o.o", 4).append("4.4", Arrays.asList(1, 3, 4, new Document("933.44", "df.sdf"))).append
                ("key", new Document("key....k", "value...2.2.2"));
        System.out.println(o);

        MongoDBUtils.replaceDotsInKeys(o);
        System.out.println(o);

        MongoDBUtils.restoreDotsInKeys(o);
        System.out.println(o);

        Assert.assertEquals(original, o);

    }
}
