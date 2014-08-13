package org.opencb.opencga.storage.mongodb.variant;

import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonWriter;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Properties;

/**
 * Created by imedina on 13/08/14.
 */
public class MongoDBVariantStorageManager extends VariantStorageManager {

    @Override
    public VariantDBAdaptor getVariantDBAdaptor() {
        return null;
    }

    @Override
    public VariantWriter getVariantDBWriter() {
        Properties properties = new Properties();
//        properties.load(new InputStreamReader(new FileInputStream(credentialsPath.toString())));
//            OpenCGACredentials credentials = new MongoCredentials(properties);
//            writers.add(new VariantMongoWriter(source, (MongoCredentials) credentials,
//                    properties.getProperty("collection_variants", "variants"),
//                    properties.getProperty("collection_files", "files")));
        return null;
    }

}
