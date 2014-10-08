package org.opencb.opencga.storage.mongodb.variant;

import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.opencga.lib.auth.OpenCGACredentials;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonWriter;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by imedina on 13/08/14.
 */
public class MongoDBVariantStorageManager extends VariantStorageManager {

    @Override
    public VariantDBAdaptor getDBAdaptor(Path credentialsPath) {
        Properties credentialsProperties = new Properties();
        try {
            credentialsProperties.load(new InputStreamReader(new FileInputStream(credentialsPath.toString())));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        MongoCredentials credentials = new MongoCredentials(credentialsProperties);
        VariantMongoDBAdaptor variantMongoDBAdaptor;
        try {
            variantMongoDBAdaptor = new VariantMongoDBAdaptor(credentials);
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return null;
        }
        return variantMongoDBAdaptor;
    }

    @Override
    public VariantWriter getDBWriter(Path credentialsPath, VariantSource source) {
        Properties credentialsProperties = new Properties();
        try {
            credentialsProperties.load(new InputStreamReader(new FileInputStream(credentialsPath.toString())));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        OpenCGACredentials credentials = new MongoCredentials(credentialsProperties);
        return new VariantMongoWriter(source, (MongoCredentials) credentials,
                credentialsProperties.getProperty("collection_variants", "variants"),
                credentialsProperties.getProperty("collection_files", "files"));
    }

}
