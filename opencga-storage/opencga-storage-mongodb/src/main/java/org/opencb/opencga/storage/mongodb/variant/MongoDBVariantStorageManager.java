package org.opencb.opencga.storage.mongodb.variant;

import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Created by imedina on 13/08/14.
 */
public class MongoDBVariantStorageManager extends VariantStorageManager {

    @Override
    public VariantWriter getDBWriter(Path credentialsPath, VariantSource source) {
        Properties credentialsProperties = new Properties(properties);
        if(credentialsPath != null && !credentialsPath.toString().isEmpty()) {
            try {
                credentialsProperties.load(new InputStreamReader(new FileInputStream(credentialsPath.toString())));
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }
//        MongoCredentials credentials = new MongoCredentials(credentialsProperties);
        MongoCredentials credentials = getMongoCredentials(credentialsProperties);
        return new VariantMongoDBWriter(source, credentials,
                credentialsProperties.getProperty("collection_variants", "variants"),       //TODO: Export to config file
                credentialsProperties.getProperty("collection_files", "files"));
    }

    @Override
    public VariantDBAdaptor getDBAdaptor(Path credentialsPath) {
        Properties credentialsProperties = new Properties(properties);
        if(credentialsPath != null) {
            try {
                credentialsProperties.load(new InputStreamReader(new FileInputStream(credentialsPath.toString())));
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }
//        MongoCredentials credentials = new MongoCredentials(credentialsProperties);
        MongoCredentials credentials = getMongoCredentials(credentialsProperties);
        VariantMongoDBAdaptor variantMongoDBAdaptor;
        try {
            variantMongoDBAdaptor = new VariantMongoDBAdaptor(credentials);
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return null;
        }
        return variantMongoDBAdaptor;
    }

    private MongoCredentials getMongoCredentials(Properties properties) {
        String host = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.HOST");
        int port = Integer.parseInt(properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.PORT", "27017"));
        String db = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.NAME");
        String user = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.USER", null);
        String pass = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.PASS", null);

        try {
            return new MongoCredentials(host, port, db, user, pass);
        } catch (IllegalOpenCGACredentialsException e) {
            e.printStackTrace();
            return null;
        }
    }

}
