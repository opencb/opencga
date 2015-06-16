package org.opencb.opencga.storage.hadoop.variant;

import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.config.DataStoreServerAddress;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.config.DatabaseCredentials;
import org.opencb.opencga.storage.core.config.StorageEtlConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.hadoop.auth.HadoopCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * Created by mh719 on 16/06/15.
 */
public class HadoopVariantStorageManager extends VariantStorageManager {
    public static final String STORAGE_ENGINE_ID = "hadoop";

    protected static Logger logger = LoggerFactory.getLogger(HadoopVariantStorageManager.class);



    @Override
    public URI load(URI input) throws IOException, StorageManagerException {
        return null;
    }

    @Override
    public VariantWriter getDBWriter(String dbName) throws StorageManagerException {

//        Properties credentialsProperties = new Properties(properties);

//        MongoCredentials credentials = getMongoCredentials(dbName);
//        String variantsCollection = options.getString(COLLECTION_VARIANTS, "variants");
//        String filesCollection = options.getString(COLLECTION_FILES, "files");
//        logger.debug("getting DBWriter to db: {}", credentials.getMongoDbName());
//        return new VariantMongoDBWriter(fileId, studyConfiguration, credentials, variantsCollection, filesCollection, false, false);

        return null;
    }

    @Override
    public VariantDBAdaptor getDBAdaptor(String dbName) throws StorageManagerException {
        try {
            return new VariantHadoopDBAdaptor(buildCredentials(dbName));
        } catch (IOException e) {
            throw new StorageManagerException("Problems creating DB Adapter",e);
        }
    }

    public HadoopCredentials buildCredentials(String table) throws IllegalStateException{
        StorageEtlConfiguration vStore = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant();

//        ObjectMap options = vStore.getOptions();

//        StudyConfiguration studyConfiguration = getStudyConfiguration(options);
//        int fileId = options.getInt(Options.FILE_ID.key());

        DatabaseCredentials db = vStore.getDatabase();
        String user = db.getUser();
        String pass = db.getPassword();
        List<String> hostList = db.getHosts();
        if (hostList.size() != 1)
            throw new IllegalStateException("Expect only one server name");
        for (String host : hostList) {
            if (host.contains(":")) {
                String[] hostPort = host.split(":");
                return new HadoopCredentials(hostPort[0], table, user,pass, Integer.valueOf(hostPort[1]));
            } else {
                return new HadoopCredentials(host, table,user,pass);
            }
        }
        return null;

    }

    public static Logger getLogger() {
        return logger;
    }
}
