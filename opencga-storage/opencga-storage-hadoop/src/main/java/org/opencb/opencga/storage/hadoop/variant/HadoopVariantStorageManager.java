package org.opencb.opencga.storage.hadoop.variant;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.config.DatabaseCredentials;
import org.opencb.opencga.storage.core.config.StorageEtlConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.hadoop.auth.HadoopCredentials;
import org.opencb.opencga.storage.hadoop.mr.GenomeVariantLoadDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mh719 on 16/06/15.
 */
public class HadoopVariantStorageManager extends VariantStorageManager {
    public static final String STORAGE_ENGINE_ID = "hadoop";

    protected static Logger logger = LoggerFactory.getLogger(HadoopVariantStorageManager.class);

    @Override
    public URI preLoad(URI input, URI output) throws StorageManagerException {
        getLogger().info("Pre input: " + input);
        getLogger().info("Pre output: " + output);
        
        ObjectMap options = configuration.getStorageEngine(storageEngineId).getVariant().getOptions();

        //Get the studyConfiguration. If there is no StudyConfiguration, create a empty one.
        StudyConfiguration studyConfiguration = getStudyConfiguration(options);
        if (studyConfiguration == null) {
            logger.info("Creating a new StudyConfiguration");
            studyConfiguration = new StudyConfiguration(options.getInt(Options.STUDY_ID.key()), options.getString(Options.STUDY_NAME.key()));
            options.put(Options.STUDY_CONFIGURATION.key(), studyConfiguration);
        }

//        checkAndUpdateStudyConfiguration(studyConfiguration, fileId, source, options);
//        options.put(Options.STUDY_CONFIGURATION.key, studyConfiguration);
        
        return input;  // TODO 
    }
    
    @Override
    public URI preTransform(URI input) throws StorageManagerException, IOException, FileFormatException {
        getLogger().info("Pretransform: " + input);
        return input;  // TODO 
    }

    @Override
    public URI load(URI input) throws IOException, StorageManagerException {
        URI vcfMeta = URI.create(input.toString()+".meta");
        
        HadoopCredentials db = getDbCredentials();
        int val = GenomeVariantLoadDriver.load(db, input, vcfMeta);
        
        return input; // TODO  change return value
    }

    private HadoopCredentials getDbCredentials() throws StorageManagerException{
        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions();
        String dbName = options.getString(Options.DB_NAME.key(), null);
        HadoopCredentials cr =  buildCredentials(dbName);
        return cr;
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
    
        DatabaseCredentials db = vStore.getDatabase();
        String user = db.getUser();
        String pass = db.getPassword();
        List<String> hostList = db.getHosts();
        if (hostList.size() != 1)
            throw new IllegalStateException("Expect only one server name");
        String target = hostList.get(0);
        try{
            URI uri = new URI(target);
            String server = uri.getHost();
            Integer port = uri.getPort() > 0?uri.getPort() : 60000;
    //        String tablename = uri.getPath();
    //        tablename = tablename.startsWith("/") ? tablename.substring(1) : tablename; // Remove leading /
    //        String master = String.join(":", server, port.toString());
            HadoopCredentials credentials = new HadoopCredentials(server, table, user,pass, port);
            return credentials;
        } catch (URISyntaxException e){
            throw new IllegalStateException(e);
        }
    }
    
    @Override
    public URI postLoad(URI input, URI output) throws IOException, StorageManagerException {
        return input; // TODO 
    }

    @Override
    public URI postTransform(URI input) throws IOException, FileFormatException {
        return input;  // TODO 
    }
    
    @Override
    @Deprecated
    public VariantWriter getDBWriter(String dbName) throws StorageManagerException { return null;}

    public static Logger getLogger() {
        return logger;
    }
}
