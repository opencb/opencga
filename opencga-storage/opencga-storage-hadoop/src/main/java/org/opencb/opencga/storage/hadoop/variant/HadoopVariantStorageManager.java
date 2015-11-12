package org.opencb.opencga.storage.hadoop.variant;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.core.exec.Command;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.config.DatabaseCredentials;
import org.opencb.opencga.storage.core.config.StorageEtlConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.hadoop.auth.HadoopCredentials;
import org.opencb.opencga.storage.hadoop.mr.GenomeVariantDriver;
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

//        VariantSource variantSource = readVariantSource(Paths.get(input), null);
//        checkAndUpdateStudyConfiguration(studyConfiguration, options.getInt(Options.FILE_ID.key()), source, options);
//        options.put(Options.STUDY_CONFIGURATION.key(), studyConfiguration);

        //TODO: CopyFromLocal input to HDFS
        if (!input.getScheme().equals("hdfs")) {
            if (!output.getScheme().equals("hdfs")) {
                throw new StorageManagerException("Output must be in HDFS");
            }

            try {
                Configuration conf = new HdfsConfiguration();
                for (Map.Entry<String, Object> entry : options.entrySet()) {
                    if (entry.getValue() != null) {
                        conf.set(entry.getKey(), options.getString(entry.getKey()));
                    }
                }
                FileSystem fs = FileSystem.get(conf);
                Path variantsOutputPath = new Path(output.resolve(Paths.get(input.getPath()).getFileName().toString()));
                logger.info("Copy from {} to {}", new Path(input).toUri(), variantsOutputPath.toUri());
                fs.copyFromLocalFile(false, new Path(input), variantsOutputPath);

                URI fileInput = URI.create(input.toString().replace("variants.avro", "file.json"));
                Path fileOutputPath = new Path(output.resolve(Paths.get(fileInput.getPath()).getFileName().toString()));
                logger.info("Copy from {} to {}", new Path(fileInput).toUri(), fileOutputPath.toUri());
                fs.copyFromLocalFile(false, new Path(fileInput), fileOutputPath);

                input = variantsOutputPath.toUri();
            } catch (IOException e) {
                e.printStackTrace();
            }

//            throw new StorageManagerException("Input must be on hdfs. Automatically CopyFromLocal pending");
        }

        return input;  // TODO 
    }

    @Override
    public URI preTransform(URI input) throws StorageManagerException, IOException, FileFormatException {
        logger.info("Pretransform: " + input);
        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions();
        options.put("transform.format", "avro");
        return super.preTransform(input);  // TODO
    }



    @Override
    public URI load(URI input) throws IOException, StorageManagerException {
        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions();
        URI vcfMeta = URI.create(input.toString().replace("variants.avro", "file.json"));
        
        HadoopCredentials db = getDbCredentials();

        String hadoopRoute = options.getString("hadoop.bin", "hadoop");
        String jarOption = "opencga.storage.hadoop.jar-with-dependencies";
        String jar = options.getString(jarOption, null);
        if (jar == null) {
            throw new StorageManagerException("Missing option " + jarOption);
        }
        
        

        // "Usage: %s [generic options] <avro> <avro-meta> <output-table>
        Class<GenomeVariantDriver> execClass = GenomeVariantDriver.class;
        String commandLine = hadoopRoute + " jar " + jar + " " + execClass.getName() + " " + input + " " + vcfMeta + " " + db.toUri();


        logger.debug("------------------------------------------------------");
        logger.debug(commandLine);
        logger.debug("------------------------------------------------------");
        Command command = new Command(commandLine, options.getAsStringList("hadoop.env"));
        command.run();
        logger.debug("------------------------------------------------------");
        logger.debug("Exit value: {}", command.getExitValue());

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
            return new VariantHadoopDBAdaptor(buildCredentials(dbName), configuration.getStorageEngine(storageEngineId));
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
        return super.postLoad(input, output); // TODO
    }

    @Override
    protected void checkLoadedVariants(URI input, int fileId, StudyConfiguration studyConfiguration, ObjectMap options) throws StorageManagerException {
        // TODO
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

    @Override
    protected StudyConfigurationManager buildStudyConfigurationManager(ObjectMap options) throws StorageManagerException {
        try {
            HadoopCredentials dbCredentials = getDbCredentials();
            Configuration configuration = VariantHadoopDBAdaptor.getHadoopConfiguration(dbCredentials, this.configuration.getStorageEngine(storageEngineId));
            return new HBaseStudyConfigurationManager(dbCredentials, configuration, options);
        } catch (IOException e) {
            e.printStackTrace();
            return super.buildStudyConfigurationManager(options);
        }
    }
}
