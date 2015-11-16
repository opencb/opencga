package org.opencb.opencga.storage.hadoop.variant;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Created by mh719 on 16/06/15.
 */
public class HadoopVariantStorageManager extends VariantStorageManager {
    public static final String STORAGE_ENGINE_ID = "hadoop";

    public static final String HADOOP_BIN = "hadoop.bin";
    public static final String HADOOP_ENV = "hadoop.env";
    public static final String OPENCGA_STORAGE_HADOOP_JAR_WITH_DEPENDENCIES = "opencga.storage.hadoop.jar-with-dependencies";

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
        VariantSource source = readVariantSource(input, options);

        int fileId;
        String fileName = source.getFileName();
        try {
            fileId = Integer.parseInt(source.getFileId());
        } catch (NumberFormatException e) {
            throw new StorageManagerException("FileId " + source.getFileId() + " is not an integer", e);
        }
        options.put(Options.FILE_ID.key(), fileId);
        checkNewFile(studyConfiguration, fileId, fileName);
        studyConfiguration.getFileIds().put(fileName, fileId);
        studyConfiguration.getHeaders().put(fileId, source.getMetadata().get("variantFileHeader").toString());

        checkAndUpdateStudyConfiguration(studyConfiguration, options.getInt(Options.FILE_ID.key()), source, options);
        options.put(Options.STUDY_CONFIGURATION.key(), studyConfiguration);



        //TODO: CopyFromLocal input to HDFS
        if (!input.getScheme().equals("hdfs")) {
            if (!output.getScheme().equals("hdfs")) {
                throw new StorageManagerException("Output must be in HDFS");
            }

            try {
                Configuration conf = getHadoopConfiguration(options);
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

        return input;
    }

    //TODO: Generalize this
    VariantSource readVariantSource(URI input, ObjectMap options) throws StorageManagerException {
        VariantSource source;
        Configuration conf = getHadoopConfiguration(options);
        try (
                FileSystem fs = FileSystem.get(conf);
                InputStream inputStream = new GZIPInputStream(fs.open(new Path(input.toString().replace("variants.avro", "file.json"))))
        ) {
            source = new ObjectMapper().readValue(inputStream, VariantSource.class);
        } catch (IOException e) {
            e.printStackTrace();
            throw new StorageManagerException("Unable to read VariantSource", e);
        }
        return source;
    }

    @Override
    public URI preTransform(URI input) throws StorageManagerException, IOException, FileFormatException {
        logger.info("PreTransform: " + input);
        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions();
        options.put(Options.TRANSFORM_FORMAT.key(), "avro");
        return super.preTransform(input);
    }



    @Override
    public URI load(URI input) throws IOException, StorageManagerException {
        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions();
        URI vcfMeta = URI.create(input.toString().replace("variants.avro", "file.json"));

        HadoopCredentials archiveTable = buildCredentials(options.getString(Options.STUDY_NAME.key()));
        HadoopCredentials variantsTable = getDbCredentials();

        String hadoopRoute = options.getString(HADOOP_BIN, "hadoop");
        String jarOption = OPENCGA_STORAGE_HADOOP_JAR_WITH_DEPENDENCIES;
        String jar = options.getString(jarOption, null);
        if (jar == null) {
            throw new StorageManagerException("Missing option " + jarOption);
        }



        // "Usage: %s [generic options] <avro> <avro-meta> <server> <output-table>
        Class execClass = ArchiveDriver.class;
        String commandLine = hadoopRoute + " jar " + jar + " " + execClass.getName()
                + " " + input
                + " " + vcfMeta
                + " " + archiveTable.getHostAndPort()
                + " " + archiveTable.getTable();


        logger.debug("------------------------------------------------------");
        logger.debug(commandLine);
        logger.debug("------------------------------------------------------");
        Command command = new Command(commandLine, options.getAsStringList(HADOOP_ENV));
        command.run();
        logger.debug("------------------------------------------------------");
        logger.debug("Exit value: {}", command.getExitValue());


        // "Usage: %s [generic options] <server> <input-table> <output-table> <column>
        execClass = VariantTableDriver.class;
        commandLine = hadoopRoute + " jar " + jar + " " + execClass.getName()
                + " " + variantsTable.getHostAndPort()
                + " " + archiveTable.getTable()
                + " " + variantsTable.getTable()
                + " " + options.getString(VariantStorageManager.Options.FILE_ID.key());

        logger.debug("------------------------------------------------------");
        logger.debug(commandLine);
        logger.debug("------------------------------------------------------");
        command = new Command(commandLine, options.getAsStringList(HADOOP_ENV));
        command.run();
        logger.debug("------------------------------------------------------");
        logger.debug("Exit value: {}", command.getExitValue());

        return input; // TODO  change return value?
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
            return new VariantHadoopDBAdaptor(buildCredentials(dbName), configuration.getStorageEngine(storageEngineId),
                    getHadoopConfiguration(configuration.getStorageEngine(storageEngineId).getOptions()));
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
            Configuration configuration = VariantHadoopDBAdaptor.getHbaseConfiguration(getHadoopConfiguration(options), dbCredentials);
            return new HBaseStudyConfigurationManager(dbCredentials, configuration, options);
        } catch (IOException e) {
            e.printStackTrace();
            return super.buildStudyConfigurationManager(options);
        }
    }

    private Configuration getHadoopConfiguration(ObjectMap options) throws StorageManagerException {
        Configuration conf = new HdfsConfiguration();
        // This is the only key needed to connect to HDFS:
        //   CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY = fs.defaultFS
        //

        if (conf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY) == null) {
            throw new StorageManagerException("Missing configuration parameter \"" + CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY + "\"");
        }

        options.entrySet().stream()
                .filter(entry -> entry.getValue() != null)
                .forEach(entry -> conf.set(entry.getKey(), options.getString(entry.getKey())));
        return conf;
    }
}
