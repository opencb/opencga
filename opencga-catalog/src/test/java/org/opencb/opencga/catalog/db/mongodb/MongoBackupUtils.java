package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.IOManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.models.file.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

/**
 * Contains two methods mainly for testing purposes. One to create a dump of the current testing OpenCGA installation and a second one to
 * restore it.
 */
public class MongoBackupUtils {

    private static final String TEMPORAL_FOLDER_HERE = "TEMPORAL_FOLDER_HERE";
    private static Logger logger = LoggerFactory.getLogger(MongoBackupUtils.class);

    public static void dump(CatalogManager catalogManager, Path opencgaHome) throws CatalogDBException {
        StopWatch stopWatch = StopWatch.createStarted();
        try (MongoDBAdaptorFactory dbAdaptorFactory = new MongoDBAdaptorFactory(catalogManager.getConfiguration(),
                catalogManager.getIoManagerFactory())) {
            MongoClient mongoClient = dbAdaptorFactory.getOrganizationMongoDBAdaptorFactory(dbAdaptorFactory.getOrganizationIds().get(0))
                    .getMongoDataStore().getMongoClient();
            MongoDatabase dumpDatabase = mongoClient.getDatabase("test_dump");
            dumpDatabase.drop();

            Bson emptyBsonQuery = new Document();
            Document databaseSummary = new Document();
            for (String organizationId : dbAdaptorFactory.getOrganizationIds()) {
                String databaseName = dbAdaptorFactory.getOrganizationMongoDBAdaptorFactory(organizationId).getMongoDataStore()
                        .getDatabaseName();
                databaseSummary.put(organizationId, databaseName);

                MongoDatabase database = mongoClient.getDatabase(databaseName);
                for (String collection : OrganizationMongoDBAdaptorFactory.COLLECTIONS_LIST) {
                    MongoCollection<Document> dbCollection = database.getCollection(collection);
                    MongoCollection<Document> dumpCollection = dumpDatabase.getCollection(organizationId + "__" + collection);

                    try (MongoCursor<Document> iterator = dbCollection.find(emptyBsonQuery).noCursorTimeout(true).iterator()) {
                        List<Document> documentList = new LinkedList<>();
                        while (iterator.hasNext()) {
                            Document document = iterator.next();
                            if (OrganizationMongoDBAdaptorFactory.FILE_COLLECTION.equals(collection)
                                    || OrganizationMongoDBAdaptorFactory.DELETED_FILE_COLLECTION.equals(collection)
                                    || OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION.equals(collection)
                                    || OrganizationMongoDBAdaptorFactory.DELETED_STUDY_COLLECTION.equals(collection)) {
                                // Store uri differently
                                String uri = document.getString("uri");
                                String temporalFolder = opencgaHome.getFileName().toString();
                                String replacedUri = uri.replace(temporalFolder, "TEMPORAL_FOLDER_HERE");
                                document.put("uri", replacedUri);
                            }
                            documentList.add(document);
                        }
                        if (!documentList.isEmpty()) {
                            dumpCollection.insertMany(documentList);
                        }
                    }
                }
            }
            dumpDatabase.getCollection("summary").insertOne(databaseSummary);
        }
        logger.info("Database dump created in {} milliseconds.", stopWatch.getTime(TimeUnit.MILLISECONDS));
    }

    public static void restore(CatalogManager catalogManager, Path opencgaHome)
            throws Exception {
        try (MongoDBAdaptorFactory dbAdaptorFactory = new MongoDBAdaptorFactory(catalogManager.getConfiguration(),
                catalogManager.getIoManagerFactory())) {
            MongoClient mongoClient = dbAdaptorFactory.getOrganizationMongoDBAdaptorFactory(ParamConstants.ADMIN_ORGANIZATION)
                    .getMongoDataStore().getMongoClient();

            MongoDatabase dumpDatabase = mongoClient.getDatabase("test_dump");
            Map<String, String> databaseNames = new HashMap<>();
            try (MongoCursor<Document> mongoIterator = dumpDatabase.getCollection("summary")
                    .find(new Document()).projection(Projections.exclude("_id")).iterator()) {
                while (mongoIterator.hasNext()) {
                    Document document = mongoIterator.next();
                    for (String s : document.keySet()) {
                        databaseNames.put(s, document.getString(s));
                    }
                }
            }

            restore(catalogManager, opencgaHome, dumpDatabase, databaseNames.keySet());
        }
    }

    public static void restore(CatalogManager catalogManager, Path opencgaHome, URL restoreFolder)
            throws Exception {
        /*
            dataset=v3.0.0
            mongo test_dump --eval 'db.getCollectionNames()' --quiet | jq .[] -r | grep -v summary | while read i ; do
              org=$(echo $i | cut -d "_" -f 1) ;
              collection=$(echo $i | cut -d "_" -f 3) ;
              mkdir -p opencga-app/src/test/resources/datasets/catalog/$dataset/mongodb/$org ;
              mongo test_dump --quiet --eval "db.getCollection(\"$i\").find().forEach(function(d){  print(tojsononeline(d)); })" | gzip > opencga-app/src/test/resources/datasets/opencga/$dataset/mongodb/$org/$collection.json.gz ;
            done
         */
        if (restoreFolder.getProtocol().equals("file")) {
            restore(catalogManager, opencgaHome, Paths.get(restoreFolder.toURI()));
        } else if (restoreFolder.getProtocol().equals("jar")) {
            throw new UnsupportedOperationException("Cannot restore from a jar file");
        }
    }

    public static void restore(CatalogManager catalogManager, Path opencgaHome, Path restoreFolder)
            throws Exception {

        List<String> organizationIds = new ArrayList<>();
        try (Stream<Path> stream = Files.list(restoreFolder)) {
            stream.forEach(file -> organizationIds.add(file.getFileName().toString()));
        }
        if (organizationIds.isEmpty()) {
            throw new CatalogDBException("No organization found in the restore folder '" + restoreFolder + "'");
        }
        restore(catalogManager, opencgaHome, restoreFolder, organizationIds);
    }

    private static void restore(CatalogManager catalogManager, Path opencgaHome, Object source, Collection<String> organizationIds)
            throws Exception {
        logger.info("Restore opencga from source " + source + " for organizations " + organizationIds);
        StopWatch stopWatch = StopWatch.createStarted();
        try (MongoDBAdaptorFactory dbAdaptorFactory = new MongoDBAdaptorFactory(catalogManager.getConfiguration(),
                catalogManager.getIoManagerFactory())) {

            for (String existingOrganizationId : dbAdaptorFactory.getOrganizationIds()) {
                // We need to completely remove databases that were not backed up so tests that attempt to create them again don't fail
                if (!organizationIds.contains(existingOrganizationId)) {
                    logger.info("Completely removing database for organization '{}'", existingOrganizationId);
                    dbAdaptorFactory.getOrganizationMongoDBAdaptorFactory(existingOrganizationId).deleteCatalogDB();
                }
            }

            // First restore the main admin database
            String adminDBName = dbAdaptorFactory.getOrganizationMongoDBAdaptorFactory(ParamConstants.ADMIN_ORGANIZATION)
                    .getMongoDataStore().getDatabaseName();
            logger.info("Restoring database for organization '{}'", ParamConstants.ADMIN_ORGANIZATION);
            restoreDatabase(catalogManager, opencgaHome, ParamConstants.ADMIN_ORGANIZATION, adminDBName, dbAdaptorFactory, source);

            for (String organizationId : organizationIds) {
                if (!ParamConstants.ADMIN_ORGANIZATION.equals(organizationId)) {
                    logger.info("Restoring database for organization '{}'", organizationId);
                    String databaseName = catalogManager.getCatalogDatabase(organizationId);
                    restoreDatabase(catalogManager, opencgaHome, organizationId, databaseName, dbAdaptorFactory, source);
                }
            }
        }
        logger.info("Databases restored in {} milliseconds.", stopWatch.getTime(TimeUnit.MILLISECONDS));
    }

    private static void restoreDatabase(CatalogManager catalogManager, Path opencgaHome, String organizationId, String databaseName,
                                        MongoDBAdaptorFactory dbAdaptorFactory, Object source)
            throws Exception {
        MongoClient mongoClient = dbAdaptorFactory.getOrganizationMongoDBAdaptorFactory(ParamConstants.ADMIN_ORGANIZATION).getMongoDataStore().getMongoClient();
        Bson emptyBsonQuery = new Document();

        MongoDatabase database = mongoClient.getDatabase(databaseName);
        boolean databaseExists = true;
        if (database.getCollection("organization").countDocuments() == 0) {
            databaseExists = false;
        }

        // Write projects folder in disk
        IOManager ioManager = catalogManager.getIoManagerFactory().getDefault();
        Path projectsFolder = opencgaHome.resolve("sessions").resolve("orgs").resolve(organizationId).resolve("projects");
        ioManager.createDirectory(projectsFolder.toUri(), true);

        for (String collection : OrganizationMongoDBAdaptorFactory.COLLECTIONS_LIST) {
            MongoCollection<Document> dbCollection = database.getCollection(collection);

            dbCollection.deleteMany(emptyBsonQuery);
            try (CloseableIterator<Document> iterator = documentIterator(source, organizationId, collection)) {
                List<Document> documentList = new LinkedList<>();
                while (iterator.hasNext()) {
                    Document document = iterator.next();
                    if (OrganizationMongoDBAdaptorFactory.PROJECT_COLLECTION.equals(collection)) {
                        URI projectFolderUri = projectsFolder.resolve(document.getLong("uid").toString()).toUri();
                        if (!ioManager.exists(projectFolderUri)) {
                            ioManager.createDirectory(projectsFolder.resolve(document.getLong("uid").toString()).toUri());
                        }
                    } else if (OrganizationMongoDBAdaptorFactory.FILE_COLLECTION.equals(collection)
                            || OrganizationMongoDBAdaptorFactory.DELETED_FILE_COLLECTION.equals(collection)
                            || OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION.equals(collection)
                            || OrganizationMongoDBAdaptorFactory.DELETED_STUDY_COLLECTION.equals(collection)) {

                        // Write actual temporal folder in database
                        String uri = document.getString("uri");
                        String uriPath = uri.substring(uri.indexOf(TEMPORAL_FOLDER_HERE) + TEMPORAL_FOLDER_HERE.length() + 1);
                        String replacedUri = opencgaHome.resolve(uriPath).toUri().toString();
                        document.put("uri", replacedUri);

                        if (OrganizationMongoDBAdaptorFactory.FILE_COLLECTION.equals(collection)) {
                            createFile(ioManager, document);
                        } else if (OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION.equals(collection)) {
                            // Create temporal study folder
                            ioManager.createDirectory(UriUtils.createUri(replacedUri), true);
                        }
                    }
                    documentList.add(document);
                }
                if (!documentList.isEmpty()) {
                    dbCollection.insertMany(documentList);
                }
            }
        }

        if (!databaseExists) {
            // Database was completely wiped so we need to regenerate non-existing collections and indexes
            logger.info("Database for organization '{}' was wiped. Restoring all indexes again.", organizationId);
            OrganizationMongoDBAdaptorFactory orgFactory = dbAdaptorFactory.getOrganizationMongoDBAdaptorFactory(organizationId);
            orgFactory.createIndexes();
        }
    }

    private static CloseableIterator<Document> documentIterator(Object source, String organizationId, String collection)
            throws IOException {
        if (source instanceof MongoDatabase) {
            MongoDatabase dumpDatabase = (MongoDatabase) source;

            MongoCollection<Document> dumpCollection = dumpDatabase.getCollection(organizationId + "__" + collection);
            logger.info("Restoring {}:{} from database - {}", organizationId, collection, dumpCollection.getNamespace().getFullName());
            return new CloseableIterator<>(dumpCollection.find(new Document()).noCursorTimeout(true).iterator());
        } else if (source instanceof Path) {
            Path dir = (Path) source;
            java.io.File file = dir.resolve(organizationId).resolve(collection + ".json.gz").toFile();
            if (!file.exists()) {
//                logger.info("File {} not found", file);
                return new CloseableIterator<>(null, Collections.emptyIterator());
            }
            logger.info("Restoring {}:{} from file - {}", organizationId, collection, file);
            // Read file lines
            Stream<String> stream = new BufferedReader(
                    new InputStreamReader(
                            new GZIPInputStream(
                                    Files.newInputStream(file.toPath())))).lines();

            Iterator<Document> iterator = stream
                    .filter(s -> !s.isEmpty())
//                    .peek(System.out::println)
                    .map(Document::parse)
                    .iterator();
            return new CloseableIterator<>(stream, iterator);
        } else {
            throw new IllegalArgumentException("Unknown restore source type " + source.getClass());
        }
    }

    private static class CloseableIterator<T> implements Iterator<T>, AutoCloseable {

        private final AutoCloseable closeable;
        private final Iterator<T> iterator;

        public CloseableIterator(AutoCloseable closeable, Iterator<T> iterator) {
            this.closeable = closeable;
            this.iterator = iterator;
        }

        public <CI extends Iterator<T> & Closeable> CloseableIterator(CI c) {
            this.closeable = c;
            this.iterator = c;
        }

        @Override
        public void close() throws Exception {
            if (closeable != null) {
                closeable.close();
            }
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public T next() {
            return iterator.next();
        }
    }

    private static void createFile(IOManager ioManager, Document document) throws IOException, CatalogIOException {
        String type = document.getString("type");
        if (File.Type.FILE.name().equals(type)) {
            String uri = document.getString("uri");
            Path uriPath;
            try {
                uriPath = Paths.get(UriUtils.createUri(uri));
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
            URI directoryUri = uriPath.getParent().toUri();
            // Ensure directories are created
            if (!ioManager.exists(directoryUri)) {
                ioManager.createDirectory(directoryUri, true);
                logger.info("{} directory created", directoryUri);
            }
            // Create dummy file
            createDebugFile(uriPath.toAbsolutePath().toString(), 1);
            logger.info("{} file created", uri);
        }
    }

    /* TYPE_FILE UTILS */
    public static java.io.File createDebugFile() throws IOException {
        String fileTestName = "/tmp/fileTest_" + RandomStringUtils.randomAlphanumeric(5);
        return createDebugFile(fileTestName);
    }

    public static java.io.File createDebugFile(String fileTestName) throws IOException {
        return createDebugFile(fileTestName, 200);
    }

    public static java.io.File createDebugFile(String fileTestName, int lines) throws IOException {
        DataOutputStream os = new DataOutputStream(new FileOutputStream(fileTestName));

        os.writeBytes("Debug file name: " + fileTestName + "\n");
        for (int i = 0; i < 100; i++) {
            os.writeBytes(i + ", ");
        }
        for (int i = 0; i < lines; i++) {
            os.writeBytes(RandomStringUtils.randomAlphanumeric(500));
            os.write('\n');
        }
        os.close();

        return Paths.get(fileTestName).toFile();
    }

    public static String createRandomString(int lines) {
        StringBuilder stringBuilder = new StringBuilder(lines);
        for (int i = 0; i < 100; i++) {
            stringBuilder.append(i + ", ");
        }
        for (int i = 0; i < lines; i++) {
            stringBuilder.append(RandomStringUtils.randomAlphanumeric(500));
            stringBuilder.append("\n");
        }
        return stringBuilder.toString();
    }

    public static String getDummyVCFContent() {
        return "##fileformat=VCFv4.0\n" +
                "##fileDate=20090805\n" +
                "##source=myImputationProgramV3.1\n" +
                "##reference=1000GenomesPilot-NCBI36\n" +
                "##phasing=partial\n" +
                "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tNA00001\tNA00002\tNA00003\n" +
                "20\t14370\trs6054257\tG\tA\t29\tPASS\tNS=3;DP=14;AF=0.5;DB;H2\tGT:GQ:DP:HQ\t0|0:48:1:51,51\t1|0:48:8:51,51\t1/1:43:5:.,.\n" +
                "20\t17330\t.\tT\tA\t3\tq10\tNS=3;DP=11;AF=0.017\tGT:GQ:DP:HQ\t0|0:49:3:58,50\t0|1:3:5:65,3\t0/0:41:3\n" +
                "20\t1110696\trs6040355\tA\tG,T\t67\tPASS\tNS=2;DP=10;AF=0.333,0.667;AA=T;DB\tGT:GQ:DP:HQ\t1|2:21:6:23,27\t2|1:2:0:18,2\t2/2:35:4\n" +
                "20\t1230237\t.\tT\t.\t47\tPASS\tNS=3;DP=13;AA=T\tGT:GQ:DP:HQ\t0|0:54:7:56,60\t0|0:48:4:51,51\t0/0:61:2\n" +
                "20\t1234567\tmicrosat1\tGTCT\tG,GTACT\t50\tPASS\tNS=3;DP=9;AA=G\tGT:GQ:DP\t0/1:35:4\t0/2:17:2\t1/1:40:3";
    }
}
