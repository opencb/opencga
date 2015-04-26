/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.mongodb.variant;

import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.tools.variant.tasks.VariantRunner;
import org.opencb.commons.containers.list.SortedList;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.Task;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.config.DataStoreServerAddress;
import org.opencb.opencga.core.auth.IllegalOpenCGACredentialsException;

import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.runner.SimpleThreadRunner;
import org.opencb.opencga.storage.core.variant.FileStudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by imedina on 13/08/14.
 */
public class MongoDBVariantStorageManager extends VariantStorageManager {

    //StorageEngine specific Properties
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_DB_HOSTS                 = "OPENCGA.STORAGE.MONGODB.VARIANT.DB.HOSTS";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_DB_AUTHENTICATION_DB     = "OPENCGA.STORAGE.MONGODB.VARIANT.DB.AUTHENTICATION.DB";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_DB_NAME                  = "OPENCGA.STORAGE.MONGODB.VARIANT.DB.NAME";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_DB_USER                  = "OPENCGA.STORAGE.MONGODB.VARIANT.DB.USER";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_DB_PASS                  = "OPENCGA.STORAGE.MONGODB.VARIANT.DB.PASS";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_DB_COLLECTION_VARIANTS   = "OPENCGA.STORAGE.MONGODB.VARIANT.DB.COLLECTION.VARIANTS";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_DB_COLLECTION_FILES      = "OPENCGA.STORAGE.MONGODB.VARIANT.DB.COLLECTION.FILES";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_LOAD_BATCH_SIZE          = "OPENCGA.STORAGE.MONGODB.VARIANT.LOAD.BATCH_SIZE";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_LOAD_BULK_SIZE           = "OPENCGA.STORAGE.MONGODB.VARIANT.LOAD.BULK_SIZE";
//    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_LOAD_WRITE_THREADS       = "OPENCGA.STORAGE.MONGODB.VARIANT.LOAD.WRITE_THREADS";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_DEFAULT_GENOTYPE         = "OPENCGA.STORAGE.MONGODB.VARIANT.LOAD.DEFAULT_GENOTYPE";
    public static final String OPENCGA_STORAGE_MONGODB_VARIANT_COMPRESS_GENEOTYPES      = "OPENCGA.STORAGE.MONGODB.VARIANT.LOAD.COMPRESS_GENOTYPES";

    //StorageEngine specific params
//    public static final String WRITE_MONGO_THREADS = "writeMongoThreads";
    public static final String BULK_SIZE = "bulkSize";
    public static final String INCLUDE_SRC = "includeSrc";
    public static final String DEFAULT_GENOTYPE = "defaultGenotype";

    protected static Logger logger = LoggerFactory.getLogger(MongoDBVariantStorageManager.class);

    @Override
    public VariantMongoDBWriter getDBWriter(String dbName, ObjectMap params) {
        StudyConfiguration studyConfiguration = getStudyConfiguration(params);
        int fileId = params.getInt(FILE_ID);
        Properties credentialsProperties = new Properties(properties);

        MongoCredentials credentials = getMongoCredentials(dbName);
        String variantsCollection = credentialsProperties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_DB_COLLECTION_VARIANTS, "variants");
        String filesCollection = credentialsProperties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_DB_COLLECTION_FILES, "files");
        logger.debug("getting DBWriter to db: {}", credentials.getMongoDbName());
        return new VariantMongoDBWriter(fileId, studyConfiguration, credentials, variantsCollection, filesCollection, false, false);
    }

    @Override
    public VariantMongoDBAdaptor getDBAdaptor(String dbName, ObjectMap params) {
        MongoCredentials credentials = getMongoCredentials(dbName);
        VariantMongoDBAdaptor variantMongoDBAdaptor;

        String variantsCollection = properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_DB_COLLECTION_VARIANTS, "variants");
        String filesCollection = properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_DB_COLLECTION_FILES, "files");
        try {
            variantMongoDBAdaptor = new VariantMongoDBAdaptor(credentials, variantsCollection, filesCollection);
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return null;
        }

        logger.debug("getting DBAdaptor to db: {}", credentials.getMongoDbName());
        return variantMongoDBAdaptor;
    }


    /* package */ MongoCredentials getMongoCredentials() {
        return getMongoCredentials(null);
    }

    /* package */ MongoCredentials getMongoCredentials(String dbName) {
        String hosts = properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_DB_HOSTS, "localhost");
        List<DataStoreServerAddress> dataStoreServerAddresses = MongoCredentials.parseDataStoreServerAddresses(hosts);

//        int port = Integer.parseInt(properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_DB_PORT, "27017"));
        if(dbName == null || dbName.isEmpty()) {
            dbName = properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_DB_NAME, "variants");
        }
        String user = properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_DB_USER, null);
        String pass = properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_DB_PASS, null);

        String authenticationDatabase = properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_DB_AUTHENTICATION_DB, null);

        try {
            MongoCredentials mongoCredentials = new MongoCredentials(dataStoreServerAddresses, dbName, user, pass);
            mongoCredentials.setAuthenticationDatabase(authenticationDatabase);
            return mongoCredentials;
        } catch (IllegalOpenCGACredentialsException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public URI preLoad(URI input, URI output, ObjectMap params) throws StorageManagerException {
        return super.preLoad(input, output, params);
    }

    @Override
    public URI load(URI inputUri, ObjectMap params) throws IOException {
        // input: getDBSchemaReader
        // output: getDBWriter()

        Path input = Paths.get(inputUri.getPath());

        boolean includeSamples = params.getBoolean(INCLUDE_SAMPLES, Boolean.parseBoolean(properties.getProperty(OPENCGA_STORAGE_VARIANT_INCLUDE_SAMPLES, "false")));
//        boolean includeEffect = params.getBoolean(INCLUDE_EFFECT, Boolean.parseBoolean(properties.getProperty(OPENCGA_STORAGE_VARIANT_INCLUDE_EFFECT, "false")));
        boolean includeStats = params.getBoolean(INCLUDE_STATS, Boolean.parseBoolean(properties.getProperty(OPENCGA_STORAGE_VARIANT_INCLUDE_STATS, "false")));
        boolean includeSrc = params.getBoolean(INCLUDE_SRC, Boolean.parseBoolean(properties.getProperty(OPENCGA_STORAGE_VARIANT_INCLUDE_SRC, "false")));

        String defaultGenotype = params.getString(DEFAULT_GENOTYPE, properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_DEFAULT_GENOTYPE, ""));
        boolean compressSamples = params.getBoolean(COMPRESS_GENOTYPES, Boolean.parseBoolean(properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_COMPRESS_GENEOTYPES, "false")));

        VariantSource source = new VariantSource(inputUri.getPath(), "", "", "");       //Create a new VariantSource. This object will be filled at the VariantJsonReader in the pre()
//        params.put(VARIANT_SOURCE, source);
        String dbName = params.getString(DB_NAME, null);

//        VariantSource variantSource = readVariantSource(input, null);
//        new StudyInformation(variantSource.getStudyId())

        int batchSize = params.getInt(BATCH_SIZE, Integer.parseInt(properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_LOAD_BATCH_SIZE, "100")));
        int bulkSize = params.getInt(BULK_SIZE, Integer.parseInt(properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_LOAD_BULK_SIZE, "" + batchSize)));
        int loadThreads = params.getInt(LOAD_THREADS, Integer.parseInt(properties.getProperty(OPENCGA_STORAGE_VARIANT_LOAD_THREADS, "8")));
//        int numWriters = params.getInt(WRITE_MONGO_THREADS, Integer.parseInt(properties.getProperty(OPENCGA_STORAGE_MONGODB_VARIANT_LOAD_WRITE_THREADS, "8")));
        final int numReaders = 1;
        final int numWriters = loadThreads  == 1? 1 : loadThreads - numReaders; //Subtract the reader thread


        //Reader
        VariantReader variantJsonReader;
        variantJsonReader = getVariantJsonReader(input, source);

        //Tasks
        List<Task<Variant>> taskList = new SortedList<>();


        //Writers
        List<VariantWriter> writers = new LinkedList<>();
        List<DataWriter> writerList = new LinkedList<>();
        AtomicBoolean atomicBoolean = new AtomicBoolean();
        for (int i = 0; i < numWriters; i++) {
            VariantMongoDBWriter variantDBWriter = this.getDBWriter(dbName, params);
            variantDBWriter.setBulkSize(bulkSize);
            variantDBWriter.includeSrc(includeSrc);
            variantDBWriter.includeSamples(includeSamples);
            variantDBWriter.includeStats(includeStats);
            variantDBWriter.setCompressDefaultGenotype(compressSamples);
            variantDBWriter.setDefaultGenotype(defaultGenotype);
            variantDBWriter.setVariantSource(source);
//            variantDBWriter.setSamplesIds(samplesIds);
            variantDBWriter.setThreadSyncronizationBoolean(atomicBoolean);
            writerList.add(variantDBWriter);
            writers.add(variantDBWriter);
        }



        logger.info("Loading variants...");
        long start = System.currentTimeMillis();

        //Runner
        if (loadThreads == 1) {
            logger.info("Single thread load...");
            VariantRunner vr = new VariantRunner(source, variantJsonReader, null, writers, taskList, batchSize);
            vr.run();
        } else {
            logger.info("Multi thread load... [{} readerThreads, {} writerThreads]", numReaders, numWriters);
//            ThreadRunner runner = new ThreadRunner(Executors.newFixedThreadPool(loadThreads), batchSize);
//            ThreadRunner.ReadNode<Variant> variantReadNode = runner.newReaderNode(variantJsonReader, 1);
//            ThreadRunner.WriterNode<Variant> variantWriterNode = runner.newWriterNode(writerList);
//
//            variantReadNode.append(variantWriterNode);
//            runner.run();

            SimpleThreadRunner threadRunner = new SimpleThreadRunner(
                    variantJsonReader,
                    Collections.<Task>emptyList(),
                    writerList,
                    batchSize,
                    loadThreads*2,
                    0);
            threadRunner.run();

        }

        long end = System.currentTimeMillis();
        logger.info("end - start = " + (end - start) / 1000.0 + "s");
        logger.info("Variants loaded!");

        return inputUri; //TODO: Return something like this: mongo://<host>/<dbName>/<collectionName>
    }

    @Override
    public URI postLoad(URI input, URI output, ObjectMap params) throws IOException, StorageManagerException {
        return input;
    }

    /* --------------------------------------- */
    /*  StudyConfiguration util methods        */
    /* --------------------------------------- */

    @Override
    protected StudyConfigurationManager buildStudyConfigurationManager(ObjectMap params) {
        if (params != null && params.getString(FileStudyConfigurationManager.STUDY_CONFIGURATION_PATH, "").isEmpty()) {
            return super.buildStudyConfigurationManager(params);
        } else {
            String string = params == null? null : params.getString(DB_NAME);
            return getDBAdaptor(string, params).getStudyConfigurationDBAdaptor();
        }
    }

    @Override
    public void checkStudyConfiguration(StudyConfiguration studyConfiguration, VariantDBAdaptor dbAdaptor) throws StorageManagerException {
        super.checkStudyConfiguration(studyConfiguration, dbAdaptor);
//        if (dbAdaptor == null) {
//            logger.debug("Do not check StudyConfiguration against the loaded in MongoDB");
//        } else {
//            if (dbAdaptor instanceof VariantMongoDBAdaptor) {
//                VariantMongoDBAdaptor mongoDBAdaptor = (VariantMongoDBAdaptor) dbAdaptor;
//                StudyConfigurationManager studyConfigurationDBAdaptor = mongoDBAdaptor.getStudyConfigurationDBAdaptor();
//                StudyConfiguration studyConfigurationFromMongo = studyConfigurationDBAdaptor.getStudyConfiguration(studyConfiguration.getStudyId(), null).first();
//
//                //Check that the provided StudyConfiguration has the same or more information that the stored in MongoDB.
//                for (Map.Entry<String, Integer> entry : studyConfigurationFromMongo.getFileIds().entrySet()) {
//                    if (!studyConfiguration.getFileIds().containsKey(entry.getKey())) {
//                        throw new StorageManagerException("StudyConfiguration do not have the file " + entry.getKey());
//                    }
//                    if (!studyConfiguration.getFileIds().get(entry.getKey()).equals(entry.getValue())) {
//                        throw new StorageManagerException("StudyConfiguration changes the fileId of '" + entry.getKey() + "' from " + entry.getValue() + " to " + studyConfiguration.getFileIds().get(entry.getKey()));
//                    }
//                }
//                for (Map.Entry<String, Integer> entry : studyConfigurationFromMongo.getCohortIds().entrySet()) {
//                    if (!studyConfiguration.getCohortIds().containsKey(entry.getKey())) {
//                        throw new StorageManagerException("StudyConfiguration do not have the cohort " + entry.getKey());
//                    }
//                    if (!studyConfiguration.getCohortIds().get(entry.getKey()).equals(entry.getValue())) {
//                        throw new StorageManagerException("StudyConfiguration changes the cohortId of '" + entry.getKey() + "' from " + entry.getValue() + " to " + studyConfiguration.getCohortIds().get(entry.getKey()));
//                    }
//                }
//                for (Map.Entry<String, Integer> entry : studyConfigurationFromMongo.getSampleIds().entrySet()) {
//                    if (!studyConfiguration.getSampleIds().containsKey(entry.getKey())) {
//                        throw new StorageManagerException("StudyConfiguration do not have the sample " + entry.getKey());
//                    }
//                    if (!studyConfiguration.getSampleIds().get(entry.getKey()).equals(entry.getValue())) {
//                        throw new StorageManagerException("StudyConfiguration changes the sampleId of '" + entry.getKey() + "' from " + entry.getValue() + " to " + studyConfiguration.getSampleIds().get(entry.getKey()));
//                    }
//                }
//                studyConfigurationDBAdaptor.updateStudyConfiguration(studyConfiguration, null);
//            } else {
//                throw new StorageManagerException("Unknown VariantDBAdaptor '" + dbAdaptor.getClass().toString() + "'. Expected '" + VariantMongoDBAdaptor.class + "'");
//            }
//        }
    }
}
