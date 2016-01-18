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

package org.opencb.opencga.storage.mongodb.alignment;

import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.sequence.fasta.dbadaptor.CellBaseSequenceDBAdaptor;
import org.opencb.biodata.formats.sequence.fasta.dbadaptor.SequenceDBAdaptor;
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.commons.run.Runner;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.core.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.json.AlignmentCoverageJsonDataReader;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.sequence.SqliteSequenceDBAdaptor;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.stream.Collectors;

/**
 * Date 15/08/14.
 *
 * @author Jacobo Coll Moragon <jcoll@ebi.ac.uk>
 */
public class MongoDBAlignmentStorageManager extends AlignmentStorageManager {

    /*
     * This field defaultValue must be the same that the one at storage-configuration.yml
     */
    public static final String STORAGE_ENGINE_ID = "mongodb";

    @Deprecated
    public static final String OPENCGA_STORAGE_SEQUENCE_DBADAPTOR = "OPENCGA.STORAGE.SEQUENCE.DB.ROOTDIR";

    public MongoDBAlignmentStorageManager() {
        this(null);
    }

    public MongoDBAlignmentStorageManager(StorageConfiguration configuration) {
        super(STORAGE_ENGINE_ID, configuration);
        logger = LoggerFactory.getLogger(MongoDBAlignmentStorageManager.class);
    }

    @Override
    public CoverageMongoDBWriter getDBWriter(String dbName) {
        String fileId = configuration.getStorageEngine(STORAGE_ENGINE_ID).getAlignment().getOptions().getString(Options.FILE_ID.key());
        return new CoverageMongoDBWriter(getMongoCredentials(dbName), fileId);
    }

    @Override
    public AlignmentDBAdaptor getDBAdaptor(String dbName) {
        SequenceDBAdaptor adaptor;
        if (dbName == null || dbName.isEmpty()) {
            dbName = configuration.getStorageEngine(STORAGE_ENGINE_ID).getAlignment().getOptions().getString(Options.DB_NAME.key(),
                    Options.DB_NAME.defaultValue());
            logger.info("Using default dbName '{}' in MongoDBAlignmentStorageManager.getDBAdaptor()", dbName);
        }
        logger.debug("Using {} : '{}'", Options.DB_NAME.key(), dbName);
        Path path = Paths.get(configuration.getStorageEngine(STORAGE_ENGINE_ID).getAlignment().getOptions()
                .getString(OPENCGA_STORAGE_SEQUENCE_DBADAPTOR, ""));
        if (path == null || path.toString() == null || path.toString().isEmpty() || !path.toFile().exists()) {
            adaptor = new CellBaseSequenceDBAdaptor();
        } else {
            if (path.toString().endsWith("sqlite.db")) {
                adaptor = new SqliteSequenceDBAdaptor(path);
            } else {
                adaptor = new CellBaseSequenceDBAdaptor(path);
            }
        }

        return new IndexedAlignmentDBAdaptor(adaptor, getMongoCredentials(dbName));
    }

    private MongoCredentials getMongoCredentials(String mongoDbName) {
        try {   //TODO: Use user and password
            String hosts = configuration.getStorageEngine(STORAGE_ENGINE_ID).getAlignment().getDatabase().getHosts()
                    .stream().map(String::toString).collect(Collectors.joining(","));
            String mongodbUser = configuration.getStorageEngine(STORAGE_ENGINE_ID).getAlignment().getDatabase().getUser();
            String mongodbPassword = configuration.getStorageEngine(STORAGE_ENGINE_ID).getAlignment().getDatabase().getPassword();
            return new MongoCredentials(
                    MongoCredentials.parseDataStoreServerAddresses(hosts),
                    mongoDbName,
                    mongodbUser,
                    mongodbPassword
            );
        } catch (IllegalOpenCGACredentialsException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }


    @Override
    public URI transform(URI inputUri, URI pedigree, URI outputUri) throws IOException, FileFormatException, StorageManagerException {
        configuration.getStorageEngine(STORAGE_ENGINE_ID).getAlignment().getOptions().put(Options.WRITE_ALIGNMENTS.key(), false);
        configuration.getStorageEngine(STORAGE_ENGINE_ID).getAlignment().getOptions().put(Options.CREATE_BAM_INDEX.key(), true);
        configuration.getStorageEngine(STORAGE_ENGINE_ID).getAlignment().getOptions().put(Options.INCLUDE_COVERAGE.key(), true);
        return super.transform(inputUri, pedigree, outputUri);
    }

    @Override
    public URI preLoad(URI input, URI output) throws IOException {
        return input;
    }

    @Override
    public URI load(URI inputUri) throws IOException {
        UriUtils.checkUri(inputUri, "input uri", "file");
        Path input = Paths.get(inputUri.getPath());

//        ObjectMapper objectMapper = new ObjectMapper();
//        System.out.println("configuration = " + objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(configuration));

        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getAlignment().getOptions();
        String fileId = options.getString(Options.FILE_ID.key(), input.getFileName().toString().split("\\.")[0]);
        String dbName = options.getString(Options.DB_NAME.key(), Options.DB_NAME.defaultValue());

        //Reader
        AlignmentCoverageJsonDataReader alignmentDataReader = getAlignmentCoverageJsonDataReader(input);
        alignmentDataReader.setReadRegionCoverage(false);   //Only load mean coverage

        //Writer
        CoverageMongoDBWriter dbWriter = this.getDBWriter(dbName);

        //Runner
        Runner<AlignmentRegion> runner = new Runner<>(alignmentDataReader, Arrays.asList(dbWriter), new LinkedList<>(), 1);

        logger.info("Loading coverage...");
        long start = System.currentTimeMillis();
        runner.run();
        long end = System.currentTimeMillis();
        logger.info("end - start = " + (end - start) / 1000.0 + "s");
        logger.info("Alignments loaded!");

        return inputUri;    //TODO: Return something like this: mongo://<host>/<dbName>/<collectionName>
    }

    @Override
    public URI postLoad(URI input, URI output) throws IOException {
        return input;
    }

}
