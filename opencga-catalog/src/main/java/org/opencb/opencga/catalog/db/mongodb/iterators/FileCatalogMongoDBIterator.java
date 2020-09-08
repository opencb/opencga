/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.ClientSession;
import org.bson.Document;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.FileMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.SampleMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotableConverter;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.models.common.Annotable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor.NATIVE_QUERY;

public class FileCatalogMongoDBIterator<E> extends AnnotableCatalogMongoDBIterator<E> {

    private long studyUid;
    private String user;

    private FileMongoDBAdaptor fileDBAdaptor;

    private Queue<Document> fileListBuffer;

    private Logger logger;

    private static final int BUFFER_SIZE = 100;

    public FileCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession,
                                      AnnotableConverter<? extends Annotable> converter, Function<Document, Document> filter,
                                      FileMongoDBAdaptor fileMongoDBAdaptor, SampleMongoDBAdaptor sampleMongoDBAdaptor,
                                      QueryOptions options) {
        this(mongoCursor, clientSession, converter, filter, fileMongoDBAdaptor, sampleMongoDBAdaptor, 0, null, options);
    }

    public FileCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession,
                                      AnnotableConverter<? extends Annotable> converter, Function<Document, Document> filter,
                                      FileMongoDBAdaptor fileMongoDBAdaptor, SampleMongoDBAdaptor sampleMongoDBAdaptor, long studyUid,
                                      String user, QueryOptions options) {
        super(mongoCursor, clientSession, converter, filter, options);

        this.user = user;
        this.studyUid = studyUid;

        this.fileDBAdaptor = fileMongoDBAdaptor;

        this.fileListBuffer = new LinkedList<>();
        this.logger = LoggerFactory.getLogger(FileCatalogMongoDBIterator.class);
    }

    @Override
    public E next() {
        Document next = fileListBuffer.remove();

        if (filter != null) {
            next = filter.apply(next);
        }

        addAclInformation(next, options);

        if (converter != null) {
            return (E) converter.convertToDataModelType(next, options);
        } else {
            return (E) next;
        }
    }

    @Override
    public boolean hasNext() {
        if (fileListBuffer.isEmpty()) {
            fetchNextBatch();
        }
        return !fileListBuffer.isEmpty();
    }

    private void fetchNextBatch() {
        Map<String, String> relatedFileMap = new HashMap<>();
        Set<Long> relatedFileSet = new HashSet<>();

        // Get next BUFFER_SIZE documents
        int counter = 0;
        while (mongoCursor.hasNext() && counter < BUFFER_SIZE) {
            Document fileDocument = mongoCursor.next();

            if (user != null && studyUid <= 0) {
                studyUid = fileDocument.getLong(PRIVATE_STUDY_UID);
            }

            fileListBuffer.add(fileDocument);
            counter++;

            String fileUid = String.valueOf(fileDocument.getLong(FileDBAdaptor.QueryParams.UID.key()));
            // Extract all the related files
            Object relatedFiles = fileDocument.get(FileDBAdaptor.QueryParams.RELATED_FILES.key());
            if (relatedFiles != null && !options.getBoolean(NATIVE_QUERY)) {
                List<Document> relatedFileList  = (List<Document>) relatedFiles;
                if (!relatedFileList.isEmpty()) {
                    for (Document relatedFile : relatedFileList) {
                        long relatedFileUid = ((Document) relatedFile.get("file")).getLong(FileDBAdaptor.QueryParams.UID.key());
                        relatedFileSet.add(relatedFileUid);
                        relatedFileMap.put(fileUid + "-" + relatedFileUid, relatedFile.getString("relation"));
                    }
                }
            }
        }

        if (!relatedFileSet.isEmpty()) {
            // Obtain all those files
            Query query = new Query(FileDBAdaptor.QueryParams.UID.key(), new ArrayList<>(relatedFileSet));
            QueryOptions fileQueryOptions = new QueryOptions(FileManager.INCLUDE_FILE_URI_PATH);
            fileQueryOptions.put(NATIVE_QUERY, true);

            List<Document> fileList;
            try {
                if (user != null) {
                    query.put(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
                    fileList = fileDBAdaptor.nativeGet(clientSession, studyUid, query, fileQueryOptions, user).getResults();
                } else {
                    fileList = fileDBAdaptor.nativeGet(clientSession, query, fileQueryOptions).getResults();
                }
            } catch (CatalogDBException | CatalogAuthorizationException | CatalogParameterException e) {
                logger.warn("Could not obtain the list of related files: {}", e.getMessage(), e);
                return;
            }

            // Map each file uid to the file entry
            Map<Long, Document> fileMap = new HashMap<>(fileList.size());
            fileList.forEach(file->
                    fileMap.put(file.getLong(FileDBAdaptor.QueryParams.UID.key()), file)
            );

            // Add the files obtained to the corresponding related files
            fileListBuffer.forEach(fileDocument -> {
                String fileId = String.valueOf(fileDocument.getLong(FileDBAdaptor.QueryParams.UID.key()));

                List<Document> tmpFileList = new ArrayList<>();

                Object relatedFiles = fileDocument.get(FileDBAdaptor.QueryParams.RELATED_FILES.key());
                if (relatedFiles != null) {
                    List<Document> relatedFileList = (List<Document>) relatedFiles;
                    if (!relatedFileList.isEmpty()) {
                        relatedFileList.forEach(f -> {
                            long relatedFileUid = ((Document) f.get("file")).getLong(FileDBAdaptor.QueryParams.UID.key());
                            String auxFileId = fileId + "-" + relatedFileUid;
                            String relation = relatedFileMap.get(auxFileId);
                            tmpFileList.add(new Document()
                                    .append("file", fileMap.get(relatedFileUid))
                                    .append("relation", relation));
                        });
                    }
                }

                // Add the generated list of related files
                fileDocument.put(FileDBAdaptor.QueryParams.RELATED_FILES.key(), tmpFileList);
            });
        }
    }

}
