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
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotableConverter;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.models.common.Annotable;
import org.opencb.opencga.core.models.file.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.UnaryOperator;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor.NATIVE_QUERY;

public class FileCatalogMongoDBIterator<E> extends AnnotableCatalogMongoDBIterator<E> {

    private long studyUid;
    private String user;

    private FileMongoDBAdaptor fileDBAdaptor;

    private Queue<Document> fileListBuffer;

    private Logger logger;

    private static final int BUFFER_SIZE = 100;

    public FileCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession,
                                      AnnotableConverter<? extends Annotable> converter, UnaryOperator<Document> filter,
                                      FileMongoDBAdaptor fileMongoDBAdaptor, QueryOptions options) {
        this(mongoCursor, clientSession, converter, filter, fileMongoDBAdaptor, 0, null, options);
    }

    public FileCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession,
                                      AnnotableConverter<? extends Annotable> converter, UnaryOperator<Document> filter,
                                      FileMongoDBAdaptor fileMongoDBAdaptor, long studyUid, String user, QueryOptions options) {
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
                studyUid = ((Number) fileDocument.get(PRIVATE_STUDY_UID)).longValue();
            }

            fileListBuffer.add(fileDocument);
            counter++;

            String fileUid = String.valueOf(fileDocument.get(FileDBAdaptor.QueryParams.UID.key()));
            // Extract all the related files
            Object relatedFiles = fileDocument.get(FileDBAdaptor.QueryParams.RELATED_FILES.key());
            if (relatedFiles != null && !options.getBoolean(NATIVE_QUERY)) {
                List<Document> relatedFileList  = (List<Document>) relatedFiles;
                if (!relatedFileList.isEmpty()) {
                    for (Document relatedFile : relatedFileList) {
                        long relatedFileUid = ((Number) ((Document) relatedFile.get("file")).get(FileDBAdaptor.QueryParams.UID.key()))
                                .longValue();
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
                    fileMap.put(((Number) file.get(FileDBAdaptor.QueryParams.UID.key())).longValue(), file)
            );

            // Add the files obtained to the corresponding related files
            fileListBuffer.forEach(fileDocument -> {
                String fileId = String.valueOf(fileDocument.get(FileDBAdaptor.QueryParams.UID.key()));
                File.Type fileType = File.Type.valueOf(fileDocument.getString(FileDBAdaptor.QueryParams.TYPE.key()));

                List<Document> tmpFileList = new ArrayList<>();

                Object relatedFiles = fileDocument.get(FileDBAdaptor.QueryParams.RELATED_FILES.key());
                if (relatedFiles != null) {
                    List<Document> relatedFileList = (List<Document>) relatedFiles;
                    if (!relatedFileList.isEmpty()) {
                        relatedFileList.forEach(f -> {
                            long relatedFileUid = ((Number) ((Document) f.get("file")).get(FileDBAdaptor.QueryParams.UID.key()))
                                    .longValue();
                            String auxFileId = fileId + "-" + relatedFileUid;
                            String relation = relatedFileMap.get(auxFileId);
                            Document relatedFileDocument = fileMap.get(relatedFileUid);
                            if (fileType == File.Type.VIRTUAL) {
                                relatedFileDocument = new Document()
                                        .append("id", relatedFileDocument.get("id"))
                                        .append("path", relatedFileDocument.get("path"))
                                        .append("uuid", relatedFileDocument.get("uuid"))
                                        .append("uid", relatedFileDocument.get("uid"));
                            }
                            tmpFileList.add(new Document()
                                    .append("file", relatedFileDocument)
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
