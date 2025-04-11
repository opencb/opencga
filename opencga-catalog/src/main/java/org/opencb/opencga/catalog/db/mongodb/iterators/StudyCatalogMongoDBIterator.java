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
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.NoteDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils;
import org.opencb.opencga.catalog.db.mongodb.NoteMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.notes.Note;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Created by pfurio on 31/07/17.
 */
public class StudyCatalogMongoDBIterator<E> extends CatalogMongoDBIterator<E> {

    private final Function<Document, Boolean> studyFilter;
    private final String user;
    private final QueryOptions options;
    private final NoteMongoDBAdaptor noteMongoDBAdaptor;
    private Document previousDocument;

    private final Logger logger;

    public StudyCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession,
                                       OrganizationMongoDBAdaptorFactory dbAdaptorFactory, QueryOptions options,
                                       GenericDocumentComplexConverter<E> converter, Function<Document, Boolean> studyFilter, String user) {
        super(mongoCursor, clientSession, converter, null);
        this.mongoCursor = mongoCursor;
        this.converter = converter;
        this.studyFilter = studyFilter;
        this.options = ParamUtils.defaultObject(options, QueryOptions::new);
        this.user = user;
        this.noteMongoDBAdaptor = dbAdaptorFactory.getCatalogNotesDBAdaptor();

        this.logger = LoggerFactory.getLogger(StudyCatalogMongoDBIterator.class);

        getNextStudy();
    }

    private void getNextStudy() {
        if (this.mongoCursor.hasNext()) {
            this.previousDocument = this.mongoCursor.next();

            if (this.studyFilter != null) {
                while (this.previousDocument != null && !this.studyFilter.apply(this.previousDocument)) {
                       if (this.mongoCursor.hasNext()) {
                           this.previousDocument = this.mongoCursor.next();
                       } else {
                           this.previousDocument = null;
                       }
                }
            }

            if (previousDocument != null) {
                List<String> noteField = Collections.singletonList(StudyDBAdaptor.QueryParams.NOTES.key());
                if (includeField(options, noteField)) {
                    Query query = new Query()
                            .append(NoteDBAdaptor.QueryParams.STUDY_UID.key(), previousDocument.get(StudyDBAdaptor.QueryParams.UID.key()))
                            .append(NoteDBAdaptor.QueryParams.SCOPE.key(), Note.Scope.STUDY.name());
                    if (!isAtLeastStudyAdmin(previousDocument)) {
                        query.append(NoteDBAdaptor.QueryParams.VISIBILITY.key(), Note.Visibility.PUBLIC.name());
                    }

                    QueryOptions noteOptions = createInnerQueryOptionsForVersionedEntity(options,
                            StudyDBAdaptor.QueryParams.NOTES.key(), true);
                    noteOptions.put(QueryOptions.LIMIT, 1000);
                    try {
                        OpenCGAResult<Document> result = noteMongoDBAdaptor.nativeGet(clientSession, query, noteOptions);
                        previousDocument.put(StudyDBAdaptor.QueryParams.NOTES.key(), result.getResults());
                    } catch (CatalogDBException e) {
                        logger.warn("Could not obtain the organization notes", e);
                    }
                }

                addAclInformation(previousDocument, options);
            }
        } else {
            this.previousDocument = null;
        }
    }

    @Override
    public boolean hasNext() {
        return this.previousDocument != null;
    }

    @Override
    public E next() {
        Document next = this.previousDocument;
        getNextStudy();

        if (converter != null) {
            return converter.convertToDataModelType(next);
        } else {
            return (E) next;
        }
    }

    private boolean isAtLeastStudyAdmin(Document studyDoc) {
        if (StringUtils.isEmpty(user)) {
            return true;
        }
        if (user.startsWith(ParamConstants.ADMIN_ORGANIZATION + ":")) {
            return true;
        }
        List<String> users = AuthorizationMongoDBUtils.getAdminUsers(studyDoc);
        return users.contains(user);
    }

    @Override
    public void close() {
        mongoCursor.close();
    }


}
