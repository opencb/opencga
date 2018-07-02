/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.catalog.managers;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.Group;
import org.opencb.opencga.core.models.PrivateStudyUid;
import org.opencb.opencga.core.models.Study;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Created by hpccoll1 on 12/05/15.
 */
public abstract class AbstractManager {

    protected static Logger logger;
    protected final AuthorizationManager authorizationManager;
    protected final AuditManager auditManager;
    protected final CatalogIOManagerFactory catalogIOManagerFactory;
    protected final CatalogManager catalogManager;

    protected Configuration configuration;

    protected final DBAdaptorFactory catalogDBAdaptorFactory;
    protected final UserDBAdaptor userDBAdaptor;
    protected final ProjectDBAdaptor projectDBAdaptor;
    protected final StudyDBAdaptor studyDBAdaptor;
    protected final FileDBAdaptor fileDBAdaptor;
    protected final IndividualDBAdaptor individualDBAdaptor;
    protected final SampleDBAdaptor sampleDBAdaptor;
    protected final CohortDBAdaptor cohortDBAdaptor;
    protected final FamilyDBAdaptor familyDBAdaptor;
    protected final DatasetDBAdaptor datasetDBAdaptor;
    protected final JobDBAdaptor jobDBAdaptor;
    @Deprecated
    protected final DiseasePanelDBAdaptor diseasePanelDBAdaptor;
    protected final PanelDBAdaptor panelDBAdaptor;
    protected final ClinicalAnalysisDBAdaptor clinicalDBAdaptor;

    protected static final String ROOT = "admin";
    protected static final String ANONYMOUS = "*";

    protected static final String INTERNAL_DELIMITER = "__";

    AbstractManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                           DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                           Configuration configuration) {
        this.authorizationManager = authorizationManager;
        this.auditManager = auditManager;
        this.configuration = configuration;
        this.userDBAdaptor = catalogDBAdaptorFactory.getCatalogUserDBAdaptor();
        this.studyDBAdaptor = catalogDBAdaptorFactory.getCatalogStudyDBAdaptor();
        this.fileDBAdaptor = catalogDBAdaptorFactory.getCatalogFileDBAdaptor();
        this.individualDBAdaptor = catalogDBAdaptorFactory.getCatalogIndividualDBAdaptor();
        this.sampleDBAdaptor = catalogDBAdaptorFactory.getCatalogSampleDBAdaptor();
        this.jobDBAdaptor = catalogDBAdaptorFactory.getCatalogJobDBAdaptor();
        this.cohortDBAdaptor = catalogDBAdaptorFactory.getCatalogCohortDBAdaptor();
        this.familyDBAdaptor = catalogDBAdaptorFactory.getCatalogFamilyDBAdaptor();
        this.datasetDBAdaptor = catalogDBAdaptorFactory.getCatalogDatasetDBAdaptor();
        this.diseasePanelDBAdaptor = catalogDBAdaptorFactory.getCatalogDiseasePanelDBAdaptor();
        this.panelDBAdaptor = catalogDBAdaptorFactory.getCatalogPanelDBAdaptor();
        this.clinicalDBAdaptor = catalogDBAdaptorFactory.getClinicalAnalysisDBAdaptor();
        this.catalogIOManagerFactory = ioManagerFactory;
        this.catalogDBAdaptorFactory = catalogDBAdaptorFactory;
        this.catalogManager = catalogManager;

        projectDBAdaptor = catalogDBAdaptorFactory.getCatalogProjectDbAdaptor();

        logger = LoggerFactory.getLogger(this.getClass());
    }

    /**
     * Prior to the conversion to a numerical featureId, there is a need to know in which user/project/study look for the string.
     * This method calculates those parameters to know how to obtain the numerical id.
     *
     * @param userId User id of the user asking for the id. If no user is found in featureStr, we will assume that it is asking for its
     *               projects/studies...
     * @param featureStr Feature id in string format. Could be one of [user@aliasProject:aliasStudy:XXXXX
     *                | user@aliasStudy:XXXXX | aliasStudy:XXXXX | XXXXX].
     * @return an objectMap with the following possible keys: "user", "project", "study", "featureName"
     */
    protected ObjectMap parseFeatureId(String userId, String featureStr) {
        ObjectMap result = new ObjectMap("user", userId);

        String[] split = featureStr.split("@");
        if (split.length == 2) { // user@project:study
            result.put("user", split[0]);
            featureStr = split[1];
        }

        split = featureStr.split(":", 3);
        if (split.length == 2) {
            result.put("study", split[0]);
            result.put("featureName", split[1]);
        } else if (split.length == 3) {
            result.put("project", split[0]);
            result.put("study", split[1]);
            result.put("featureName", split[2]);
        } else {
            result.put("featureName", featureStr);
        }
        return result;
    }

    AuthenticationOrigin getAuthenticationOrigin(String authOrigin) {
        if (configuration.getAuthentication().getAuthenticationOrigins() != null) {
            for (AuthenticationOrigin authenticationOrigin : configuration.getAuthentication().getAuthenticationOrigins()) {
                if (authOrigin.equals(authenticationOrigin.getId())) {
                    return authenticationOrigin;
                }
            }
        }
        return null;
    }

    /**
     * Checks if the list of members are all valid.
     *
     * The "members" can be:
     *  - '*' referring to all the users.
     *  - 'anonymous' referring to the anonymous user.
     *  - '@{groupId}' referring to a {@link Group}.
     *  - '{userId}' referring to a specific user.
     * @param studyId studyId
     * @param members List of members
     * @throws CatalogDBException CatalogDBException
     */
    protected void checkMembers(long studyId, List<String> members) throws CatalogDBException {
        for (String member : members) {
            checkMember(studyId, member);
        }
    }

    /**
     * Checks if the member is valid.
     *
     * The "member" can be:
     *  - '*' referring to all the users.
     *  - '@{groupId}' referring to a {@link Group}.
     *  - '{userId}' referring to a specific user.
     * @param studyId studyId
     * @param member member
     * @throws CatalogDBException CatalogDBException
     */
    protected void checkMember(long studyId, String member) throws CatalogDBException {
        if (member.equals("*")) {
            return;
        } else if (member.startsWith("@")) {
            QueryResult<Group> queryResult = studyDBAdaptor.getGroup(studyId, member,
                    Collections.emptyList());
            if (queryResult.getNumResults() == 0) {
                throw CatalogDBException.idNotFound("Group", member);
            }
        } else {
            userDBAdaptor.checkId(member);
        }
    }

    public static class MyResource<T extends PrivateStudyUid> {
        private String user;
        private Study study;
        private T resource;

        public MyResource() {
        }

        public MyResource(String user, Study study, T resource) {
            this.user = user;
            this.study = study;
            this.resource = resource;
        }

        public String getUser() {
            return user;
        }

        public MyResource setUser(String user) {
            this.user = user;
            return this;
        }

        public Study getStudy() {
            return study;
        }

        public MyResource setStudy(Study study) {
            this.study = study;
            return this;
        }

        public T getResource() {
            return resource;
        }

        public MyResource setResource(T resource) {
            this.resource = resource;
            return this;
        }
    }

    public static class MyResources<T> {
        private String user;
        private Study study;
        private List<T> resourceList;

        public MyResources() {
        }

        public MyResources(String user, Study study, List<T> resourceList) {
            this.user = user;
            this.study = study;
            this.resourceList = resourceList;
        }

        public String getUser() {
            return user;
        }

        public MyResources setUser(String user) {
            this.user = user;
            return this;
        }

        public Study getStudy() {
            return study;
        }

        public MyResources setStudy(Study study) {
            this.study = study;
            return this;
        }

        public List<T> getResourceList() {
            return resourceList;
        }

        public MyResources setResourceList(List<T> resourceList) {
            this.resourceList = resourceList;
            return this;
        }
    }

    @Deprecated
    public static class MyResourceId {
        private String user;
        private long studyId;
        private long resourceId;

        public MyResourceId() {
        }

        public MyResourceId(String user, long studyId, long resourceId) {
            this.user = user;
            this.studyId = studyId;
            this.resourceId = resourceId;
        }

        public String getUser() {
            return user;
        }

        public MyResourceId setUser(String user) {
            this.user = user;
            return this;
        }

        public long getStudyId() {
            return studyId;
        }

        public MyResourceId setStudyId(long studyId) {
            this.studyId = studyId;
            return this;
        }

        public long getResourceId() {
            return resourceId;
        }

        public MyResourceId setResourceId(long resourceId) {
            this.resourceId = resourceId;
            return this;
        }
    }

    @Deprecated
    public static class MyResourceIds {
        private String user;
        private long studyId;
        private List<Long> resourceIds;

        public MyResourceIds() {
        }

        public MyResourceIds(String user, long studyId, List<Long> resourceIds) {
            this.user = user;
            this.studyId = studyId;
            this.resourceIds = resourceIds;
        }

        public String getUser() {
            return user;
        }

        public MyResourceIds setUser(String user) {
            this.user = user;
            return this;
        }

        public long getStudyId() {
            return studyId;
        }

        public MyResourceIds setStudyId(long studyId) {
            this.studyId = studyId;
            return this;
        }

        public List<Long> getResourceIds() {
            return resourceIds;
        }

        public MyResourceIds setResourceIds(List<Long> resourceIds) {
            this.resourceIds = resourceIds;
            return this;
        }
    }
}
