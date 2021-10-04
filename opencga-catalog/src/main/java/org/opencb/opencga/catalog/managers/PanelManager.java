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

package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.db.api.PanelDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.panel.PanelAclEntry;
import org.opencb.opencga.core.models.panel.PanelUpdateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyAclEntry;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;

public class PanelManager extends ResourceManager<Panel> {

    protected static Logger logger = LoggerFactory.getLogger(PanelManager.class);
    private UserManager userManager;
    private StudyManager studyManager;

    public static final QueryOptions INCLUDE_PANEL_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            PanelDBAdaptor.QueryParams.ID.key(), PanelDBAdaptor.QueryParams.UID.key(), PanelDBAdaptor.QueryParams.UUID.key(),
            PanelDBAdaptor.QueryParams.VERSION.key(), PanelDBAdaptor.QueryParams.STUDY_UID.key()));

    PanelManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                 DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
    }

    @Override
    Enums.Resource getEntity() {
        return Enums.Resource.DISEASE_PANEL;
    }

    @Override
    InternalGetDataResult<Panel> internalGet(long studyUid, List<String> entryList, @Nullable Query query, QueryOptions options,
                                             String user, boolean ignoreException) throws CatalogException {
        if (ListUtils.isEmpty(entryList)) {
            throw new CatalogException("Missing panel entries.");
        }
        List<String> uniqueList = ListUtils.unique(entryList);

        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.put(PanelDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

        boolean versioned = queryCopy.getBoolean(Constants.ALL_VERSIONS)
                || queryCopy.containsKey(PanelDBAdaptor.QueryParams.VERSION.key());
        if (versioned && uniqueList.size() > 1) {
            throw new CatalogException("Only one panel allowed when requesting multiple versions");
        }

        PanelDBAdaptor.QueryParams idQueryParam = getFieldFilter(uniqueList);
        queryCopy.put(idQueryParam.key(), uniqueList);

        // Ensure the field by which we are querying for will be kept in the results
        queryOptions = keepFieldInQueryOptions(queryOptions, idQueryParam.key());

        OpenCGAResult<Panel> panelDataResult = panelDBAdaptor.get(studyUid, queryCopy, queryOptions, user);

        Function<Panel, String> panelStringFunction = Panel::getId;
        if (idQueryParam.equals(PanelDBAdaptor.QueryParams.UUID)) {
            panelStringFunction = Panel::getUuid;
        }

        if (ignoreException || panelDataResult.getNumResults() >= uniqueList.size()) {
            return keepOriginalOrder(uniqueList, panelStringFunction, panelDataResult, ignoreException, versioned);
        }
        // Query without adding the user check
        OpenCGAResult<Panel> resultsNoCheck = panelDBAdaptor.get(queryCopy, queryOptions);

        if (resultsNoCheck.getNumResults() == panelDataResult.getNumResults()) {
            throw CatalogException.notFound("panels", getMissingFields(uniqueList, panelDataResult.getResults(), panelStringFunction));
        } else {
            throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the panels.");
        }
    }

    PanelDBAdaptor.QueryParams getFieldFilter(List<String> idList) throws CatalogException {
        PanelDBAdaptor.QueryParams idQueryParam = null;
        for (String entry : idList) {
            PanelDBAdaptor.QueryParams param = PanelDBAdaptor.QueryParams.ID;
            if (UuidUtils.isOpenCgaUuid(entry)) {
                param = PanelDBAdaptor.QueryParams.UUID;
            }
            if (idQueryParam == null) {
                idQueryParam = param;
            }
            if (idQueryParam != param) {
                throw new CatalogException("Found uuids and ids in the same query. Please, choose one or do two different queries.");
            }
        }
        return idQueryParam;
    }

    private OpenCGAResult<Panel> getPanel(long studyUid, String panelUuid, QueryOptions options) throws CatalogException {
        Query query = new Query()
                .append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(PanelDBAdaptor.QueryParams.UUID.key(), panelUuid);
        return panelDBAdaptor.get(query, options);
    }

    @Override
    public OpenCGAResult<Panel> create(String studyStr, Panel panel, QueryOptions options, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("panel", panel)
                .append("options", options)
                .append("token", token);
        try {
            // 1. We check everything can be done
            authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_PANELS);

            // Check all the panel fields
            ParamUtils.checkIdentifier(panel.getId(), "id");
            panel.setName(ParamUtils.defaultString(panel.getName(), panel.getId()));
            panel.setRelease(studyManager.getCurrentRelease(study));
            panel.setVersion(1);
            panel.setAuthor(ParamUtils.defaultString(panel.getAuthor(), ""));
            panel.setCreationDate(TimeUtils.getTime());
            panel.setModificationDate(TimeUtils.getTime());
            panel.setStatus(new Status());
            panel.setCategories(ParamUtils.defaultObject(panel.getCategories(), Collections.emptyList()));
            panel.setTags(ParamUtils.defaultObject(panel.getTags(), Collections.emptyList()));
            panel.setDescription(ParamUtils.defaultString(panel.getDescription(), ""));
            panel.setDisorders(ParamUtils.defaultObject(panel.getDisorders(), Collections.emptyList()));
            panel.setVariants(ParamUtils.defaultObject(panel.getVariants(), Collections.emptyList()));
            panel.setRegions(ParamUtils.defaultObject(panel.getRegions(), Collections.emptyList()));
            panel.setGenes(ParamUtils.defaultObject(panel.getGenes(), Collections.emptyList()));
            panel.setAttributes(ParamUtils.defaultObject(panel.getAttributes(), Collections.emptyMap()));
            panel.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.PANEL));

            fillDefaultStats(panel);

            options = ParamUtils.defaultObject(options, QueryOptions::new);

            panelDBAdaptor.insert(study.getUid(), panel, options);
            auditManager.auditCreate(userId, Enums.Resource.DISEASE_PANEL, panel.getId(), panel.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return getPanel(study.getUid(), panel.getUuid(), options);
        } catch (CatalogException e) {
            auditManager.auditCreate(userId, Enums.Resource.DISEASE_PANEL, panel.getId(), "", study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<Panel> importFromSource(String studyId, String source, String panelIds, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("source", source)
                .append("panelIds", panelIds)
                .append("token", token);
        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        try {
            // 1. We check everything can be done
            authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_PANELS);
            ParamUtils.checkParameter(source, "source");

            List<String> sources = Arrays.asList(source.split(","));
            if (StringUtils.isNotEmpty(panelIds) && sources.size() > 1) {
                throw new CatalogParameterException("List of panel ids only valid for 1 source. More than 1 source found.");
            }

            HashSet<String> sourceSet = new HashSet<>(sources);
            if (sourceSet.size() < sources.size()) {
                throw new CatalogException("Duplicated sources found.");
            }

            String host = configuration.getPanel().getHost();
            if (StringUtils.isEmpty(host)) {
                throw new CatalogException("Configuration of panel host missing. Please, consult with your administrator.");
            }
            if (!host.endsWith("/")) {
                host = host + "/";
            }

            // Obtain available sources from panel host
            Set<String> availableSources = new HashSet<>();
            URL url = new URL(host + "sources.txt");
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    availableSources.add(line.toLowerCase());
                }
            }

            // Validate sources are known
            for (String auxSource : sources) {
                if (!availableSources.contains(auxSource.toLowerCase())) {
                    throw new CatalogException("Unknown source '" + source + "'. Available sources are " + availableSources);
                }
            }

            OpenCGAResult<Panel> result = OpenCGAResult.empty();
            List<Panel> importedPanels = new LinkedList<>();
            for (String auxSource : sources) {
                // Obtain available panel ids from panel host
                Set<String> availablePanelIds = new HashSet<>();
                url = new URL(host + auxSource + "/panels.txt");
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        availablePanelIds.add(line);
                    }
                }

                List<String> panelIdList;
                if (StringUtils.isNotEmpty(panelIds)) {
                    panelIdList = Arrays.asList(panelIds.split(","));

                    // Validate all panel ids exist
                    for (String panelId : panelIdList) {
                        if (!availablePanelIds.contains(panelId)) {
                            throw new CatalogException("Unknown panel id '" + panelId + "'.");
                        }
                    }
                } else {
                    panelIdList = new ArrayList<>(availablePanelIds);
                }

                List<Panel> panelList = new ArrayList<>(panelIdList.size());
                // First we download all the parsed panels to avoid possible issues
                for (String panelId : panelIdList) {
                    url = new URL(host + auxSource + "/" + panelId + ".json");
                    try (InputStream inputStream = url.openStream()) {
                        Panel panel = JacksonUtils.getDefaultObjectMapper().readValue(inputStream, Panel.class);
                        autoCompletePanel(study, panel);
                        panelList.add(panel);
                    }
                }

                result.append(panelDBAdaptor.insert(study.getUid(), panelList));
            }
            result.setResults(importedPanels);
            auditManager.initAuditBatch(operationId);
            // Audit creation
            for (Panel importedPanel : importedPanels) {
                auditManager.audit(operationId, userId, Enums.Action.IMPORT, Enums.Resource.DISEASE_PANEL, importedPanel.getId(),
                        importedPanel.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
            }
            auditManager.finishAuditBatch(operationId);

            return result;
        } catch (CatalogException e) {
            auditManager.audit(operationId, userId, Enums.Action.IMPORT, Enums.Resource.DISEASE_PANEL, "", "",
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()),
                    new ObjectMap());
            throw e;
        } catch (IOException e) {
            CatalogException exception = new CatalogException("Error parsing panels: " + e.getMessage(), e);
            auditManager.audit(operationId, userId, Enums.Action.IMPORT, Enums.Resource.DISEASE_PANEL, "", "",
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
                            exception.getError()), new ObjectMap());
            throw exception;
        }
    }

    private void autoCompletePanel(Study study, Panel panel) throws CatalogException {
        ParamUtils.checkParameter(panel.getId(), "id");

        panel.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.PANEL));
        panel.setCreationDate(TimeUtils.getTime());
        panel.setRelease(studyManager.getCurrentRelease(study));
        panel.setVersion(1);
    }

    public OpenCGAResult<Panel> update(String studyId, Query query, PanelUpdateParams updateParams, QueryOptions options, String token)
            throws CatalogException {
        return update(studyId, query, updateParams, false, options, token);
    }

    public OpenCGAResult<Panel> update(String studyId, Query query, PanelUpdateParams updateParams, boolean ignoreException,
                                       QueryOptions options, String token) throws CatalogException {
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));

        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, userId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse PanelUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyId)
                .append("query", query)
                .append("updateParams", updateMap)
                .append("ignoreException", ignoreException)
                .append("options", options)
                .append("token", token);
        fixQueryObject(finalQuery);

        DBIterator<Panel> iterator;
        try {
            finalQuery.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            iterator = panelDBAdaptor.iterator(study.getUid(), finalQuery, INCLUDE_PANEL_IDS, userId);
        } catch (CatalogException e) {
            auditManager.auditUpdate(operationId, userId, Enums.Resource.DISEASE_PANEL, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<Panel> result = OpenCGAResult.empty();
        while (iterator.hasNext()) {
            Panel panel = iterator.next();
            try {
                OpenCGAResult updateResult = update(study, panel, updateParams, options, userId);
                result.append(updateResult);

                auditManager.auditUpdate(operationId, userId, Enums.Resource.DISEASE_PANEL, panel.getId(), panel.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, panel.getId(), e.getMessage());
                result.getEvents().add(event);

                logger.error("Could not update panel {}: {}", panel.getId(), e.getMessage(), e);
                auditManager.auditUpdate(operationId, userId, Enums.Resource.DISEASE_PANEL, panel.getId(), panel.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationId);

        return endResult(result, ignoreException);
    }

    public OpenCGAResult<Panel> update(String studyStr, String panelId, PanelUpdateParams updateParams, QueryOptions options, String token)
            throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse PanelUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("panelId", panelId)
                .append("updateParams", updateMap)
                .append("options", options)
                .append("token", token);

        OpenCGAResult<Panel> result = OpenCGAResult.empty();
        String panelUuid = "";
        try {
            OpenCGAResult<Panel> internalResult = internalGet(study.getUid(), panelId, QueryOptions.empty(), userId);
            if (internalResult.getNumResults() == 0) {
                throw new CatalogException("Panel '" + panelId + "' not found");
            }
            Panel panel = internalResult.first();

            // We set the proper values for the audit
            panelId = panel.getId();
            panelUuid = panel.getUuid();

            OpenCGAResult updateResult = update(study, panel, updateParams, options, userId);
            result.append(updateResult);

            auditManager.auditUpdate(userId, Enums.Resource.DISEASE_PANEL, panel.getId(), panel.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (CatalogException e) {
            Event event = new Event(Event.Type.ERROR, panelId, e.getMessage());
            result.getEvents().add(event);

            logger.error("Could not update panel {}: {}", panelId, e.getMessage(), e);
            auditManager.auditUpdate(operationId, userId, Enums.Resource.DISEASE_PANEL, panelId, panelUuid, study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        return result;
    }

    /**
     * Update Panel from catalog.
     *
     * @param studyStr     Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param panelIds     List of Panel ids. Could be either the id or uuid.
     * @param updateParams Data model filled only with the parameters to be updated.
     * @param options      QueryOptions object.
     * @param token        Session id of the user logged in.
     * @return A OpenCGAResult.
     * @throws CatalogException if there is any internal error, the user does not have proper permissions or a parameter passed does not
     *                          exist or is not allowed to be updated.
     */
    public OpenCGAResult<Panel> update(String studyStr, List<String> panelIds, PanelUpdateParams updateParams, QueryOptions options,
                                       String token) throws CatalogException {
        return update(studyStr, panelIds, updateParams, false, options, token);
    }

    public OpenCGAResult<Panel> update(String studyStr, List<String> panelIds, PanelUpdateParams updateParams, boolean ignoreException,
                                       QueryOptions options, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse PanelUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("panelIds", panelIds)
                .append("updateParams", updateMap)
                .append("ignoreException", ignoreException)
                .append("options", options)
                .append("token", token);

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<Panel> result = OpenCGAResult.empty();
        for (String id : panelIds) {
            String panelId = id;
            String panelUuid = "";

            try {
                OpenCGAResult<Panel> internalResult = internalGet(study.getUid(), panelId, QueryOptions.empty(), userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Panel '" + id + "' not found");
                }
                Panel panel = internalResult.first();

                // We set the proper values for the audit
                panelId = panel.getId();
                panelUuid = panel.getUuid();

                OpenCGAResult updateResult = update(study, panel, updateParams, options, userId);
                result.append(updateResult);

                auditManager.auditUpdate(userId, Enums.Resource.DISEASE_PANEL, panel.getId(), panel.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, panelId, e.getMessage());
                result.getEvents().add(event);

                logger.error("Could not update panel {}: {}", panelId, e.getMessage(), e);
                auditManager.auditUpdate(operationId, userId, Enums.Resource.DISEASE_PANEL, panelId, panelUuid, study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationId);

        return endResult(result, ignoreException);
    }

    private OpenCGAResult update(Study study, Panel panel, PanelUpdateParams updateParams, QueryOptions options, String userId)
            throws CatalogException {
        ObjectMap parameters = new ObjectMap();
        if (updateParams != null) {
            try {
                parameters = updateParams.getUpdateMap();
            } catch (JsonProcessingException e) {
                throw new CatalogException("Could not parse PanelUpdateParams object: " + e.getMessage(), e);
            }
        }

        options = ParamUtils.defaultObject(options, QueryOptions::new);

        if (parameters.isEmpty() && !options.getBoolean(Constants.INCREMENT_VERSION, false)) {
            ParamUtils.checkUpdateParametersMap(parameters);
        }

        // Check update permissions
        authorizationManager.checkPanelPermission(study.getUid(), panel.getUid(), userId, PanelAclEntry.PanelPermissions.WRITE);

        if (parameters.containsKey(PanelDBAdaptor.QueryParams.ID.key())) {
            ParamUtils.checkIdentifier(parameters.getString(PanelDBAdaptor.QueryParams.ID.key()),
                    PanelDBAdaptor.QueryParams.ID.key());
        }

        if (options.getBoolean(Constants.INCREMENT_VERSION)) {
            // We do need to get the current release to properly create a new version
            options.put(Constants.CURRENT_RELEASE, studyManager.getCurrentRelease(study));
        }

        return panelDBAdaptor.update(panel.getUid(), parameters, options);
    }

    @Override
    public DBIterator<Panel> iterator(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        fixQueryObject(query);
        query.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        return panelDBAdaptor.iterator(study.getUid(), query, options, userId);
    }

    @Override
    public OpenCGAResult<Panel> search(String studyId, Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("options", options)
                .append("token", token);

        try {
            fixQueryObject(query);
            query.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<Panel> result = panelDBAdaptor.get(study.getUid(), query, options, userId);

            auditManager.auditSearch(userId, Enums.Resource.DISEASE_PANEL, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return result;
        } catch (CatalogException e) {
            auditManager.auditSearch(userId, Enums.Resource.DISEASE_PANEL, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult<?> distinct(String studyId, String field, Query query, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("field", new Query(query))
                .append("query", new Query(query))
                .append("token", token);
        fixQueryObject(query);
        try {
            PanelDBAdaptor.QueryParams param = PanelDBAdaptor.QueryParams.getParam(field);
            if (param == null) {
                throw new CatalogException("Unknown '" + field + "' parameter.");
            }
            Class<?> clazz = getTypeClass(param.type());

            fixQueryObject(query);

            query.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<?> result = panelDBAdaptor.distinct(study.getUid(), field, query, userId, clazz);

            auditManager.auditDistinct(userId, Enums.Resource.DISEASE_PANEL, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (CatalogException e) {
            auditManager.auditDistinct(userId, Enums.Resource.DISEASE_PANEL, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult<Panel> count(String studyId, Query query, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("token", token);
        fixQueryObject(query);
        try {
            query.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            // Here view permissions will be checked
            OpenCGAResult<Long> queryResultAux = panelDBAdaptor.count(query, userId);

            auditManager.auditCount(userId, Enums.Resource.DISEASE_PANEL, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>(queryResultAux.getTime(), queryResultAux.getEvents(), 0, Collections.emptyList(),
                    queryResultAux.getNumMatches());
        } catch (CatalogException e) {
            auditManager.auditCount(userId, Enums.Resource.DISEASE_PANEL, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult delete(String studyStr, List<String> panelIds, QueryOptions options, String token) throws CatalogException {
        return delete(studyStr, panelIds, options, false, token);
    }

    public OpenCGAResult delete(String studyStr, List<String> panelIds, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        if (panelIds == null || ListUtils.isEmpty(panelIds)) {
            throw new CatalogException("Missing list of panel ids");
        }

        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("panelIds", panelIds)
                .append("params", params)
                .append("ignoreException", ignoreException)
                .append("token", token);

        boolean checkPermissions;
        try {
            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            checkPermissions = !authorizationManager.isOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(operationId, userId, Enums.Resource.DISEASE_PANEL, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<Panel> result = OpenCGAResult.empty();
        for (String id : panelIds) {

            String panelId = id;
            String panelUuid = "";
            try {
                OpenCGAResult<Panel> internalResult = internalGet(study.getUid(), id, INCLUDE_PANEL_IDS, userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Panel '" + id + "' not found");
                }

                Panel panel = internalResult.first();
                // We set the proper values for the audit
                panelId = panel.getId();
                panelUuid = panel.getUuid();

                if (checkPermissions) {
                    authorizationManager.checkPanelPermission(study.getUid(), panel.getUid(), userId,
                            PanelAclEntry.PanelPermissions.DELETE);
                }

                // Check if the panel can be deleted
                // TODO: Check if the panel is used in an interpretation. At this point, it can be deleted no matter what.

                // Delete the panel
                result.append(panelDBAdaptor.delete(panel));

                auditManager.auditDelete(operationId, userId, Enums.Resource.DISEASE_PANEL, panel.getId(), panel.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, id, e.getMessage());
                result.getEvents().add(event);

                logger.error("Cannot delete panel {}: {}", panelId, e.getMessage());
                auditManager.auditDelete(operationId, userId, Enums.Resource.DISEASE_PANEL, panelId, panelUuid, study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationId);

        return endResult(result, ignoreException);
    }

    @Override
    public OpenCGAResult delete(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        return delete(studyStr, query, options, false, token);
    }

    public OpenCGAResult delete(String studyStr, Query query, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));
        OpenCGAResult result = OpenCGAResult.empty();

        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("query", new Query(query))
                .append("params", params)
                .append("ignoreException", ignoreException)
                .append("token", token);
        fixQueryObject(finalQuery);

        // If the user is the owner or the admin, we won't check if he has permissions for every single entry
        boolean checkPermissions;

        // We try to get an iterator containing all the families to be deleted
        DBIterator<Panel> iterator;
        try {
            finalQuery.append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = panelDBAdaptor.iterator(study.getUid(), finalQuery, INCLUDE_PANEL_IDS, userId);

            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            checkPermissions = !authorizationManager.isOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(operationId, userId, Enums.Resource.DISEASE_PANEL, "", "", study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationId);
        while (iterator.hasNext()) {
            Panel panel = iterator.next();

            try {
                if (checkPermissions) {
                    authorizationManager.checkPanelPermission(study.getUid(), panel.getUid(), userId,
                            PanelAclEntry.PanelPermissions.DELETE);
                }

                // Check if the panel can be deleted
                // TODO: Check if the panel is used in an interpretation. At this point, it can be deleted no matter what.

                // Delete the panel
                result.append(panelDBAdaptor.delete(panel));

                auditManager.auditDelete(operationId, userId, Enums.Resource.DISEASE_PANEL, panel.getId(), panel.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                String errorMsg = "Cannot delete panel " + panel.getId() + ": " + e.getMessage();

                Event event = new Event(Event.Type.ERROR, panel.getId(), e.getMessage());
                result.getEvents().add(event);

                logger.error(errorMsg);
                auditManager.auditDelete(operationId, userId, Enums.Resource.DISEASE_PANEL, panel.getId(), panel.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationId);

        return endResult(result, ignoreException);
    }

    @Override
    public OpenCGAResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.VIEW_PANELS);

        fixQueryObject(query);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        query.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        OpenCGAResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = panelDBAdaptor.rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }

    @Override
    public OpenCGAResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        fixQueryObject(query);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        if (fields == null || fields.size() == 0) {
            throw new CatalogException("Empty fields parameter.");
        }

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        // Add study id to the query
        query.put(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        OpenCGAResult queryResult = sampleDBAdaptor.groupBy(query, fields, options, userId);
        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }

    // **************************   ACLs  ******************************** //
    public OpenCGAResult<Map<String, List<String>>> getAcls(String studyId, List<String> panelList, String member, boolean ignoreException,
                                                            String token) throws CatalogException {
        String user = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, user);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("panelList", panelList)
                .append("member", member)
                .append("ignoreException", ignoreException)
                .append("token", token);
        try {
            OpenCGAResult<Map<String, List<String>>> panelAclList = OpenCGAResult.empty();
            InternalGetDataResult<Panel> queryResult = internalGet(study.getUid(), panelList, INCLUDE_PANEL_IDS, user, ignoreException);

            Map<String, InternalGetDataResult.Missing> missingMap = new HashMap<>();
            if (queryResult.getMissing() != null) {
                missingMap = queryResult.getMissing().stream()
                        .collect(Collectors.toMap(InternalGetDataResult.Missing::getId, Function.identity()));
            }
            int counter = 0;
            for (String panelId : panelList) {
                if (!missingMap.containsKey(panelId)) {
                    Panel panel = queryResult.getResults().get(counter);
                    try {
                        OpenCGAResult<Map<String, List<String>>> allPanelAcls;
                        if (StringUtils.isNotEmpty(member)) {
                            allPanelAcls = authorizationManager.getPanelAcl(study.getUid(), panel.getUid(), user, member);
                        } else {
                            allPanelAcls = authorizationManager.getAllPanelAcls(study.getUid(), panel.getUid(), user);
                        }
                        panelAclList.append(allPanelAcls);
                        auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.DISEASE_PANEL, panel.getId(),
                                panel.getUuid(), study.getId(), study.getUuid(), auditParams,
                                new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
                    } catch (CatalogException e) {
                        auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.DISEASE_PANEL, panel.getId(),
                                panel.getUuid(), study.getId(), study.getUuid(), auditParams,
                                new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());

                        if (!ignoreException) {
                            throw e;
                        } else {
                            Event event = new Event(Event.Type.ERROR, panelId, missingMap.get(panelId).getErrorMsg());
                            panelAclList.append(new OpenCGAResult<>(0, Collections.singletonList(event), 0,
                                    Collections.singletonList(Collections.emptyMap()), 0));
                        }
                    }
                    counter += 1;
                } else {
                    Event event = new Event(Event.Type.ERROR, panelId, missingMap.get(panelId).getErrorMsg());
                    panelAclList.append(new OpenCGAResult<>(0, Collections.singletonList(event), 0,
                            Collections.singletonList(Collections.emptyMap()), 0));

                    auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.DISEASE_PANEL, panelId, "",
                            study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
                                    new Error(0, "", missingMap.get(panelId).getErrorMsg())), new ObjectMap());
                }
            }
            return panelAclList;
        } catch (CatalogException e) {
            for (String panelId : panelList) {
                auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.DISEASE_PANEL, panelId, "", study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()),
                        new ObjectMap());
            }
            throw e;
        }
    }

    public OpenCGAResult<Map<String, List<String>>> updateAcl(String studyId, List<String> panelStrList, String memberList,
                                                              AclParams aclParams, ParamUtils.AclAction action, String token)
            throws CatalogException {
        String user = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, user);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("panelStrList", panelStrList)
                .append("memberList", memberList)
                .append("aclParams", aclParams)
                .append("action", action)
                .append("token", token);
        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        try {
            if (panelStrList == null || panelStrList.isEmpty()) {
                throw new CatalogException("Update ACL: Missing panel parameter");
            }

            if (action == null) {
                throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
            }

            List<String> permissions = Collections.emptyList();
            if (StringUtils.isNotEmpty(aclParams.getPermissions())) {
                permissions = Arrays.asList(aclParams.getPermissions().trim().replaceAll("\\s", "").split(","));
                checkPermissions(permissions, PanelAclEntry.PanelPermissions::valueOf);
            }

            OpenCGAResult<Panel> panelDataResult = internalGet(study.getUid(), panelStrList, INCLUDE_PANEL_IDS, user, false);
            authorizationManager.checkCanAssignOrSeePermissions(study.getUid(), user);

            // Validate that the members are actually valid members
            List<String> members;
            if (memberList != null && !memberList.isEmpty()) {
                members = Arrays.asList(memberList.split(","));
            } else {
                members = Collections.emptyList();
            }
            authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
            checkMembers(study.getUid(), members);

            List<Long> panelUids = panelDataResult.getResults().stream().map(Panel::getUid).collect(Collectors.toList());
            AuthorizationManager.CatalogAclParams catalogAclParams = new AuthorizationManager.CatalogAclParams(panelUids, permissions,
                    Enums.Resource.DISEASE_PANEL);

            OpenCGAResult<Map<String, List<String>>> queryResultList;
            switch (action) {
                case SET:
                    queryResultList = authorizationManager.setAcls(study.getUid(), members, catalogAclParams);
                    break;
                case ADD:
                    queryResultList = authorizationManager.addAcls(study.getUid(), members, catalogAclParams);
                    break;
                case REMOVE:
                    queryResultList = authorizationManager.removeAcls(members, catalogAclParams);
                    break;
                case RESET:
                    catalogAclParams.setPermissions(null);
                    queryResultList = authorizationManager.removeAcls(members, catalogAclParams);
                    break;
                default:
                    throw new CatalogException("Unexpected error occurred. No valid action found.");
            }
            for (Panel panel : panelDataResult.getResults()) {
                auditManager.audit(operationId, user, Enums.Action.UPDATE_ACLS, Enums.Resource.DISEASE_PANEL, panel.getId(),
                        panel.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
            }
            return queryResultList;
        } catch (CatalogException e) {
            if (panelStrList != null) {
                for (String panelId : panelStrList) {
                    auditManager.audit(operationId, user, Enums.Action.UPDATE_ACLS, Enums.Resource.DISEASE_PANEL, panelId, "",
                            study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
                }
            }
            throw e;
        }
    }

    protected void fixQueryObject(Query query) {
        super.fixQueryObject(query);
        changeQueryId(query, ParamConstants.PANEL_STATUS_PARAM, PanelDBAdaptor.QueryParams.STATUS_NAME.key());
    }

    void fillDefaultStats(Panel panel) {
        if (panel.getStats() == null || panel.getStats().isEmpty()) {
            Map<String, Integer> stats = new HashMap<>();
            stats.put("numberOfVariants", panel.getVariants().size());
            stats.put("numberOfGenes", panel.getGenes().size());
            stats.put("numberOfRegions", panel.getRegions().size());

            panel.setStats(stats);
        }
    }

}
