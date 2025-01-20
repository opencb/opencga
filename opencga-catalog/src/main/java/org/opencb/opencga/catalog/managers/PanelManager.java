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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.common.Status;
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
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.panel.PanelInternal;
import org.opencb.opencga.core.models.panel.PanelPermissions;
import org.opencb.opencga.core.models.panel.PanelUpdateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyPermissions;
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

    public static final QueryOptions INCLUDE_PANEL_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            PanelDBAdaptor.QueryParams.ID.key(), PanelDBAdaptor.QueryParams.UID.key(), PanelDBAdaptor.QueryParams.UUID.key(),
            PanelDBAdaptor.QueryParams.VERSION.key(), PanelDBAdaptor.QueryParams.STUDY_UID.key()));
    protected static Logger logger = LoggerFactory.getLogger(PanelManager.class);
    private UserManager userManager;
    private StudyManager studyManager;

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
    InternalGetDataResult<Panel> internalGet(String organizationId, long studyUid, List<String> entryList, @Nullable Query query,
                                             QueryOptions options, String user, boolean ignoreException) throws CatalogException {
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

        OpenCGAResult<Panel> panelDataResult = getPanelDBAdaptor(organizationId).get(studyUid, queryCopy, queryOptions, user);

        Function<Panel, String> panelStringFunction = Panel::getId;
        if (idQueryParam.equals(PanelDBAdaptor.QueryParams.UUID)) {
            panelStringFunction = Panel::getUuid;
        }

        if (ignoreException || panelDataResult.getNumResults() >= uniqueList.size()) {
            return keepOriginalOrder(uniqueList, panelStringFunction, panelDataResult, ignoreException, versioned);
        }
        // Query without adding the user check
        OpenCGAResult<Panel> resultsNoCheck = getPanelDBAdaptor(organizationId).get(queryCopy, queryOptions);

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

    private OpenCGAResult<Panel> getPanel(String organizationId, long studyUid, String panelUuid, QueryOptions options)
            throws CatalogException {
        Query query = new Query()
                .append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(PanelDBAdaptor.QueryParams.UUID.key(), panelUuid);
        return getPanelDBAdaptor(organizationId).get(query, options);
    }

    @Override
    public OpenCGAResult<Panel> create(String studyStr, Panel panel, QueryOptions options, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("panel", panel)
                .append("options", options)
                .append("token", token);
        try {
            // 1. We check everything can be done
            authorizationManager.checkStudyPermission(organizationId, study.getUid(), userId, StudyPermissions.Permissions.WRITE_PANELS);

            autoCompletePanel(organizationId, study, panel);
            options = ParamUtils.defaultObject(options, QueryOptions::new);

            OpenCGAResult<Panel> insert = getPanelDBAdaptor(organizationId).insert(study.getUid(), panel, options);
            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch created panel
                OpenCGAResult<Panel> result = getPanel(organizationId, study.getUid(), panel.getUuid(), options);
                insert.setResults(result.getResults());
            }
            auditManager.auditCreate(organizationId, userId, Enums.Resource.DISEASE_PANEL, panel.getId(), panel.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return insert;
        } catch (CatalogException e) {
            auditManager.auditCreate(organizationId, userId, Enums.Resource.DISEASE_PANEL, panel.getId(), "", study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<Panel> importFromSource(String studyId, String source, String panelIds, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId, organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("source", source)
                .append("panelIds", panelIds)
                .append("token", token);
        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        try {
            // 1. We check everything can be done
            authorizationManager.checkStudyPermission(organizationId, study.getUid(), userId, StudyPermissions.Permissions.WRITE_PANELS);
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
            logger.info("Fetching available sources from '{}'", url);
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
                logger.info("Fetching available panel ids from '{}'", url);
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
                    logger.info("Downloading panel '{}' from '{}'", panelId, url);
                    try (InputStream inputStream = url.openStream()) {
                        Panel panel = JacksonUtils.getDefaultObjectMapper().readValue(inputStream, Panel.class);
                        autoCompletePanel(organizationId, study, panel);
                        panelList.add(panel);
                    }
                }

                logger.info("Inserting panels in database");
                result.append(getPanelDBAdaptor(organizationId).insert(study.getUid(), panelList));
            }
            result.setResults(importedPanels);
            auditManager.initAuditBatch(operationId);
            // Audit creation
            for (Panel importedPanel : importedPanels) {
                auditManager.audit(organizationId, operationId, userId, Enums.Action.IMPORT, Enums.Resource.DISEASE_PANEL,
                        importedPanel.getId(), importedPanel.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
            }
            auditManager.finishAuditBatch(organizationId, operationId);

            return result;
        } catch (CatalogException e) {
            auditManager.audit(organizationId, operationId, userId, Enums.Action.IMPORT, Enums.Resource.DISEASE_PANEL, "", "",
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()),
                    new ObjectMap());
            throw e;
        } catch (IOException e) {
            CatalogException exception = new CatalogException("Error parsing panels: " + e.getMessage(), e);
            auditManager.audit(organizationId, operationId, userId, Enums.Action.IMPORT, Enums.Resource.DISEASE_PANEL, "", "",
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
                            exception.getError()), new ObjectMap());
            throw exception;
        }
    }

    private void autoCompletePanel(String organizationId, Study study, Panel panel) throws CatalogException {
        // Check all the panel fields
        ParamUtils.checkIdentifier(panel.getId(), "id");
        panel.setName(ParamUtils.defaultString(panel.getName(), panel.getId()));
        panel.setRelease(studyManager.getCurrentRelease(study));
        panel.setVersion(1);
        panel.setAuthor(ParamUtils.defaultString(panel.getAuthor(), ""));
        panel.setCreationDate(TimeUtils.getTime());
        panel.setModificationDate(TimeUtils.getTime());
        panel.setStatus(new Status());
        panel.setInternal(PanelInternal.init());
        panel.setCategories(ParamUtils.defaultObject(panel.getCategories(), Collections.emptyList()));
        panel.setTags(ParamUtils.defaultObject(panel.getTags(), Collections.emptyList()));
        panel.setDescription(ParamUtils.defaultString(panel.getDescription(), ""));
        panel.setDisorders(ParamUtils.defaultObject(panel.getDisorders(), Collections.emptyList()));
        panel.setVariants(ParamUtils.defaultObject(panel.getVariants(), Collections.emptyList()));
        panel.setRegions(ParamUtils.defaultObject(panel.getRegions(), Collections.emptyList()));
        panel.setGenes(ParamUtils.defaultObject(panel.getGenes(), Collections.emptyList()));
        panel.setAttributes(ParamUtils.defaultObject(panel.getAttributes(), Collections.emptyMap()));
        panel.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.PANEL));
    }

    public OpenCGAResult<Panel> update(String studyId, Query query, PanelUpdateParams updateParams, QueryOptions options, String token)
            throws CatalogException {
        return update(studyId, query, updateParams, false, options, token);
    }

    public OpenCGAResult<Panel> update(String studyId, Query query, PanelUpdateParams updateParams, boolean ignoreException,
                                       QueryOptions options, String token) throws CatalogException {
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyId, userId, organizationId);

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
            iterator = getPanelDBAdaptor(organizationId).iterator(study.getUid(), finalQuery, INCLUDE_PANEL_IDS, userId);
        } catch (CatalogException e) {
            auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.DISEASE_PANEL, "", "", study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<Panel> result = OpenCGAResult.empty();
        while (iterator.hasNext()) {
            Panel panel = iterator.next();
            try {
                OpenCGAResult updateResult = update(organizationId, study, panel, updateParams, options, userId);
                result.append(updateResult);

                auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.DISEASE_PANEL, panel.getId(), panel.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, panel.getId(), e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error("Could not update panel {}: {}", panel.getId(), e.getMessage(), e);
                auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.DISEASE_PANEL, panel.getId(), panel.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(organizationId, operationId);

        return endResult(result, ignoreException);
    }

    public OpenCGAResult<Panel> update(String studyStr, String panelId, PanelUpdateParams updateParams, QueryOptions options, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyStr, userId, organizationId);

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
            OpenCGAResult<Panel> internalResult = internalGet(organizationId, study.getUid(), panelId, QueryOptions.empty(), userId);
            if (internalResult.getNumResults() == 0) {
                throw new CatalogException("Panel '" + panelId + "' not found");
            }
            Panel panel = internalResult.first();

            // We set the proper values for the audit
            panelId = panel.getId();
            panelUuid = panel.getUuid();

            OpenCGAResult updateResult = update(organizationId, study, panel, updateParams, options, userId);
            result.append(updateResult);

            auditManager.auditUpdate(organizationId, userId, Enums.Resource.DISEASE_PANEL, panel.getId(), panel.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (CatalogException e) {
            Event event = new Event(Event.Type.ERROR, panelId, e.getMessage());
            result.getEvents().add(event);
            result.setNumErrors(result.getNumErrors() + 1);

            logger.error("Could not update panel {}: {}", panelId, e.getMessage(), e);
            auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.DISEASE_PANEL, panelId, panelUuid, study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        return result;
    }

    /**
     * Update Panel from catalog.
     *
     * @param studyStr     Study id in string format. Could be one of
     *                     [id|organization@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy]
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
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyStr, userId, organizationId);

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
                OpenCGAResult<Panel> internalResult = internalGet(organizationId, study.getUid(), panelId, QueryOptions.empty(), userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Panel '" + id + "' not found");
                }
                Panel panel = internalResult.first();

                // We set the proper values for the audit
                panelId = panel.getId();
                panelUuid = panel.getUuid();

                OpenCGAResult updateResult = update(organizationId, study, panel, updateParams, options, userId);
                result.append(updateResult);

                auditManager.auditUpdate(organizationId, userId, Enums.Resource.DISEASE_PANEL, panel.getId(), panel.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, panelId, e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error("Could not update panel {}: {}", panelId, e.getMessage(), e);
                auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.DISEASE_PANEL, panelId, panelUuid,
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(organizationId, operationId);

        return endResult(result, ignoreException);
    }

    private OpenCGAResult update(String organizationId, Study study, Panel panel, PanelUpdateParams updateParams, QueryOptions options,
                                 String userId) throws CatalogException {
        ObjectMap parameters = new ObjectMap();
        if (updateParams != null) {
            try {
                parameters = updateParams.getUpdateMap();
            } catch (JsonProcessingException e) {
                throw new CatalogException("Could not parse PanelUpdateParams object: " + e.getMessage(), e);
            }
        }

        options = ParamUtils.defaultObject(options, QueryOptions::new);

        ParamUtils.checkUpdateParametersMap(parameters);

        // Check update permissions
        authorizationManager.checkPanelPermission(organizationId, study.getUid(), panel.getUid(), userId, PanelPermissions.WRITE);

        if (parameters.containsKey(PanelDBAdaptor.QueryParams.ID.key())) {
            ParamUtils.checkIdentifier(parameters.getString(PanelDBAdaptor.QueryParams.ID.key()),
                    PanelDBAdaptor.QueryParams.ID.key());
        }

        OpenCGAResult<Panel> update = getPanelDBAdaptor(organizationId).update(panel.getUid(), parameters, options);
        if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
            // Fetch updated panel
            OpenCGAResult<Panel> result = getPanelDBAdaptor(organizationId).get(study.getUid(),
                    new Query(PanelDBAdaptor.QueryParams.UID.key(), panel.getUid()), options, userId);
            update.setResults(result.getResults());
        }
        return update;
    }

    @Override
    public DBIterator<Panel> iterator(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(sessionId);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, organizationId);

        fixQueryObject(query);
        query.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        return getPanelDBAdaptor(organizationId).iterator(study.getUid(), query, options, userId);
    }

    @Override
    public OpenCGAResult<Panel> search(String studyId, Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()), userId, organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("options", options)
                .append("token", token);

        try {
            fixQueryObject(query);
            query.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<Panel> result = getPanelDBAdaptor(organizationId).get(study.getUid(), query, options, userId);

            auditManager.auditSearch(organizationId, userId, Enums.Resource.DISEASE_PANEL, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return result;
        } catch (CatalogException e) {
            auditManager.auditSearch(organizationId, userId, Enums.Resource.DISEASE_PANEL, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult<?> distinct(String studyId, List<String> fields, Query query, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId, organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("fields", fields)
                .append("query", new Query(query))
                .append("token", token);
        fixQueryObject(query);
        try {
            fixQueryObject(query);

            query.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<?> result = getPanelDBAdaptor(organizationId).distinct(study.getUid(), fields, query, userId);

            auditManager.auditDistinct(organizationId, userId, Enums.Resource.DISEASE_PANEL, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (CatalogException e) {
            auditManager.auditDistinct(organizationId, userId, Enums.Resource.DISEASE_PANEL, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult<Panel> count(String studyId, Query query, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()), userId, organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("token", token);
        fixQueryObject(query);
        try {
            query.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            // Here view permissions will be checked
            OpenCGAResult<Long> queryResultAux = getPanelDBAdaptor(organizationId).count(query, userId);

            auditManager.auditCount(organizationId, userId, Enums.Resource.DISEASE_PANEL, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>(queryResultAux.getTime(), queryResultAux.getEvents(), 0, Collections.emptyList(),
                    queryResultAux.getNumMatches());
        } catch (CatalogException e) {
            auditManager.auditCount(organizationId, userId, Enums.Resource.DISEASE_PANEL, study.getId(), study.getUuid(), auditParams,
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

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyStr, userId, organizationId);

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
            long studyId = study.getUid();
            checkPermissions = !authorizationManager.isAtLeastStudyAdministrator(organizationId, studyId, userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(organizationId, operationId, userId, Enums.Resource.DISEASE_PANEL, "", "", study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<Panel> result = OpenCGAResult.empty();
        for (String id : panelIds) {

            String panelId = id;
            String panelUuid = "";
            try {
                OpenCGAResult<Panel> internalResult = internalGet(organizationId, study.getUid(), id, INCLUDE_PANEL_IDS, userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Panel '" + id + "' not found");
                }

                Panel panel = internalResult.first();
                // We set the proper values for the audit
                panelId = panel.getId();
                panelUuid = panel.getUuid();

                if (checkPermissions) {
                    authorizationManager.checkPanelPermission(organizationId, study.getUid(), panel.getUid(), userId,
                            PanelPermissions.DELETE);
                }

                // Check if the panel can be deleted
                // TODO: Check if the panel is used in an interpretation. At this point, it can be deleted no matter what.

                // Delete the panel
                result.append(getPanelDBAdaptor(organizationId).delete(panel));

                auditManager.auditDelete(organizationId, operationId, userId, Enums.Resource.DISEASE_PANEL, panel.getId(), panel.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, id, e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error("Cannot delete panel {}: {}", panelId, e.getMessage());
                auditManager.auditDelete(organizationId, operationId, userId, Enums.Resource.DISEASE_PANEL, panelId, panelUuid,
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(organizationId, operationId);

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

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyStr, userId, organizationId);

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

            iterator = getPanelDBAdaptor(organizationId).iterator(study.getUid(), finalQuery, INCLUDE_PANEL_IDS, userId);

            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            long studyId = study.getUid();
            checkPermissions = !authorizationManager.isAtLeastStudyAdministrator(organizationId, studyId, userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(organizationId, operationId, userId, Enums.Resource.DISEASE_PANEL, "", "", study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationId);
        while (iterator.hasNext()) {
            Panel panel = iterator.next();

            try {
                if (checkPermissions) {
                    authorizationManager.checkPanelPermission(organizationId, study.getUid(), panel.getUid(), userId,
                            PanelPermissions.DELETE);
                }

                // Check if the panel can be deleted
                // TODO: Check if the panel is used in an interpretation. At this point, it can be deleted no matter what.

                // Delete the panel
                result.append(getPanelDBAdaptor(organizationId).delete(panel));

                auditManager.auditDelete(organizationId, operationId, userId, Enums.Resource.DISEASE_PANEL, panel.getId(), panel.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                String errorMsg = "Cannot delete panel " + panel.getId() + ": " + e.getMessage();

                Event event = new Event(Event.Type.ERROR, panel.getId(), e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error(errorMsg);
                auditManager.auditDelete(organizationId, operationId, userId, Enums.Resource.DISEASE_PANEL, panel.getId(), panel.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(organizationId, operationId);

        return endResult(result, ignoreException);
    }

    @Override
    public OpenCGAResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String token)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(token, "token");

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, organizationId);

        authorizationManager.checkStudyPermission(organizationId, study.getUid(), userId, StudyPermissions.Permissions.VIEW_PANELS);

        fixQueryObject(query);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        query.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        OpenCGAResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = getPanelDBAdaptor(organizationId).rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }

    @Override
    public OpenCGAResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        fixQueryObject(query);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        if (CollectionUtils.isEmpty(fields)) {
            throw new CatalogException("Empty fields parameter.");
        }

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(sessionId);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, organizationId);

        // Add study id to the query
        query.put(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        OpenCGAResult queryResult = getSampleDBAdaptor(organizationId).groupBy(query, fields, options, userId);
        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }

    // **************************   ACLs  ******************************** //
    public OpenCGAResult<AclEntryList<PanelPermissions>> getAcls(String studyId, List<String> panelList, String member,
                                                                 boolean ignoreException, String token) throws CatalogException {
        return getAcls(studyId, panelList,
                StringUtils.isNotEmpty(member) ? Collections.singletonList(member) : Collections.emptyList(), ignoreException, token);
    }

    public OpenCGAResult<AclEntryList<PanelPermissions>> getAcls(String studyId, List<String> panelList, List<String> members,
                                                                 boolean ignoreException, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyId, userId, organizationId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("panelList", panelList)
                .append("members", members)
                .append("ignoreException", ignoreException)
                .append("token", token);

        OpenCGAResult<AclEntryList<PanelPermissions>> panelAcls = OpenCGAResult.empty();
        Map<String, InternalGetDataResult.Missing> missingMap = new HashMap<>();
        try {
            auditManager.initAuditBatch(operationId);
            InternalGetDataResult<Panel> queryResult = internalGet(organizationId, study.getUid(), panelList, INCLUDE_PANEL_IDS, userId,
                    ignoreException);

            if (queryResult.getMissing() != null) {
                missingMap = queryResult.getMissing().stream()
                        .collect(Collectors.toMap(InternalGetDataResult.Missing::getId, Function.identity()));
            }

            List<Long> panelUids = queryResult.getResults().stream().map(Panel::getUid).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(members)) {
                panelAcls = authorizationManager.getAcl(organizationId, study.getUid(), panelUids, members, Enums.Resource.DISEASE_PANEL,
                        PanelPermissions.class, userId);
            } else {
                panelAcls = authorizationManager.getAcl(organizationId, study.getUid(), panelUids, Enums.Resource.DISEASE_PANEL,
                        PanelPermissions.class, userId);
            }

            // Include non-existing panels to the result list
            List<AclEntryList<PanelPermissions>> resultList = new ArrayList<>(panelList.size());
            List<Event> eventList = new ArrayList<>(missingMap.size());
            int counter = 0;
            for (String panelId : panelList) {
                if (!missingMap.containsKey(panelId)) {
                    Panel panel = queryResult.getResults().get(counter);
                    resultList.add(panelAcls.getResults().get(counter));
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.FETCH_ACLS, Enums.Resource.DISEASE_PANEL,
                            panel.getId(), panel.getUuid(), study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
                    counter++;
                } else {
                    resultList.add(new AclEntryList<>());
                    eventList.add(new Event(Event.Type.ERROR, panelId, missingMap.get(panelId).getErrorMsg()));
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.FETCH_ACLS, Enums.Resource.DISEASE_PANEL, panelId,
                            "", study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
                                    new Error(0, "", missingMap.get(panelId).getErrorMsg())), new ObjectMap());
                }
            }
            for (int i = 0; i < queryResult.getResults().size(); i++) {
                panelAcls.getResults().get(i).setId(queryResult.getResults().get(i).getId());
            }
            panelAcls.setResults(resultList);
            panelAcls.setEvents(eventList);
        } catch (CatalogException e) {
            for (String panelId : panelList) {
                auditManager.audit(organizationId, operationId, userId, Enums.Action.FETCH_ACLS, Enums.Resource.DISEASE_PANEL, panelId, "",
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()),
                        new ObjectMap());
            }
            if (!ignoreException) {
                throw e;
            } else {
                for (String panelId : panelList) {
                    Event event = new Event(Event.Type.ERROR, panelId, e.getMessage());
                    panelAcls.append(new OpenCGAResult<>(0, Collections.singletonList(event), 0, Collections.emptyList(), 0));
                }
            }
        } finally {
            auditManager.finishAuditBatch(organizationId, operationId);
        }

        return panelAcls;
    }

    public OpenCGAResult<AclEntryList<PanelPermissions>> updateAcl(String studyId, List<String> panelStrList, String memberList,
                                                                   AclParams aclParams, ParamUtils.AclAction action, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyFqn, StudyManager.INCLUDE_CONFIGURATION, tokenPayload);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("panelStrList", panelStrList)
                .append("memberList", memberList)
                .append("aclParams", aclParams)
                .append("action", action)
                .append("token", token);
        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        try {
            auditManager.initAuditBatch(operationId);

            if (panelStrList == null || panelStrList.isEmpty()) {
                throw new CatalogException("Update ACL: Missing panel parameter");
            }

            if (action == null) {
                throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
            }

            List<String> permissions = Collections.emptyList();
            if (StringUtils.isNotEmpty(aclParams.getPermissions())) {
                permissions = Arrays.asList(aclParams.getPermissions().trim().replaceAll("\\s", "").split(","));
                checkPermissions(permissions, PanelPermissions::valueOf);
            }

            OpenCGAResult<Panel> panelDataResult = internalGet(organizationId, study.getUid(), panelStrList, INCLUDE_PANEL_IDS, userId,
                    false);
            authorizationManager.checkCanAssignOrSeePermissions(organizationId, study.getUid(), userId);

            // Validate that the members are actually valid members
            List<String> members;
            if (memberList != null && !memberList.isEmpty()) {
                members = Arrays.asList(memberList.split(","));
            } else {
                members = Collections.emptyList();
            }
            authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
            checkMembers(organizationId, study.getUid(), members);
            if (study.getInternal().isFederated()) {
                try {
                    checkIsNotAFederatedUser(organizationId, members);
                } catch (CatalogException e) {
                    throw new CatalogException("Cannot provide access to federated users to a federated study.", e);
                }
            }

            List<Long> panelUids = panelDataResult.getResults().stream().map(Panel::getUid).collect(Collectors.toList());
            AuthorizationManager.CatalogAclParams catalogAclParams = new AuthorizationManager.CatalogAclParams(panelUids, permissions,
                    Enums.Resource.DISEASE_PANEL);

            switch (action) {
                case SET:
                    authorizationManager.setAcls(organizationId, study.getUid(), members, catalogAclParams);
                    break;
                case ADD:
                    authorizationManager.addAcls(organizationId, study.getUid(), members, catalogAclParams);
                    break;
                case REMOVE:
                    authorizationManager.removeAcls(organizationId, members, catalogAclParams);
                    break;
                case RESET:
                    catalogAclParams.setPermissions(null);
                    authorizationManager.removeAcls(organizationId, members, catalogAclParams);
                    break;
                default:
                    throw new CatalogException("Unexpected error occurred. No valid action found.");
            }
            OpenCGAResult<AclEntryList<PanelPermissions>> queryResultList = authorizationManager.getAcls(organizationId, study.getUid(),
                    panelUids, members, Enums.Resource.DISEASE_PANEL, PanelPermissions.class);
            for (int i = 0; i < queryResultList.getResults().size(); i++) {
                queryResultList.getResults().get(i).setId(panelDataResult.getResults().get(i).getId());
            }
            for (Panel panel : panelDataResult.getResults()) {
                auditManager.audit(organizationId, operationId, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.DISEASE_PANEL,
                        panel.getId(), panel.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
            }
            return queryResultList;
        } catch (CatalogException e) {
            if (panelStrList != null) {
                for (String panelId : panelStrList) {
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.DISEASE_PANEL, panelId,
                            "", study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
                }
            }
            throw e;
        } finally {
            auditManager.finishAuditBatch(organizationId, operationId);
        }
    }

    protected void fixQueryObject(Query query) {
        super.fixQueryObject(query);
        changeQueryId(query, ParamConstants.PANEL_STATUS_PARAM, PanelDBAdaptor.QueryParams.STATUS_ID.key());
    }

}
