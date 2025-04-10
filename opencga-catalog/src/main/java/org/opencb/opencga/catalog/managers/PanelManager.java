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
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.db.api.PanelDBAdaptor;
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
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.common.EntryParam;
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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;

public class PanelManager extends ResourceManager<Panel, PanelPermissions> {

    public static final QueryOptions INCLUDE_PANEL_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            PanelDBAdaptor.QueryParams.ID.key(), PanelDBAdaptor.QueryParams.UID.key(), PanelDBAdaptor.QueryParams.UUID.key(),
            PanelDBAdaptor.QueryParams.VERSION.key(), PanelDBAdaptor.QueryParams.STUDY_UID.key()));
    protected static Logger logger = LoggerFactory.getLogger(PanelManager.class);
    private UserManager userManager;
    private StudyManager studyManager;

    PanelManager(AuthorizationManager authorizationManager, AuditManager auditManager,
                 CatalogManager catalogManager, DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
    }

    @Override
    Enums.Resource getResource() {
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
        return create(studyStr, panel, options, token, StudyManager.INCLUDE_STUDY_IDS, (organizationId, study, userId, entryParam) -> {
            QueryOptions finalOptions = options != null ? new QueryOptions(options) : new QueryOptions();
            entryParam.setId(panel.getId());

            authorizationManager.checkStudyPermission(organizationId, study.getUid(), userId, StudyPermissions.Permissions.WRITE_PANELS);

            autoCompletePanel(organizationId, study, panel);
            entryParam.setUuid(panel.getUuid());

            OpenCGAResult<Panel> insert = getPanelDBAdaptor(organizationId).insert(study.getUid(), panel, finalOptions);
            if (finalOptions.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch created panel
                OpenCGAResult<Panel> result = getPanel(organizationId, study.getUid(), panel.getUuid(), finalOptions);
                insert.setResults(result.getResults());
            }
            return insert;
        });
    }

    public OpenCGAResult<Panel> importFromSource(String studyId, String source, String panelIds, String token) throws CatalogException {
        ObjectMap methodParams = new ObjectMap()
                .append("studyStr", studyId)
                .append("source", source)
                .append("panelIds", panelIds)
                .append("token", token);
        return runForMultiOperation(methodParams, Enums.Action.IMPORT, studyId, token, (organizationId, study, userId, entryParamList) -> {
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

            OpenCGAResult<Panel> result = OpenCGAResult.empty(Panel.class);
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
                        entryParamList.add(new EntryParam(panel.getId(), panel.getUuid()));
                        panelList.add(panel);
                    }
                }

                logger.info("Inserting panels in database");
                result.append(getPanelDBAdaptor(organizationId).insert(study.getUid(), panelList));
            }
            result.setResults(importedPanels);
            return result;
        });
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
        return updateMany(studyId, query, updateParams, ignoreException, options, token, StudyManager.INCLUDE_STUDY_IDS,
                (organizationId, study, userId) -> {
                    Query finalQuery = query != null ? new Query(query) : new Query();
                    finalQuery.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
                    return getPanelDBAdaptor(organizationId).iterator(study.getUid(), finalQuery, INCLUDE_PANEL_IDS, userId);
                }, (organizationId, study, userId, panel) -> update(organizationId, study, panel, updateParams, options, userId),
                "Could not update disease panel");
    }

    public OpenCGAResult<Panel> update(String studyStr, String panelId, PanelUpdateParams updateParams, QueryOptions options, String token)
            throws CatalogException {
        return update(studyStr, panelId, updateParams, options, token, StudyManager.INCLUDE_STUDY_IDS,
                (organizationId, study, userId, entryParam) -> {
                    entryParam.setId(panelId);
                    OpenCGAResult<Panel> internalResult = internalGet(organizationId, study.getUid(), panelId, QueryOptions.empty(),
                            userId);
                    if (internalResult.getNumResults() == 0) {
                        throw new CatalogException("Panel '" + panelId + "' not found");
                    }
                    Panel panel = internalResult.first();

                    // We set the proper values for the entry param object
                    entryParam.setId(panel.getId());
                    entryParam.setUuid(panel.getUuid());

                    return update(organizationId, study, panel, updateParams, options, userId);
                });
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
        return updateMany(studyStr, panelIds, updateParams, ignoreException, options, token, StudyManager.INCLUDE_STUDY_IDS,
                (organizationId, study, userId, entryParam) -> {
                    String panelId = entryParam.getId();
                    OpenCGAResult<Panel> internalResult = internalGet(organizationId, study.getUid(), panelId, QueryOptions.empty(),
                            userId);
                    if (internalResult.getNumResults() == 0) {
                        throw new CatalogException("Panel '" + panelId + "' not found");
                    }
                    Panel panel = internalResult.first();

                    // We set the proper values for the entry param object
                    entryParam.setId(panel.getId());
                    entryParam.setUuid(panel.getUuid());

                    return update(organizationId, study, panel, updateParams, options, userId);
                });
    }

    private OpenCGAResult<Panel> update(String organizationId, Study study, Panel panel, PanelUpdateParams updateParams,
                                        QueryOptions options, String userId) throws CatalogException {
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
        return iterator(studyStr, query, options, StudyManager.INCLUDE_STUDY_IDS, sessionId, (organizationId, study, userId) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            QueryOptions finalOptions = options != null ? new QueryOptions(options) : new QueryOptions();
            fixQueryObject(finalQuery);
            finalQuery.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            return getPanelDBAdaptor(organizationId).iterator(study.getUid(), finalQuery, finalOptions, userId);
        });
    }

    @Override
    public OpenCGAResult<FacetField> facet(String studyStr, Query query, String facet, String token) throws CatalogException {
        return facet(studyStr, query, facet, token, StudyManager.INCLUDE_STUDY_IDS, (organizationId, study, userId) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            fixQueryObject(finalQuery);
            finalQuery.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            return getPanelDBAdaptor(organizationId).facet(study.getUid(), finalQuery, facet, userId);
        });
    }

    @Override
    public OpenCGAResult<Panel> search(String studyId, Query query, QueryOptions options, String token) throws CatalogException {
        return search(studyId, query, options, token, StudyManager.INCLUDE_STUDY_IDS, (organizationId, study, userId) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            QueryOptions finalOptions = options != null ? new QueryOptions(options) : new QueryOptions();
            fixQueryObject(finalQuery);
            finalQuery.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            return getPanelDBAdaptor(organizationId).get(study.getUid(), finalQuery, finalOptions, userId);
        });
    }

    @Override
    public OpenCGAResult<?> distinct(String studyId, List<String> fields, Query query, String token) throws CatalogException {
        return distinct(studyId, fields, query, token, StudyManager.INCLUDE_STUDY_IDS, (organizationId, study, userId) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            fixQueryObject(finalQuery);
            finalQuery.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            return getPanelDBAdaptor(organizationId).distinct(study.getUid(), fields, finalQuery, userId);
        });
    }

    @Override
    public OpenCGAResult<Panel> count(String studyId, Query query, String token) throws CatalogException {
        return count(studyId, query, token, StudyManager.INCLUDE_STUDY_IDS, (organizationId, study, userId) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            fixQueryObject(finalQuery);
            finalQuery.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<Long> queryResultAux = getPanelDBAdaptor(organizationId).count(finalQuery, userId);
            return new OpenCGAResult<>(queryResultAux.getTime(), queryResultAux.getEvents(), 0, Collections.emptyList(),
                    queryResultAux.getNumMatches());
        });
    }

    @Override
    public OpenCGAResult<Panel> delete(String studyStr, List<String> panelIds, QueryOptions options, String token) throws CatalogException {
        return delete(studyStr, panelIds, options, false, token);
    }

    public OpenCGAResult<Panel> delete(String studyStr, List<String> panelIds, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        return deleteMany(studyStr, panelIds, params, ignoreException, token, (organizationId, study, userId, entryParam) -> {
            if (StringUtils.isEmpty(entryParam.getId())) {
                throw new CatalogException("Internal error: Missing disease panel id. This id should have been provided internally.");
            }
            String panelId = entryParam.getId();

            Query query = new Query();
            authorizationManager.buildAclCheckQuery(userId, PanelPermissions.DELETE.name(), query);
            OpenCGAResult<Panel> internalResult = internalGet(organizationId, study.getUid(), panelId, INCLUDE_PANEL_IDS, userId);
            if (internalResult.getNumResults() == 0) {
                throw new CatalogException("Disease panel '" + panelId + "' not found or user " + userId + " does not have the proper "
                        + "permissions to delete it.");
            }

            Panel panel = internalResult.first();
            // We set the proper values for the entry param object
            entryParam.setId(panel.getId());
            entryParam.setUuid(panel.getUuid());

            // Check if the panel can be deleted
            // TODO: Check if the panel is used in an interpretation. At this point, it can be deleted no matter what.

            // Delete the panel
            return getPanelDBAdaptor(organizationId).delete(panel);
        });
    }

    @Override
    public OpenCGAResult<Panel> delete(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        return delete(studyStr, query, options, false, token);
    }

    public OpenCGAResult<Panel> delete(String studyStr, Query query, QueryOptions options, boolean ignoreException, String token)
            throws CatalogException {
        return deleteMany(studyStr, query, options, ignoreException, token, (organizationId, study, userId) -> {
            Query finalQuery = query != null ? new Query(query) : new Query();
            fixQueryObject(finalQuery);
            finalQuery.append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            return getPanelDBAdaptor(organizationId).iterator(study.getUid(), finalQuery, INCLUDE_PANEL_IDS, userId);
        }, (organizationId, study, userId, panel) -> {
            QueryOptions finalOptions = options != null ? new QueryOptions(options) : new QueryOptions();
            // TODO: Check if the panel is used in an interpretation. At this point, it can be deleted no matter what.
            // Delete the panel
            return getPanelDBAdaptor(organizationId).delete(panel);
        });
    }

    @Override
    public OpenCGAResult<Panel> rank(String studyStr, Query query, String field, int numResults, boolean asc, String token)
            throws CatalogException {
        return rank(studyStr, query, field, numResults, asc, token, (organizationId, study, userId) -> {
            authorizationManager.checkStudyPermission(organizationId, study.getUid(), userId, StudyPermissions.Permissions.VIEW_PANELS);
            Query finalQuery = query != null ? new Query(query) : new Query();
            fixQueryObject(finalQuery);

            // TODO: In next release, we will have to check the count parameter from the queryOptions object.
            boolean count = true;
            finalQuery.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<Panel> queryResult = null;
            if (count) {
                // We do not need to check for permissions when we show the count of files
                queryResult = getPanelDBAdaptor(organizationId).rank(finalQuery, field, numResults, asc);
            }
            return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
        });
    }

    @Override
    public OpenCGAResult<Panel> groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String token)
            throws CatalogException {
        return groupBy(studyStr, query, fields, options, token, (organizationId, study, userId) -> {
            authorizationManager.checkStudyPermission(organizationId, study.getUid(), userId, StudyPermissions.Permissions.VIEW_PANELS);
            Query finalQuery = query != null ? new Query(query) : new Query();
            QueryOptions finalOptions = options != null ? new QueryOptions(options) : new QueryOptions();
            if (CollectionUtils.isEmpty(fields)) {
                throw new CatalogException("Empty fields parameter.");
            }
            fixQueryObject(finalQuery);
            // Add study id to the query
            finalQuery.put(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            return getPanelDBAdaptor(organizationId).groupBy(finalQuery, fields, finalOptions, userId);
        });
    }

    // **************************   ACLs  ******************************** //
    public OpenCGAResult<AclEntryList<PanelPermissions>> getAcls(String studyId, List<String> panelList, String member,
                                                                 boolean ignoreException, String token) throws CatalogException {
        return getAcls(studyId, panelList,
                StringUtils.isNotEmpty(member) ? Collections.singletonList(member) : Collections.emptyList(), ignoreException, token);
    }

    public OpenCGAResult<AclEntryList<PanelPermissions>> getAcls(String studyId, List<String> panelList, List<String> members,
                                                                 boolean ignoreException, String token) throws CatalogException {
        return getAcls(studyId, panelList, members, ignoreException, token, (organizationId, study, userId, entryParamList) -> {
            OpenCGAResult<AclEntryList<PanelPermissions>> panelAcls;
            Map<String, InternalGetDataResult<?>.Missing> missingMap = new HashMap<>();

            for (String panelId : panelList) {
                entryParamList.add(new EntryParam(panelId, null));
            }

            InternalGetDataResult<Panel> queryResult = internalGet(organizationId, study.getUid(), panelList, INCLUDE_PANEL_IDS, userId,
                    ignoreException);
            entryParamList.clear();
            for (Panel panel : queryResult.getResults()) {
                entryParamList.add(new EntryParam(panel.getId(), panel.getUuid()));
            }

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
                    resultList.add(panelAcls.getResults().get(counter));
                    counter++;
                } else {
                    resultList.add(new AclEntryList<>());
                    eventList.add(new Event(Event.Type.ERROR, panelId, missingMap.get(panelId).getErrorMsg()));
                }
            }
            for (int i = 0; i < queryResult.getResults().size(); i++) {
                panelAcls.getResults().get(i).setId(queryResult.getResults().get(i).getId());
            }
            panelAcls.setResults(resultList);
            panelAcls.setEvents(eventList);

            return panelAcls;
        });
    }

    public OpenCGAResult<AclEntryList<PanelPermissions>> updateAcl(String studyId, List<String> panelStrList, String memberList,
                                                                   AclParams aclParams, ParamUtils.AclAction action, String token)
            throws CatalogException {
        return updateAcls(studyId, panelStrList, memberList, aclParams, action, token, (organizationId, study, userId, entryParamList) -> {
            authorizationManager.checkCanAssignOrSeePermissions(organizationId, study.getUid(), userId);

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
            return queryResultList;
        });
    }

    protected void fixQueryObject(Query query) {
        super.fixQueryObject(query);
        changeQueryId(query, ParamConstants.PANEL_STATUS_PARAM, PanelDBAdaptor.QueryParams.STATUS_ID.key());
    }

}
