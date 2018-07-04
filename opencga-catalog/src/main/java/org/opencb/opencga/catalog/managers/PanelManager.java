package org.opencb.opencga.catalog.managers;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.WriteResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.PanelDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.Panel;
import org.opencb.opencga.core.models.Status;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.core.models.acls.permissions.DiseasePanelAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public class PanelManager extends ResourceManager<Panel> {

    protected static Logger logger = LoggerFactory.getLogger(PanelManager.class);
    private UserManager userManager;
    private StudyManager studyManager;

    PanelManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
               DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
               Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
    }

    @Override
    Panel smartResolutor(long studyUid, String entry, String user) throws CatalogException {
        Query query = new Query(PanelDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

        if (UUIDUtils.isOpenCGAUUID(entry)) {
            query.put(PanelDBAdaptor.QueryParams.UUID.key(), entry);
        } else {
            query.put(PanelDBAdaptor.QueryParams.ID.key(), entry);
        }
        QueryResult<Panel> panelQueryResult = panelDBAdaptor.get(query, QueryOptions.empty(), user);
        if (panelQueryResult.getNumResults() == 0) {
            panelQueryResult = panelDBAdaptor.get(query, QueryOptions.empty());
            if (panelQueryResult.getNumResults() == 0) {
                throw new CatalogException("Panel " + entry + " not found");
            } else {
                throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see the panel " + entry);
            }
        } else if (panelQueryResult.getNumResults() > 1) {
            throw new CatalogException("More than one panel found based on " + entry);
        } else {
            return panelQueryResult.first();
        }
    }

    @Override
    public QueryResult<Panel> create(String studyStr, Panel panel, QueryOptions options, String sessionId) throws CatalogException {
        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        // 1. We check everything can be done
        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_PANELS);

        // Check all the panel fields
        ParamUtils.checkAlias(panel.getId(), "id");
        panel.setName(ParamUtils.defaultString(panel.getName(), panel.getId()));
        panel.setRelease(studyManager.getCurrentRelease(study, userId));
        panel.setVersion(1);
        panel.setAuthor(ParamUtils.defaultString(panel.getAuthor(), ""));
        panel.setCreationDate(TimeUtils.getTime());
        panel.setStatus(new Status());
        panel.setDescription(ParamUtils.defaultString(panel.getDescription(), ""));
        panel.setPhenotypes(ParamUtils.defaultObject(panel.getPhenotypes(), Collections.emptyList()));
        panel.setVariants(ParamUtils.defaultObject(panel.getVariants(), Collections.emptyList()));
        panel.setRegions(ParamUtils.defaultObject(panel.getRegions(), Collections.emptyList()));
        panel.setGenes(ParamUtils.defaultObject(panel.getGenes(), Collections.emptyList()));
        panel.setAttributes(ParamUtils.defaultObject(panel.getAttributes(), Collections.emptyMap()));
        panel.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.PANEL));

        options = ParamUtils.defaultObject(options, QueryOptions::new);

        return panelDBAdaptor.insert(study.getUid(), panel, options);
    }

    @Override
    public QueryResult<Panel> update(String studyStr, String panelId, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(parameters, "parameters");
        parameters = new ObjectMap(parameters);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        MyResource<Panel> resource = getUid(panelId, studyStr, sessionId);

        // Check update permissions
        authorizationManager.checkDiseasePanelPermission(resource.getStudy().getUid(), resource.getResource().getUid(), resource.getUser(),
                DiseasePanelAclEntry.DiseasePanelPermissions.UPDATE);

        try {
            ParamUtils.checkAllParametersExist(parameters.keySet().iterator(), (a) -> PanelDBAdaptor.UpdateParams.getParam(a) != null);
        } catch (CatalogParameterException e) {
            throw new CatalogException("Could not update: " + e.getMessage(), e);
        }
        if (parameters.containsKey(PanelDBAdaptor.UpdateParams.ID.key())) {
            ParamUtils.checkAlias(parameters.getString(PanelDBAdaptor.UpdateParams.ID.key()), PanelDBAdaptor.UpdateParams.ID.key());
        }

        if (options.getBoolean(Constants.INCREMENT_VERSION)) {
            // We do need to get the current release to properly create a new version
            options.put(Constants.CURRENT_RELEASE, studyManager.getCurrentRelease(resource.getStudy(), resource.getUser()));
        }

        QueryResult<Panel> queryResult = panelDBAdaptor.update(resource.getResource().getUid(), parameters, options);
        auditManager.recordUpdate(AuditRecord.Resource.panel, resource.getResource().getUid(), resource.getUser(), parameters, null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Panel> get(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        String userId = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, userId);

        QueryResult<Panel> panelQueryResult = search(studyStr, query, options, sessionId);

        if (panelQueryResult.getNumResults() == 0 && query.containsKey(PanelDBAdaptor.QueryParams.UID.key())) {
            List<Long> panelIds = query.getAsLongList(PanelDBAdaptor.QueryParams.UID.key());
            for (Long panelId : panelIds) {
                authorizationManager.checkDiseasePanelPermission(study.getUid(), panelId, userId,
                        DiseasePanelAclEntry.DiseasePanelPermissions.VIEW);
            }
        }

        return panelQueryResult;
    }

    @Override
    public DBIterator<Panel> iterator(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        query.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        return panelDBAdaptor.iterator(query, options, userId);
    }

    @Override
    public QueryResult<Panel> search(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, userId);

        query.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return panelDBAdaptor.get(query, options, userId);
    }

    @Override
    public QueryResult<Panel> count(String studyStr, Query query, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        query.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        QueryResult<Long> queryResultAux = panelDBAdaptor.count(query, userId, StudyAclEntry.StudyPermissions.VIEW_PANELS);
        return new QueryResult<>("count", queryResultAux.getDbTime(), 0, queryResultAux.first(), queryResultAux.getWarningMsg(),
                queryResultAux.getErrorMsg(), Collections.emptyList());
    }

    @Override
    public WriteResult delete(String studyStr, Query query, ObjectMap params, String sessionId) {
        throw new UnsupportedOperationException("not implemented yet");
    }

    @Override
    public QueryResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.VIEW_PANELS);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        query.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = panelDBAdaptor.rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    @Override
    public QueryResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        if (fields == null || fields.size() == 0) {
            throw new CatalogException("Empty fields parameter.");
        }

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        // Add study id to the query
        query.put(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        QueryResult queryResult = sampleDBAdaptor.groupBy(query, fields, options, userId);
        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

}
