package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.core.Xref;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.commons.datastore.core.result.WriteResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.db.api.PanelDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.Entity;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.Panel;
import org.opencb.opencga.core.models.Status;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.core.models.acls.permissions.PanelAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opencb.biodata.models.clinical.interpretation.DiseasePanel.*;
import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;

public class PanelManager extends ResourceManager<Panel> {

    protected static Logger logger = LoggerFactory.getLogger(PanelManager.class);
    private UserManager userManager;
    private StudyManager studyManager;

    // Reserved word to query over installation panels instead of the ones belonging to a study.
    public static final String INSTALLATION_PANELS = "__INSTALLATION__";

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

    private Panel getInstallationPanel(String entry) throws CatalogException {
        Query query = new Query(PanelDBAdaptor.QueryParams.STUDY_UID.key(), -1);

        if (UUIDUtils.isOpenCGAUUID(entry)) {
            query.put(PanelDBAdaptor.QueryParams.UUID.key(), entry);
        } else {
            query.put(PanelDBAdaptor.QueryParams.ID.key(), entry);
        }

        QueryResult<Panel> panelQueryResult = panelDBAdaptor.get(query, QueryOptions.empty());
        if (panelQueryResult.getNumResults() == 0) {
            throw new CatalogException("Panel " + entry + " not found");
        } else if (panelQueryResult.getNumResults() > 1) {
            throw new CatalogException("More than one panel found based on " + entry);
        } else {
            return panelQueryResult.first();
        }
    }

    @Override
    public QueryResult<Panel> create(String studyStr, Panel panel, QueryOptions options, String sessionId)
            throws CatalogException {
        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        // 1. We check everything can be done
        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_PANELS);

        // Check all the panel fields
        ParamUtils.checkAlias(panel.getDiseasePanel().getId(), "id");
        panel.getDiseasePanel().setName(ParamUtils.defaultString(panel.getDiseasePanel().getName(), panel.getDiseasePanel().getId()));
        panel.setRelease(studyManager.getCurrentRelease(study, userId));
        panel.setVersion(1);
        panel.setAuthor(ParamUtils.defaultString(panel.getAuthor(), ""));
        panel.getDiseasePanel().setCreationDate(TimeUtils.getTime());
        panel.getDiseasePanel().setModificationDate(TimeUtils.getTime());
        panel.setStatus(new Status());
        panel.getDiseasePanel().setCategories(ParamUtils.defaultObject(panel.getDiseasePanel().getCategories(), Collections.emptyList()));
        panel.getDiseasePanel().setTags(ParamUtils.defaultObject(panel.getDiseasePanel().getTags(), Collections.emptyList()));
        panel.getDiseasePanel().setDescription(ParamUtils.defaultString(panel.getDiseasePanel().getDescription(), ""));
        panel.getDiseasePanel().setPhenotypes(ParamUtils.defaultObject(panel.getDiseasePanel().getPhenotypes(), Collections.emptyList()));
        panel.getDiseasePanel().setVariants(ParamUtils.defaultObject(panel.getDiseasePanel().getVariants(), Collections.emptyList()));
        panel.getDiseasePanel().setRegions(ParamUtils.defaultObject(panel.getDiseasePanel().getRegions(), Collections.emptyList()));
        panel.getDiseasePanel().setGenes(ParamUtils.defaultObject(panel.getDiseasePanel().getGenes(), Collections.emptyList()));
        panel.getDiseasePanel().setAttributes(ParamUtils.defaultObject(panel.getDiseasePanel().getAttributes(), Collections.emptyMap()));
        panel.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.PANEL));

        fillDefaultStats(panel);

        options = ParamUtils.defaultObject(options, QueryOptions::new);

        return panelDBAdaptor.insert(study.getUid(), panel, options);
    }

    public QueryResult<Panel> importAllGlobalPanels(String studyStr, QueryOptions options, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        // 1. We check everything can be done
        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_PANELS);

        DBIterator<Panel> iterator = iterator(INSTALLATION_PANELS, new Query(), QueryOptions.empty(), token);

        List<Panel> importedPanels = new ArrayList<>();
        int dbTime = 0;
        while (iterator.hasNext()) {
            Panel globalPanel = iterator.next();
            QueryResult<Panel> panelQueryResult = importGlobalPanel(study, globalPanel, options, userId);
            importedPanels.add(panelQueryResult.first());
            dbTime += panelQueryResult.getDbTime();
        }
        iterator.close();

        return new QueryResult<>("Import panel", dbTime, importedPanels.size(), importedPanels.size(), "", "", importedPanels);
    }

    public QueryResult<Panel> importGlobalPanels(String studyStr, List<String> panelIds, QueryOptions options, String token)
            throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        // 1. We check everything can be done
        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_PANELS);

        List<Panel> importedPanels = new ArrayList<>(panelIds.size());
        int dbTime = 0;
        for (String panelId : panelIds) {
            // Fetch the installation Panel (if it exists)
            Panel globalPanel = getInstallationPanel(panelId);
            QueryResult<Panel> panelQueryResult = importGlobalPanel(study, globalPanel, options, userId);
            importedPanels.add(panelQueryResult.first());
            dbTime += panelQueryResult.getDbTime();
        }

        return new QueryResult<>("Import panel", dbTime, importedPanels.size(), importedPanels.size(), "", "", importedPanels);
    }

    private QueryResult<Panel> importGlobalPanel(Study study, Panel diseasePanel, QueryOptions options, String userId)
            throws CatalogException {
        diseasePanel.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.PANEL));
        diseasePanel.getDiseasePanel().setCreationDate(TimeUtils.getTime());
        diseasePanel.setRelease(studyManager.getCurrentRelease(study, userId));
        diseasePanel.setVersion(1);

        // Install the current diseasePanel
        return panelDBAdaptor.insert(study.getUid(), diseasePanel, options);
    }

    public void importPanelApp(String token, boolean overwrite) throws CatalogException {
        // We check it is the admin of the installation
        authorizationManager.checkIsAdmin(userManager.getUserId(token));

        Client client = ClientBuilder.newClient();
        int i = 1;
        int max = Integer.MAX_VALUE;

        while (true) {
            if (i == max) {
                // no more pages to fetch
                return;
            }

            WebTarget resource = client.target("https://panelapp.genomicsengland.co.uk/api/v1/panels/?format=json&page=" + i);
            String jsonString = resource.request().get().readEntity(String.class);

            Map<String, Object> panels;
            try {
                panels = JacksonUtils.getDefaultObjectMapper().readValue(jsonString, Map.class);
            } catch (IOException e) {
                logger.error("{}", e.getMessage(), e);
                return;
            }

            if (i == 1) {
                // We set the maximum number of pages we will need to fetch
                max = Double.valueOf(Math.ceil(Integer.parseInt(String.valueOf(panels.get("count"))) / 100.0)).intValue() + 1;
            }

            for (Map<String, Object> panel : (List<Map>) panels.get("results")) {

                if (String.valueOf(panel.get("version")).startsWith("0")) {
                    logger.info("Panel is not ready for interpretation: '{}', version: '{}'", panel.get("name"),
                            panel.get("version"));
                } else {
                    logger.info("Processing {} {} ...", panel.get("name"), panel.get("id"));

                    resource = client.target("https://panelapp.genomicsengland.co.uk/api/v1/panels/" + panel.get("id") + "?format=json");
                    jsonString = resource.request().get().readEntity(String.class);

                    Map<String, Object> panelInfo;
                    try {
                        panelInfo = JacksonUtils.getDefaultObjectMapper().readValue(jsonString, Map.class);
                    } catch (IOException e) {
                        logger.error("{}", e.getMessage(), e);
                        continue;
                    }

                    List<PanelCategory> categories = new ArrayList<>(2);
                    categories.add(new PanelCategory(String.valueOf(panelInfo.get("disease_group")), 1));
                    categories.add(new PanelCategory(String.valueOf(panelInfo.get("disease_sub_group")), 2));

                    List<Phenotype> phenotypes = new ArrayList<>();
                    for (String relevantDisorder : (List<String>) panelInfo.get("relevant_disorders")) {
                        if (StringUtils.isNotEmpty(relevantDisorder)) {
                            phenotypes.add(new Phenotype("", relevantDisorder, ""));
                        }
                    }

                    List<GenePanel> genes = new ArrayList<>();
                    for (Map<String, Object> gene : (List<Map>) panelInfo.get("genes")) {
                        String ensemblGeneId = "";
                        List<Xref> xrefs = new ArrayList<>();
                        List<String> publications = new ArrayList<>();

                        Map<String, Object> geneData = (Map) gene.get("gene_data");

                        // read the first Ensembl gene ID if exists
                        Map<String, Object> ensemblGenes = (Map) geneData.get("ensembl_genes");
                        if (ensemblGenes.containsKey("GRch37")) {
                            ensemblGeneId = String.valueOf(((Map) ((Map) ensemblGenes.get("GRch37")).get("82")).get("ensembl_id"));
                        } else if (ensemblGenes.containsKey("GRch38")) {
                            ensemblGeneId = String.valueOf(((Map) ((Map) ensemblGenes.get("GRch38")).get("90")).get("ensembl_id"));
                        }

                        // read OMIM ID
                        if (geneData.containsKey("omim_gene") && geneData.get("omim_gene") != null) {
                            for (String omim : (List<String>) geneData.get("omim_gene")) {
                                xrefs.add(new Xref(omim, "OMIM", "OMIM"));
                            }
                        }
                        xrefs.add(new Xref(String.valueOf(geneData.get("gene_name")), "GeneName", "GeneName"));

                        // read publications
                        if (gene.containsKey("publications")) {
                            publications = (List<String>) gene.get("publications");
                        }

                        // add gene panel
                        genes.add(new GenePanel(ensemblGeneId, String.valueOf(geneData.get("hgnc_symbol")), xrefs,
                                String.valueOf(gene.get("mode_of_inheritance")), null, String.valueOf(gene.get("confidence_level")),
                                (List<String>) gene.get("evidence"), publications));
                    }

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("PanelAppInfo", panel);

                    Panel diseasePanel = new Panel();
                    diseasePanel.getDiseasePanel().setId(String.valueOf(panelInfo.get("name"))
                            .replace(" - ", "-")
                            .replace("/", "-")
                            .replace(" (", "-")
                            .replace("(", "-")
                            .replace(") ", "-")
                            .replace(")", "")
                            .replace(" & ", "_and_")
                            .replace(", ", "-")
                            .replace(" ", "_") + "-PanelAppId-" + panelInfo.get("id"));
                    diseasePanel.getDiseasePanel().setName(String.valueOf(panelInfo.get("name")));
                    diseasePanel.getDiseasePanel().setCategories(categories);
                    diseasePanel.getDiseasePanel().setPhenotypes(phenotypes);
                    diseasePanel.getDiseasePanel().setGenes(genes);
                    diseasePanel.getDiseasePanel().setSource(new SourcePanel()
                            .setId(String.valueOf(panelInfo.get("id")))
                            .setName(String.valueOf(panelInfo.get("name")))
                            .setVersion(String.valueOf(panelInfo.get("version")))
                            .setProject("PanelApp (GEL)")
                    );
                    diseasePanel.getDiseasePanel().setDescription(panelInfo.get("disease_sub_group")
                            + " (" + panelInfo.get("disease_group") + ")");
                    diseasePanel.getDiseasePanel().setAttributes(attributes);

                    if ("Cancer Programme".equals(String.valueOf(panelInfo.get("disease_group")))) {
                        diseasePanel.getDiseasePanel().setTags(Collections.singletonList("cancer"));
                    }

                    create(diseasePanel, overwrite, token);
                }
            }

            i++;
        }
    }

    @Override
    public QueryResult<Panel> update(String studyStr, String panelId, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(parameters, "parameters");
        parameters = new ObjectMap(parameters);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        MyResource<Panel> resource = getUid(panelId, studyStr, sessionId);

        // Check update permissions
        authorizationManager.checkPanelPermission(resource.getStudy().getUid(), resource.getResource().getUid(), resource.getUser(),
                PanelAclEntry.PanelPermissions.UPDATE);

        try {
            ParamUtils.checkAllParametersExist(parameters.keySet().iterator(),
                    (a) -> PanelDBAdaptor.UpdateParams.getParam(a) != null);
        } catch (CatalogParameterException e) {
            throw new CatalogException("Could not update: " + e.getMessage(), e);
        }
        if (parameters.containsKey(PanelDBAdaptor.UpdateParams.ID.key())) {
            ParamUtils.checkAlias(parameters.getString(PanelDBAdaptor.UpdateParams.ID.key()),
                    PanelDBAdaptor.UpdateParams.ID.key());
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
    public QueryResult<Panel> get(String studyStr, String entryStr, QueryOptions options, String sessionId) throws CatalogException {
        if (StringUtils.isNotEmpty(studyStr) && INSTALLATION_PANELS.equals(studyStr)) {
            Panel installationPanel = getInstallationPanel(entryStr);
            return new QueryResult<>(entryStr, -1, 1, 1, "", "", Collections.singletonList(installationPanel));
        } else {
            return super.get(studyStr, entryStr, options, sessionId);
        }
    }

    @Override
    public QueryResult<Panel> get(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        String userId = userManager.getUserId(sessionId);

        long studyUid;
        if (studyStr.equals(INSTALLATION_PANELS)) {
            studyUid = -1;
        } else {
            studyUid = catalogManager.getStudyManager().resolveId(studyStr, userId).getUid();
        }

        QueryResult<Panel> panelQueryResult = search(studyStr, query, options, sessionId);

        if (panelQueryResult.getNumResults() == 0 && query.containsKey(PanelDBAdaptor.QueryParams.UID.key())) {
            List<Long> panelIds = query.getAsLongList(PanelDBAdaptor.QueryParams.UID.key());
            for (Long panelId : panelIds) {
                authorizationManager.checkPanelPermission(studyUid, panelId, userId,
                        PanelAclEntry.PanelPermissions.VIEW);
            }
        }

        return panelQueryResult;
    }

    @Override
    public DBIterator<Panel> iterator(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);

        if (StringUtils.isNotEmpty(studyStr) && INSTALLATION_PANELS.equals(studyStr)) {
            query.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), -1);
            // We don't need to check any further permissions. Any user can see installation panels
            return panelDBAdaptor.iterator(query, options);
        } else {
            Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);
            query.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            return panelDBAdaptor.iterator(query, options, userId);
        }
    }

    @Override
    public QueryResult<Panel> search(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        if (INSTALLATION_PANELS.equals(studyStr)) {
            query.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), -1);

            // Here view permissions won't be checked
            return panelDBAdaptor.get(query, options);
        } else {
            String userId = userManager.getUserId(sessionId);
            long studyUid = catalogManager.getStudyManager().resolveId(studyStr, userId).getUid();
            query.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

            // Here permissions will be checked
            return panelDBAdaptor.get(query, options, userId);
        }
    }

    @Override
    public QueryResult<Panel> count(String studyStr, Query query, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        QueryResult<Long> queryResultAux;
        if (studyStr.equals(INSTALLATION_PANELS)) {
            query.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), -1);

            // Here view permissions won't be checked
            queryResultAux = panelDBAdaptor.count(query);
        } else {
            String userId = userManager.getUserId(sessionId);
            long studyUid = catalogManager.getStudyManager().resolveId(studyStr, userId).getUid();
            query.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

            // Here view permissions will be checked
            queryResultAux = panelDBAdaptor.count(query, userId, StudyAclEntry.StudyPermissions.VIEW_PANELS);
        }

        return new QueryResult<>("count", queryResultAux.getDbTime(), 0, queryResultAux.first(), queryResultAux.getWarningMsg(),
                queryResultAux.getErrorMsg(), Collections.emptyList());
    }

    @Override
    public WriteResult delete(String studyStr, Query query, ObjectMap params, String sessionId) {
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));
        WriteResult writeResult = new WriteResult("delete", -1, -1, -1, null, null, null);

        String userId;
        Study study;

        StopWatch watch = StopWatch.createStarted();

        // If the user is the owner or the admin, we won't check if he has permissions for every single entry
        boolean checkPermissions;

        // We try to get an iterator containing all the families to be deleted
        DBIterator<Panel> iterator;
        try {
            userId = catalogManager.getUserManager().getUserId(sessionId);
            study = studyManager.resolveId(studyStr, userId);

            finalQuery.append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = panelDBAdaptor.iterator(finalQuery, QueryOptions.empty(), userId);

            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            checkPermissions = !authorizationManager.checkIsOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            logger.error("Delete panel: {}", e.getMessage(), e);
            writeResult.setError(new Error(-1, null, e.getMessage()));
            writeResult.setDbTime((int) watch.getTime(TimeUnit.MILLISECONDS));
            return writeResult;
        }

        long numMatches = 0;
        long numModified = 0;
        List<WriteResult.Fail> failedList = new ArrayList<>();

        String suffixName = INTERNAL_DELIMITER + "DELETED_" + TimeUtils.getTime();

        while (iterator.hasNext()) {
            Panel panel = iterator.next();
            numMatches += 1;

            try {
                if (checkPermissions) {
                    authorizationManager.checkPanelPermission(study.getUid(), panel.getUid(), userId,
                            PanelAclEntry.PanelPermissions.DELETE);
                }

                // Check if the panel can be deleted
                // TODO: Check if the panel is used in an interpretation. At this point, it can be deleted no matter what.

                // Delete the panel
                Query updateQuery = new Query()
                        .append(PanelDBAdaptor.QueryParams.UID.key(), panel.getUid())
                        .append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                        .append(Constants.ALL_VERSIONS, true);
                ObjectMap updateParams = new ObjectMap()
                        .append(PanelDBAdaptor.QueryParams.STATUS_NAME.key(), Status.DELETED)
                        .append(PanelDBAdaptor.QueryParams.ID.key(), panel.getDiseasePanel().getName() + suffixName);
                QueryResult<Long> update = panelDBAdaptor.update(updateQuery, updateParams, QueryOptions.empty());
                if (update.first() > 0) {
                    numModified += 1;
                    auditManager.recordDeletion(AuditRecord.Resource.panel, panel.getUid(), userId, null, updateParams, null, null);
                } else {
                    failedList.add(new WriteResult.Fail(panel.getDiseasePanel().getId(), "Unknown reason"));
                }
            } catch (Exception e) {
                failedList.add(new WriteResult.Fail(panel.getDiseasePanel().getId(), e.getMessage()));
                logger.debug("Cannot delete panel {}: {}", panel.getDiseasePanel().getId(), e.getMessage(), e);
            }
        }

        writeResult.setDbTime((int) watch.getTime(TimeUnit.MILLISECONDS));
        writeResult.setNumMatches(numMatches);
        writeResult.setNumModified(numModified);
        writeResult.setFailed(failedList);

        if (!failedList.isEmpty()) {
            writeResult.setWarning(Collections.singletonList(new Error(-1, null, "There are panels that could not be deleted")));
        }

        return writeResult;
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

    // **************************   ACLs  ******************************** //
    public List<QueryResult<PanelAclEntry>> getAcls(String studyStr, List<String> panelList, String member, boolean silent,
                                                    String sessionId) throws CatalogException {
        List<QueryResult<PanelAclEntry>> panelAclList = new ArrayList<>(panelList.size());

        for (String panel : panelList) {
            try {
                MyResource<Panel> resource = getUid(panel, studyStr, sessionId);

                QueryResult<PanelAclEntry> allPanelAcls;
                if (StringUtils.isNotEmpty(member)) {
                    allPanelAcls =
                            authorizationManager.getPanelAcl(resource.getStudy().getUid(), resource.getResource().getUid(),
                                    resource.getUser(), member);
                } else {
                    allPanelAcls = authorizationManager.getAllPanelAcls(resource.getStudy().getUid(), resource.getResource().getUid(),
                            resource.getUser());
                }
                allPanelAcls.setId(panel);
                panelAclList.add(allPanelAcls);
            } catch (CatalogException e) {
                if (silent) {
                    panelAclList.add(new QueryResult<>(panel, 0, 0, 0, "", e.toString(), new ArrayList<>(0)));
                } else {
                    throw e;
                }
            }
        }
        return panelAclList;
    }

    public List<QueryResult<PanelAclEntry>> updateAcl(String studyStr, List<String> panelList, String memberIds,
                                                      AclParams panelAclParams, String sessionId) throws CatalogException {
        if (panelList == null || panelList.isEmpty()) {
            throw new CatalogException("Update ACL: Missing panel parameter");
        }

        if (panelAclParams.getAction() == null) {
            throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
        }

        List<String> permissions = Collections.emptyList();
        if (StringUtils.isNotEmpty(panelAclParams.getPermissions())) {
            permissions = Arrays.asList(panelAclParams.getPermissions().trim().replaceAll("\\s", "").split(","));
            checkPermissions(permissions, PanelAclEntry.PanelPermissions::valueOf);
        }

        MyResources<Panel> resource = getUids(panelList, studyStr, sessionId);
        authorizationManager.checkCanAssignOrSeePermissions(resource.getStudy().getUid(), resource.getUser());

        // Validate that the members are actually valid members
        List<String> members;
        if (memberIds != null && !memberIds.isEmpty()) {
            members = Arrays.asList(memberIds.split(","));
        } else {
            members = Collections.emptyList();
        }
        authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
        checkMembers(resource.getStudy().getUid(), members);

        switch (panelAclParams.getAction()) {
            case SET:
                List<String> allPanelPermissions = EnumSet.allOf(PanelAclEntry.PanelPermissions.class)
                        .stream()
                        .map(String::valueOf)
                        .collect(Collectors.toList());
                return authorizationManager.setAcls(resource.getStudy().getUid(), resource.getResourceList().stream()
                                .map(Panel::getUid)
                                .collect(Collectors.toList()), members, permissions,
                        allPanelPermissions, Entity.PANEL);
            case ADD:
                return authorizationManager.addAcls(resource.getStudy().getUid(), resource.getResourceList().stream()
                        .map(Panel::getUid)
                        .collect(Collectors.toList()), members, permissions, Entity.PANEL);
            case REMOVE:
                return authorizationManager.removeAcls(resource.getResourceList().stream().map(Panel::getUid)
                        .collect(Collectors.toList()), members, permissions, Entity.PANEL);
            case RESET:
                return authorizationManager.removeAcls(resource.getResourceList().stream().map(Panel::getUid)
                        .collect(Collectors.toList()), members, null, Entity.PANEL);
            default:
                throw new CatalogException("Unexpected error occurred. No valid action found.");
        }
    }


    /* ********************************   Admin methods  ***********************************************/
    /**
     * Create a new installation panel. This method can only be run by the main OpenCGA administrator.
     *
     * @param panel Panel.
     * @param overwrite Flag indicating to overwrite an already existing panel in case of an ID conflict.
     * @param token token.
     * @throws CatalogException In case of an ID conflict or an unauthorized action.
     */
    public void create(Panel panel, boolean overwrite, String token) throws CatalogException {
        String userId = userManager.getUserId(token);

        if (!authorizationManager.checkIsAdmin(userId)) {
            throw new CatalogAuthorizationException("Only the main OpenCGA administrator can import global panels");
        }

        // Check all the panel fields
        ParamUtils.checkAlias(panel.getDiseasePanel().getId(), "id");
        panel.getDiseasePanel().setName(ParamUtils.defaultString(panel.getDiseasePanel().getName(), panel.getDiseasePanel().getId()));
        panel.setRelease(-1);
        panel.setVersion(1);
        panel.setAuthor(ParamUtils.defaultString(panel.getAuthor(), ""));
        panel.getDiseasePanel().setCreationDate(TimeUtils.getTime());
        panel.getDiseasePanel().setModificationDate(TimeUtils.getTime());
        panel.setStatus(new Status());
        panel.getDiseasePanel().setCategories(ParamUtils.defaultObject(panel.getDiseasePanel().getCategories(), Collections.emptyList()));
        panel.getDiseasePanel().setTags(ParamUtils.defaultObject(panel.getDiseasePanel().getTags(), Collections.emptyList()));
        panel.getDiseasePanel().setDescription(ParamUtils.defaultString(panel.getDiseasePanel().getDescription(), ""));
        panel.getDiseasePanel().setPhenotypes(ParamUtils.defaultObject(panel.getDiseasePanel().getPhenotypes(), Collections.emptyList()));
        panel.getDiseasePanel().setVariants(ParamUtils.defaultObject(panel.getDiseasePanel().getVariants(), Collections.emptyList()));
        panel.getDiseasePanel().setRegions(ParamUtils.defaultObject(panel.getDiseasePanel().getRegions(), Collections.emptyList()));
        panel.getDiseasePanel().setGenes(ParamUtils.defaultObject(panel.getDiseasePanel().getGenes(), Collections.emptyList()));
        panel.getDiseasePanel().setAttributes(ParamUtils.defaultObject(panel.getDiseasePanel().getAttributes(), Collections.emptyMap()));
        panel.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.PANEL));

        fillDefaultStats(panel);

        panelDBAdaptor.insert(panel, overwrite);
    }

    void fillDefaultStats(Panel panel) {
        if (panel.getDiseasePanel().getStats() == null || panel.getDiseasePanel().getStats().isEmpty()) {
            Map<String, Integer> stats = new HashMap<>();
            stats.put("numberOfVariants", panel.getDiseasePanel().getVariants().size());
            stats.put("numberOfGenes", panel.getDiseasePanel().getGenes().size());
            stats.put("numberOfRegions", panel.getDiseasePanel().getRegions().size());

            panel.getDiseasePanel().setStats(stats);
        }
    }

    public void delete(String panelId, String token) throws CatalogException {
        String userId = userManager.getUserId(token);

        if (!authorizationManager.checkIsAdmin(userId)) {
            throw new CatalogAuthorizationException("Only the main OpenCGA administrator can delete global panels");
        }

        Panel panel = getInstallationPanel(panelId);
        panelDBAdaptor.delete(panel.getUid());
    }

}
