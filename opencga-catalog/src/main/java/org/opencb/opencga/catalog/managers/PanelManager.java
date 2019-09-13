package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.commons.OntologyTerm;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.core.Xref;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.WriteResult;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.db.api.PanelDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.models.InternalGetQueryResult;
import org.opencb.opencga.catalog.models.update.PanelUpdateParams;
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
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.biodata.models.clinical.interpretation.DiseasePanel.*;
import static org.opencb.opencga.catalog.audit.AuditRecord.ERROR;
import static org.opencb.opencga.catalog.audit.AuditRecord.SUCCESS;
import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;

public class PanelManager extends ResourceManager<Panel> {

    protected static Logger logger = LoggerFactory.getLogger(PanelManager.class);
    private UserManager userManager;
    private StudyManager studyManager;

    // Reserved word to query over installation panels instead of the ones belonging to a study.
    public static final String INSTALLATION_PANELS = "__INSTALLATION__";

    public static final QueryOptions INCLUDE_PANEL_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            PanelDBAdaptor.QueryParams.ID.key(), PanelDBAdaptor.QueryParams.UID.key(), PanelDBAdaptor.QueryParams.UUID.key(),
            PanelDBAdaptor.QueryParams.VERSION.key()));

    PanelManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                 DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                 Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
    }

    @Override
    QueryResult<Panel> internalGet(long studyUid, String entry, @Nullable Query query, QueryOptions options, String user)
            throws CatalogException {
        ParamUtils.checkIsSingleID(entry);
        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.put(PanelDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();

        if (UUIDUtils.isOpenCGAUUID(entry)) {
            queryCopy.put(PanelDBAdaptor.QueryParams.UUID.key(), entry);
        } else {
            queryCopy.put(PanelDBAdaptor.QueryParams.ID.key(), entry);
        }
        QueryResult<Panel> panelQueryResult = panelDBAdaptor.get(queryCopy, queryOptions, user);
        if (panelQueryResult.getNumResults() == 0) {
            panelQueryResult = panelDBAdaptor.get(queryCopy, queryOptions);
            if (panelQueryResult.getNumResults() == 0) {
                throw new CatalogException("Panel " + entry + " not found");
            } else {
                throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see the panel " + entry);
            }
        } else if (panelQueryResult.getNumResults() > 1 && !queryCopy.getBoolean(Constants.ALL_VERSIONS)) {
            throw new CatalogException("More than one panel found based on " + entry);
        } else {
            return panelQueryResult;
        }
    }

    @Override
    InternalGetQueryResult<Panel> internalGet(long studyUid, List<String> entryList, @Nullable Query query, QueryOptions options,
                                              String user, boolean silent) throws CatalogException {
        if (ListUtils.isEmpty(entryList)) {
            throw new CatalogException("Missing panel entries.");
        }
        List<String> uniqueList = ListUtils.unique(entryList);

        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.put(PanelDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

        Function<Panel, String> panelStringFunction = Panel::getId;
        PanelDBAdaptor.QueryParams idQueryParam = null;
        for (String entry : uniqueList) {
            PanelDBAdaptor.QueryParams param = PanelDBAdaptor.QueryParams.ID;
            if (UUIDUtils.isOpenCGAUUID(entry)) {
                param = PanelDBAdaptor.QueryParams.UUID;
                panelStringFunction = Panel::getUuid;
            }
            if (idQueryParam == null) {
                idQueryParam = param;
            }
            if (idQueryParam != param) {
                throw new CatalogException("Found uuids and ids in the same query. Please, choose one or do two different queries.");
            }
        }
        queryCopy.put(idQueryParam.key(), uniqueList);

        // Ensure the field by which we are querying for will be kept in the results
        queryOptions = keepFieldInQueryOptions(queryOptions, idQueryParam.key());

        QueryResult<Panel> panelQueryResult = panelDBAdaptor.get(queryCopy, queryOptions, user);
        if (silent || panelQueryResult.getNumResults() >= uniqueList.size()) {
            return keepOriginalOrder(uniqueList, panelStringFunction, panelQueryResult, silent,
                    queryCopy.getBoolean(Constants.ALL_VERSIONS));
        }
        // Query without adding the user check
        QueryResult<Panel> resultsNoCheck = panelDBAdaptor.get(queryCopy, queryOptions);

        if (resultsNoCheck.getNumResults() == panelQueryResult.getNumResults()) {
            throw CatalogException.notFound("panels", getMissingFields(uniqueList, panelQueryResult.getResult(), panelStringFunction));
        } else {
            throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the panels.");
        }
    }

    private QueryResult<Panel> getPanel(long studyUid, String panelUuid, QueryOptions options) throws CatalogDBException {
        Query query = new Query()
                .append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(PanelDBAdaptor.QueryParams.UUID.key(), panelUuid);
        return panelDBAdaptor.get(query, options);
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
    public QueryResult<Panel> create(String studyStr, Panel panel, QueryOptions options, String token) throws CatalogException {
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
            ParamUtils.checkAlias(panel.getId(), "id");
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
            panel.setPhenotypes(ParamUtils.defaultObject(panel.getPhenotypes(), Collections.emptyList()));
            panel.setVariants(ParamUtils.defaultObject(panel.getVariants(), Collections.emptyList()));
            panel.setRegions(ParamUtils.defaultObject(panel.getRegions(), Collections.emptyList()));
            panel.setGenes(ParamUtils.defaultObject(panel.getGenes(), Collections.emptyList()));
            panel.setAttributes(ParamUtils.defaultObject(panel.getAttributes(), Collections.emptyMap()));
            panel.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.PANEL));

            fillDefaultStats(panel);

            options = ParamUtils.defaultObject(options, QueryOptions::new);

            panelDBAdaptor.insert(study.getUid(), panel, options);
            auditManager.auditCreate(userId, panel.getId(), panel.getUuid(), study.getId(), study.getUuid(), auditParams,
                    AuditRecord.Entity.PANEL, ERROR);

            return getPanel(study.getUid(), panel.getUuid(), options);
        } catch (CatalogException e) {
            auditManager.auditCreate(userId, panel.getId(), "", study.getId(), study.getUuid(), auditParams, AuditRecord.Entity.PANEL,
                    ERROR);
            throw e;
        }
    }

    public QueryResult<Panel> importAllGlobalPanels(String studyStr, QueryOptions options, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        // 1. We check everything can be done
        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_PANELS);

        DBIterator<Panel> iterator = iterator(INSTALLATION_PANELS, new Query(), QueryOptions.empty(), token);

        int dbTime = 0;
        int counter = 0;
        while (iterator.hasNext()) {
            Panel globalPanel = iterator.next();
            QueryResult<Panel> panelQueryResult = importGlobalPanel(study, globalPanel, options, userId);
            dbTime += panelQueryResult.getDbTime();
            counter++;
        }
        iterator.close();

        return new QueryResult<>("Import panel", dbTime, counter, counter, "", "", Collections.emptyList());
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

        if (importedPanels.size() > 5) {
            // If we have imported more than 5 panels, we will only return the number of imported panels
            return new QueryResult<>("Import panel", dbTime, importedPanels.size(), importedPanels.size(), "", "", Collections.emptyList());
        }

        return new QueryResult<>("Import panel", dbTime, importedPanels.size(), importedPanels.size(), "", "", importedPanels);
    }

    private QueryResult<Panel> importGlobalPanel(Study study, Panel diseasePanel, QueryOptions options, String userId)
            throws CatalogException {
        diseasePanel.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.PANEL));
        diseasePanel.setCreationDate(TimeUtils.getTime());
        diseasePanel.setRelease(studyManager.getCurrentRelease(study));
        diseasePanel.setVersion(1);

        // Install the current diseasePanel
        panelDBAdaptor.insert(study.getUid(), diseasePanel, options);
        return getPanel(study.getUid(), diseasePanel.getUuid(), options);
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
                            phenotypes.add(new Phenotype(relevantDisorder, relevantDisorder, ""));
                        }
                    }

                    List<GenePanel> genes = new ArrayList<>();
                    for (Map<String, Object> gene : (List<Map>) panelInfo.get("genes")) {
                        GenePanel genePanel = new GenePanel();

                        extractCommonInformationFromPanelApp(gene, genePanel);

                        List<Coordinate> coordinates = new ArrayList<>();

                        Map<String, Object> geneData = (Map) gene.get("gene_data");
                        Map<String, Object> ensemblGenes = (Map) geneData.get("ensembl_genes");
                        // Read coordinates
                        for (String assembly : ensemblGenes.keySet()) {
                            Map<String, Object> assemblyObject = (Map<String, Object>) ensemblGenes.get(assembly);
                            for (String version : assemblyObject.keySet()) {
                                Map<String, Object> coordinateObject = (Map<String, Object>) assemblyObject.get(version);
                                String correctAssembly = "GRch37".equals(assembly) ? "GRCh37" : "GRCh38";
                                coordinates.add(new Coordinate(correctAssembly, String.valueOf(coordinateObject.get("location")),
                                        "Ensembl v" + version));
                            }
                        }

                        genePanel.setName(String.valueOf(geneData.get("hgnc_symbol")));
                        genePanel.setCoordinates(coordinates);

                        genes.add(genePanel);
                    }

                    List<RegionPanel> regions = new ArrayList<>();
                    for (Map<String, Object> panelAppRegion : (List<Map>) panelInfo.get("regions")) {
                        RegionPanel region = new RegionPanel();

                        extractCommonInformationFromPanelApp(panelAppRegion, region);

                        List<Integer> coordinateList = null;
                        if (ListUtils.isNotEmpty((Collection<?>) panelAppRegion.get("grch38_coordinates"))) {
                            coordinateList = (List<Integer>) panelAppRegion.get("grch38_coordinates");
                        } else if (ListUtils.isNotEmpty((Collection<?>) panelAppRegion.get("grch37_coordinates"))) {
                            coordinateList = (List<Integer>) panelAppRegion.get("grch37_coordinates");
                        }

                        String id;
                        if (panelAppRegion.get("entity_name") != null
                                && StringUtils.isNotEmpty(String.valueOf(panelAppRegion.get("entity_name")))) {
                            id = String.valueOf(panelAppRegion.get("entity_name"));
                        } else {
                            id = (String) panelAppRegion.get("chromosome");
                            if (coordinateList != null && coordinateList.size() == 2) {
                                id = id + ":" + coordinateList.get(0) + "-" + coordinateList.get(1);
                            } else {
                                logger.warn("Could not read region coordinates");
                            }
                        }

                        VariantType variantType = null;
                        String typeOfVariant = String.valueOf(panelAppRegion.get("type_of_variants"));
                        if ("cnv_loss".equals(typeOfVariant)) {
                            variantType = VariantType.LOSS;
                        } else if ("cnv_gain".equals(typeOfVariant)) {
                            variantType = VariantType.GAIN;
                        } else {
                            System.out.println(typeOfVariant);
                        }

                        region.setId(id);
                        region.setDescription(String.valueOf(panelAppRegion.get("verbose_name")));
                        region.setHaploinsufficiencyScore(String.valueOf(panelAppRegion.get("haploinsufficiency_score")));
                        region.setTriplosensitivityScore(String.valueOf(panelAppRegion.get("triplosensitivity_score")));
                        region.setRequiredOverlapPercentage((int) panelAppRegion.get("required_overlap_percentage"));
                        region.setTypeOfVariants(variantType);

                        regions.add(region);
                    }

                    List<STR> strs = new ArrayList<>();
                    for (Map<String, Object> panelAppSTR : (List<Map>) panelInfo.get("strs")) {
                        STR str = new STR();

                        extractCommonInformationFromPanelApp(panelAppSTR, str);

                        str.setRepeatedSequence(String.valueOf(panelAppSTR.get("repeated_sequence")));
                        str.setNormalRepeats((int) panelAppSTR.get("normal_repeats"));
                        str.setPathogenicRepeats((int) panelAppSTR.get("pathogenic_repeats"));

                        strs.add(str);
                    }

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("PanelAppInfo", panel);

                    Panel diseasePanel = new Panel();
                    diseasePanel.setId(String.valueOf(panelInfo.get("name"))
                            .replace(" - ", "-")
                            .replace("/", "-")
                            .replace(" (", "-")
                            .replace("(", "-")
                            .replace(") ", "-")
                            .replace(")", "")
                            .replace(" & ", "_and_")
                            .replace(", ", "-")
                            .replace(" ", "_") + "-PanelAppId-" + panelInfo.get("id"));
                    diseasePanel.setName(String.valueOf(panelInfo.get("name")));
                    diseasePanel.setCategories(categories);
                    diseasePanel.setPhenotypes(phenotypes);
                    diseasePanel.setGenes(genes);
                    diseasePanel.setStrs(strs);
                    diseasePanel.setRegions(regions);
                    diseasePanel.setSource(new SourcePanel()
                            .setId(String.valueOf(panelInfo.get("id")))
                            .setName(String.valueOf(panelInfo.get("name")))
                            .setVersion(String.valueOf(panelInfo.get("version")))
                            .setProject("PanelApp (GEL)")
                    );
                    diseasePanel.setDescription(panelInfo.get("disease_sub_group")
                            + " (" + panelInfo.get("disease_group") + ")");
                    diseasePanel.setAttributes(attributes);

                    if ("Cancer Programme".equals(String.valueOf(panelInfo.get("disease_group")))) {
                        diseasePanel.setTags(Collections.singletonList("cancer"));
                    }

                    create(diseasePanel, overwrite, token);
                }
            }

            i++;
        }
    }

    private <T extends Common> void extractCommonInformationFromPanelApp(Map<String, Object> panelAppCommonMap, T common) {
        String ensemblGeneId = "";
        List<Xref> xrefs = new ArrayList<>();
        List<String> publications = new ArrayList<>();
        List<OntologyTerm> phenotypes = new ArrayList<>();
        List<Coordinate> coordinates = new ArrayList<>();

        Map<String, Object> geneData = (Map) panelAppCommonMap.get("gene_data");
        if (geneData != null) {
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
        }

        // Add coordinates
        String chromosome = String.valueOf(panelAppCommonMap.get("chromosome"));
        if (ListUtils.isNotEmpty((Collection<?>) panelAppCommonMap.get("grch38_coordinates"))) {
            List<Integer> auxCoordinates = (List<Integer>) panelAppCommonMap.get("grch38_coordinates");
            coordinates.add(new Coordinate("GRCh38", chromosome + ":" + auxCoordinates.get(0) + "-" + auxCoordinates.get(1),
                    "Ensembl"));
        }
        if (ListUtils.isNotEmpty((Collection<?>) panelAppCommonMap.get("grch37_coordinates"))) {
            List<Integer> auxCoordinates = (List<Integer>) panelAppCommonMap.get("grch37_coordinates");
            coordinates.add(new Coordinate("GRCh37", chromosome + ":" + auxCoordinates.get(0) + "-" + auxCoordinates.get(1),
                    "Ensembl"));
        }


        // read publications
        if (panelAppCommonMap.containsKey("publications")) {
            publications = (List<String>) panelAppCommonMap.get("publications");
        }

        // Read phenotypes
        if (panelAppCommonMap.containsKey("phenotypes") && !((List<String>) panelAppCommonMap.get("phenotypes")).isEmpty()) {
            for (String phenotype : ((List<String>) panelAppCommonMap.get("phenotypes"))) {
                String id = phenotype;
                String source = "";
                if (phenotype.length() >= 6) {
                    String substring = phenotype.substring(phenotype.length() - 6);
                    try {
                        Integer.parseInt(substring);
                        // If the previous call doesn't raise any exception, we are reading an OMIM id.
                        id = substring;
                        source = "OMIM";
                    } catch (NumberFormatException e) {
                        id = phenotype;
                    }
                }

                phenotypes.add(new OntologyTerm(id, phenotype, source, Collections.emptyMap()));
            }
        }

        // Read penetrance
        String panelAppPenetrance = String.valueOf(panelAppCommonMap.get("penetrance"));
        ClinicalProperty.Penetrance penetrance = null;
        if (StringUtils.isNotEmpty(panelAppPenetrance)) {
            try {
                penetrance = ClinicalProperty.Penetrance.valueOf(panelAppPenetrance.toUpperCase());
            } catch (IllegalArgumentException e) {
                logger.warn("Could not parse penetrance. Value found: " + panelAppPenetrance);
            }
        }

        common.setId(ensemblGeneId);
        common.setXrefs(xrefs);
        common.setModeOfInheritance(String.valueOf(panelAppCommonMap.get("mode_of_inheritance")));
        common.setPenetrance(penetrance);
        common.setConfidence(String.valueOf(panelAppCommonMap.get("confidence_level")));
        common.setEvidences((List<String>) panelAppCommonMap.get("evidence"));
        common.setPublications(publications);
        common.setPhenotypes(phenotypes);
        common.setCoordinates(coordinates);
    }

    /**
     * Update a Panel from catalog.
     *
     * @param studyStr   Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param panelId   Panel id in string format. Could be either the id or uuid.
     * @param updateParams Data model filled only with the parameters to be updated.
     * @param options      QueryOptions object.
     * @param token  Session id of the user logged in.
     * @return A QueryResult with the object updated.
     * @throws CatalogException if there is any internal error, the user does not have proper permissions or a parameter passed does not
     *                          exist or is not allowed to be updated.
     */
    public QueryResult<Panel> update(String studyStr, String panelId, PanelUpdateParams updateParams, QueryOptions options, String token)
            throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("panelId", panelId)
                .append("updateParams", updateParams != null ? updateParams.getUpdateMap() : null)
                .append("options", options)
                .append("token", token);
        Panel panel;
        try {
            panel = internalGet(study.getUid(), panelId, INCLUDE_PANEL_IDS, userId).first();
        } catch (CatalogException e) {
            auditManager.auditUpdate(userId, panelId, "", study.getId(), study.getUuid(), auditParams, AuditRecord.Entity.PANEL, ERROR);
            throw e;
        }

        try {
            ObjectMap parameters = new ObjectMap();
            if (updateParams != null) {
                parameters = updateParams.getUpdateMap();
            }

            options = ParamUtils.defaultObject(options, QueryOptions::new);

            if (parameters.isEmpty() && !options.getBoolean(Constants.INCREMENT_VERSION, false)) {
                ParamUtils.checkUpdateParametersMap(parameters);
            }

            // Check update permissions
            authorizationManager.checkPanelPermission(study.getUid(), panel.getUid(), userId, PanelAclEntry.PanelPermissions.UPDATE);

            if (parameters.containsKey(PanelDBAdaptor.QueryParams.ID.key())) {
                ParamUtils.checkAlias(parameters.getString(PanelDBAdaptor.QueryParams.ID.key()),
                        PanelDBAdaptor.QueryParams.ID.key());
            }

            if (options.getBoolean(Constants.INCREMENT_VERSION)) {
                // We do need to get the current release to properly create a new version
                options.put(Constants.CURRENT_RELEASE, studyManager.getCurrentRelease(study));
            }

            WriteResult result = panelDBAdaptor.update(panel.getUid(), parameters, options);
            auditManager.auditUpdate(userId, panel.getId(), panel.getUuid(), study.getId(), study.getUuid(), auditParams,
                    AuditRecord.Entity.PANEL, SUCCESS);

            QueryResult<Panel> queryResult = panelDBAdaptor.get(panel.getUid(),
                    new QueryOptions(QueryOptions.INCLUDE, parameters.keySet()));
            queryResult.setDbTime(result.getDbTime() + queryResult.getDbTime());

            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditUpdate(userId, panel.getId(), panel.getUuid(), study.getId(), study.getUuid(), auditParams,
                    AuditRecord.Entity.PANEL, ERROR);
            throw e;
        }
    }

    @Override
    public QueryResult<Panel> get(String studyStr, String entryStr, QueryOptions options, String token) throws CatalogException {
        if (StringUtils.isNotEmpty(studyStr) && INSTALLATION_PANELS.equals(studyStr)) {
            Panel installationPanel = getInstallationPanel(entryStr);
            return new QueryResult<>(entryStr, -1, 1, 1, "", "", Collections.singletonList(installationPanel));
        } else {
            return super.get(studyStr, entryStr, options, token);
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
    public WriteResult delete(String studyStr, Query query, ObjectMap params, String token) throws CatalogException {
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));
        WriteResult writeResult = new WriteResult();

        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        String operationUuid = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT);

        Query auditQuery = new Query(query);
        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("params", params)
                .append("token", token);

        // If the user is the owner or the admin, we won't check if he has permissions for every single entry
        boolean checkPermissions;

        // We try to get an iterator containing all the families to be deleted
        DBIterator<Panel> iterator;
        try {
            finalQuery.append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = panelDBAdaptor.iterator(finalQuery, QueryOptions.empty(), userId);

            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            checkPermissions = !authorizationManager.checkIsOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(userId, operationUuid, "", "", study.getId(), study.getUuid(), auditQuery, auditParams,
                    AuditRecord.Entity.PANEL, ERROR);
            throw e;
        }

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
                writeResult.concat(panelDBAdaptor.delete(panel.getUid()));

                auditManager.auditDelete(userId, operationUuid, panel.getId(), panel.getUuid(), study.getId(), study.getUuid(), auditQuery,
                        auditParams, AuditRecord.Entity.PANEL, SUCCESS);
            } catch (Exception e) {
                writeResult.getFailed().add(new WriteResult.Fail(panel.getId(), e.getMessage()));
                logger.debug("Cannot delete panel {}: {}", panel.getId(), e.getMessage(), e);

                auditManager.auditDelete(userId, operationUuid, panel.getId(), panel.getUuid(), study.getId(), study.getUuid(), auditQuery,
                        auditParams, AuditRecord.Entity.PANEL, ERROR);
            }
        }

        if (!writeResult.getFailed().isEmpty()) {
            writeResult.setWarning(Collections.singletonList("Some panels could not be deleted"));
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
        String user = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, user);

        InternalGetQueryResult<Panel> queryResult = internalGet(study.getUid(), panelList, INCLUDE_PANEL_IDS, user, silent);

        Map<String, InternalGetQueryResult.Missing> missingMap = new HashMap<>();
        if (queryResult.getMissing() != null) {
            missingMap = queryResult.getMissing().stream()
                    .collect(Collectors.toMap(InternalGetQueryResult.Missing::getId, Function.identity()));
        }
        int counter = 0;
        for (String panelId : panelList) {
            if (!missingMap.containsKey(panelId)) {
                try {
                    QueryResult<PanelAclEntry> allPanelAcls;
                    if (StringUtils.isNotEmpty(member)) {
                        allPanelAcls =
                                authorizationManager.getPanelAcl(study.getUid(), queryResult.getResult().get(counter).getUid(), user,
                                        member);
                    } else {
                        allPanelAcls = authorizationManager.getAllPanelAcls(study.getUid(), queryResult.getResult().get(counter).getUid(),
                                user);
                    }
                    allPanelAcls.setId(panelId);
                    panelAclList.add(allPanelAcls);
                } catch (CatalogException e) {
                    if (!silent) {
                        throw e;
                    } else {
                        panelAclList.add(new QueryResult<>(panelId, queryResult.getDbTime(), 0, 0, "",
                                missingMap.get(panelId).getErrorMsg(), Collections.emptyList()));
                    }
                }
                counter += 1;
            } else {
                panelAclList.add(new QueryResult<>(panelId, queryResult.getDbTime(), 0, 0, "", missingMap.get(panelId).getErrorMsg(),
                        Collections.emptyList()));
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

        String user = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, user);

        QueryResult<Panel> panelQueryResult = internalGet(study.getUid(), panelList, INCLUDE_PANEL_IDS, user, false);
        authorizationManager.checkCanAssignOrSeePermissions(study.getUid(), user);

        // Validate that the members are actually valid members
        List<String> members;
        if (memberIds != null && !memberIds.isEmpty()) {
            members = Arrays.asList(memberIds.split(","));
        } else {
            members = Collections.emptyList();
        }
        authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
        checkMembers(study.getUid(), members);

        switch (panelAclParams.getAction()) {
            case SET:
                return authorizationManager.setAcls(study.getUid(), panelQueryResult.getResult().stream().map(Panel::getUid)
                                .collect(Collectors.toList()), members, permissions, Entity.PANEL);
            case ADD:
                return authorizationManager.addAcls(study.getUid(), panelQueryResult.getResult().stream()
                        .map(Panel::getUid)
                        .collect(Collectors.toList()), members, permissions, Entity.PANEL);
            case REMOVE:
                return authorizationManager.removeAcls(panelQueryResult.getResult().stream().map(Panel::getUid)
                        .collect(Collectors.toList()), members, permissions, Entity.PANEL);
            case RESET:
                return authorizationManager.removeAcls(panelQueryResult.getResult().stream().map(Panel::getUid)
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
        ParamUtils.checkAlias(panel.getId(), "id");
        panel.setName(ParamUtils.defaultString(panel.getName(), panel.getId()));
        panel.setRelease(-1);
        panel.setVersion(1);
        panel.setAuthor(ParamUtils.defaultString(panel.getAuthor(), ""));
        panel.setCreationDate(TimeUtils.getTime());
        panel.setModificationDate(TimeUtils.getTime());
        panel.setStatus(new Status());
        panel.setCategories(ParamUtils.defaultObject(panel.getCategories(), Collections.emptyList()));
        panel.setTags(ParamUtils.defaultObject(panel.getTags(), Collections.emptyList()));
        panel.setDescription(ParamUtils.defaultString(panel.getDescription(), ""));
        panel.setPhenotypes(ParamUtils.defaultObject(panel.getPhenotypes(), Collections.emptyList()));
        panel.setVariants(ParamUtils.defaultObject(panel.getVariants(), Collections.emptyList()));
        panel.setRegions(ParamUtils.defaultObject(panel.getRegions(), Collections.emptyList()));
        panel.setGenes(ParamUtils.defaultObject(panel.getGenes(), Collections.emptyList()));
        panel.setAttributes(ParamUtils.defaultObject(panel.getAttributes(), Collections.emptyMap()));
        panel.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.PANEL));

        fillDefaultStats(panel);

        panelDBAdaptor.insert(panel, overwrite);
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

    public void delete(String panelId, String token) throws CatalogException {
        String userId = userManager.getUserId(token);

        if (!authorizationManager.checkIsAdmin(userId)) {
            throw new CatalogAuthorizationException("Only the main OpenCGA administrator can delete global panels");
        }

        Panel panel = getInstallationPanel(panelId);
        panelDBAdaptor.delete(panel.getUid());
    }

}
