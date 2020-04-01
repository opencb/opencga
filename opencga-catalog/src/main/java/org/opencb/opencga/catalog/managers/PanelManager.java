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
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.commons.OntologyTerm;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.core.Xref;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.db.api.PanelDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.AclParams;
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
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.biodata.models.clinical.interpretation.DiseasePanel.*;
import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;

public class PanelManager extends ResourceManager<Panel> {

    protected static Logger logger = LoggerFactory.getLogger(PanelManager.class);
    private UserManager userManager;
    private StudyManager studyManager;

    // Reserved word to query over installation panels instead of the ones belonging to a study.
    public static final String INSTALLATION_PANELS = "__INSTALLATION__";

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
    OpenCGAResult<Panel> internalGet(long studyUid, String entry, @Nullable Query query, QueryOptions options, String user)
            throws CatalogException {
        ParamUtils.checkIsSingleID(entry);
        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.put(PanelDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();

        if (UuidUtils.isOpenCgaUuid(entry)) {
            queryCopy.put(PanelDBAdaptor.QueryParams.UUID.key(), entry);
        } else {
            queryCopy.put(PanelDBAdaptor.QueryParams.ID.key(), entry);
        }
        OpenCGAResult<Panel> panelDataResult = panelDBAdaptor.get(studyUid, queryCopy, queryOptions, user);
        if (panelDataResult.getNumResults() == 0) {
            panelDataResult = panelDBAdaptor.get(queryCopy, queryOptions);
            if (panelDataResult.getNumResults() == 0) {
                throw new CatalogException("Panel " + entry + " not found");
            } else {
                throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see the panel " + entry);
            }
        } else if (panelDataResult.getNumResults() > 1 && !queryCopy.getBoolean(Constants.ALL_VERSIONS)) {
            throw new CatalogException("More than one panel found based on " + entry);
        } else {
            return panelDataResult;
        }
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

        Function<Panel, String> panelStringFunction = Panel::getId;
        PanelDBAdaptor.QueryParams idQueryParam = null;
        for (String entry : uniqueList) {
            PanelDBAdaptor.QueryParams param = PanelDBAdaptor.QueryParams.ID;
            if (UuidUtils.isOpenCgaUuid(entry)) {
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

        OpenCGAResult<Panel> panelDataResult = panelDBAdaptor.get(studyUid, queryCopy, queryOptions, user);
        if (ignoreException || panelDataResult.getNumResults() >= uniqueList.size()) {
            return keepOriginalOrder(uniqueList, panelStringFunction, panelDataResult, ignoreException,
                    queryCopy.getBoolean(Constants.ALL_VERSIONS));
        }
        // Query without adding the user check
        OpenCGAResult<Panel> resultsNoCheck = panelDBAdaptor.get(queryCopy, queryOptions);

        if (resultsNoCheck.getNumResults() == panelDataResult.getNumResults()) {
            throw CatalogException.notFound("panels", getMissingFields(uniqueList, panelDataResult.getResults(), panelStringFunction));
        } else {
            throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the panels.");
        }
    }

    private OpenCGAResult<Panel> getPanel(long studyUid, String panelUuid, QueryOptions options) throws CatalogException {
        Query query = new Query()
                .append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(PanelDBAdaptor.QueryParams.UUID.key(), panelUuid);
        return panelDBAdaptor.get(query, options);
    }

    private OpenCGAResult<Panel> getInstallationPanel(String entry) throws CatalogException {
        Query query = new Query(PanelDBAdaptor.QueryParams.STUDY_UID.key(), -1);

        if (UuidUtils.isOpenCgaUuid(entry)) {
            query.put(PanelDBAdaptor.QueryParams.UUID.key(), entry);
        } else {
            query.put(PanelDBAdaptor.QueryParams.ID.key(), entry);
        }

        OpenCGAResult<Panel> panelDataResult = panelDBAdaptor.get(query, QueryOptions.empty());
        if (panelDataResult.getNumResults() == 0) {
            throw new CatalogException("Panel " + entry + " not found");
        } else if (panelDataResult.getNumResults() > 1) {
            throw new CatalogException("More than one panel found based on " + entry);
        } else {
            return panelDataResult;
        }
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

    public OpenCGAResult<Panel> importAllGlobalPanels(String studyId, QueryOptions options, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("options", options)
                .append("token", token);
        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
        try {
            // 1. We check everything can be done
            authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_PANELS);

            DBIterator<Panel> iterator = iterator(INSTALLATION_PANELS, new Query(), QueryOptions.empty(), token);

            int dbTime = 0;
            int counter = 0;
            while (iterator.hasNext()) {
                Panel globalPanel = iterator.next();
                OpenCGAResult<Panel> panelDataResult = importGlobalPanel(study, globalPanel, options);
                dbTime += panelDataResult.getTime();
                counter++;

                auditManager.audit(operationId, userId, Enums.Action.CREATE, Enums.Resource.DISEASE_PANEL,
                        panelDataResult.first().getId(), panelDataResult.first().getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
            }
            iterator.close();

            return new OpenCGAResult<>(dbTime, Collections.emptyList(), counter, counter, 0, 0);
        } catch (CatalogException e) {
            auditManager.audit(operationId, userId, Enums.Action.CREATE, Enums.Resource.DISEASE_PANEL, "", "",
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()),
                    new ObjectMap());
            throw e;
        }
    }

    public OpenCGAResult<Panel> importGlobalPanels(String studyId, List<String> panelIds, QueryOptions options, String token)
            throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("panelIds", panelIds)
                .append("options", options)
                .append("token", token);
        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        int counter = 0;
        try {
            // 1. We check everything can be done
            authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_PANELS);

            List<Panel> importedPanels = new ArrayList<>(panelIds.size());
            int dbTime = 0;
            for (counter = 0; counter < panelIds.size(); counter++) {
                // Fetch the installation Panel (if it exists)
                Panel globalPanel = getInstallationPanel(panelIds.get(counter)).first();
                OpenCGAResult<Panel> panelDataResult = importGlobalPanel(study, globalPanel, options);
                importedPanels.add(panelDataResult.first());
                dbTime += panelDataResult.getTime();

                auditManager.audit(operationId, userId, Enums.Action.CREATE, Enums.Resource.DISEASE_PANEL,
                        panelDataResult.first().getId(), panelDataResult.first().getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
            }

            if (importedPanels.size() > 5) {
                // If we have imported more than 5 panels, we will only return the number of imported panels
                return new OpenCGAResult<>(dbTime, Collections.emptyList(), importedPanels.size(), importedPanels.size(), 0, 0);
            }

            return new OpenCGAResult<>(dbTime, Collections.emptyList(), importedPanels.size(), importedPanels, importedPanels.size(),
                    importedPanels.size(), 0, 0, new ObjectMap());
        } catch (CatalogException e) {
            // Audit panels that could not be imported
            for (int i = counter; i < panelIds.size(); i++) {
                auditManager.audit(operationId, userId, Enums.Action.CREATE, Enums.Resource.DISEASE_PANEL, panelIds.get(i),
                        "", study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
            }
            throw e;
        }
    }

    private OpenCGAResult<Panel> importGlobalPanel(Study study, Panel diseasePanel, QueryOptions options) throws CatalogException {
        diseasePanel.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.PANEL));
        diseasePanel.setCreationDate(TimeUtils.getTime());
        diseasePanel.setRelease(studyManager.getCurrentRelease(study));
        diseasePanel.setVersion(1);

        // Install the current diseasePanel
        panelDBAdaptor.insert(study.getUid(), diseasePanel, options);
        return getPanel(study.getUid(), diseasePanel.getUuid(), options);
    }

    public void importPanelApp(String token, boolean overwrite) throws CatalogException {
        // We check it is the admin of the installation
        String userId = userManager.getUserId(token);

        ObjectMap auditParams = new ObjectMap()
                .append("overwrite", overwrite)
                .append("token", token);

        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        try {
            authorizationManager.checkIsAdmin(userId);

            Client client = ClientBuilder.newClient();
            int i = 1;
            int max = Integer.MAX_VALUE;

            while (i < max) {
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

                        resource = client.target("https://panelapp.genomicsengland.co.uk/api/v1/panels/" + panel.get("id")
                                + "?format=json");
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

                        create(diseasePanel, overwrite, operationUuid, token);
                    }
                }

                i++;
            }

            auditManager.audit(operationUuid, userId, Enums.Action.IMPORT, Enums.Resource.DISEASE_PANEL, "", "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
        } catch (CatalogException e) {
            auditManager.audit(operationUuid, userId, Enums.Action.IMPORT, Enums.Resource.DISEASE_PANEL, "", "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
            throw e;
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
     * @param studyStr   Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param panelIds   List of Panel ids. Could be either the id or uuid.
     * @param updateParams Data model filled only with the parameters to be updated.
     * @param options      QueryOptions object.
     * @param token  Session id of the user logged in.
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
        authorizationManager.checkPanelPermission(study.getUid(), panel.getUid(), userId, PanelAclEntry.PanelPermissions.UPDATE);

        if (parameters.containsKey(PanelDBAdaptor.QueryParams.ID.key())) {
            ParamUtils.checkAlias(parameters.getString(PanelDBAdaptor.QueryParams.ID.key()),
                    PanelDBAdaptor.QueryParams.ID.key());
        }

        if (options.getBoolean(Constants.INCREMENT_VERSION)) {
            // We do need to get the current release to properly create a new version
            options.put(Constants.CURRENT_RELEASE, studyManager.getCurrentRelease(study));
        }

        return panelDBAdaptor.update(panel.getUid(), parameters, options);
    }

    @Override
    public OpenCGAResult<Panel> get(String studyStr, String entryStr, QueryOptions options, String token) throws CatalogException {
        if (StringUtils.isNotEmpty(studyStr) && INSTALLATION_PANELS.equals(studyStr)) {
            return getInstallationPanel(entryStr);
        } else {
            return super.get(studyStr, entryStr, options, token);
        }
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
            return panelDBAdaptor.iterator(study.getUid(), query, options, userId);
        }
    }

    @Override
    public OpenCGAResult<Panel> search(String studyId, Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        Study study;
        if (studyId.equals(INSTALLATION_PANELS)) {
            study = new Study().setId("").setUuid("");
        } else {
            study = studyManager.resolveId(studyId, userId, new QueryOptions(QueryOptions.INCLUDE,
                    StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));
        }

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("options", options)
                .append("token", token);

        try {
            OpenCGAResult<Panel> result = OpenCGAResult.empty();
            if (INSTALLATION_PANELS.equals(studyId)) {
                query.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), -1);

                // Here view permissions won't be checked
                result = panelDBAdaptor.get(query, options);
            } else {
                query.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
                // Here permissions will be checked
                result = panelDBAdaptor.get(study.getUid(), query, options, userId);
            }

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
    public OpenCGAResult<Panel> count(String studyId, Query query, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = userManager.getUserId(token);
        Study study;
        if (studyId.equals(INSTALLATION_PANELS)) {
            study = new Study().setId("").setUuid("");
        } else {
            study = studyManager.resolveId(studyId, userId, new QueryOptions(QueryOptions.INCLUDE,
                    StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));
        }

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("token", token);
        try {
            OpenCGAResult<Long> queryResultAux;
            if (studyId.equals(INSTALLATION_PANELS)) {
                query.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), -1);

                // Here view permissions won't be checked
                queryResultAux = panelDBAdaptor.count(query);
            } else {
                query.append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

                // Here view permissions will be checked
                queryResultAux = panelDBAdaptor.count(query, userId);
            }

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
    public OpenCGAResult delete(String studyStr, List<String> panelIds, ObjectMap params, String token) throws CatalogException {
        return delete(studyStr, panelIds, params, false, token);
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
    public OpenCGAResult delete(String studyStr, Query query, ObjectMap params, String token) throws CatalogException {
        return delete(studyStr, query, params, false, token);
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
                                                              AclParams aclParams, String token) throws CatalogException {
        String user = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, user);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("panelStrList", panelStrList)
                .append("memberList", memberList)
                .append("aclParams", aclParams)
                .append("token", token);
        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        try {
            if (panelStrList == null || panelStrList.isEmpty()) {
                throw new CatalogException("Update ACL: Missing panel parameter");
            }

            if (aclParams.getAction() == null) {
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

            OpenCGAResult<Map<String, List<String>>> queryResultList;
            switch (aclParams.getAction()) {
                case SET:
                    queryResultList = authorizationManager.setAcls(study.getUid(), panelDataResult.getResults().stream().map(Panel::getUid)
                            .collect(Collectors.toList()), members, permissions, Enums.Resource.DISEASE_PANEL);
                    break;
                case ADD:
                    queryResultList = authorizationManager.addAcls(study.getUid(), panelDataResult.getResults().stream().map(Panel::getUid)
                            .collect(Collectors.toList()), members, permissions, Enums.Resource.DISEASE_PANEL);
                    break;
                case REMOVE:
                    queryResultList = authorizationManager.removeAcls(panelDataResult.getResults().stream().map(Panel::getUid)
                            .collect(Collectors.toList()), members, permissions, Enums.Resource.DISEASE_PANEL);
                    break;
                case RESET:
                    queryResultList = authorizationManager.removeAcls(panelDataResult.getResults().stream().map(Panel::getUid)
                            .collect(Collectors.toList()), members, null, Enums.Resource.DISEASE_PANEL);
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
        create(panel, overwrite, null, token);
    }

    private void create(Panel panel, boolean overwrite, String operationUuid, String token) throws CatalogException {
        String userId = userManager.getUserId(token);

        ObjectMap auditParams = new ObjectMap()
                .append("Panel", panel)
                .append("overwrite", overwrite)
                .append("operationUuid", operationUuid)
                .append("token", token);

        if (StringUtils.isEmpty(operationUuid)) {
            operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
        }

        try {
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
            panel.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.PANEL));

            fillDefaultStats(panel);

            panelDBAdaptor.insert(panel, overwrite);
            auditManager.audit(operationUuid, userId, Enums.Action.CREATE, Enums.Resource.DISEASE_PANEL, panel.getId(), "", "", "",
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
        } catch (CatalogException e) {
            auditManager.audit(operationUuid, userId, Enums.Action.CREATE, Enums.Resource.DISEASE_PANEL, panel.getId(), "", "", "",
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
            throw e;
        }
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

        ObjectMap auditParams = new ObjectMap()
                .append("panelId", panelId)
                .append("token", token);
        try {
            if (!authorizationManager.checkIsAdmin(userId)) {
                throw new CatalogAuthorizationException("Only the main OpenCGA administrator can delete global panels");
            }

            Panel panel = getInstallationPanel(panelId).first();
            panelDBAdaptor.delete(panel);

            auditManager.auditDelete(userId, Enums.Resource.DISEASE_PANEL, panel.getId(), panel.getUuid(), "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (CatalogException e) {
            auditManager.auditDelete(userId, Enums.Resource.DISEASE_PANEL, panelId, "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

}
