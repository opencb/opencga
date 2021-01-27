package org.opencb.opencga.analysis.clinical.rga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.opencga.analysis.StorageManager;
import org.opencb.opencga.catalog.db.api.DBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.managers.IndividualManager;
import org.opencb.opencga.catalog.managers.SampleManager;
import org.opencb.opencga.core.config.Catalog;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByGene;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByIndividual;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByVariant;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualAclEntry;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleAclEntry;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.RgaException;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.io.managers.IOConnectorProvider;
import org.opencb.opencga.storage.core.rga.RgaEngine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class RgaManager extends StorageManager {

    private final RgaEngine rgaEngine;

    private static final int KNOCKOUT_INSERT_BATCH_SIZE = 25;

    public RgaManager(CatalogManager catalogManager, StorageEngineFactory storageEngineFactory) {
        super(catalogManager, storageEngineFactory);
        this.rgaEngine = new RgaEngine(getStorageConfiguration());
    }

    public RgaManager(Configuration configuration, StorageConfiguration storageConfiguration) throws CatalogException {
        super(configuration, storageConfiguration);
        this.rgaEngine = new RgaEngine(storageConfiguration);
    }

    public OpenCGAResult<KnockoutByIndividual> individualQuery(String studyStr, Query query, QueryOptions options, String token)
            throws CatalogException, IOException, RgaException {
        Study study = catalogManager.getStudyManager().get(studyStr, QueryOptions.empty(), token).first();
        // TODO: Check Permissions
        String collection = getCollectionName(study.getFqn());
        return rgaEngine.individualQuery(collection, query, options);
    }

    public OpenCGAResult<KnockoutByGene> geneQuery(String studyStr, Query query, QueryOptions options, String token)
            throws CatalogException, IOException, RgaException {
        Study study = catalogManager.getStudyManager().get(studyStr, QueryOptions.empty(), token).first();
        // TODO: Check Permissions
        String collection = getCollectionName(study.getFqn());
        return rgaEngine.geneQuery(collection, query, options);
    }

    public OpenCGAResult<KnockoutByVariant> variantQuery(String studyStr, Query query, QueryOptions options, String token)
            throws CatalogException, IOException, RgaException {
        Study study = catalogManager.getStudyManager().get(studyStr, QueryOptions.empty(), token).first();
        // TODO: Check Permissions
        String collection = getCollectionName(study.getFqn());
        return rgaEngine.variantQuery(collection, query, options);
    }

    public void index(String studyStr, String fileStr, String token) throws CatalogException, RgaException, IOException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().get(studyStr, QueryOptions.empty(), token).first();
        try {
            catalogManager.getAuthorizationManager().isOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            logger.error(e.getMessage(), e);
            throw new CatalogException("Only owners or admins can index", e.getCause());
        }

        File file = catalogManager.getFileManager().get(studyStr, fileStr, FileManager.INCLUDE_FILE_URI_PATH, token).first();

        load(study.getFqn(), Paths.get(file.getUri()), token);
    }

    /**
     * Load a multi KnockoutByIndividual JSON file into the Solr core/collection.
     *
     * @param path Path to the JSON file
     * @param path Path to the JSON file
     * @param path Path to the JSON file
     * @param path Path to the JSON file
     * @throws IOException
     * @throws SolrException
     */
    private void load(String study, Path path, String token) throws IOException, RgaException {
        String fileName = path.getFileName().toString();
        if (fileName.endsWith("json") || fileName.endsWith("json.gz")) {
            String collection = getCollectionName(study);

            try {
                if (!rgaEngine.exists(collection)) {
                    rgaEngine.create(collection);
                }
            } catch (RgaException e) {
                logger.error("Could not perform RGA index in collection {}", collection, e);
                throw new RgaException("Could not perform RGA index in collection '" + collection + "'.");
            }

            try {
                IOConnectorProvider ioConnectorProvider = new IOConnectorProvider(storageConfiguration);

                // This opens json and json.gz files automatically
                try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
                        ioConnectorProvider.newInputStream(path.toUri())))) {
                    Map<String, List<String>> grantedPermissionMemberList = new HashMap<>();
                    Map<String, List<String>> deniedPermissionMemberList = new HashMap<>();

                    List<KnockoutByIndividual> knockoutByIndividualList = new ArrayList<>(KNOCKOUT_INSERT_BATCH_SIZE);
                    int count = 0;
                    String line;
                    ObjectReader objectReader = new ObjectMapper().readerFor(KnockoutByIndividual.class);
                    while ((line = bufferedReader.readLine()) != null) {
                        KnockoutByIndividual knockoutByIndividual = objectReader.readValue(line);
                        knockoutByIndividualList.add(knockoutByIndividual);
                        count++;
                        if (count % KNOCKOUT_INSERT_BATCH_SIZE == 0) {
                            checkMemberPermissions(knockoutByIndividualList, grantedPermissionMemberList, deniedPermissionMemberList, study,
                                    token);
                            rgaEngine.insert(collection, knockoutByIndividualList, grantedPermissionMemberList, deniedPermissionMemberList);
                            logger.debug("Loaded {} knockoutByIndividual entries from '{}'", count, path);
                            knockoutByIndividualList.clear();
                        }
                    }

                    // Insert the remaining entries
                    if (CollectionUtils.isNotEmpty(knockoutByIndividualList)) {
                        checkMemberPermissions(knockoutByIndividualList, grantedPermissionMemberList, deniedPermissionMemberList, study,
                                token);
                        rgaEngine.insert(collection, knockoutByIndividualList, grantedPermissionMemberList, deniedPermissionMemberList);
                        logger.debug("Loaded remaining {} knockoutByIndividual entries from '{}'", count, path);
                    }
                }
            } catch (SolrServerException e) {
                throw new RgaException("Error loading KnockoutIndividual from JSON file.", e);
            }
        } else {
            throw new RgaException("File format " + path + " not supported. Please, use JSON file format.");
        }
    }

    /**
     * Check VIEW_SAMPLE, VIEW_VARIANT and VIEW_INDIVIDUAL permissions for each individual/sample of the KnockoutByIndividual objects.
     * If a member has all 3 permissions, it will be added to the grantedPermissionMemberList.
     * If a member has some permissions set but missing any of the 3, it will be added to the deniedPermissionMemberList.
     *
     * @param knockoutByIndividualList List of KnockoutByIndividual objects.
     * @param grantedPermissionMemberList Map containing the list of members for which permissions are satisfied.
     * @param deniedPermissionMemberList Map containing the list of members for which permissions are revoked.
     * @param study Study id.
     * @param token Must be a valid 'opencga' administrator token.
     * @throws RgaException if there is any problem retrieving the list of permissions from Catalog.
     */
    private void checkMemberPermissions(List<KnockoutByIndividual> knockoutByIndividualList,
                                        Map<String, List<String>> grantedPermissionMemberList,
                                        Map<String, List<String>> deniedPermissionMemberList, String study, String token)
            throws RgaException {
        // Fetch all individual and sample ids
        List<String> individualIds = new ArrayList<>(knockoutByIndividualList.size());
        List<String> sampleIds = new ArrayList<>(knockoutByIndividualList.size());
        for (KnockoutByIndividual knockoutByIndividual : knockoutByIndividualList) {
            individualIds.add(knockoutByIndividual.getId());
            sampleIds.add(knockoutByIndividual.getSampleId());
        }

        // Retrieve permissions for each individual/sample
        QueryOptions options = new QueryOptions(SampleManager.INCLUDE_SAMPLE_IDS)
                .append(DBAdaptor.INCLUDE_ACLS, true);
        OpenCGAResult<Sample> sampleResult;
        try {
            sampleResult = catalogManager.getSampleManager().get(study, sampleIds, options, token);
        } catch (CatalogAuthorizationException e) {
            throw new RgaException("Missing permissions to retrieve list of sample permissions", e.getCause());
        } catch (CatalogException e) {
            throw new RgaException("Some samples were not found in catalog", e.getCause());
        }

        options = new QueryOptions(IndividualManager.INCLUDE_INDIVIDUAL_IDS)
                .append(DBAdaptor.INCLUDE_ACLS, true);
        OpenCGAResult<Individual> individualResult;
        try {
            individualResult = catalogManager.getIndividualManager().get(study, individualIds, options, token);
        } catch (CatalogAuthorizationException e) {
            throw new RgaException("Missing permissions to retrieve list of individual permissions", e.getCause());
        } catch (CatalogException e) {
            throw new RgaException("Some individuals were not found in catalog", e.getCause());
        }

        // Set that will contain all member ids that were granted specific permissions
        Set<String> memberIds = new HashSet<>();

        // Generate a map<individualId/sampleId, Map<memberId, Set<String> permissions>>
        Map<String, Map<String, Set<String>>> samplePermissions = new HashMap<>();
        Map<String, Map<String, Set<String>>> individualPermissions = new HashMap<>();

        for (Sample sample : sampleResult.getResults()) {
            generateIntermediatePermissionMap(sample.getId(), sample.getAttributes(), samplePermissions, memberIds);
        }
        for (Individual individual : individualResult.getResults()) {
            generateIntermediatePermissionMap(individual.getId(), individual.getAttributes(), individualPermissions, memberIds);
        }

        if (!memberIds.isEmpty()) {
            // Check permissions
            for (int i = 0; i < individualIds.size(); i++) {
                String individualId = individualIds.get(i);
                String sampleId = sampleIds.get(i);

                Map<String, Set<String>> auxSamplePermissions = samplePermissions.get(sampleId);
                Map<String, Set<String>> auxIndividualPermissions = individualPermissions.get(individualId);
                for (String memberId : memberIds) {
                    Set<String> individualAcls = auxIndividualPermissions.get(memberId);
                    Set<String> sampleAcls = auxSamplePermissions.get(memberId);

                    if (individualAcls != null && sampleAcls != null) {
                        // Only grant permissions at the document level if it has been granted all 3 permissions
                        if (individualAcls.contains(IndividualAclEntry.IndividualPermissions.VIEW.name())
                                && sampleAcls.contains(SampleAclEntry.SamplePermissions.VIEW.name())
                                && sampleAcls.contains(SampleAclEntry.SamplePermissions.VIEW_VARIANTS.name())) {
                            addMemberToAclList(individualId, memberId, grantedPermissionMemberList);
                        } else {
                            addMemberToAclList(individualId, memberId, deniedPermissionMemberList);
                        }
                    } else if (individualAcls != null) {
                        // If VIEW_INDIVIDUAL permission is not granted, we will add this member to the list of revoked users
                        if (!individualAcls.contains(IndividualAclEntry.IndividualPermissions.VIEW.name())) {
                            addMemberToAclList(individualId, memberId, deniedPermissionMemberList);
                        }
                    } else if (sampleAcls != null) {
                        // If VIEW_SAMPLE or VIEW_VARIANTS permissions are not granted, we will add this member to the list of revoked users
                        if (!sampleAcls.contains(SampleAclEntry.SamplePermissions.VIEW.name())
                                || !sampleAcls.contains(SampleAclEntry.SamplePermissions.VIEW_VARIANTS.name())) {
                            addMemberToAclList(individualId, memberId, deniedPermissionMemberList);
                        }
                    }
                }
            }
        }
    }

    private void addMemberToAclList(String entryId, String memberId, Map<String, List<String>> entryMemberListMap) {
        if (!entryMemberListMap.containsKey(entryId)) {
            entryMemberListMap.put(entryId, new LinkedList<>());
        }
        entryMemberListMap.get(entryId).add(memberId);
    }

    private void generateIntermediatePermissionMap(String id, Map<String, Object> attributes,
                                                   Map<String, Map<String, Set<String>>> permissionMap, Set<String> memberIds) {
        Map<String, Set<String>> permissions = new HashMap<>();
        List<Map<String, Object>> aclEntries = (List<Map<String, Object>>) attributes.get("OPENCGA_ACL");
        for (Map<String, Object> entry : aclEntries) {
            String memberId = (String) entry.get("member");
            permissions.put(memberId, new HashSet<>((List<String>) entry.get("permissions")));
            memberIds.add(memberId);
        }
        permissionMap.put(id, permissions);
    }

    @Override
    public void testConnection() throws StorageEngineException {
        rgaEngine.isAlive("test");
    }

    private String getCollectionName(String study) {
        return catalogManager.getConfiguration().getDatabasePrefix() + "-rga-" + study.replace("@", "_").replace(":", "_");
    }
}
