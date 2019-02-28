package org.opencb.opencga.analysis.clinical;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.Project;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class InterpretationAnalysisUtils {

    public static Map<String, List<String>> loadActionableVariants(Path path) throws IOException {
        Map<String, List<String>> actionableVariants = new HashMap<>();

        // Check file
        File file = getFile(path);
        if (file != null && file.exists()) {

            BufferedReader bufferedReader = org.opencb.commons.utils.FileUtils.newBufferedReader(file.toPath());
            List<String> lines = bufferedReader.lines().collect(Collectors.toList());
            for (String line : lines) {
                if (line.startsWith("#")) {
                    continue;
                }
                String[] split = line.split("\t");
                if (split.length > 4) {
                    List<String> phenotypes = new ArrayList<>();
                    if (split.length > 8 && StringUtils.isNotEmpty(split[8])) {
                        phenotypes.addAll(Arrays.asList(split[8].split(";")));
                    }

                    actionableVariants.put(split[0] + ":" + split[1] + "-" + split[2] + ":" + split[3] + ":" + split[4], phenotypes);
                }
            }
        }

        return actionableVariants;
    }

    public static Map<String, ClinicalProperty.RoleInCancer> loadRoleInCancer(Path path) throws IOException {
        Map<String, ClinicalProperty.RoleInCancer> roleInCancer = new HashMap<>();

        // Check file
        File file = getFile(path);

        if (file != null && file.exists()) {
            BufferedReader bufferedReader = org.opencb.commons.utils.FileUtils.newBufferedReader(file.toPath());
            List<String> lines = bufferedReader.lines().collect(Collectors.toList());
            Set<ClinicalProperty.RoleInCancer> set = new HashSet<>();
            for (String line : lines) {
                if (line.startsWith("#")) {
                    continue;
                }

                set.clear();
                String[] split = line.split("\t");
                // Sanity check
                if (split.length > 1) {
                    String[] roles = split[1].replace("\"", "").split(",");
                    for (String role : roles) {
                        switch (role.trim().toLowerCase()) {
                            case "oncogene":
                                set.add(ClinicalProperty.RoleInCancer.ONCOGENE);
                                break;
                            case "tsg":
                                set.add(ClinicalProperty.RoleInCancer.TUMOR_SUPPRESSOR_GENE);
                                break;
                            default:
                                break;
                        }
                    }
                }

                // Update set
                if (set.size() > 0) {
                    if (set.size() == 2) {
                        roleInCancer.put(split[0], ClinicalProperty.RoleInCancer.BOTH);
                    } else {
                        roleInCancer.put(split[0], set.iterator().next());
                    }
                }
            }
        }
        return roleInCancer;
    }

    public static List<Variant> secondaryFindings(Query query, Set<String> actionableVariants,
                                                  VariantStorageManager variantStorageManager, String token)
            throws CatalogException, IOException, StorageEngineException {
        List<Variant> variants = new ArrayList<>();

        final int batchSize = 1000;
        int count = 0;
        StringBuilder ids = new StringBuilder();
        Iterator<String> iterator = actionableVariants.iterator();
        while (iterator.hasNext()) {
            ids.append(iterator.next()).append(",");
            if (++count >= batchSize) {
                query.put(VariantQueryParam.ID.key(), ids);
                VariantQueryResult<Variant> result = variantStorageManager.get(query, QueryOptions.empty(), token);
                if (CollectionUtils.isNotEmpty(result.getResult())) {
                    variants.addAll(result.getResult());
                }

                // Reset
                count = 0;
                ids.setLength(0);
            }
        }

        if (count > 0) {
            query.put(VariantQueryParam.ID.key(), ids);
            VariantQueryResult<Variant> result = variantStorageManager.get(query, QueryOptions.empty(), token);
            if (CollectionUtils.isNotEmpty(result.getResult())) {
                variants.addAll(result.getResult());
            }
        }

        return variants;
    }

    private static File getFile(Path path) {
        File file = null;
        if (path.toFile().exists()) {
            file = path.toFile();
        } else if (Paths.get(path.toString() + ".gz").toFile().exists()) {
            file = Paths.get(path.toString() + ".gz").toFile();
        }

        return file;
    }

    public static Map<String, ClinicalProperty.RoleInCancer> getRoleInCancer(String opencgaHome) throws IOException {
        // Load role in cancer, if presents
        java.nio.file.Path path = Paths.get(opencgaHome + "/analysis/resources/roleInCancer.txt");
        return InterpretationAnalysisUtils.loadRoleInCancer(path);

    }

    public static Map<String, Map<String, List<String>>> getActionableVariantsByAssembly(String opencgaHome) throws IOException {
        // Load actionable variants for each assembly, if present
        // First, read all actionableVariants filenames, actionableVariants_xxx.txt[.gz] where xxx = assembly in lower case
        Map<String, Map<String, List<String>>> actionableVariantsByAssembly = new HashMap<>();
        java.io.File folder = Paths.get(opencgaHome + "/analysis/resources/").toFile();
        java.io.File[] files = folder.listFiles();
        if (ArrayUtils.isNotEmpty(files)) {
            for (java.io.File file : files) {
                if (file.isFile() && file.getName().startsWith("actionableVariants_")) {
                    String[] split = file.getName().split("[_\\.]");
                    if (split.length > 1) {
                        String assembly = split[1].toLowerCase();
                        actionableVariantsByAssembly.put(assembly.toLowerCase(),
                                InterpretationAnalysisUtils.loadActionableVariants(file.toPath()));
                    }
                }
            }
        }
        return actionableVariantsByAssembly;
    }

    public static String getAssembly(CatalogManager catalogManager, String studyStr, String sessionId) {
        String assembly = "";
        QueryResult<Project> projectQueryResult;
        try {
            projectQueryResult = catalogManager.getProjectManager().get(
                    new Query(ProjectDBAdaptor.QueryParams.STUDY.key(), studyStr),
                    new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.ORGANISM.key()), sessionId);
            if (CollectionUtils.isNotEmpty(projectQueryResult.getResult())) {
                assembly = projectQueryResult.first().getOrganism().getAssembly();
            }
        } catch (CatalogException e) {
            e.printStackTrace();
        }
        if (org.apache.commons.lang3.StringUtils.isNotEmpty(assembly)) {
            assembly = assembly.toLowerCase();
        }
        return assembly;
    }

}
