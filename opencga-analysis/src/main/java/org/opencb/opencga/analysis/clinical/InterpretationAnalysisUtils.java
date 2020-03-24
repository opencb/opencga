package org.opencb.opencga.analysis.clinical;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.clinical.interpretation.exceptions.InterpretationAnalysisException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
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
                    try {
                        Variant variant = new VariantBuilder(split[0], Integer.parseInt(split[1]), Integer.parseInt(split[2]), split[3],
                                split[4]).build();
                        actionableVariants.put(variant.toString(), phenotypes);
                    } catch (NumberFormatException e) {
                        // Skip this variant
                        System.err.println("Skip actionable variant: " + line + "\nCause: " + e.getMessage());
                    }
                } else {
                    // Skip this variant
                    System.err.println("Skip actionable variant, invalid format: " + line);
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

    public static List<Variant> secondaryFindings(String study, List<String> sampleNames, Set<String> actionableVariants,
                                                  List<String> excludeIds, VariantStorageManager variantStorageManager, String token)
            throws CatalogException, IOException, StorageEngineException, InterpretationAnalysisException {
        // Sanity check
        if (CollectionUtils.isEmpty(sampleNames)) {
            throw new InterpretationAnalysisException("Missing study when retrieving secondary findings");
        }
        if (CollectionUtils.isEmpty(sampleNames)) {
            throw new InterpretationAnalysisException("Missing sample names when retrieving secondary findings");
        }

        final int batchSize = 1000;
        List<Variant> variants = new ArrayList<>();

        Set<String> excludeSet = new HashSet<>();
        if (CollectionUtils.isNotEmpty(excludeIds)) {
            excludeSet.addAll(excludeIds);
        }

        // Prepare query
        Query query = new Query();
        query.put(VariantQueryParam.STUDY.key(), study);
        query.put(VariantQueryParam.SAMPLE.key(), org.apache.commons.lang3.StringUtils.join(sampleNames, ","));

        List<String> ids = new ArrayList<>();
        Iterator<String> iterator = actionableVariants.iterator();
        while (iterator.hasNext()) {
            String id = iterator.next();
            ids.add(id);
            if (ids.size() >= batchSize) {
                query.put(VariantQueryParam.ID.key(), ids);
                VariantQueryResult<Variant> result = variantStorageManager.get(query, QueryOptions.empty(), token);
                addVariant(result, excludeSet, variants);

                // Reset
                ids.clear();
            }
        }

        if (ids.size() > 0) {
            query.put(VariantQueryParam.ID.key(), ids);
            VariantQueryResult<Variant> result = variantStorageManager.get(query, QueryOptions.empty(), token);
            addVariant(result, excludeSet, variants);
        }

        return variants;
    }

    private static void addVariant(VariantQueryResult<Variant> result, Set<String> excludeIds, List<Variant> variants) {
        if (CollectionUtils.isNotEmpty(result.getResult())) {
            for (Variant variant : result.getResult()) {
                if (!excludeIds.contains(variant.getId())) {
                    variants.add(variant);
                }
            }
        }
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

    public static Map<String, ClinicalProperty.RoleInCancer> getRoleInCancer(Path opencgaHome) throws IOException {
        // Load role in cancer, if presents
        java.nio.file.Path path = opencgaHome.resolve("/analysis/resources/roleInCancer.txt");
        return InterpretationAnalysisUtils.loadRoleInCancer(path);

    }

    public static Map<String, Map<String, List<String>>> getActionableVariantsByAssembly(Path opencgaHome) throws IOException {
        // Load actionable variants for each assembly, if present
        // First, read all actionableVariants filenames, actionableVariants_xxx.txt[.gz] where xxx = assembly in lower case
        Map<String, Map<String, List<String>>> actionableVariantsByAssembly = new HashMap<>();
        java.io.File folder = opencgaHome.resolve("/analysis/resources/").toFile();
        java.io.File[] files = folder.listFiles();
        if (files != null) {
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
