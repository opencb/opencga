package org.opencb.opencga.analysis.clinical;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
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

    public static List<Variant> queryActionableVariants(Query query, Set<String> actionableVariants,
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
}
