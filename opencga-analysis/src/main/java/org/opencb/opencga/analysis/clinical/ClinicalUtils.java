package org.opencb.opencga.analysis.clinical;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.clinical.interpretation.exceptions.InterpretationAnalysisException;
import org.opencb.biodata.models.clinical.pedigree.Member;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.Family;
import org.opencb.opencga.core.models.Individual;
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

public class ClinicalUtils {

    @Deprecated
    private static File getFile(Path path) {
        File file = null;
        if (path.toFile().exists()) {
            file = path.toFile();
        } else if (Paths.get(path.toString() + ".gz").toFile().exists()) {
            file = Paths.get(path.toString() + ".gz").toFile();
        }

        return file;
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



    public static void addVariant(VariantQueryResult<Variant> result, Set<String> excludeIds, List<Variant> variants) {
        if (CollectionUtils.isNotEmpty(result.getResult())) {
            for (Variant variant : result.getResult()) {
                if (!excludeIds.contains(variant.getId())) {
                    variants.add(variant);
                }
            }
        }
    }


    public static Map<String, ClinicalProperty.RoleInCancer> getRoleInCancer(String opencgaHome) throws IOException {
        // Load role in cancer, if presents
        java.nio.file.Path path = Paths.get(opencgaHome + "/analysis/resources/roleInCancer.txt");
        return ClinicalUtils.loadRoleInCancer(path);

    }


    // OpenCgaClinicalAnalysis
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


    public static void removeMembersWithoutSamples(Pedigree pedigree, Family family) {
        Set<String> membersWithoutSamples = new HashSet<>();
        for (Individual member : family.getMembers()) {
            if (ListUtils.isEmpty(member.getSamples())) {
                membersWithoutSamples.add(member.getId());
            }
        }

        Iterator<Member> iterator = pedigree.getMembers().iterator();
        while (iterator.hasNext()) {
            Member member = iterator.next();
            if (membersWithoutSamples.contains(member.getId())) {
                iterator.remove();
            } else {
                if (member.getFather() != null && membersWithoutSamples.contains(member.getFather().getId())) {
                    member.setFather(null);
                }
                if (member.getMother() != null && membersWithoutSamples.contains(member.getMother().getId())) {
                    member.setMother(null);
                }
            }
        }

        if (pedigree.getProband().getFather() != null && membersWithoutSamples.contains(pedigree.getProband().getFather().getId())) {
            pedigree.getProband().setFather(null);
        }
        if (pedigree.getProband().getMother() != null && membersWithoutSamples.contains(pedigree.getProband().getMother().getId())) {
            pedigree.getProband().setMother(null);
        }
    }

}
