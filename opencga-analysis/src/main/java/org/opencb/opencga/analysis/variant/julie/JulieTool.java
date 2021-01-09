package org.opencb.opencga.analysis.variant.julie;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.models.operations.variant.JulieParams;
import org.opencb.opencga.core.tools.variant.JulieToolExecutor;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Tool(id= JulieTool.ID,
        resource = Enums.Resource.VARIANT,
        type = Tool.Type.OPERATION,
        scope = Tool.Scope.PROJECT,
        description = JulieTool.DESCRIPTION)
public class JulieTool extends OpenCgaTool {

    public static final String ID = "julie";
    public static final String DESCRIPTION = "Transform VariantStats into PopulationFrequency values and updates the VariantAnnotation.";

    private JulieParams params = new JulieParams();
    private Map<String, List<String>> cohorts;

    @Override
    protected void check() throws Exception {
        String project = getParams().getString(ParamConstants.PROJECT_PARAM);

        if (StringUtils.isEmpty(project)) {
            String userId = getCatalogManager().getUserManager().getUserId(getToken());
            User user = catalogManager.getUserManager().get(userId, null, getToken()).first();
            if (CollectionUtils.isEmpty(user.getProjects()) || user.getProjects().size() > 1) {
                throw new CatalogException("Missing '" + ParamConstants.PROJECT_PARAM + "' parameter");
            } else {
                project = user.getProjects().get(0).getFqn();
            }
        }
        setUpStorageEngineExecutorByProjectId(project);

        params.updateParams(getParams());

        cohorts = new HashMap<>();
        if (CollectionUtils.isNotEmpty(params.getCohorts())) {
            for (String value : params.getCohorts()) {
                int idx = value.lastIndexOf(":");
                if (idx < 0) {
                    throw new IllegalArgumentException("Error parsing cohort parameter. Expected \"{studyId}:{cohortId}\"");
                }
                String study = value.substring(0, idx);
                String cohort = value.substring(idx + 1);
                String studyFqn = getCatalogManager().getStudyManager()
                        .get(study, new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.FQN.key()), getToken())
                        .first()
                        .getFqn();
                cohorts.computeIfAbsent(studyFqn, key -> new LinkedList<>()).add(cohort);
            }
            for (Map.Entry<String, List<String>> entry : cohorts.entrySet()) {
                String study = entry.getKey();
                List<Cohort> cohorts = getCatalogManager().getCohortManager()
                        .get(study, entry.getValue(), new QueryOptions(QueryOptions.INCLUDE, "id"), getToken()).getResults();
                if (cohorts.size() != entry.getValue().size()) {
                    // This should not happen. The previous command should throw exception if some cohort is missing
                    throw new IllegalArgumentException("Missing cohorts from study " + study);
                }
            }
        }
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            JulieToolExecutor toolExecutor = getToolExecutor(JulieToolExecutor.class);
            if (!cohorts.isEmpty()) {
                toolExecutor.setCohorts(cohorts);
            }
            toolExecutor.setOverwrite(params.getOverwrite());
            toolExecutor.setRegion(params.getRegion());
            toolExecutor.execute();
        });
    }
}
