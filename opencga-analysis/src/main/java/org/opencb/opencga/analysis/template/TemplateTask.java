package org.opencb.opencga.analysis.template;

import org.apache.solr.common.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.template.manager.TemplateManager;
import org.opencb.opencga.analysis.template.manager.config.TemplateManifest;
import org.opencb.opencga.analysis.variant.operations.OperationTool;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.TemplateParams;
import org.opencb.opencga.core.tools.annotations.Tool;

import java.nio.file.Path;
import java.nio.file.Paths;

@Tool(id = TemplateTask.ID, description = TemplateTask.DESCRIPTION, type = Tool.Type.OPERATION, resource = Enums.Resource.STUDY,
        scope = Tool.Scope.STUDY)
public class TemplateTask extends OperationTool {

    public static final String ID = "template";
    public static final String DESCRIPTION = "Load the data described in the templates in OpenCGA.";

    private Path path;
    private TemplateManifest manifest;

    private TemplateManager templateManager;

    @Override
    protected void check() throws Exception {
        super.check();

        TemplateParams templateParams = TemplateParams.fromParams(TemplateParams.class, params);
        if (StringUtils.isEmpty(templateParams.getId())) {
            throw new IllegalArgumentException("Missing template id");
        }

        templateManager = new TemplateManager(catalogManager, templateParams.isResume(), templateParams.isOverwrite(), token);

        String studyFqn = getStudyFqn();
        Study study = catalogManager.getStudyManager().get(studyFqn,
                new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.URI.key()), token).first();

        // We obtain the basic studyPath where we will upload the file temporarily
        java.nio.file.Path studyPath = Paths.get(study.getUri());
        path = studyPath.resolve("OPENCGA").resolve("TEMPLATE").resolve(templateParams.getId());

        if (!path.toFile().exists() || !path.toFile().isDirectory()) {
            throw new IllegalArgumentException("Template '" + templateParams.getId() + "' not found");
        }
        if (!path.resolve("manifest.yml").toFile().exists()) {
            throw new IllegalArgumentException("Cannot find manifest.yml file for template '" + templateParams.getId() + "'.");
        }
        manifest = TemplateManifest.load(path.resolve("manifest.yml"));

        // TODO: Validate manifest file
        templateManager.validate(manifest);
    }

    @Override
    protected void run() throws Exception {
        templateManager.execute(manifest, path);
    }
}
