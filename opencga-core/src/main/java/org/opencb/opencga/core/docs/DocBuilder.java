package org.opencb.opencga.core.docs;

import org.opencb.commons.docs.DocParser;
import org.opencb.commons.docs.config.DocConfiguration;
import org.opencb.commons.docs.doc.DocFactory;
import org.opencb.commons.utils.GitRepositoryState;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DocBuilder {


    private static final Logger LOG = LoggerFactory.getLogger(DocBuilder.class);

    public static void main(String[] args) {
        DocConfiguration config = new DocConfiguration();
        List<Class> classes = new ArrayList();
        classes.add(Sample.class);
        classes.add(Individual.class);
        classes.add(Family.class);
        classes.add(Cohort.class);
        classes.add(File.class);
        classes.add(ClinicalAnalysis.class);
        classes.add(Job.class);
        classes.add(Study.class);
        classes.add(Project.class);
        classes.add(User.class);

        config.setDocClasses(classes);
        config.setType(DocFactory.DocFactoryType.MARKDOWN);
        config.setOutputDir("/workspace/opencga/docs/data-models/");
        config.setGithubServerURL("https://github.com/opencb/opencga/tree/"
                + GitRepositoryState.get().getBranch() + "/opencga-core");
        config.setJsondir("/workspace/opencga/opencga-core/src/main/resources/doc/json");
        config.setGitbookServerURL("https://docs.opencga.opencb.org/data-models");
        try {
            DocParser parser = new DocParser();
            parser.parse(config);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
