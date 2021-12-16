package org.opencb.opencga.core.docs;

import org.opencb.commons.docs.DocParser;
import org.opencb.commons.docs.config.DocConfiguration;
import org.opencb.commons.docs.factories.DocFactory;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.models.sample.Sample;
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
        config.setDocClasses(classes);
        config.setType(DocFactory.DocFactoryType.MARKDOWN);
        config.setOutputDir("/workspace/opencga/docs/data-models/");
        config.setGithubServer("https://github.com/opencb/opencga/tree/"
                + GitRepositoryState.get().getBranch() + "/opencga-core/");
        config.setJsondir("/workspace/opencga/opencga-core/src/main/resources/doc/json");
        try {
            DocParser parser = new DocParser();
            parser.parse(config);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
