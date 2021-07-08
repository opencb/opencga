package org.opencb.opencga.catalog.templates.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.nio.file.Path;

public class TemplateManifest {

    private TemplateConfiguration configuration;
    private TemplateStudy study;

    public TemplateManifest() {
    }

    public TemplateManifest(TemplateConfiguration configuration, TemplateStudy study) {
        this.configuration = configuration;
        this.study = study;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TemplateManifest{");
        sb.append("configuration=").append(configuration);
        sb.append(", study=").append(study);
        sb.append('}');
        return sb.toString();
    }

    public static TemplateManifest load(Path manifestPath) throws IOException {
        manifestPath = manifestPath.toAbsolutePath();
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        TemplateManifest templateConfiguration;
        try {
            templateConfiguration = objectMapper.readValue(manifestPath.toFile(), TemplateManifest.class);
        } catch (IOException e) {
            // Enrich IOException with fileName
            throw new IOException("Error parsing main template manifest file '" + manifestPath + "'", e);
        }

        return templateConfiguration;
    }

    public TemplateConfiguration getConfiguration() {
        return configuration;
    }

    public TemplateManifest setConfiguration(TemplateConfiguration configuration) {
        this.configuration = configuration;
        return this;
    }

    public TemplateStudy getStudy() {
        return study;
    }

    public TemplateManifest setStudy(TemplateStudy study) {
        this.study = study;
        return this;
    }
}
