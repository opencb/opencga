package com.zettagenomics.opencga.enterprise.core.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class EnterpriseConfiguration {

    private SsoConfiguration sso;

    private static final String DEFAULT_CONFIGURATION_FORMAT = "YAML";


    public EnterpriseConfiguration() {
        sso = new SsoConfiguration();
    }


    public void serialize(OutputStream configurationOututStream) throws IOException {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        yamlMapper.writerWithDefaultPrettyPrinter().writeValue(configurationOututStream, this);
    }

    public static EnterpriseConfiguration load(InputStream configurationInputStream) throws IOException {
        return load(configurationInputStream, DEFAULT_CONFIGURATION_FORMAT);
    }

    public static EnterpriseConfiguration load(InputStream configurationInputStream, String format) throws IOException {
        if (configurationInputStream == null) {
            throw new IOException("EnterpriseConfiguration file not found");
        }

        EnterpriseConfiguration enterpriseConfiguration;
        ObjectMapper objectMapper;
        try {
            switch (format.toUpperCase()) {
                case "JSON":
                    objectMapper = new ObjectMapper();
                    enterpriseConfiguration = objectMapper.readValue(configurationInputStream, EnterpriseConfiguration.class);
                    break;
                case "YML":
                case "YAML":
                default:
                    objectMapper = new ObjectMapper(new YAMLFactory());
                    enterpriseConfiguration = objectMapper.readValue(configurationInputStream, EnterpriseConfiguration.class);
                    break;
            }
        } catch (IOException e) {
            throw new IOException("EnterpriseConfiguration file could not be parsed: " + e.getMessage(), e);
        }

        return enterpriseConfiguration;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("EnterpriseConfiguration{");
        sb.append("ssoConfiguration=").append(sso);
        sb.append('}');
        return sb.toString();
    }

    public SsoConfiguration getSso() {
        return sso;
    }

    public EnterpriseConfiguration setSso(SsoConfiguration sso) {
        this.sso = sso;
        return this;
    }
}
