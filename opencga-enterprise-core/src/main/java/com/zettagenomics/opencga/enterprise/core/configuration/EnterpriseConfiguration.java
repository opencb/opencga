package com.zettagenomics.opencga.enterprise.core.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class EnterpriseConfiguration {

    private SsoConfiguration sso;
    private CvdbConfiguration cvdb;

    private static final String DEFAULT_CONFIGURATION_FORMAT = "YAML";

    private static Logger logger;

    static {
        logger = LoggerFactory.getLogger(EnterpriseConfiguration.class);
    }

    public EnterpriseConfiguration() {
        this.sso = new SsoConfiguration();
        this.cvdb = new CvdbConfiguration();
    }


    public void serialize(OutputStream configurationOututStream) throws IOException {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        yamlMapper.writerWithDefaultPrettyPrinter().writeValue(configurationOututStream, this);
    }

    public static EnterpriseConfiguration load(Path opencgaHome) {
        // Read enterprise configuration file
        Path configDirPath = opencgaHome.resolve("conf");
        InputStream configInputStream;
        try {
            String confPath = configDirPath.toFile().getAbsolutePath()  + "/enterprise-configuration.yml";
            logger.info("Reading enterprise-configuration.yml file: '{}'", confPath);
            configInputStream = Files.newInputStream(Paths.get(confPath));
            return EnterpriseConfiguration.load(configInputStream);
        } catch (IOException e) {
            logger.error("Could not load enterprise-configuration.yml file");
            throw new RuntimeException(e);
        }
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

        overwriteWithEnvironmentVariables(enterpriseConfiguration);
        return enterpriseConfiguration;
    }

    private static void overwriteWithEnvironmentVariables(EnterpriseConfiguration configuration) {
        Map<String, String> envVariables = System.getenv();
        for (String variable : envVariables.keySet()) {
            if (variable.startsWith("ENTERPRISE_")) {
                logger.debug("Overwriting environment parameter '{}'", variable);
                String value = envVariables.get(variable);
                switch (variable) {
                    case "ENTERPRISE_SSO_ACTIVE":
                        configuration.getSso().setActive(Boolean.parseBoolean(value));
                        break;
                    case "ENTERPRISE_SSO_CAS_SERVER_PREFIX_URL":
                        configuration.getSso().setCasServerPrefixUrl(value);
                        break;
                    case "ENTERPRISE_SSO_SERVER_NAME":
                        configuration.getSso().setServerName(value);
                        break;
                    case "ENTERPRISE_SSO_PROTOCOL":
                        configuration.getSso().setProtocol(value);
                        break;
                    case "ENTERPRISE_SSO_ATTRIBUTES_NAME":
                        if (configuration.getSso().getAttributes() != null) {
                            configuration.getSso().getAttributes().setName(value);
                        } else {
                            configuration.getSso().setAttributes(new SsoPrincipalAttributesConfiguration().setName(value));
                        }
                        break;
                    case "ENTERPRISE_SSO_ATTRIBUTES_SURNAME":
                        if (configuration.getSso().getAttributes() != null) {
                            configuration.getSso().getAttributes().setSurname(value);
                        } else {
                            configuration.getSso().setAttributes(new SsoPrincipalAttributesConfiguration().setSurname(value));
                        }
                        break;
                    case "ENTERPRISE_SSO_ATTRIBUTES_EMAIL":
                        if (configuration.getSso().getAttributes() != null) {
                            configuration.getSso().getAttributes().setEmail(value);
                        } else {
                            configuration.getSso().setAttributes(new SsoPrincipalAttributesConfiguration().setEmail(value));
                        }
                        break;
                    case "ENTERPRISE_SSO_ATTRIBUTES_ORGANIZATION":
                        if (configuration.getSso().getAttributes() != null) {
                            configuration.getSso().getAttributes().setOrganization(value);
                        } else {
                            configuration.getSso().setAttributes(new SsoPrincipalAttributesConfiguration().setOrganization(value));
                        }
                        break;
                    case "ENTERPRISE_SSO_ATTRIBUTES_GROUPS":
                        if (configuration.getSso().getAttributes() != null) {
                            configuration.getSso().getAttributes().setGroups(value);
                        } else {
                            configuration.getSso().setAttributes(new SsoPrincipalAttributesConfiguration().setGroups(value));
                        }
                        break;
                    default:
                        break;
                }
            }
        }
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

    public CvdbConfiguration getCvdb() {
        return cvdb;
    }

    public EnterpriseConfiguration setCvdb(CvdbConfiguration cvdb) {
        this.cvdb = cvdb;
        return this;
    }
}
