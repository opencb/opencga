package org.opencb.opencga.core.cellbase;

import io.jsonwebtoken.JwtException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.cellbase.core.config.SpeciesConfiguration;
import org.opencb.cellbase.core.config.SpeciesProperties;
import org.opencb.cellbase.core.models.DataRelease;
import org.opencb.cellbase.core.result.CellBaseDataResponse;
import org.opencb.cellbase.core.token.DataAccessTokenManager;
import org.opencb.cellbase.core.token.DataAccessTokenSources;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.VersionUtils;
import org.opencb.opencga.core.config.storage.CellBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class CellBaseValidator {

    private static Logger logger = LoggerFactory.getLogger(CellBaseValidator.class);
    protected CellBaseClient cellBaseClient;
    protected String assembly;
    private String serverVersion;


    public CellBaseValidator(CellBaseClient cellBaseClient) {
        this(cellBaseClient, cellBaseClient.getAssembly());
    }

    public CellBaseValidator(CellBaseClient cellBaseClient, String assembly) {
        this.cellBaseClient = cellBaseClient;
        this.assembly = assembly;
    }

    public CellBaseValidator(CellBaseConfiguration cellBaseConfiguration, String species, String assembly) {
        this.cellBaseClient = newCellBaseClient(cellBaseConfiguration, species, assembly);
        this.assembly = assembly;
    }

    private CellBaseClient newCellBaseClient(CellBaseConfiguration cellBaseConfiguration, String species, String assembly) {
        return new CellBaseClient(
                toCellBaseSpeciesName(species),
                assembly,
                cellBaseConfiguration.getDataRelease(),
                cellBaseConfiguration.getToken(),
                cellBaseConfiguration.toClientConfiguration());
    }

    public static String toCellBaseSpeciesName(String scientificName) {
        if (scientificName != null && scientificName.contains(" ")) {
            String[] split = scientificName.split(" ", 2);
            scientificName = (split[0].charAt(0) + split[1]).toLowerCase();
        }
        return scientificName;
    }

    public String getAssembly() {
        return assembly;
    }

    public String getSpecies() {
        return cellBaseClient.getSpecies();
    }

    public String getDataRelease() {
        return cellBaseClient.getDataRelease();
    }

    public String getToken() {
        return cellBaseClient.getToken();
    }

    public DataAccessTokenSources getTokenSources() {
        DataAccessTokenManager tokenManager = new DataAccessTokenManager();
        return tokenManager.decode(cellBaseClient.getToken());
    }

    public String getURL() {
        return cellBaseClient.getClientConfiguration().getRest().getHosts().get(0);
    }

    public String getVersion() {
        return cellBaseClient.getClientConfiguration().getVersion();
    }

    public CellBaseClient getCellBaseClient() {
        return cellBaseClient;
    }

    public CellBaseConfiguration getCellBaseConfiguration() {
        return new CellBaseConfiguration(getURL(), getVersion(), getDataRelease(), getToken());
    }

    public String getLatestActiveDataRelease() throws IOException {
        if (supportsDataRelease()) {
            Optional<DataRelease> dataRelease = cellBaseClient.getMetaClient().dataReleases()
                    .allResults()
                    .stream()
                    .filter(DataRelease::isActive)
                    .max(Comparator.comparing(DataRelease::getDate));
            if (!dataRelease.isPresent()) {
                throw new IllegalArgumentException("No active data releases found on cellbase " + this);
            } else {
                return String.valueOf(dataRelease.get().getRelease());
            }
        } else {
            return null;
        }
    }

    public static CellBaseConfiguration validate(CellBaseConfiguration configuration, String species, String assembly, boolean autoComplete)
            throws IOException {
        return new CellBaseValidator(configuration, species, assembly).validate(autoComplete);
    }

    public void validate() throws IOException {
        validate(false);
    }

    private CellBaseConfiguration validate(boolean autoComplete) throws IOException {
        CellBaseConfiguration cellBaseConfiguration = getCellBaseConfiguration();
        String inputVersion = getVersion();
        CellBaseDataResponse<SpeciesProperties> species;
        try {
            species = cellBaseClient.getMetaClient().species();
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Unable to access cellbase url '" + getURL() + "', version '" + inputVersion + "'", e);
        }
        if (species == null || species.firstResult() == null) {
            if (autoComplete && !cellBaseConfiguration.getVersion().startsWith("v")) {
                // Version might be missing the starting "v"
                cellBaseConfiguration.setVersion("v" + cellBaseConfiguration.getVersion());
                cellBaseClient = newCellBaseClient(cellBaseConfiguration, getSpecies(), getAssembly());
                species = cellBaseClient.getMetaClient().species();
            }
        }
        if (species == null || species.firstResult() == null) {
            throw new IllegalArgumentException("Unable to access cellbase url '" + getURL() + "', version '" + inputVersion + "'");
        }
        validateSpeciesAssembly(species.firstResult());

        String serverVersion = getVersionFromServer();
        if (!supportsDataRelease(serverVersion)) {
            logger.warn("DataRelease not supported on version '" + serverVersion + ".x'");
        } else {
            String dataRelease = getDataRelease();
            if (dataRelease == null) {
                if (autoComplete) {
                    cellBaseConfiguration.setDataRelease(getLatestActiveDataRelease());
                } else {
                    throw new IllegalArgumentException("Missing DataRelease for cellbase "
                            + "url: '" + getURL() + "'"
                            + ", version: '" + inputVersion
                            + "', species: '" + getSpecies()
                            + "', assembly: '" + getAssembly() + "'");
                }
            } else {
                boolean dataReleaseExists = cellBaseClient.getMetaClient().dataReleases()
                        .allResults()
                        .stream()
                        .anyMatch(dr -> {
                            return dataRelease.equals(String.valueOf(dr.getRelease()));
                        });
                if (!dataReleaseExists) {
                    throw new IllegalArgumentException("DataRelease '" + dataRelease + "' not found on cellbase "
                            + "url: '" + getURL() + "'"
                            + ", version: '" + inputVersion
                            + "', species: '" + getSpecies()
                            + "', assembly: '" + getAssembly() + "'");
                }
            }
        }
        String token = getToken();
        if (StringUtils.isEmpty(token)) {
            cellBaseConfiguration.setToken(null);
        } else {
            // Check it's supported
            if (!supportsToken(serverVersion)) {
                throw new IllegalArgumentException("Token not supported for cellbase "
                        + "url: '" + getURL() + "'"
                        + ", version: '" + inputVersion + "'");
            }

            // Check it's an actual token
            DataAccessTokenManager tokenManager = new DataAccessTokenManager();
            try {
                tokenManager.decode(token);
            } catch (JwtException e) {
                throw new IllegalArgumentException("Malformed token for cellbase "
                        + "url: '" + getURL() + "'"
                        + ", version: '" + inputVersion
                        + "', species: '" + getSpecies()
                        + "', assembly: '" + getAssembly() + "'");
            }

            // Check it's a valid token
            CellBaseDataResponse<VariantAnnotation> response = cellBaseClient.getVariantClient()
                    .getAnnotationByVariantIds(Collections.singletonList("1:1:N:C"), new QueryOptions(), true);
            if (response.firstResult() == null) {
                throw new IllegalArgumentException("Invalid token for cellbase "
                        + "url: '" + getURL() + "'"
                        + ", version: '" + inputVersion
                        + "', species: '" + getSpecies()
                        + "', assembly: '" + getAssembly() + "'");
            }
        }

        return cellBaseConfiguration;
    }

    private void validateSpeciesAssembly(SpeciesProperties speciesProperties) {
        for (SpeciesConfiguration sc : speciesProperties.getVertebrates()) {
            if (sc.getId().equals(getSpecies())) {
                List<String> assemblies = new ArrayList<>();
                for (SpeciesConfiguration.Assembly scAssembly : sc.getAssemblies()) {
                    assemblies.add(scAssembly.getName());
                    if (scAssembly.getName().equalsIgnoreCase(getAssembly())) {
                        return;
                    }
                }
                throw new IllegalArgumentException("Assembly '" + getAssembly() + "' not found in cellbase "
                        + "url: '" + getURL() + "'"
                        + ", version: '" + getVersion()
                        + "', species: '" + getSpecies() + ". Supported assemblies : " + assemblies);
            }
        }
        throw new IllegalArgumentException("Species '" + getSpecies() + "' not found in cellbase "
                + "url: '" + getURL() + "'"
                + ", version: '" + getVersion());
    }

    public boolean supportsDataRelease() throws IOException {
        return supportsDataRelease(getVersionFromServer());
    }

    public static boolean supportsDataRelease(String serverVersion) {
        // Data Release support starts at versio 5.1.0
        return VersionUtils.isMinVersion("5.1.0", serverVersion);
    }

    public static boolean supportsToken(String serverVersion) {
        // Tokens support starts at version 5.4.0
        return VersionUtils.isMinVersion("5.4.0", serverVersion);
    }

    public String getVersionFromServerMajor() throws IOException {
        return major(getVersionFromServer());
    }

    public String getVersionFromServerMajorMinor() throws IOException {
        String serverVersion = getVersionFromServer();
        serverVersion = majorMinor(serverVersion);
        return serverVersion;
    }

    private static String major(String version) {
        return version.split("\\.")[0];
    }

    private static String majorMinor(String version) {
        String[] split = version.split("\\.");
        if (split.length > 1) {
            version = split[0] + "." + split[1];
        }
        return version;
    }

    public String getVersionFromServer() throws IOException {
        if (serverVersion == null) {
            synchronized (this) {
                String serverVersion = cellBaseClient.getMetaClient().about().firstResult().getString("Version");
                if (StringUtils.isEmpty(serverVersion)) {
                    serverVersion = cellBaseClient.getMetaClient().about().firstResult().getString("Version: ");
                }
                this.serverVersion = serverVersion;
            }
        }
        return serverVersion;
    }

    public boolean isMinVersion(String minVersion) throws IOException {
        String serverVersion = getVersionFromServer();
        return VersionUtils.isMinVersion(minVersion, serverVersion);
    }

    @Override
    public String toString() {
        return "URL: '" + getURL() + "', "
                + "version '" + getVersion() + "', "
                + "species '" + getSpecies() + "', "
                + "assembly '" + getAssembly() + "', "
                + "dataRelease '" + getDataRelease() + "', "
                + "token '" + getToken() + "'";

    }
}
