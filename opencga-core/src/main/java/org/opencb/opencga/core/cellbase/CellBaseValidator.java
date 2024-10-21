package org.opencb.opencga.core.cellbase;

import io.jsonwebtoken.JwtException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.cellbase.core.api.key.ApiKeyManager;
import org.opencb.cellbase.core.config.SpeciesConfiguration;
import org.opencb.cellbase.core.config.SpeciesProperties;
import org.opencb.cellbase.core.models.DataRelease;
import org.opencb.cellbase.core.result.CellBaseDataResponse;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.VersionUtils;
import org.opencb.opencga.core.config.storage.CellBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;


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
                cellBaseConfiguration.getApiKey(),
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

    public String getApiKey() {
        return cellBaseClient.getApiKey();
    }

    public List<String> getApiKeyDataSources() {
        ApiKeyManager apiKeyManager = new ApiKeyManager();
        return new ArrayList<>(apiKeyManager.getValidSources(cellBaseClient.getApiKey()));
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
        return new CellBaseConfiguration(getURL(), getVersion(), getDataRelease(), getApiKey());
    }

    public String getDefaultDataRelease() throws IOException {
        if (supportsDataRelease()) {
            List<DataRelease> dataReleases = cellBaseClient.getMetaClient().dataReleases()
                    .allResults();
            DataRelease dataRelease = null;
            if (supportsDataReleaseActiveByDefaultIn()) {
                // ActiveByDefault versions are stored in form `v<major>.<minor>` , i.e. v5.5 , v5.7, ...
                String majorMinor = "v" + getVersionFromServerMajorMinor();
                List<DataRelease> drs = dataReleases
                        .stream()
                        .filter(dr -> dr.getActiveByDefaultIn() != null && dr.getActiveByDefaultIn().contains(majorMinor))
                        .collect(Collectors.toList());
                if (drs.size() == 1) {
                    dataRelease = drs.get(0);
                } else if (drs.size() > 1) {
                    throw new IllegalArgumentException("More than one default active data releases found on cellbase " + this);
                }
            } else {
                dataRelease = dataReleases
                        .stream()
                        .filter(DataRelease::isActive)
                        .max(Comparator.comparing(DataRelease::getDate)).orElse(null);
            }
            if (dataRelease == null) {
                throw new IllegalArgumentException("No active data releases found on cellbase " + this);
            } else {
                return String.valueOf(dataRelease.getRelease());
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
        SpeciesProperties species;
        try {
            species = retryMetaSpecies();
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Unable to access cellbase url '" + getURL() + "', version '" + inputVersion + "'", e);
        }
        if (species == null) {
            if (autoComplete && !cellBaseConfiguration.getVersion().startsWith("v")) {
                // Version might be missing the starting "v"
                cellBaseConfiguration.setVersion("v" + cellBaseConfiguration.getVersion());
                cellBaseClient = newCellBaseClient(cellBaseConfiguration, getSpecies(), getAssembly());
                species = retryMetaSpecies();
            }
        }
        if (species == null) {
            throw new IllegalArgumentException("Unable to access cellbase url '" + getURL() + "', version '" + inputVersion + "'");
        }
        validateSpeciesAssembly(species);

        String serverVersion = getVersionFromServer();
        if (!supportsDataRelease(serverVersion)) {
            logger.warn("DataRelease not supported on version '" + serverVersion + ".x'");
        } else {
            String dataRelease = getDataRelease();
            if (StringUtils.isEmpty(dataRelease)) {
                if (autoComplete) {
                    cellBaseConfiguration.setDataRelease(getDefaultDataRelease());
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

        String apiKey = getApiKey();
        if (StringUtils.isEmpty(apiKey)) {
            cellBaseConfiguration.setApiKey(null);
        } else {
            // Check it's supported
            if (!supportsApiKey(serverVersion)) {
                throw new IllegalArgumentException("API key not supported for cellbase "
                        + "url: '" + getURL() + "'"
                        + ", version: '" + inputVersion + "'");
            }

            // Check it's an actual API key
            ApiKeyManager apiKeyManager = new ApiKeyManager();
            try {
                apiKeyManager.decode(apiKey);
            } catch (JwtException e) {
                throw new IllegalArgumentException("Malformed API key for cellbase "
                        + "url: '" + getURL() + "'"
                        + ", version: '" + inputVersion
                        + "', species: '" + getSpecies()
                        + "', assembly: '" + getAssembly() + "'");
            }

            // Check it's a valid API key
            CellBaseDataResponse<VariantAnnotation> response = cellBaseClient.getVariantClient()
                    .getAnnotationByVariantIds(Collections.singletonList("1:1:N:C"), new QueryOptions(), true);
            if (response.firstResult() == null) {
                throw new IllegalArgumentException("Invalid API key for cellbase "
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
        // Data Release support starts at version 5.1.0
        return VersionUtils.isMinVersion("5.1.0", serverVersion);
    }

    public boolean supportsDataReleaseActiveByDefaultIn() throws IOException {
        return supportsDataReleaseActiveByDefaultIn(getVersionFromServer());
    }

    public static boolean supportsDataReleaseActiveByDefaultIn(String serverVersion) {
        // Data Release Default Active In Version support starts at version 5.5.0 , TASK-4157
        return VersionUtils.isMinVersion("5.5.0", serverVersion, true);
    }

    public static boolean supportsApiKey(String serverVersion) {
        // API keys support starts at version 5.4.0
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
//        return String.valueOf(new VersionUtils.Version(version).getMajor());
        return version.split("\\.")[0];
    }

    private static String majorMinor(String version) {
//        VersionUtils.Version v = new VersionUtils.Version(version);
//        return v.getMajor() + "." + v.getMinor();
        String[] split = version.split("\\.");
        if (split.length > 1) {
            version = split[0] + "." + split[1];
        }
        return version;
    }

    public String getVersionFromServer() throws IOException {
        if (serverVersion == null) {
            synchronized (this) {
                ObjectMap result = retryMetaAbout();
                if (result == null) {
                    throw new IOException("Unable to get version from server for cellbase " + toString());
                }
                String serverVersion = result.getString("Version");
                if (StringUtils.isEmpty(serverVersion)) {
                    serverVersion = result.getString("Version: ");
                }
                this.serverVersion = serverVersion;
            }
        }
        return serverVersion;
    }

    private ObjectMap retryMetaAbout() throws IOException {
        return retry("meta/about", () -> cellBaseClient.getMetaClient().about().firstResult());
    }

    private SpeciesProperties retryMetaSpecies() throws IOException {
        return retry("meta/species", () -> cellBaseClient.getMetaClient().species().firstResult());
    }

    private <T> T retry(String name, Callable<T> function) throws IOException {
        return retry(name, function, 3);
    }

    private <T> T retry(String name, Callable<T> function, int retries) throws IOException {
        if (retries <= 0) {
            return null;
        }
        T result = null;
        Exception e = null;
        try {
            result = function.call();
        } catch (Exception e1) {
            e = e1;
        }
        if (result == null) {
            try {
                // Retry
                logger.warn("Unable to get '{}' from cellbase " + toString() + ". Retrying...", name);
                result = retry(name, function, retries - 1);
            } catch (Exception e1) {
                if (e == null) {
                    e = e1;
                } else {
                    e.addSuppressed(e1);
                }
                if (e instanceof IOException) {
                    throw (IOException) e;
                } else {
                    throw new IOException("Error reading from cellbase " + toString(), e);
                }
            }
        }
        return result;
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
                + "apiKey '" + getApiKey() + "'";

    }
}
