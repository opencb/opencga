package org.opencb.opencga.catalog.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.ResourceManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Catalet {

    public static final Pattern FETCH_PATTERN = Pattern.compile("^FETCH_FIELD\\((\\w+),\\s*(\\S+),\\s*(\\S*),\\s*(\\S+)\\)$");
    public static final Set<Enums.Resource> RESOURCE_SET;

    private final CatalogManager catalogManager;
    private final String studyStr;
    private final String token;

    static {
        RESOURCE_SET = new HashSet<>();
//        RESOURCE_SET.add(Enums.Resource.USER);
//        RESOURCE_SET.add(Enums.Resource.PROJECT);
//        RESOURCE_SET.add(Enums.Resource.STUDY);
        RESOURCE_SET.add(Enums.Resource.SAMPLE);
        RESOURCE_SET.add(Enums.Resource.JOB);
        RESOURCE_SET.add(Enums.Resource.EXECUTION);
        RESOURCE_SET.add(Enums.Resource.COHORT);
        RESOURCE_SET.add(Enums.Resource.FILE);
        RESOURCE_SET.add(Enums.Resource.INDIVIDUAL);
        RESOURCE_SET.add(Enums.Resource.FAMILY);
        RESOURCE_SET.add(Enums.Resource.DISEASE_PANEL);
        RESOURCE_SET.add(Enums.Resource.CLINICAL_ANALYSIS);
        RESOURCE_SET.add(Enums.Resource.INTERPRETATION);
    }

    public Catalet(CatalogManager catalogManager, String studyStr, String token) {
        this.catalogManager = catalogManager;
        this.studyStr = studyStr;
        this.token = token;
    }

    public Object fetch(String fetchString) throws CatalogException {
        ParamUtils.checkParameter(fetchString, "string");

        Matcher matcher = FETCH_PATTERN.matcher(fetchString);
        if (matcher.find()) {
            String resource = matcher.group(1);
            String queryKey = matcher.group(2);
            String queryValue = matcher.group(3);
            String expectedField  = matcher.group(4);
            return fetch(resource, queryKey, queryValue, expectedField);
        } else {
            throw new CatalogParameterException("Unexpected string '" + fetchString + "'. Pattern should be of the form "
                    + "\"FETCH_FIELD(resource, queryKey, queryValue, expectedField)\"");
        }
    }

    public Object fetch(String resourceStr, String queryKey, String queryValue, String expectedField) throws CatalogException {
        Enums.Resource resource = getResource(resourceStr);
        ResourceManager<?> manager = getManager(resource);

        Query query = new Query(queryKey, queryValue);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, expectedField);
        OpenCGAResult<?> search = manager.search(studyStr, query, options, token);
        if (search.getNumResults() == 0) {
            return "";
        } else if (search.getNumResults() > 1) {
            throw new CatalogException("More than one result found. Please narrow down your query.");
        }

        return fetchFieldValue(search.first(), expectedField);
    }

    private Object fetchFieldValue(Object entry, String expectedField) {
        ObjectMap entryMap;
        // Convert to ObjectMap
        try {
            String entryStr = JacksonUtils.getDefaultObjectMapper().writeValueAsString(entry);
            entryMap = JacksonUtils.getDefaultObjectMapper().readValue(entryStr, ObjectMap.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return entryMap.get(expectedField);
    }

    private ResourceManager<?> getManager(Enums.Resource resource) throws CatalogParameterException {
        switch (resource) {
            case FILE:
                return catalogManager.getFileManager();
            case SAMPLE:
                return catalogManager.getSampleManager();
            case EXECUTION:
                return catalogManager.getExecutionManager();
            case JOB:
                return catalogManager.getJobManager();
            case INDIVIDUAL:
                return catalogManager.getIndividualManager();
            case COHORT:
                return catalogManager.getCohortManager();
            case DISEASE_PANEL:
                return catalogManager.getPanelManager();
            case FAMILY:
                return catalogManager.getFamilyManager();
            case CLINICAL_ANALYSIS:
                return catalogManager.getClinicalAnalysisManager();
            case INTERPRETATION:
                return catalogManager.getInterpretationManager();
            case VARIANT:
            case ALIGNMENT:
            case CLINICAL:
            case PIPELINE:
            case EXPRESSION:
            case RGA:
            case FUNCTIONAL:
            case AUDIT:
            case USER:
            case PROJECT:
            case STUDY:
            default:
                throw new CatalogParameterException("Unexpected resource '" + resource + "'");
        }
    }

    private Enums.Resource getResource(String resourceStr) throws CatalogParameterException {
        String res = resourceStr.toUpperCase();
        CatalogParameterException e = new CatalogParameterException("Unexpected resource string '" + resourceStr + "'. Expected values "
                 + "are: '" + RESOURCE_SET.stream().map(Enums.Resource::name).collect(Collectors.joining(", ")) + "'");
        try {
            Enums.Resource resource = Enums.Resource.valueOf(res);
            if (!RESOURCE_SET.contains(resource)) {
                throw e;
            }
            return resource;
        } catch (IllegalArgumentException e1) {
            throw e;
        }
    }
}
