/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.core.variant.annotation;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.cellbase.core.client.CellBaseClient;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResponse;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.io.json.VariantAnnotationMixin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by jacobo on 9/01/15.
 */
public class CellBaseVariantAnnotator extends VariantAnnotator {

    private final JsonFactory factory;
//    private VariantAnnotationDBAdaptor variantAnnotationDBAdaptor = null;
//    private VariationDBAdaptor variationDBAdaptor = null;
//    private DBAdaptorFactory dbAdaptorFactory = null;
    private CellBaseClient cellBaseClient = null;
    private ObjectMapper jsonObjectMapper;

//    public static final String CELLBASE_VERSION = "CELLBASE.VERSION";
//    public static final String CELLBASE_REST_URL = "CELLBASE.REST.URL";
//
//    public static final String CELLBASE_DB_HOST = "CELLBASE.DB.HOST";
//    public static final String CELLBASE_DB_NAME = "CELLBASE.DB.NAME";
//    public static final String CELLBASE_DB_PORT = "CELLBASE.DB.PORT";
//    public static final String CELLBASE_DB_USER = "CELLBASE.DB.USER";
//    public static final String CELLBASE_DB_PASSWORD = "CELLBASE.DB.PASSWORD";
//    public static final String CELLBASE_DB_MAX_POOL_SIZE = "CELLBASE.DB.MAX_POOL_SIZE";
//    public static final String CELLBASE_DB_TIMEOUT = "CELLBASE.DB.TIMEOUT";


    protected static Logger logger = LoggerFactory.getLogger(CellBaseVariantAnnotator.class);

    public CellBaseVariantAnnotator(StorageConfiguration storageConfiguration, ObjectMap options) throws VariantAnnotatorException {
        this(storageConfiguration, options, true);
    }

    public CellBaseVariantAnnotator(StorageConfiguration storageConfiguration, ObjectMap options, boolean restConnection)
            throws VariantAnnotatorException {
        super(storageConfiguration, options);

        this.factory = new JsonFactory();
        this.jsonObjectMapper = new ObjectMapper(factory);
        jsonObjectMapper.addMixIn(VariantAnnotation.class, VariantAnnotationMixin.class);
        jsonObjectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);

        if (restConnection) {
            String species = options.getString(VariantAnnotationManager.SPECIES);
            String assembly = options.getString(VariantAnnotationManager.ASSEMBLY);
            String cellbaseVersion = storageConfiguration.getCellbase().getVersion();
            List<String> hosts = storageConfiguration.getCellbase().getHosts();
            if (hosts.isEmpty()) {
                throw new VariantAnnotatorException("Missing defaultValue \"CellBase Hosts\"");
            }
            String cellbaseRest = hosts.get(0);

            checkNotNull(cellbaseVersion, "cellbase version");
            checkNotNull(cellbaseRest, "cellbase hosts");
            checkNotNull(species, "species");

            CellBaseClient cellBaseClient;
            try {
                URI url = new URI(cellbaseRest);
                cellBaseClient = new CellBaseClient(url, cellbaseVersion, species);
            } catch (URISyntaxException e) {
                e.printStackTrace();
                throw new VariantAnnotatorException("Invalid URL : " + cellbaseRest, e);
            }

            this.cellBaseClient = cellBaseClient;
            cellBaseClient.getObjectMapper().addMixIn(VariantAnnotation.class, VariantAnnotationMixin.class);
        } else {
            throw new UnsupportedOperationException("Unimplemented CellBase dbAdaptor connection. Use CellBaseClient instead");
//            String cellbaseHost = annotatorProperties.getProperty(CELLBASE_DB_HOST, "");
//            String cellbaseDatabase = annotatorProperties.getProperty(CELLBASE_DB_NAME, "");
//            int cellbasePort = Integer.parseInt(annotatorProperties.getProperty(CELLBASE_DB_PORT, "27017"));
//            String cellbaseUser = annotatorProperties.getProperty(CELLBASE_DB_USER, "");
//            String cellbasePassword = annotatorProperties.getProperty(CELLBASE_DB_PASSWORD, "");
//            int maxPoolSize = Integer.parseInt(annotatorProperties.getProperty(CELLBASE_DB_MAX_POOL_SIZE, "10"));
//            int timeout = Integer.parseInt(annotatorProperties.getProperty(CELLBASE_DB_TIMEOUT, "200"));
//
//            checkNotNull(cellbaseHost, CELLBASE_DB_HOST);
//            checkNotNull(cellbaseDatabase, CELLBASE_DB_NAME);
//            checkNotNull(cellbaseUser, CELLBASE_DB_USER);
//            checkNotNull(cellbasePassword, CELLBASE_DB_PASSWORD);
//
//
//            CellBaseConfiguration cellbaseConfiguration = new CellBaseConfiguration();
//            cellbaseConfiguration.addSpeciesConnection(
//                    species,
//                    assembly,
//                    cellbaseHost,
//                    cellbaseDatabase,
//                    cellbasePort,
//                    "mongo",    //TODO: Change to "mongodb"
//                    cellbaseUser,
//                    cellbasePassword,
//                    maxPoolSize,
//                    timeout);
//            cellbaseConfiguration.addSpeciesAlias(species, species);
//
//            return new CellBaseVariantAnnotator(cellbaseConfiguration, species, assembly);
        }
    }

//    public CellBaseVariantAnnotator(CellBaseConfiguration cellbaseConfiguration, String cellbaseSpecies, String cellbaseAssembly) {
//        this();
//        /**
//         * Connecting to CellBase database
//         */
//        dbAdaptorFactory = new MongoDBAdaptorFactory(cellbaseConfiguration);
//        variantAnnotationDBAdaptor = dbAdaptorFactory.getVariantAnnotationDBAdaptor(cellbaseSpecies, cellbaseAssembly);
//        variationDBAdaptor = dbAdaptorFactory.getVariationDBAdaptor(cellbaseSpecies, cellbaseAssembly);
//    }

    private static void checkNotNull(String value, String name) throws VariantAnnotatorException {
        if (value == null || value.isEmpty()) {
            throw new VariantAnnotatorException("Missing defaultValue: " + name);
        }
    }


    /////// CREATE ANNOTATION - AUX METHODS

    @Override
    public List<VariantAnnotation> annotate(List<Variant> variants) throws IOException {

        List<Variant> nonStructuralVariations = filterStructuralVariants(variants);

        if (cellBaseClient != null) {
            return getVariantAnnotationsREST(nonStructuralVariations);
        } else {
            return getVariantAnnotationsDbAdaptor(nonStructuralVariations);
        }
    }

    List<Variant> filterStructuralVariants(List<Variant> variants) {
        List<Variant> nonStructuralVariants = new ArrayList<>(variants.size());
        for (Variant variant : variants) {
            // If Variant is SV some work is needed
            if (variant.getAlternate().length() + variant.getReference().length() > Variant.SV_THRESHOLD * 2) { // TODO: Manage SV variants
//                logger.info("Skip variant! {}", genomicVariant);
                logger.info("Skip variant! {}", variant.getChromosome() + ":" + variant.getStart() + ":"
                        + (variant.getReference().length() > 10
                        ? variant.getReference().substring(0, 10) + "...[" + variant.getReference().length() + "]"
                        : variant.getReference()) + ":"
                        + (variant.getAlternate().length() > 10
                        ? variant.getAlternate().substring(0, 10) + "...[" + variant.getAlternate().length() + "]"
                        : variant.getAlternate())
                );
                logger.debug("Skip variant! {}", variant);
            } else {
                nonStructuralVariants.add(variant);
            }
        }
        return nonStructuralVariants;
    }

    private List<VariantAnnotation> getVariantAnnotationsREST(List<Variant> variants) throws IOException {
        QueryResponse<QueryResult<VariantAnnotation>> queryResponse;
//        List<String> genomicVariantStringList = new ArrayList<>(variants.size());
//        for (GenomicVariant genomicVariant : variants) {
//            genomicVariantStringList.add(genomicVariant.toString());
//        }

        boolean queryError = false;
        try {
            queryResponse = cellBaseClient.nativeGet(
                    CellBaseClient.Category.genomic.toString(),
                    CellBaseClient.SubCategory.variant.toString(),
                    variants.stream().map(Object::toString).collect(Collectors.joining(",")),
                    "full_annotation",
                    new QueryOptions("post", true), VariantAnnotation.class);
            if (queryResponse == null) {
                logger.warn("CellBase REST fail. Returned null. {}", cellBaseClient.getLastQuery());
                queryError = true;
            }
        } catch (JsonProcessingException e) {
            logger.warn("CellBase REST fail. Error parsing " + cellBaseClient.getLastQuery(), e);
            queryError = true;
            queryResponse = null;
        }

        if (queryResponse != null && queryResponse.getResponse().size() != variants.size()) {
            logger.warn("QueryResult size (" + queryResponse.getResponse().size() + ") != variants size (" + variants.size() + ").");
            //throw new IOException("QueryResult size != " + variants.size() + ". " + queryResponse);
            queryError = true;
        }

        if (queryError) {
//            logger.warn("CellBase REST error. {}", cellBaseClient.getLastQuery());

            if (variants.size() == 1) {
                logger.error("CellBase REST error. Skipping variant. {}", variants.get(0));
                return Collections.emptyList();
            }

            List<VariantAnnotation> variantAnnotationList = new LinkedList<>();
            List<Variant> variants1 = variants.subList(0, variants.size() / 2);
            if (!variants1.isEmpty()) {
                variantAnnotationList.addAll(getVariantAnnotationsREST(variants1));
            }
            List<Variant> variants2 = variants.subList(variants.size() / 2, variants.size());
            if (!variants2.isEmpty()) {
                variantAnnotationList.addAll(getVariantAnnotationsREST(variants2));
            }
            return variantAnnotationList;
        }

        Collection<QueryResult<VariantAnnotation>> response = queryResponse.getResponse();

        QueryResult<VariantAnnotation>[] queryResults = response.toArray(new QueryResult[1]);
        List<VariantAnnotation> variantAnnotationList = new ArrayList<>(variants.size());
        for (QueryResult<VariantAnnotation> queryResult : queryResults) {
            variantAnnotationList.addAll(queryResult.getResult());
        }
        return variantAnnotationList;
    }

    private List<VariantAnnotation> getVariantAnnotationsDbAdaptor(List<Variant> genomicVariantList) throws IOException {
//        QueryOptions queryOptions = new QueryOptions();
//
//        List<VariantAnnotation> variantAnnotationList = new ArrayList<>(genomicVariantList.size());
//        Map<String, List<ConsequenceType>> consequenceTypes = getConsequenceTypes(genomicVariantList, queryOptions);
//        Map<String, String> variantIds = getVariantId(genomicVariantList, queryOptions);
//        for (GenomicVariant genomicVariant : genomicVariantList) {
//            VariantAnnotation variantAnnotation = new VariantAnnotation(
//                    genomicVariant.getChromosome(),
//                    genomicVariant.getPosition(),
//                    genomicVariant.getPosition(),   //TODO: ¿?¿?
//                    genomicVariant.getReference(),
//                    genomicVariant.getAlternative());
//
//            String key = genomicVariant.toString();
//            variantAnnotation.setConsequenceTypes(consequenceTypes.get(key));
//            variantAnnotation.setId(variantIds.get(key));
//
//            variantAnnotationList.add(variantAnnotation);
//        }
//        return variantAnnotationList;
        throw new UnsupportedOperationException("Unsupported operation. Try with REST annotation");
    }
//
//    // FIXME To delete when available in cellbase
//    private Map<String, List<ConsequenceType>> getConsequenceTypes(List<GenomicVariant> genomicVariants,
//                                                                   QueryOptions queryOptions) throws IOException {
//        Map<String, List<ConsequenceType>> map = new HashMap<>(genomicVariants.size());
//        List<QueryResult> queryResultList = variantAnnotationDBAdaptor.getAllConsequenceTypesByVariantList(genomicVariants, queryOptions);
//        for (QueryResult queryResult : queryResultList) {
//            Object result = queryResult.getResult();
//            List list = result instanceof Collection ? new ArrayList((Collection) result) : Collections.singletonList(result);
//
//            if(list.get(0) instanceof ConsequenceType) {
//                map.put(queryResult.getId(), list);
//            } else {
//                throw new IOException("queryResult result : " + queryResult + " is not a ConsequenceType");
//            }
//        }
//        return map;
//    }
//
//    // FIXME To delete when available in cellbase
//    private Map<String, String> getVariantId(List<GenomicVariant> genomicVariant, QueryOptions queryOptions) throws IOException {
//        List<QueryResult> variationQueryResultList = variationDBAdaptor.getIdByVariantList(genomicVariant, queryOptions);
//        Map<String, String> map = new HashMap<>(genomicVariant.size());
//        for (QueryResult queryResult : variationQueryResultList) {
//            map.put(queryResult.getId(), queryResult.getResult().toString());
//        }
//        return map;
//    }
//

    /////// LOAD ANNOTATION

}
