package org.opencb.opencga.storage.core.variant.annotation;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.ConsequenceType;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.biodata.models.variation.GenomicVariant;
import org.opencb.cellbase.core.client.CellBaseClient;
import org.opencb.cellbase.core.common.core.CellbaseConfiguration;
import org.opencb.cellbase.core.lib.DBAdaptorFactory;
import org.opencb.cellbase.core.lib.api.variation.ClinicalVarDBAdaptor;
import org.opencb.cellbase.core.lib.api.variation.VariantAnnotationDBAdaptor;
import org.opencb.cellbase.core.lib.api.variation.VariationDBAdaptor;
import org.opencb.cellbase.lib.mongodb.db.MongoDBAdaptorFactory;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResponse;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by jacobo on 9/01/15.
 */
public class CellBaseVariantAnnotator implements VariantAnnotator {

    private JsonFactory factory;
    private VariantAnnotationDBAdaptor variantAnnotationDBAdaptor;
    private CellBaseClient cellBaseClient;
    private ObjectMapper jsonObjectMapper;
    private VariationDBAdaptor variationDBAdaptor;
    private DBAdaptorFactory dbAdaptorFactory;

    protected static Logger logger = LoggerFactory.getLogger(CellBaseVariantAnnotator.class);

    public CellBaseVariantAnnotator() {
        this.factory = new JsonFactory();
        this.jsonObjectMapper = new ObjectMapper(factory);
        this.dbAdaptorFactory = null;
        this.cellBaseClient = null;
    }

    public CellBaseVariantAnnotator(CellbaseConfiguration cellbaseConfiguration, String cellbaseSpecies, String cellbaseAssembly) {
        this();
        /**
         * Connecting to CellBase database
         */
        dbAdaptorFactory = new MongoDBAdaptorFactory(cellbaseConfiguration);
        variantAnnotationDBAdaptor = dbAdaptorFactory.getGenomicVariantAnnotationDBAdaptor(cellbaseSpecies, cellbaseAssembly);
        variationDBAdaptor = dbAdaptorFactory.getVariationDBAdaptor(cellbaseSpecies, cellbaseAssembly);
    }

    public CellBaseVariantAnnotator(CellBaseClient cellBaseClient) {
        this();
        this.cellBaseClient = cellBaseClient;
        if(cellBaseClient == null) {
            throw new NullPointerException("CellBaseClient can not be null");
        }
    }


    @Override
    public URI createAnnotation(VariantDBAdaptor variantDBAdaptor, Path outDir, String fileName, QueryOptions options) throws IOException {
        if(cellBaseClient == null && dbAdaptorFactory == null) {
            throw new IllegalArgumentException("Cant createAnnotation without a CellBase source (DBAdaptorFactory or a CellBaseClient)");
        }

        URI fileUri;
        Path path = Paths.get(outDir != null? outDir.toString() : "/tmp" ,fileName + ".annot.json.gz");
        try {
            fileUri = new URI("file", path.toString(), null);
        } catch (URISyntaxException e) {
            e.printStackTrace();
            return null;
        }

        /** Open output stream **/
        OutputStream outputStream;
        outputStream = new FileOutputStream(path.toFile());
        outputStream = new GZIPOutputStream(outputStream);

        /** Innitialice Json parse**/
        ObjectWriter writer = jsonObjectMapper.writerWithType(VariantAnnotation.class);

        /** Getting iterator from OpenCGA Variant database. **/
        QueryOptions iteratorQueryOptions = new QueryOptions();
//        ArrayList<String> exclude = new ArrayList<>();
//        iteratorQueryOptions.add("exclude", exclude);
        if(options != null) { //Parse query options
            if (!options.getBoolean(VariantAnnotationManager.ANNOTATE_ALL, false)) {
                iteratorQueryOptions.put("annotationExists", false);
            }
        }

        int batchSize = 100;
        List<GenomicVariant> genomicVariantList = new ArrayList<>(batchSize);
        Iterator<Variant> iterator = variantDBAdaptor.iterator(iteratorQueryOptions);
        while(iterator.hasNext()) {
            Variant variant = iterator.next();
            if(variant.getAlternate().length() + variant.getReference().length() > 100) {
                logger.info("Skip variant! ");
                logger.debug("Skip variant! {}", variant);
            } else {
                GenomicVariant genomicVariant = new GenomicVariant(variant.getChromosome(), variant.getStart(), variant.getReference(), variant.getAlternate());
                genomicVariantList.add(genomicVariant);
            }

            if(genomicVariantList.size() == batchSize || !iterator.hasNext()) {
                List<VariantAnnotation> variantAnnotationList;
                if(cellBaseClient != null) {
                    variantAnnotationList = getVariantAnnotationsREST(genomicVariantList);
                } else {
                    variantAnnotationList = getVariantAnnotationsDbAdaptor(genomicVariantList);
                }
                for (VariantAnnotation variantAnnotation : variantAnnotationList) {
                    outputStream.write(writer.writeValueAsString(variantAnnotation).getBytes());
                    outputStream.write('\n');
                }
                genomicVariantList.clear();
            }
        }

        outputStream.close();

        return fileUri;
    }

    private List<VariantAnnotation> getVariantAnnotationsREST(List<GenomicVariant> genomicVariantList) throws IOException {
        QueryResponse<QueryResult<VariantAnnotation>> queryResponse;
        List<String> genomicVariantStringList = new ArrayList<>(genomicVariantList.size());
        for (GenomicVariant genomicVariant : genomicVariantList) {
            genomicVariantStringList.add(genomicVariant.toString());
        }
        //              queryResponse = cellBaseClient.nativeGet("genomic", "variant", genomicVariant.toString(), "full_annotation", new QueryOptions("of", "json"), VariantAnnotation.class);
        //              queryResponse = cellBaseClient.nativeGet("genomic", "variant", genomicVariant.toString(), "full_annotation", new QueryOptions("of", "json"));
        queryResponse = cellBaseClient.get(
                CellBaseClient.Category.genomic,
                CellBaseClient.SubCategory.variant,
                genomicVariantStringList,
                CellBaseClient.Resource.fullAnnotation,
                null);
        if(queryResponse == null) {
            logger.error("Cellbase REST error. Returned null. Skipping variants.");
            return Collections.emptyList();
        }

        Collection<QueryResult<VariantAnnotation>> response = queryResponse.getResponse();
        if(response.size() != genomicVariantList.size()) {
            throw new IOException("QueryResult size != " + genomicVariantList.size() + ". " + queryResponse);
        }
        QueryResult<VariantAnnotation>[] queryResults = response.toArray(new QueryResult[1]);
        List<VariantAnnotation> variantAnnotationList = new ArrayList<>(genomicVariantList.size());
        for (QueryResult<VariantAnnotation> queryResult : queryResults) {
            variantAnnotationList.addAll(queryResult.getResult());
        }
        return variantAnnotationList;
    }

    private List<VariantAnnotation> getVariantAnnotationsDbAdaptor(List<GenomicVariant> genomicVariantList) throws IOException {
        org.opencb.cellbase.core.lib.dbquery.QueryOptions queryOptions = new org.opencb.cellbase.core.lib.dbquery.QueryOptions();

        List<VariantAnnotation> variantAnnotationList = new ArrayList<>(genomicVariantList.size());
        Map<String, List<ConsequenceType>> consequenceTypes = getConsequenceTypes(genomicVariantList, queryOptions);
        Map<String, String> variantIds = getVariantId(genomicVariantList, queryOptions);
        for (GenomicVariant genomicVariant : genomicVariantList) {
            VariantAnnotation variantAnnotation = new VariantAnnotation(
                    genomicVariant.getChromosome(),
                    genomicVariant.getPosition(),
                    genomicVariant.getPosition(),   //TODO: ¿?¿?
                    genomicVariant.getReference(),
                    genomicVariant.getAlternative());

            String key = genomicVariant.toString();
            variantAnnotation.setConsequenceTypes(consequenceTypes.get(key));
            variantAnnotation.setId(variantIds.get(key));

            variantAnnotationList.add(variantAnnotation);
        }
        return variantAnnotationList;
    }

    private Map<String, List<ConsequenceType>> getConsequenceTypes(List<GenomicVariant> genomicVariants, org.opencb.cellbase.core.lib.dbquery.QueryOptions queryOptions) throws IOException {
        Map<String, List<ConsequenceType>> map = new HashMap<>(genomicVariants.size());

        List<org.opencb.cellbase.core.lib.dbquery.QueryResult> queryResultList =
                variantAnnotationDBAdaptor.getAllConsequenceTypesByVariantList(genomicVariants, queryOptions);
        for (org.opencb.cellbase.core.lib.dbquery.QueryResult queryResult : queryResultList) {
            List list = getList(queryResult.getResult());

            if(list.get(0) instanceof ConsequenceType) {
                map.put(queryResult.getId(), list);
            } else {
                throw new IOException("queryResult result : " + queryResult + " is not a ConsequenceType");
            }
        }
        return map;
    }

    private List<ConsequenceType> getConsequenceTypes(GenomicVariant genomicVariant, org.opencb.cellbase.core.lib.dbquery.QueryOptions queryOptions) throws IOException {
        org.opencb.cellbase.core.lib.dbquery.QueryResult queryResult =
                variantAnnotationDBAdaptor.getAllConsequenceTypesByVariant(genomicVariant, queryOptions);
        List list = getList(queryResult.getResult());

        List<ConsequenceType> consequenceTypes;
        if(list.get(0) instanceof ConsequenceType) {
            consequenceTypes = list;
        } else {
            throw new IOException("queryResult result : " + queryResult + " is not a ConsequenceType");
        }
        return consequenceTypes;
    }

    private Map<String, String> getVariantId(List<GenomicVariant> genomicVariant, org.opencb.cellbase.core.lib.dbquery.QueryOptions queryOptions) throws IOException {
        List<org.opencb.cellbase.core.lib.dbquery.QueryResult> variationQueryResultList =
                variationDBAdaptor.getIdByVariantList(genomicVariant, queryOptions);
        Map<String, String> map = new HashMap<>(genomicVariant.size());
        for (org.opencb.cellbase.core.lib.dbquery.QueryResult queryResult : variationQueryResultList) {
            map.put(queryResult.getId(), queryResult.getResult().toString());
        }
        return map;
    }

    private String getVariantId(GenomicVariant genomicVariant, org.opencb.cellbase.core.lib.dbquery.QueryOptions queryOptions) throws IOException {
        List<org.opencb.cellbase.core.lib.dbquery.QueryResult> variationQueryResultList =
                variationDBAdaptor.getIdByVariantList(Collections.singletonList(genomicVariant), queryOptions);
        if(variationQueryResultList.get(0).getResult() != null) {
            return variationQueryResultList.get(0).getResult().toString();
        }
        return null;
    }

    private List getList(Object result) throws IOException {
        if(result instanceof Collection) {
            return new ArrayList((Collection) result);
        } else {
            return Collections.singletonList(result);
        }
    }

    @Override
    public void loadAnnotation(VariantDBAdaptor variantDBAdaptor, URI uri, boolean clean) throws IOException {

        /** Open input stream **/
        InputStream inputStream;
        inputStream = new FileInputStream(Paths.get(uri).toFile());
        inputStream = new GZIPInputStream(inputStream);

        /** Innitialice Json parse**/
        JsonParser parser = factory.createParser(inputStream);

        int batchSize = 100;
        List<VariantAnnotation> variantAnnotationList = new ArrayList<>(batchSize);
        while (parser.nextToken() != null) {
            VariantAnnotation variantAnnotation = parser.readValueAs(VariantAnnotation.class);
//            System.out.println("variantAnnotation = " + variantAnnotation);
            variantAnnotationList.add(variantAnnotation);
            if(variantAnnotationList.size() == batchSize || parser.nextToken() == null) {
                variantDBAdaptor.updateAnnotations(variantAnnotationList, new QueryOptions());
                variantAnnotationList.clear();
            }
        }

        inputStream.close();

    }
}
