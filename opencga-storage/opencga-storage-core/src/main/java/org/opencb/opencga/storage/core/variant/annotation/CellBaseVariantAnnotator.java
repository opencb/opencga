package org.opencb.opencga.storage.core.variant.annotation;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variation.GenomicVariant;
import org.opencb.cellbase.core.client.CellBaseClient;
import org.opencb.cellbase.core.common.core.CellbaseConfiguration;
import org.opencb.cellbase.core.lib.DBAdaptorFactory;
import org.opencb.cellbase.core.lib.api.variation.VariantAnnotationDBAdaptor;
import org.opencb.cellbase.lib.mongodb.db.MongoDBAdaptorFactory;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResponse;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPOutputStream;

/**
 * Created by jacobo on 9/01/15.
 */
public class CellBaseVariantAnnotator implements VariantAnnotator {

    private VariantAnnotationDBAdaptor variantAnnotationDBAdaptor;
    private CellBaseClient cellBaseClient;

    public CellBaseVariantAnnotator(String cellbaseRestHost, int cellbaseRestPort, String cellbasePath, String cellbaseVersion,
                                    String cellbaseSpecies, String cellbaseAssembly)
            throws URISyntaxException {
        this.cellBaseClient = new CellBaseClient(cellbaseRestHost, cellbaseRestPort, cellbasePath, cellbaseVersion, cellbaseSpecies);
    }

    public CellBaseVariantAnnotator(CellbaseConfiguration cellbaseConfiguration, String cellbaseSpecies, String cellbaseAssembly) {
        /**
         * Connecting to CellBase database
         */
        DBAdaptorFactory dbAdaptorFactory = new MongoDBAdaptorFactory(cellbaseConfiguration);
        variantAnnotationDBAdaptor = dbAdaptorFactory.getGenomicVariantAnnotationDBAdaptor(cellbaseSpecies, cellbaseAssembly);
    }

    public CellBaseVariantAnnotator(CellBaseClient cellBaseClient) {
        this.cellBaseClient = cellBaseClient;
        if(cellBaseClient == null) {
            throw new NullPointerException("CellBaseClient can not be null");
        }
    }

    public CellBaseVariantAnnotator(VariantAnnotationDBAdaptor variantAnnotationDBAdaptor) {
        this.variantAnnotationDBAdaptor = variantAnnotationDBAdaptor;
        if(variantAnnotationDBAdaptor == null) {
            throw new NullPointerException("VariantAnnotationDBAdaptor can not be null");
        }
    }

    @Override
    public URI createAnnotation(VariantDBAdaptor variantDBAdaptor, Path outDir, String fileName, QueryOptions options) throws IOException {
        OutputStream outputStream;
        URI fileUri;
        Path path = Paths.get(outDir != null? outDir.toString() : "/tmp" ,fileName + ".annot.gz");
        try {
            fileUri = new URI("file", path.toString(), null);
        } catch (URISyntaxException e) {
            e.printStackTrace();
            return null;
        }
        try {
            outputStream = new FileOutputStream(path.toFile());
            outputStream = new GZIPOutputStream(outputStream);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw e;
        }

        QueryOptions iteratorQueryOptions = new QueryOptions();
//        ArrayList<String> exclude = new ArrayList<>();
//        iteratorQueryOptions.add("exclude", exclude);
        if(options != null) { //Parse query options
            if (!options.getBoolean(VariantAnnotationManager.ANNOTATE_ALL, false)) {
                iteratorQueryOptions.put("annotationExists", false);
            }
        }

        Iterator<Variant> iterator = variantDBAdaptor.iterator(iteratorQueryOptions);
        while(iterator.hasNext()) {
            Variant variant = iterator.next();
            GenomicVariant genomicVariant = new GenomicVariant(variant.getChromosome(), variant.getStart(), variant.getReference(), variant.getAlternate());

            ObjectMap objectMap;
            if(cellBaseClient != null) {
                QueryResponse<QueryResult<ObjectMap>> queryResponse;
//              queryResponse = cellBaseClient.nativeGet("genomic", "variant", genomicVariant.toString(), "full_annotation", new QueryOptions("of", "json"), VariantAnnotation.class);
//              queryResponse = cellBaseClient.nativeGet("genomic", "variant", genomicVariant.toString(), "full_annotation", new QueryOptions("of", "json"));
                queryResponse = cellBaseClient.getObjectMap(
                        CellBaseClient.Category.genomic,
                        CellBaseClient.SubCategory.variant,
                        Collections.singletonList(genomicVariant.toString()),
                        CellBaseClient.Resource.fullAnnotation,
                        null);
                Collection<QueryResult<ObjectMap>> response = queryResponse.getResponse();
                if(response.size() != 1) {
                    throw new IOException("QueryResult size != 1. " + queryResponse);
                }
                QueryResult<ObjectMap>[] queryResults = response.toArray(new QueryResult[1]);
                QueryResult<ObjectMap> queryResult = queryResults[0];
                objectMap = queryResult.getResult().get(0);
            }  else {
                org.opencb.cellbase.core.lib.dbquery.QueryOptions queryOptions = new org.opencb.cellbase.core.lib.dbquery.QueryOptions();
                org.opencb.cellbase.core.lib.dbquery.QueryResult queryResult1 =
                        variantAnnotationDBAdaptor.getAllConsequenceTypesByVariant(genomicVariant, queryOptions);
                Object result = queryResult1.getResult();
                if(result instanceof Collection) {
                    if(((Collection) result).size() != 1 ) {
                        throw new IOException("QueryResult size != 1. " + queryResult1);
                    }
                    result = new ArrayList((Collection)result).get(0);
                }
                if(result instanceof Map) {
                    objectMap = new ObjectMap((Map) result);
                } else {
                    throw new IOException("Object result : " + result + " is not a Map");
                }
            }

            outputStream.write(objectMap.toJson().getBytes());
            outputStream.write('\n');
        }
        outputStream.close();

        return fileUri;
    }

    @Override
    public void loadAnnotation(VariantDBAdaptor variantDBAdaptor, URI uri, boolean clean) {




    }
}
