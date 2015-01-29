package org.opencb.opencga.storage.core.variant.stats;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.cellbase.core.lib.dbquery.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPOutputStream;

/**
 * Created by jmmut on 28/01/15.
 */
public class VariantStatisticsCalculator {


    private ObjectMapper jsonObjectMapper;

    public VariantStatisticsCalculator() {
        this.jsonObjectMapper = new ObjectMapper(new JsonFactory());
    }

    public URI createStats(VariantDBAdaptor variantDBAdaptor, Path outDir, String filename, QueryOptions options) throws IOException {

        Path path = Paths.get(outDir != null ? outDir.toString() : "/tmp", filename + ".stats.json.gz");
        URI fileUri = path.toUri();

        /** Open output stream **/
        OutputStream outputStream;
        outputStream = new FileOutputStream(path.toFile());
        if(options != null && options.getBoolean("gzip", true)) {
            outputStream = new GZIPOutputStream(outputStream);
        }

        /** Initialize Json serializer**/
        ObjectWriter writer = jsonObjectMapper.writerWithType(VariantStats.class);

        /** Getting iterator from OpenCGA Variant database. **/
        QueryOptions iteratorQueryOptions = new QueryOptions();
        int batchSize = 100;


        List<String> include = Arrays.asList("chromosome", "start", "alternative", "reference");
        iteratorQueryOptions.add("include", include);
        if(options != null) { //Parse query options
            iteratorQueryOptions = options;
//            iteratorQueryOptions = new QueryOptions(options.getMap(VariantAnnotationManager.ANNOTATOR_QUERY_OPTIONS, Collections.<String, Object>emptyMap()));
            batchSize = options.getInt(VariantAnnotationManager.BATCH_SIZE, batchSize);
        }

        Variant variant = null;
        List<GenomicVariant> genomicVariantList = new ArrayList<>(batchSize);
        Iterator<Variant> iterator = variantDBAdaptor.iterator(iteratorQueryOptions);
        while(iterator.hasNext()) {
            variant = iterator.next();

            // If Variant is SV some work is needed
            if(variant.getAlternate().length() + variant.getReference().length() > Variant.SV_THRESHOLD*2) {       //TODO: Manage SV variants
//                logger.info("Skip variant! {}", genomicVariant);
                logger.info("Skip variant! {}", variant.getChromosome() + ":" +
                                variant.getStart() + ":" +
                                (variant.getReference().length() > 10? variant.getReference().substring(0,10) + "...[" + variant.getReference().length() + "]" : variant.getReference()) + ":" +
                                (variant.getAlternate().length() > 10? variant.getAlternate().substring(0,10) + "...[" + variant.getAlternate().length() + "]" : variant.getAlternate())
                );
                logger.debug("Skip variant! {}", variant);
            } else {
                GenomicVariant genomicVariant = new GenomicVariant(variant.getChromosome(), variant.getStart(),
                        variant.getReference().isEmpty() && variant.getType() == Variant.VariantType.INDEL ? "-" : variant.getReference(),
                        variant.getAlternate().isEmpty() && variant.getType() == Variant.VariantType.INDEL ? "-" : variant.getAlternate());
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

    public void loadStats(VariantDBAdaptor variantDBAdaptor, URI uri, QueryOptions options) throws IOException {

    }
}
