package org.opencb.opencga.storage.hadoop.variant.search;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.phoenix.schema.types.PIntegerArray;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AdditionalAttribute;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchLoadListener;
import org.opencb.opencga.storage.hadoop.utils.HBaseDataWriter;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsDBCleaner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.GROUP_NAME;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.INDEX_STUDIES;


/**
 * Created on 19/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantSearchLoadListener extends VariantSearchLoadListener {

    private final HBaseDataWriter<Mutation> writer;
    private final PendingVariantsDBCleaner cleaner;
    private final byte[] family;

    public HadoopVariantSearchLoadListener(VariantHadoopDBAdaptor dbAdaptor, PendingVariantsDBCleaner cleaner) {
        super(dbAdaptor.getMetadataManager().getStudies(null));
        this.cleaner = cleaner;
        family = GenomeHelper.COLUMN_FAMILY_BYTES;
        writer = new HBaseDataWriter<>(dbAdaptor.getHBaseManager(), dbAdaptor.getVariantTable());
        writer.open();
        writer.pre();
        cleaner.open();
        cleaner.pre();
    }

    @Override
    protected void processAlreadySynchronizedVariants(List<Variant> alreadySynchronizedVariants) {
    }

    @Override
    public void postLoad(List<Variant> variants) throws IOException {
        if (!variants.isEmpty()) {
            List<Mutation> mutations = new ArrayList<>(variants.size());
            List<byte[]> variantRows = new ArrayList<>(variants.size());
            Map<Set<Integer>, List<Variant>> map = variants.stream()
                    .collect(Collectors.groupingBy(
                            variant -> variant.getStudies()
                                    .stream()
                                    .map(StudyEntry::getStudyId).map(studiesMap::get)
                                    .collect(Collectors.toSet())));

            for (Map.Entry<Set<Integer>, List<Variant>> entry : map.entrySet()) {
                Set<Integer> studies = entry.getKey();
                byte[] bytes = PhoenixHelper.toBytes(studies, PIntegerArray.INSTANCE);
                for (Variant variant : entry.getValue()) {
                    if (variant.getAnnotation() != null
                            && variant.getAnnotation().getAdditionalAttributes() != null
                            && variant.getAnnotation().getAdditionalAttributes().get(GROUP_NAME.key()) != null) {
                        AdditionalAttribute additionalAttribute = variant.getAnnotation().getAdditionalAttributes().get(GROUP_NAME.key());
                        String indexedStudies = additionalAttribute.getAttribute().get(INDEX_STUDIES.key());
                        // If the field indexedStudies exists, and it contains one less ',' as one studies should have, skip this variant
                        if (indexedStudies != null && StringUtils.countMatches(indexedStudies, ',') + 1 == studies.size()) {
                            // Variant already synchronized. Skip this variant!
                            continue;
                        }
                    }
                    byte[] row = VariantPhoenixKeyFactory.generateVariantRowKey(variant);
                    variantRows.add(row);
                    Put put = new Put(row)
                            .addColumn(family, VariantPhoenixHelper.VariantColumn.INDEX_STUDIES.bytes(), bytes);
                    mutations.add(put);
                }
            }
            writer.write(mutations);
            cleaner.write(variantRows);
        }
    }

    @Override
    public void close() {
        writer.post();
        writer.close();
        cleaner.post();
        cleaner.close();
    }
}
