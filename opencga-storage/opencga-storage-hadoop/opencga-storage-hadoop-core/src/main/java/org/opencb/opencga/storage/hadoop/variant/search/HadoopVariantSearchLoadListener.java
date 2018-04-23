package org.opencb.opencga.storage.hadoop.variant.search;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.phoenix.schema.types.PIntegerArray;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchLoadListener;
import org.opencb.opencga.storage.hadoop.utils.HBaseDataWriter;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * Created on 19/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantSearchLoadListener extends VariantSearchLoadListener {

    private final HBaseDataWriter<Mutation> writer;
    private final byte[] family;

    public HadoopVariantSearchLoadListener(VariantHadoopDBAdaptor dbAdaptor) {
        super(dbAdaptor.getStudyConfigurationManager().getStudies(null));
        family = dbAdaptor.getGenomeHelper().getColumnFamily();
        writer = new HBaseDataWriter<>(dbAdaptor.getHBaseManager(), dbAdaptor.getVariantTable());
        writer.open();
        writer.pre();
    }

    @Override
    protected void processAlreadySynchronizedVariants(List<Variant> alreadySynchronizedVariants) {
        for (Variant variant : alreadySynchronizedVariants) {
            byte[] row = VariantPhoenixKeyFactory.generateVariantRowKey(variant);
            Delete delete = new Delete(row)
                    .addColumn(family, VariantPhoenixHelper.VariantColumn.INDEX_UNKNOWN.bytes())
                    .addColumn(family, VariantPhoenixHelper.VariantColumn.INDEX_NOT_SYNC.bytes());
            writer.write(delete);
        }
    }

    @Override
    public void postLoad(List<Variant> variants) throws IOException {
        if (!variants.isEmpty()) {
            List<Mutation> mutations = new ArrayList<>(variants.size() * 2);
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
                    byte[] row = VariantPhoenixKeyFactory.generateVariantRowKey(variant);
                    Put put = new Put(row)
                            .addColumn(family, VariantPhoenixHelper.VariantColumn.INDEX_STUDIES.bytes(), bytes);
//                            .addColumn(family, VariantPhoenixHelper.VariantColumn.INDEX_NOT_SYNC.bytes(), PBoolean.FALSE_BYTES)
//                            .addColumn(family, VariantPhoenixHelper.VariantColumn.INDEX_UNKNOWN.bytes(), PBoolean.FALSE_BYTES);

                    Delete delete = new Delete(row)
                            .addColumn(family, VariantPhoenixHelper.VariantColumn.INDEX_UNKNOWN.bytes())
                            .addColumn(family, VariantPhoenixHelper.VariantColumn.INDEX_NOT_SYNC.bytes());

                    mutations.add(put);
                    mutations.add(delete);
                }
            }
            writer.write(mutations);
        }
    }

    @Override
    public void close() {
        writer.post();
        writer.close();
    }
}
