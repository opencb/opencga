package org.opencb.opencga.storage.hadoop.variant.index.sample.file;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine.SplitData;
import org.opencb.opencga.storage.core.variant.index.sample.file.SampleIndexWriter;
import org.opencb.opencga.storage.core.variant.index.sample.file.SampleIndexEntryBuilder;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.hadoop.utils.HBaseDataWriter;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.sample.HBaseSampleIndexDBAdaptor;

import java.util.LinkedList;
import java.util.List;

/**
 * Created on 14/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseSampleIndexDBWriter extends SampleIndexWriter {

    private final HBaseSampleIndexDBAdaptor dbAdaptor;
    private final HBaseDataWriter<Mutation> hbaseWriter;
    private final byte[] family;

    public HBaseSampleIndexDBWriter(HBaseSampleIndexDBAdaptor dbAdaptor, HBaseManager hBaseManager,
                                    VariantStorageMetadataManager metadataManager,
                                    int studyId, int fileId, List<Integer> sampleIds,
                                    SplitData splitData, ObjectMap options, SampleIndexSchema schema) {

        super(dbAdaptor, metadataManager, studyId, fileId, sampleIds, splitData, options, schema);
        hbaseWriter = new HBaseDataWriter<>(hBaseManager, dbAdaptor.getSampleIndexTableName(studyId,
                schema.getVersion()));
        family = GenomeHelper.COLUMN_FAMILY_BYTES;
        this.dbAdaptor = dbAdaptor;
    }

    @Override
    public boolean open() {
        super.open();
        dbAdaptor.createTableIfNeeded(studyId, schema.getVersion(), options);
        dbAdaptor.expandTableIfNeeded(studyId, schema.getVersion(), sampleIds, options);
        hbaseWriter.open();
        return true;
    }

    @Override
    public boolean pre() {
        super.pre();
        return hbaseWriter.pre();
    }

    @Override
    protected void write(int remain) {
        List<Mutation> mutations = getMutations(remain);
        hbaseWriter.write(mutations);
    }

    @Override
    public boolean post() {
        super.post();
        return hbaseWriter.post();
    }

    @Override
    public boolean close() {
        super.close();
        return hbaseWriter.close();
    }

    protected List<Mutation> getMutations(int remain) {
        List<Mutation> mutations = new LinkedList<>();

        while (buffer.size() > remain) {
            IndexChunk indexChunk = buffer.keySet().iterator().next();
            Chunk chunk = buffer.remove(indexChunk);
            for (SampleIndexEntryBuilder builder : chunk) {
                Put put = SampleIndexEntryPutBuilder.build(builder.buildEntry());
                if (!put.isEmpty()) {
                    mutations.add(put);

                    if (chunk.isMerging() && !builder.isEmpty()) {
                        Delete delete = new Delete(put.getRow());
                        for (String gt : builder.getGtSet()) {
                            delete.addColumns(family, SampleIndexSchema.toAnnotationClinicalIndexColumn(gt));
                            delete.addColumns(family, SampleIndexSchema.toAnnotationBiotypeIndexColumn(gt));
                            delete.addColumns(family, SampleIndexSchema.toAnnotationConsequenceTypeIndexColumn(gt));
                            delete.addColumns(family, SampleIndexSchema.toAnnotationCtBtTfIndexColumn(gt));
                            delete.addColumns(family, SampleIndexSchema.toAnnotationIndexColumn(gt));
                            delete.addColumns(family, SampleIndexSchema.toAnnotationPopFreqIndexColumn(gt));
                            delete.addColumns(family, SampleIndexSchema.toAnnotationIndexCountColumn(gt));
                            delete.addColumns(family, SampleIndexSchema.toParentsGTColumn(gt));
                        }
                        delete.addColumns(family, SampleIndexSchema.toMendelianErrorColumn());
                        mutations.add(delete);
                    }
                }
            }
        }

        return mutations;
    }

}
