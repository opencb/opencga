package org.opencb.opencga.storage.hadoop.variant.index.sample.genotype;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleIndexEntryWriter;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
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
public class HBaseSampleIndexEntryDBWriter extends SampleIndexEntryWriter {

    private final HBaseSampleIndexDBAdaptor dbAdaptor;
    private final HBaseDataWriter<Mutation> hbaseWriter;
    protected final List<Integer> sampleIds;
    private final byte[] family;
    private final ObjectMap options;
    private final boolean isMerging;

    public HBaseSampleIndexEntryDBWriter(HBaseSampleIndexDBAdaptor dbAdaptor, HBaseManager hBaseManager,
                                         int studyId, List<Integer> sampleIds,
                                         ObjectMap options, SampleIndexSchema schema, boolean isMerging) {

        super(dbAdaptor, studyId, schema);
        this.options = options;
        this.sampleIds = sampleIds;
        hbaseWriter = new HBaseDataWriter<>(hBaseManager, dbAdaptor.getSampleIndexTableName(studyId,
                schema.getVersion()));
        family = GenomeHelper.COLUMN_FAMILY_BYTES;
        this.isMerging = isMerging;
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
    public boolean post() {
        super.post();
        return hbaseWriter.post();
    }

    @Override
    public boolean close() {
        super.close();
        return hbaseWriter.close();
    }

    @Override
    public boolean write(List<SampleIndexEntry> list) {
        List<Mutation> mutations = new LinkedList<>();
        for (SampleIndexEntry sampleIndexEntry : list) {
            Put put = SampleIndexEntryPutBuilder.build(sampleIndexEntry);
            if (!put.isEmpty()) {
                mutations.add(put);
                if (isMerging) {
                    Delete delete = new Delete(put.getRow());
                    for (String gt : sampleIndexEntry.getGts().keySet()) {
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
        return true;
    }

}
