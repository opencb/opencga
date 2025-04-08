package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.hadoop.mapreduce.Mapper;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;

import java.io.IOException;

public class VariantRowMapper<KEYOUT, VALUEOUT> extends Mapper<Object, VariantRow, KEYOUT, VALUEOUT> {

    private VariantsTableMapReduceHelper mrHelper;

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
        mrHelper = new VariantsTableMapReduceHelper(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        mrHelper.close();
    }

    public VariantTableHelper getHelper() {
        return mrHelper.getHelper();
    }

    public HBaseManager getHBaseManager() {
        return mrHelper.getHBaseManager();
    }

    public long getTimestamp() {
        return mrHelper.getTimestamp();
    }

    public int getStudyId() {
        return mrHelper.getStudyId();
    }

    public StudyMetadata getStudyMetadata() {
        return mrHelper.getStudyMetadata();
    }

    public VariantStorageMetadataManager getMetadataManager() {
        return mrHelper.getMetadataManager();
    }

    public void startStep() {
        mrHelper.startStep();
    }

    public void endStep(String name) {
        mrHelper.endStep(name);
    }

    public void addStepDuration(String name, long nanotime) {
        mrHelper.addStepDuration(name, nanotime);
    }

}
