package org.opencb.opencga.storage.hadoop.variant.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StudyConfiguration;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * Created on 11/03/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */

public class HBaseVariantStudyConfiguration extends StudyConfiguration {

    public static final String BATCHES_FIELD = "batches";

    @JsonIgnore
    private List<BatchFileOperation> batches = new ArrayList<>();

    protected HBaseVariantStudyConfiguration() {
        super();
    }

    public HBaseVariantStudyConfiguration(StudyConfiguration other) {
        super(other);
        batches = new ArrayList<>();
        if (other instanceof HBaseVariantStudyConfiguration) {
            batches.addAll(((HBaseVariantStudyConfiguration) other).getBatches());
        }
    }

    @Override
    public StudyConfiguration newInstance() {
        return new HBaseVariantStudyConfiguration(this);
    }

    public List<BatchFileOperation> getBatches() {
        return batches;
    }

    public HBaseVariantStudyConfiguration setBatches(List<BatchFileOperation> batches) {
        this.batches = batches;
        return this;
    }

    @Override
    public ObjectMap getAttributes() {
        return super.getAttributes() == null
                ? new ObjectMap(BATCHES_FIELD, batches)
                : super.getAttributes().append(BATCHES_FIELD, batches);
    }

    @Override
    public String toString() {
        return "HBaseVariantStudyConfiguration{"
                + "super=" + super.toString() + ", "
                + "batches=" + batches + '}';
    }

}
