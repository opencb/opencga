package org.opencb.opencga.storage.hadoop.variant.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StudyConfiguration;

import java.util.List;

/**
 *
 * Created on 11/03/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */

public class HBaseVariantStudyConfiguration extends StudyConfiguration {

    public static final String BATCHES_FIELD = "batches";

    protected HBaseVariantStudyConfiguration() {
        super();
    }

    public HBaseVariantStudyConfiguration(StudyConfiguration other) {
        super(other);
    }

    @Override
    public StudyConfiguration newInstance() {
        return new HBaseVariantStudyConfiguration(this);
    }

    @JsonIgnore
    public List<BatchFileOperation> getBatches() {
        return getAttributes().getAsList(BATCHES_FIELD, BatchFileOperation.class, null);
    }

    public HBaseVariantStudyConfiguration setBatches(List<BatchFileOperation> batches) {
        if (getAttributes() == null) {
            setAttributes(new ObjectMap());
        }
        getAttributes().put(BATCHES_FIELD, batches);
        return this;
    }

}
