package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.opencb.opencga.core.config.storage.FieldConfiguration;

import java.util.List;

public abstract class AbstractSchema<FIELD extends AbstractField> {

    public abstract List<FIELD> getFields();

    public FIELD getField(FieldConfiguration.Source source, String key) {
        return getFields().stream().filter(i -> i.getSource() == source && i.getKey().equals(key)).findFirst().orElse(null);
    }

}
