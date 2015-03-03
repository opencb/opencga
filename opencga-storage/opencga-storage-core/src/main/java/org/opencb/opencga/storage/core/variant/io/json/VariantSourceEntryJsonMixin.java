package org.opencb.opencga.storage.core.variant.io.json;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.opencb.biodata.models.variant.stats.VariantStats;

import java.util.Map;
import java.util.Set;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public abstract class VariantSourceEntryJsonMixin {
    
    @JsonIgnore public abstract String getFileName();
    
    @JsonIgnore public abstract Set<String> getSampleNames();

    @JsonIgnore public abstract VariantStats getStats();

    @JsonIgnore public abstract VariantStats getCohortStats(String cohortName);

}
