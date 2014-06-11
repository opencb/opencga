package org.opencb.opencga.storage.variant.json;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public abstract class VariantStatsJsonMixin {
    
    @JsonIgnore public abstract String[] getAltAlleles();
    
    @JsonIgnore public abstract boolean isIndel();
          
    @JsonIgnore public abstract boolean isSNP();
    
    @JsonIgnore public abstract String getId();
}
