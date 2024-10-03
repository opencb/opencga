package org.opencb.opencga.storage.core.variant.query;

import org.opencb.commons.datastore.core.Query;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.SOURCE;

public enum VariantQuerySource {
//    SECONDARY_ANNOTATION_INDEX,
    SECONDARY_SAMPLE_INDEX,
    VARIANT_INDEX;


    public boolean isSecondary() {
        return this != VARIANT_INDEX;
    }

    public static VariantQuerySource get(Query query) {
        if (query == null) {
            return VARIANT_INDEX;
        }
        return get(query.getString(SOURCE.key(), null));
    }

    public static VariantQuerySource get(String source) {
        if (source == null) {
            return VARIANT_INDEX;
        }
        switch (source.toLowerCase().replace("_", "").replace("-", "")){
            case "variantindex":
                return VARIANT_INDEX;
            case "secondarysampleindex":
                return SECONDARY_SAMPLE_INDEX;
//            case "secondaryannotationindex":
//                return SECONDARY_ANNOTATION_INDEX;
            default:
                throw new IllegalArgumentException("Unknown VariantQuerySource " + source);
        }
    }
}
