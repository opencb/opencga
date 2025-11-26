package org.opencb.opencga.storage.core.variant.search.solr;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.search.VariantSearchModel;

import java.util.List;

public abstract class VariantSearchIdGenerator {

    public abstract VariantSearchModel addId(Variant variant, VariantSearchModel searchModel);

    public abstract String getId(Variant variant);

    public abstract Variant getVariant(VariantSearchModel searchModel);

    public static VariantSearchIdGenerator getGenerator(SearchIndexMetadata indexMetadata) {
        String version = indexMetadata.getAttributes().getString(VariantStorageOptions.SEARCH_STATS_VARIANT_ID_VERSION.key(), null);
        if (StringUtils.isEmpty(version)) {
            return new VariantSearchIdGeneratorV1();
        }
        switch (version) {
            case "v1":
                return new VariantSearchIdGeneratorV1();
            case "v2":
                return new VariantSearchIdGeneratorV2();
            default:
                throw new IllegalArgumentException("Unknown ID version: " + version);
        }
    }

    private static class VariantSearchIdGeneratorV1 extends VariantSearchIdGenerator {
        static final String HASH_PREFIX = "#";

        @Override
        public VariantSearchModel addId(Variant variant, VariantSearchModel variantSearchModel) {

            String variantId = getId(variant);
            variantSearchModel.setId(variantId);   // Internal unique ID e.g.  3:1000:AT:-
            variantSearchModel.getAttr().put("attr_id", variant.toString());

            return variantSearchModel;
        }


        @Override
        public String getId(Variant variant) {
            String variantString = variant.toString();
            if (variantString.length() > 32766) {
                // variantString.length() >= Short.MAX_VALUE
                return hashVariantId(variant, variantString);
            } else {
                return variantString;
            }
        }

        @Override
        public Variant getVariant(VariantSearchModel searchModel) {
            String variantId = searchModel.getId();
            if (variantId.startsWith(HASH_PREFIX)) {
                Object o = searchModel.getAttr().get("attr_id");
                variantId = o instanceof String ? (String) o : ((List<String>) o).get(0);
            }
            Variant variant = new Variant(variantId);
            variant.setId(variantId);
            return variant;
        }

        private static String hashVariantId(Variant variant, String variantString) {
            return HASH_PREFIX + variant.getChromosome() + ":" + variant.getStart() + ":" + Integer.toString(variantString.hashCode());
        }

    }

    private static class VariantSearchIdGeneratorV2 extends VariantSearchIdGenerator {
        @Override
        public VariantSearchModel addId(Variant variant, VariantSearchModel variantSearchModel) {

            String variantIdFull = variant.toString();
            String variantId = getId(variant, variantIdFull);

            variantSearchModel.setFullId(variantIdFull);
            variantSearchModel.setId(variantId);

            return variantSearchModel;
        }

        @Override
        public String getId(Variant variant) {
            return getId(variant, variant.toString());
        }

        @Override
        public Variant getVariant(VariantSearchModel searchModel) {
            Variant variant = new Variant(searchModel.getFullId());
            variant.setId(searchModel.getFullId());
            return variant;
        }

        private static String getId(Variant variant, String variantIdFull) {
            StringBuilder variantId = new StringBuilder();
            String chr = variant.getChromosome();
            if (Character.isDigit(chr.charAt(0))) {
                if (chr.length() == 1 || !Character.isDigit(chr.charAt(1))) {
                    // Chromosome is a single digit number (e.g. 1, 2, ..., 9)
                    // Need to add a pad zero to make it sortable
                    variantId.append("0");
                }
            }

            variantId.append(variant.getChromosome())
                    .append(":")
                    .append(StringUtils.leftPad(String.valueOf(variant.getStart()), 12, '0'))
                    .append(":#")
                    .append(variantIdFull.hashCode());
            return variantId.toString();
        }
    }

}
