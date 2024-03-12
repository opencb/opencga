package org.opencb.opencga.storage.core.variant.adaptors;

import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Genotype;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created on 04/06/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public enum GenotypeClass implements Predicate<String> {
    /**
     * Homozygous reference.
     * <p>
     * 0, 0/0, 0/0/0, ...
     */
    HOM_REF(gt -> {
        if (gt.length() == 3) {
            return gt.equals("0/0") || gt.equals("0|0");
        } else if (gt.length() == 1) {
            return gt.charAt(0) == '0';
        }
        return StringUtils.containsOnly(gt, '0', '/', '|');
    }),

    /**
     * Homozygous alternate.
     * <p>
     * 1, 1/1, 1|1, 1/1/1, ...
     */
    HOM_ALT(str -> {
        if (str.equals("1/1") || str.equals("1|1")) {
            return true;
        }
        Genotype gt = parseGenotype(str);
        if (gt == null) {
            // Skip invalid genotypes
            return false;
        }
        int[] alleles = gt.getAllelesIdx();
        if (alleles.length == 2) {
            return alleles[0] == 1 && alleles[1] == 1;
        }
        for (int allele : alleles) {
            if (allele != 1) {
                return false;
            }
        }
        return true;
    }),

    /**
     * Heterozygous.
     * <p>
     * 0/1, 1/2, 0|1, 1|0, ./1, 0/1/2, ...
     */
    HET(str -> {
        if (str.equals("0/1")) {
            return true;
        }
        Genotype gt = parseGenotype(str);
        if (gt == null) {
            // Skip invalid genotypes
            return false;
        }
        int[] alleles = gt.getAllelesIdx();
        if (alleles.length == 2) {
            return alleles[0] != alleles[1] && (alleles[0] == 1 || alleles[1] == 1);
        }
        if (gt.isHaploid()) {
            // Discard if haploid
            return false;
        }
        int firstAllele = alleles[0];
        for (int i = 1; i < alleles.length; i++) {
            int allele = alleles[i];
            if (allele == firstAllele) {
                return false;
            }
        }
        return true;
    }),

    /**
     * Heterozygous Reference.
     * <p>
     * 0/1, 0|1, 1|0, ...
     */
    HET_REF(str -> {
        if (str.equals("0/1")) {
            return true;
        }
        Genotype gt = parseGenotype(str);
        if (gt == null) {
            // Skip invalid genotypes
            return false;
        }
        int[] alleles = gt.getAllelesIdx();
        if (alleles.length == 2) {
            return alleles[0] == 0 && alleles[1] == 1 || alleles[0] == 1 && alleles[1] == 0;
        }
        if (gt.isHaploid()) {
            // Discard if haploid
            return false;
        }
        boolean hasReference = false;
        boolean hasAlternate = false;
        boolean hasMissing = false;
        boolean hasOtherAlternate = false;

        for (int allele : alleles) {
            hasReference |= allele == 0;
            hasAlternate |= allele == 1; // Discard ref and missing
            hasMissing |= allele < 0;
            hasOtherAlternate |= allele > 1;
        }
        return hasReference && hasAlternate && !hasMissing && !hasOtherAlternate;
    }),

    /**
     * Heterozygous Alternate.
     * <p>
     * 1/2, 1/3, 1/4, 2|1, ...
     */
    HET_ALT(str -> {
        Genotype gt = parseGenotype(str);
        if (gt == null) {
            // Skip invalid genotypes
            return false;
        }
        int[] alleles = gt.getAllelesIdx();
        if (alleles.length == 2) {
            return alleles[0] == 1 && alleles[1] > 1 || alleles[0] > 1 && alleles[1] == 1;
        }
        if (gt.isHaploid()) {
            // Discard if haploid
            return false;
        }
        boolean hasReference = false;
        boolean hasAlternate = false;
        boolean hasMissing = false;
        boolean hasOtherAlternate = false;

        for (int allele : alleles) {
            hasReference |= allele == 0;
            hasAlternate |= allele == 1;
            hasMissing |= allele < 0;
            hasOtherAlternate |= allele > 1;
        }
        return hasAlternate && hasOtherAlternate && !hasReference && !hasMissing;
    }),

    /**
     * Heterozygous Missing.
     * <p>
     * 1/., ./1, ...
     */
    HET_MISS(str -> {
        Genotype gt = parseGenotype(str);
        if (gt == null) {
            // Skip invalid genotypes
            return false;
        }
        int[] alleles = gt.getAllelesIdx();
        if (alleles.length == 2) {
            return alleles[0] == 1 && alleles[1] < 0 || alleles[0] < 0 && alleles[1] == 1;
        }
        if (gt.isHaploid()) {
            // Discard if haploid
            return false;
        }
        boolean hasReference = false;
        boolean hasAlternate = false;
        boolean hasMissing = false;
        boolean hasOtherAlternate = false;

        for (int allele : alleles) {
            hasReference |= allele == 0;
            hasAlternate |= allele == 1;
            hasMissing |= allele < 0;
            hasOtherAlternate |= allele > 1;
        }
        return hasAlternate && hasMissing && !hasReference && !hasOtherAlternate;
    }),

    /**
     * Missing.
     * <p>
     * ., ./., ././., ...
     */
    MISS(str -> {
        Genotype gt = parseGenotype(str);
        if (gt == null) {
            // Skip invalid genotypes
            return false;
        }
        for (int allele : gt.getAllelesIdx()) {
            if (allele != -1) {
                return false;
            }
        }
        return true;
    }),

    /**
     * Genotypes containing any secondary alternate.
     * <p>
     * 1/2, 2/3, ./2, 0/2, ...
     */
    SEC(str -> {
        Genotype gt = parseGenotype(str);
        if (gt == null) {
            // Skip invalid genotypes
            return false;
        }
        for (int allele : gt.getAllelesIdx()) {
            if (allele > 1) {
                return true;
            }
        }
        return false;
    }),

    /**
     * Genotypes containing reference and secondary alternates only.
     * <p>
     * 0/2, 2/3, ./2, 2/2, ...
     */
    SEC_ALT(str -> {
        Genotype gt = parseGenotype(str);
        if (gt == null) {
            // Skip invalid genotypes
            return false;
        }
        boolean hasSecondaryAlternate = false;
        for (int allele : gt.getAllelesIdx()) {
            if (allele == 1) {
                return false;
            } else if (allele > 1) {
                hasSecondaryAlternate = true;
            }
        }
        return hasSecondaryAlternate;
    }),


    /**
     * Contains the main alternate.
     * <p>
     * 0/1, 1/1, 1/2, 1, ./1, ...
     */
    MAIN_ALT(str -> {
        if (str.length() == 3) {
            char sep = str.charAt(1);
            if (sep == '/' || sep == '|') {
                return str.charAt(0) == '1' || str.charAt(2) == '1';
            }
        } else if (str.length() == 1) {
            return str.charAt(0) == '1';
        }

        Genotype genotype = parseGenotype(str);
        if (genotype == null) {
            return false;
        }
        for (int i : genotype.getAllelesIdx()) {
            if (i == 1) {
                return true;
            }
        }
        return false;
    }),

    NA(Genotype.NA::equals);

    /**
     * Indicate that the genotype information is unknown.
     *
     * It could be any value: 0/0, ./., 2/2 ...
     */
    public static final String UNKNOWN_GENOTYPE = "?/?";
    /**
     * Indicate that the genotype value was not available in the input variant file.
     */
    public static final String NA_GT_VALUE = Genotype.NA;
    /**
     * Indicate that none genotype should match with this value.
     */
    public static final String NONE_GT_VALUE = "x/x";

    private final Predicate<String> predicate;

    private static final Logger LOGGER = LoggerFactory.getLogger(GenotypeClass.class);

    GenotypeClass(Predicate<String> predicate) {
        final char first = UNKNOWN_GENOTYPE.charAt(0);
        Predicate<String> notUnknown = gt -> {
            if (gt.charAt(0) == first) {
                return !gt.equals(UNKNOWN_GENOTYPE);
            } else {
                return true;
            }
        };
//        Predicate<String> notUnknown = gt -> !gt.equals(UNKNOWN_GENOTYPE);
        this.predicate = notUnknown.and(predicate);
    }

    public Predicate<String> predicate() {
        return predicate;
    }

    @Override
    public boolean test(String genotype) {
        return predicate.test(genotype);
    }

    public List<String> filter(String... gts) {
        return filter(Arrays.asList(gts));
    }

    public List<String> filter(Collection<String> gts) {
        return gts.stream().filter(predicate).collect(Collectors.toList());
    }

    public static List<String> filter(Collection<String> genotypesFilter, List<String> loadedGts) {
        return filter(genotypesFilter, loadedGts, Arrays.asList("0/0", "./."));
    }

    public static List<String> filter(Collection<String> genotypesFilter, List<String> loadedGts, List<String> defaultGts) {
        Set<String> filteredGts = new LinkedHashSet<>(genotypesFilter.size());
        for (String gt : genotypesFilter) {
            GenotypeClass genotypeClass = GenotypeClass.from(gt);
            if (gt.equals(NONE_GT_VALUE) || gt.equals(NA_GT_VALUE) || gt.equals(UNKNOWN_GENOTYPE)) {
                filteredGts.add(gt);
            } else if (genotypeClass == null) {
                boolean negated = VariantQueryUtils.isNegated(gt);
                if (negated) {
                    gt = VariantQueryUtils.removeNegation(gt);
                }
                Genotype genotype = parseGenotype(gt);
                if (genotype == null) {
                    // Skip invalid genotypes
                    continue;
                }

                // Normalize if needed
                if (!genotype.isPhased()) {
                    genotype.normalizeAllelesIdx();
                }

                // If unphased, add phased genotypes, if any
                List<String> phasedGenotypes = getPhasedGenotypes(genotype, loadedGts);

                if (negated) {
                    filteredGts.add(VariantQueryUtils.NOT + genotype.toString());
                    for (String phasedGenotype : phasedGenotypes) {
                        filteredGts.add(VariantQueryUtils.NOT + phasedGenotype);
                    }
                } else {
                    filteredGts.add(genotype.toString());
                    filteredGts.addAll(phasedGenotypes);
                }

            } else {
                filteredGts.addAll(genotypeClass.filter(loadedGts));
                filteredGts.addAll(genotypeClass.filter(defaultGts));
            }
        }
        return new ArrayList<>(filteredGts);
    }

    public static List<String> getPhasedGenotypes(Genotype genotype) {
        return getPhasedGenotypes(genotype, null);
    }

    public static List<String> getPhasedGenotypes(Genotype genotype, List<String> loadedGts) {
        genotype = new Genotype(genotype);
        if (!genotype.isPhased()) {
            List<String> phasedGts = new ArrayList<>(2);
            genotype.setPhased(true);
            String phased = genotype.toString();
            if (loadedGts == null || loadedGts.contains(phased)) {
                phasedGts.add(phased);
            }
            int[] allelesIdx = genotype.getAllelesIdx();
            if (allelesIdx.length == 2) {
                int allelesIdx0 = allelesIdx[0];
                allelesIdx[0] = allelesIdx[1];
                allelesIdx[1] = allelesIdx0;
                phased = genotype.toString();
                if (loadedGts == null || loadedGts.contains(phased)) {
                    phasedGts.add(phased);
                }
            }
            return phasedGts;
        } else {
            return new ArrayList<>();
        }
    }

    /**
     * Gets the GenotypeClass, returning {@code null} if not found.
     *
     * @param gt Genotype class name
     * @return the enum, null if not found
     */
    public static GenotypeClass from(String gt) {
        GenotypeClass genotypeClass = EnumUtils.getEnum(GenotypeClass.class, gt.toUpperCase());
        if (genotypeClass == null && VariantQueryUtils.isNegated(gt)) {
            if (EnumUtils.getEnum(GenotypeClass.class, VariantQueryUtils.removeNegation(gt.toUpperCase())) != null) {
                throw VariantQueryException.malformedParam(VariantQueryParam.GENOTYPE, gt,
                        "Unsupported negated genotype alias");
            }
        }
        return genotypeClass;
    }

    public static List<String> expandMultiAllelicGenotype(String genotypeStr, List<String> loadedGenotypes) {
        List<String> genotypes = new ArrayList<>(5);
        if (from(genotypeStr) != null) {
            // Discard GenotypeClass
            return genotypes;
        }
        if (genotypeStr.equals(NA_GT_VALUE)) {
            // Discard special genotypes
            return genotypes;
        }
        Genotype genotype;
        try {
            genotype = new Genotype(genotypeStr);
        } catch (RuntimeException e) {
            throw new VariantQueryException("Malformed genotype '" + genotypeStr + "'", e);
        }
        int[] allelesIdx = genotype.getAllelesIdx();
        boolean hasSecAlt = false;
        for (int i = 0; i < allelesIdx.length; i++) {
            if (allelesIdx[i] > 1) {
                allelesIdx[i] = 2;
                hasSecAlt = true;
            }
        }
        if (hasSecAlt) {
            List<String> phasedGenotypes = getPhasedGenotypes(genotype);
            phasedGenotypes.add(genotype.toString());
            for (String phasedGenotype : phasedGenotypes) {
                String regex = phasedGenotype
                        .replace(".", "\\.")
                        .replace("|", "\\|")
                        .replace("2", "([2-9]|[0-9][0-9])"); // Replace allele "2" with "any number >= 2")
                Pattern pattern = Pattern.compile(regex);
                for (String loadedGenotype : loadedGenotypes) {
                    if (pattern.matcher(loadedGenotype).matches()) {
                        genotypes.add(loadedGenotype);
                    }
                }
            }
        }
        return genotypes;
    }

    private static Genotype parseGenotype(String gt) {
        if (VariantQueryUtils.isNegated(gt)) {
            throw new IllegalStateException("Unable to parse negated genotype " + gt);
        }
        if (NONE_GT_VALUE.equals(gt) || NA_GT_VALUE.equals(gt) || UNKNOWN_GENOTYPE.equals(gt)) {
            return null;
        }
        Genotype genotype;
        try {
            genotype = new Genotype(gt);
        } catch (IllegalArgumentException e) {
            LOGGER.debug("Invalid genotype " + gt, e);
            return null;
        }
        return genotype;
    }

    public static Set<GenotypeClass> classify(String gt) {
        Set<GenotypeClass> genotypeClasses = new HashSet<>();
        for (GenotypeClass value : values()) {
            if (value.test(gt)) {
                genotypeClasses.add(value);
            }
        }
        return genotypeClasses;
    }
}
