package org.opencb.opencga.storage.core.variant.adaptors;

import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.feature.Genotype;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Created on 04/06/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public enum GenotypeClass {
    /**
     * Homozygous reference.
     * <p>
     * 0, 0/0, 0/0/0, ...
     */
    HOM_REF(gt -> StringUtils.containsOnly(gt, '0', '/', '|')),

    /**
     * Homozygous alternate.
     * <p>
     * 1, 1/1, 1|1, 2/2, 3/3, 1/1/1, ...
     */
    HOM_ALT(str -> {
        Genotype gt = new Genotype(str);
        int[] alleles = gt.getAllelesIdx();
        int firstAllele = alleles[0];
        if (firstAllele <= 0) {
            // Discard if first allele is reference or missing
            return false;
        }
        for (int i = 1; i < alleles.length; i++) {
            if (alleles[i] != firstAllele) {
                return false;
            }
        }
        return true;
    }),

    /**
     * Heterozygous.
     * <p>
     * 0/1, 1/2, 0/2, 2/4, 0|1, 1|0, 0/0/1, ...
     */
    HET(str -> {
        Genotype gt = new Genotype(str);
        int[] alleles = gt.getAllelesIdx();
        int firstAllele = alleles[0];
        if (firstAllele < 0 || gt.isHaploid()) {
            // Discard if first allele is missing, or if haploid
            return false;
        }
        for (int i = 1; i < alleles.length; i++) {
            int allele = alleles[i];
            if (allele == firstAllele || allele < 0) {
                return false;
            }
        }
        return true;
    }),

    /**
     * Heterozygous Reference.
     * <p>
     * 0/1, 0/2, 0/3, 0|1, ...
     */
    HET_REF(str -> {
        Genotype gt = new Genotype(str);
        if (gt.isHaploid()) {
            // Discard if haploid
            return false;
        }
        boolean hasReference = false;
        boolean hasAlternate = false;

        for (int allele : gt.getAllelesIdx()) {
            hasReference |= allele == 0;
            hasAlternate |= allele > 0; // Discard ref and missing
        }
        return hasReference && hasAlternate;
    }),

    /**
     * Heterozygous Alternate.
     * <p>
     * 1/2, 1/3, 2/4, 2|1, ...
     */
    HET_ALT(str -> {
        Genotype gt = new Genotype(str);
        int[] alleles = gt.getAllelesIdx();
        int firstAllele = alleles[0];
        if (firstAllele <= 0 || gt.isHaploid()) {
            // Discard if first allele is reference or missing, or if haploid
            return false;
        }
        for (int i = 1; i < alleles.length; i++) {
            int allele = alleles[i];
            if (allele == firstAllele || allele <= 0) {
                return false;
            }
        }
        return true;
    }),

    /**
     * Missing.
     * <p>
     * ., ./., ././., ...
     */
    MISS(str -> {
        Genotype gt = new Genotype(str);
        for (int allele : gt.getAllelesIdx()) {
            if (allele != -1) {
                return false;
            }
        }
        return true;
    });

    public static final String UNKNOWN_GENOTYPE = "?/?";
    private final Predicate<String> predicate;

    GenotypeClass(Predicate<String> predicate) {
        Predicate<String> stringPredicate = gt -> !gt.equals(UNKNOWN_GENOTYPE);
        this.predicate = stringPredicate.and(predicate);
    }

    public Predicate<String> predicate() {
        return predicate;
    }

    public boolean test(String genotype) {
        return predicate.test(genotype);
    }

    public List<String> filter(String... gts) {
        return filter(Arrays.asList(gts));
    }

    public List<String> filter(List<String> gts) {
        return gts.stream().filter(predicate).collect(Collectors.toList());
    }

    public static List<String> filter(List<String> gts, List<String> loadedGts) {
        return filter(gts, loadedGts, Arrays.asList("0/0", "./."));
    }

    public static List<String> filter(List<String> gts, List<String> loadedGts, List<String> defaultGts) {
        Set<String> filteredGts = new HashSet<>(gts.size());
        for (String gt : gts) {
            GenotypeClass genotypeClass = GenotypeClass.from(gt);
            if (genotypeClass == null) {
                filteredGts.add(gt);
            } else {
                filteredGts.addAll(genotypeClass.filter(loadedGts));
                filteredGts.addAll(genotypeClass.filter(defaultGts));
            }
        }
        return new ArrayList<>(filteredGts);
    }

    /**
     * Gets the GenotypeClass, returning {@code null} if not found.
     *
     * @param gt Genotype class name
     * @return the enum, null if not found
     */
    public static GenotypeClass from(String gt) {
        return EnumUtils.getEnum(GenotypeClass.class, gt.toUpperCase());
    }
}
