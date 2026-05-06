package org.opencb.opencga.storage.core.variant.walker;

/**
 * Utility methods for chromosome ordering and comparison.
 */
public final class ChromosomeUtils {

    private ChromosomeUtils() {
    }

    /**
     * Check if two lexicographically ordered chromosomes are consecutive in natural order or if there
     * might be other chromosomes in between.
     * e.g.
     *  naturalConsecutiveChromosomes("1", "2") == true
     *  naturalConsecutiveChromosomes("1", "10") == false
     *  naturalConsecutiveChromosomes("1", "X") == true
     * @param prevChromosome    Previous chromosome
     * @param newChromosome     New chromosome
     * @return                  True if the chromosomes are consecutive in natural order
     */
    public static boolean naturalConsecutiveChromosomes(String prevChromosome, String newChromosome) {
        if (newChromosome.equals(prevChromosome)) {
            return true;
        }
        if (isDigitChromosome(prevChromosome)) {
            // prevChromosome == 1 or 10
            if (isSingleDigitChromosome(prevChromosome)) {
                // prevChromosome == 1
                if (isDigitChromosome(newChromosome)) {
                    // newChromosome == 2 or 10
                    // 1 -> 2  : TRUE
                    // 1 -> 10 : FALSE
                    return isSingleDigitChromosome(newChromosome);
                } else {
                    // newChromosome == X
                    // 1 -> X  : FALSE
                    return false;
                }
            } else {
                // prevChromosome == 10
                if (isDigitChromosome(newChromosome)) {
                    // newChromosome == 11 or 2
                    // 10 -> 11  : TRUE
                    // 10 -> 2   : FALSE
                    return !isSingleDigitChromosome(newChromosome);
                } else {
                    // newChromosome == X
                    // 10 -> X : FALSE
                    return false;
                }
            }
        } else {
            // prevChromosome == X
            // X -> Y  : TRUE
            // X -> 1  : FALSE
            return !isDigitChromosome(newChromosome);
        }
    }

    public static boolean isSingleDigitChromosome(String chromosome) {
        return Character.isDigit(chromosome.charAt(0)) && (chromosome.length() == 1 || !Character.isDigit(chromosome.charAt(1)));
    }

    public static boolean isDigitChromosome(String chromosome) {
        return Character.isDigit(chromosome.charAt(0));
    }
}
