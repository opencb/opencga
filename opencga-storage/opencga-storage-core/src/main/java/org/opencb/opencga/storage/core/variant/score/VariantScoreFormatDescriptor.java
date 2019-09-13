package org.opencb.opencga.storage.core.variant.score;

public class VariantScoreFormatDescriptor {

    private final int variantColumnIdx;
    private final int chrColumnIdx;
    private final int posColumnIdx;
    private final int refColumnIdx;
    private final int altColumnIdx;
    private final int scoreColumnIdx;
    private final int pvalueColumnIdx;

    public VariantScoreFormatDescriptor(int variantColumnIdx, int scoreColumnIdx, int pvalueColumnIdx) {
        this.variantColumnIdx = check("variant", variantColumnIdx);
        this.chrColumnIdx = -1;
        this.posColumnIdx = -1;
        this.refColumnIdx = -1;
        this.altColumnIdx = -1;
        this.scoreColumnIdx = check("score", scoreColumnIdx);
        this.pvalueColumnIdx = check("pvalue", pvalueColumnIdx);
    }

    public VariantScoreFormatDescriptor(int chrColumnIdx, int posColumnIdx, int refColumnIdx, int altColumnIdx,
                                        int scoreColumnIdx, int pvalueColumnIdx) {
        this.variantColumnIdx = -1;
        this.chrColumnIdx = check("chr", chrColumnIdx);
        this.posColumnIdx = check("pos", posColumnIdx);
        this.refColumnIdx = check("ref", refColumnIdx);
        this.altColumnIdx = check("alt", altColumnIdx);
        this.scoreColumnIdx = check("score", scoreColumnIdx);
        this.pvalueColumnIdx = check("pvalue", pvalueColumnIdx);
    }

    private int check(String name, int value) {
        if (value < 0) {
            throw new IllegalArgumentException("Index for column '" + name + "' should be positive");
        }
        return value;
    }

    public int getVariantColumnIdx() {
        return variantColumnIdx;
    }

    public int getChrColumnIdx() {
        return chrColumnIdx;
    }

    public int getPosColumnIdx() {
        return posColumnIdx;
    }

    public int getRefColumnIdx() {
        return refColumnIdx;
    }

    public int getAltColumnIdx() {
        return altColumnIdx;
    }

    public int getScoreColumnIdx() {
        return scoreColumnIdx;
    }

    public int getPvalueColumnIdx() {
        return pvalueColumnIdx;
    }
}
