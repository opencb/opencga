package org.opencb.opencga.storage.core.variant.score;

public class VariantScoreFormatDescriptor {

    private int variantColumnIdx = -1;
    private int chrColumnIdx = -1;
    private int posColumnIdx = -1;
    private int refColumnIdx = -1;
    private int altColumnIdx = -1;
    private int scoreColumnIdx = -1;
    private int pvalueColumnIdx = -1;


    public VariantScoreFormatDescriptor() {
    }

    public VariantScoreFormatDescriptor(int variantColumnIdx, int scoreColumnIdx, int pvalueColumnIdx) {
        this.variantColumnIdx = check("variant", variantColumnIdx);
        this.scoreColumnIdx = check("score", scoreColumnIdx);
        this.pvalueColumnIdx = pvalueColumnIdx;
    }

    public VariantScoreFormatDescriptor(int chrColumnIdx, int posColumnIdx, int refColumnIdx, int altColumnIdx,
                                        int scoreColumnIdx, int pvalueColumnIdx) {
        this.chrColumnIdx = check("chr", chrColumnIdx);
        this.posColumnIdx = check("pos", posColumnIdx);
        this.refColumnIdx = check("ref", refColumnIdx);
        this.altColumnIdx = check("alt", altColumnIdx);
        this.scoreColumnIdx = check("score", scoreColumnIdx);
        this.pvalueColumnIdx = pvalueColumnIdx;
    }

    public int getVariantColumnIdx() {
        return variantColumnIdx;
    }

    public VariantScoreFormatDescriptor setVariantColumnIdx(int variantColumnIdx) {
        this.variantColumnIdx = check("variant", variantColumnIdx);
        return this;
    }

    public int getChrColumnIdx() {
        return chrColumnIdx;
    }

    public VariantScoreFormatDescriptor setChrColumnIdx(int chrColumnIdx) {
        this.chrColumnIdx = check("chr", chrColumnIdx);
        return this;
    }

    public int getPosColumnIdx() {
        return posColumnIdx;
    }

    public VariantScoreFormatDescriptor setPosColumnIdx(int posColumnIdx) {
        this.posColumnIdx = check("pos", posColumnIdx);
        return this;
    }

    public int getRefColumnIdx() {
        return refColumnIdx;
    }

    public VariantScoreFormatDescriptor setRefColumnIdx(int refColumnIdx) {
        this.refColumnIdx = check("ref", refColumnIdx);
        return this;
    }

    public int getAltColumnIdx() {
        return altColumnIdx;
    }

    public VariantScoreFormatDescriptor setAltColumnIdx(int altColumnIdx) {
        this.altColumnIdx = check("alt", altColumnIdx);
        return this;
    }

    public int getScoreColumnIdx() {
        return scoreColumnIdx;
    }

    public VariantScoreFormatDescriptor setScoreColumnIdx(int scoreColumnIdx) {
        this.scoreColumnIdx = check("score", scoreColumnIdx);
        return this;
    }

    public int getPvalueColumnIdx() {
        return pvalueColumnIdx;
    }

    public VariantScoreFormatDescriptor setPvalueColumnIdx(int pvalueColumnIdx) {
        this.pvalueColumnIdx = check("pvalue", pvalueColumnIdx);
        return this;
    }

    public void checkValid() {
        if (scoreColumnIdx < 0) {
            throw new IllegalArgumentException("Missing score column position");
        }
        if (variantColumnIdx < 0) {
            if (chrColumnIdx < 0
                    && posColumnIdx < 0
                    && refColumnIdx < 0
                    && altColumnIdx < 0) {
                throw new IllegalArgumentException("Either CHROM,POS,REF,ALT columns or VAR column are required");
            } else {
                if (chrColumnIdx < 0) {
                    throw new IllegalArgumentException("Missing CHROM column position");
                } else if (posColumnIdx < 0) {
                    throw new IllegalArgumentException("Missing POS column position");
                } else if (refColumnIdx < 0) {
                    throw new IllegalArgumentException("Missing REF column position");
                } else if (altColumnIdx < 0) {
                    throw new IllegalArgumentException("Missing ALT column position");
                }
            }
        }
    }

    private int check(String name, int value) {
        if (value < 0) {
            throw new IllegalArgumentException("Index for column '" + name + "' should be positive");
        }
        return value;
    }

}
