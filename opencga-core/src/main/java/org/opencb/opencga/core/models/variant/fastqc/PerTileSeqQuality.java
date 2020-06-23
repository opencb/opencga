package org.opencb.opencga.core.models.variant.fastqc;

public class PerTileSeqQuality {
// #Tile	Base	Mean

    private String tile;
    private String base;
    private double mean;

    public PerTileSeqQuality() {
    }

    public PerTileSeqQuality(String tile, String base, double mean) {
        this.tile = tile;
        this.base = base;
        this.mean = mean;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PerTileSeqQuality{");
        sb.append("tile='").append(tile).append('\'');
        sb.append(", base='").append(base).append('\'');
        sb.append(", mean=").append(mean);
        sb.append('}');
        return sb.toString();
    }

    public String getTile() {
        return tile;
    }

    public PerTileSeqQuality setTile(String tile) {
        this.tile = tile;
        return this;
    }

    public String getBase() {
        return base;
    }

    public PerTileSeqQuality setBase(String base) {
        this.base = base;
        return this;
    }

    public double getMean() {
        return mean;
    }

    public PerTileSeqQuality setMean(double mean) {
        this.mean = mean;
        return this;
    }
}
