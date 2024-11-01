package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.hadoop.io.WritableComparable;
import org.opencb.biodata.models.variant.Variant;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Genomic locus key.
 */
public class VariantLocusKey implements WritableComparable<VariantLocusKey>  {
    private String chromosome;
    private int position;
    private String other;

    public VariantLocusKey() {
    }

    public VariantLocusKey(String chromosome, int position) {
        this.chromosome = chromosome;
        this.position = position;
        this.other = null;
    }

    public VariantLocusKey(Variant variant) {
        this(variant.getChromosome(), variant.getStart(), variant.getReference() + "_" + variant.getAlternate());
    }

    public VariantLocusKey(String chromosome, int position, String other) {
        this.chromosome = chromosome;
        this.position = position;
        this.other = other;
    }

    @Override
    public int compareTo(VariantLocusKey o) {
        String chr1;
        String chr2;
        if (isSingleDigitChromosome(chromosome)) {
            chr1 = "0" + chromosome;
        } else {
            chr1 = chromosome;
        }
        if (isSingleDigitChromosome(o.chromosome)) {
            chr2 = "0" + o.chromosome;
        } else {
            chr2 = o.chromosome;
        }
        int i = chr1.compareTo(chr2);
        if (i == 0) {
            i = position - o.position;
        }
        if (i == 0) {
            if (other == null) {
                i = o.other == null ? 0 : -1;
            } else if (o.other == null) {
                i = 1;
            } else {
                i = other.compareTo(o.other);
            }
        }
        return i;
    }

    public static boolean isSingleDigitChromosome(String chromosome) {
        return Character.isDigit(chromosome.charAt(0)) && (chromosome.length() == 1 || !Character.isDigit(chromosome.charAt(1)));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(chromosome);
        out.writeInt(position);
        if (other != null) {
            out.writeUTF(other);
        } else {
            out.writeUTF("");
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        chromosome = in.readUTF();
        position = in.readInt();
        other = in.readUTF();
    }

    public String getChromosome() {
        return chromosome;
    }

    public VariantLocusKey setChromosome(String chromosome) {
        this.chromosome = chromosome;
        return this;
    }

    public int getPosition() {
        return position;
    }

    public VariantLocusKey setPosition(int position) {
        this.position = position;
        return this;
    }

    public String getOther() {
        return other;
    }

    public VariantLocusKey setOther(String other) {
        this.other = other;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VariantLocusKey that = (VariantLocusKey) o;
        return position == that.position
                && Objects.equals(chromosome, that.chromosome)
                && Objects.equals(other, that.other);
    }

    @Override
    public int hashCode() {
        return Objects.hash(chromosome, position, other);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VariantLocusKey{");
        sb.append("chromosome='").append(chromosome).append('\'');
        sb.append(", position=").append(position);
        sb.append(", other='").append(other).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
