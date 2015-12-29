/**
 *
 */
package org.opencb.opencga.storage.hadoop.variant.index;

import org.opencb.biodata.models.feature.AllelesCode;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfRecord;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk.
 */
public class VcfRecordWrapper {

//    public static enum CallStatus {REF,VARIANT,NOCALL,MULTI_ALLELIC}

    private Genotype genotype;
    //    private CallStatus status;
    private Integer id;
    private VcfSlice slice;
    private VcfRecord rec;


    public VcfRecordWrapper(Integer id, VcfSlice vcfSlice, VcfRecord rec) {
        this.id = id;
        this.slice = vcfSlice;
        this.rec = rec;
    }

    public Integer getId() {
        return id;
    }

    public boolean isRegionVariant() {
        return this.rec.getRelativeEnd() != 0;
    }

    public void setGenotype(String gt) {
        this.genotype = new Genotype(gt, getReference(), getAlternates());
    }

    public Genotype getGenotype() {
        return genotype;
    }

    public int getStartPosition() {
        return this.slice.getPosition() + this.rec.getRelativeStart();
    }

    public int getEndPosition() {
        if (isRegionVariant()) {
            return this.slice.getPosition() + this.rec.getRelativeEnd();
        }
        return getStartPosition();
    }

    public List<Integer> getPositions() {
        if (isRegionVariant()) {
            int end = getEndPosition();
            int start = getStartPosition();
            List<Integer> lst = new ArrayList<Integer>(end - start);
            for (int i = start; i <= end; ++i) {
                lst.add(i);
            }
            return lst;
        }
        return Collections.singletonList(getStartPosition());
    }

    public String getReference() {
        return rec.getReference();
    }

    public String getChromosome() {
        return this.slice.getChromosome();
    }

    public String getAlternate() {
        return rec.getAlternate(0);
    }

    public List<String> getAlternates() {
        return rec.getAlternateList();
    }

    public boolean isMultiAlternates() {
        return getGenotype().getCode().equals(AllelesCode.MULTIPLE_ALTERNATES);
    }

    public boolean hasVariant() {
        int[] alidx = getGenotype().getAllelesIdx();
        int idxlen = alidx.length;
        for (int i = 0; i < idxlen; ++i) {
            if (alidx[i] > 0) {
                return true;
            }
        }
        return false;
    }

    public boolean hasNoCall() {
        int[] alidx = getGenotype().getAllelesIdx();
        int idxlen = alidx.length;
        for (int i = 0; i < idxlen; ++i) {
            if (alidx[i] == -1) {
                return true;
            }
        }
        return false;
    }

    public boolean isAllelesRef() {
        return getGenotype().isAllelesRefs();
    }

    public boolean isHaploid() {
        return getGenotype().getAllelesIdx().length <= 1;
    }
}
