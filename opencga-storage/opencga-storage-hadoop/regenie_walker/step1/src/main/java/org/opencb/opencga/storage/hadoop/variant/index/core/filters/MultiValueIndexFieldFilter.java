package org.opencb.opencga.storage.hadoop.variant.index.core.filters;

import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.core.CategoricalIndexField;

import java.util.List;

public class MultiValueIndexFieldFilter extends IndexFieldFilter {

    private final boolean[] validValues;
    private boolean exactFilter;

    public <T> MultiValueIndexFieldFilter(CategoricalIndexField<T> index, List<T> acceptedValues) {
        super(index);
        int indexLength = index.getBitLength();
        validValues = new boolean[1 << indexLength];
        exactFilter = true;
        for (T expectedValue : acceptedValues) {
            int code = index.encode(expectedValue);
            validValues[code] = true;
            exactFilter &= !index.ambiguous(code);
        }
    }

    public boolean allValid() {
        for (boolean validValue : validValues) {
            if (!validValue) {
                return false;
            }
        }
        return true;
    }

    public boolean noneValid() {
        for (boolean validValue : validValues) {
            if (validValue) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean test(int code) {
        return validValues[code];
    }

    @Override
    public boolean isExactFilter() {
        return exactFilter;
    }

    @Override
    public String toString() {
        StringBuilder msg = new StringBuilder(getClass().getSimpleName() + "{" + getIndex().getId()
                + " (offset=" + getIndex().getBitOffset() + ", length=" + getIndex().getBitLength() + ") : [ ");
        boolean first = true;
        for (int i = 0; i < validValues.length; i++) {
            boolean validValue = validValues[i];
            if (validValue) {
                if (!first) {
                    msg.append(" , ");
                }
                first = false;
                msg.append(IndexUtils.binaryToString(i, getIndex().getBitLength())).append(" (").append(getIndex().decode(i)).append(")");
            }
        }
        msg.append(" ] }");
        return msg.toString();
    }
}
