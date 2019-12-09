package org.opencb.opencga.storage.hadoop.variant.index.family;

import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created on 11/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MendelianErrorSampleIndexConverter {

    protected static final byte SEPARATOR = ',';
    protected static final byte MENDELIAN_ERROR_SEPARATOR = '_';
    protected static final byte MENDELIAN_ERROR_CODE_SEPARATOR = ':'; // optional

    public static void toBytes(ByteArrayOutputStream stream, Variant variant, String gt, int gtIdx, int errorCode) throws IOException {
        if (stream.size() != 0) {
            stream.write(SEPARATOR);
        }
        stream.write(Bytes.toBytes(variant.toString()));
        stream.write(MENDELIAN_ERROR_SEPARATOR);
        stream.write(Bytes.toBytes(gt));
        stream.write(MENDELIAN_ERROR_SEPARATOR);
        stream.write(Bytes.toBytes(Integer.toString(gtIdx)));
        stream.write(MENDELIAN_ERROR_CODE_SEPARATOR);
        stream.write(Bytes.toBytes(Integer.toString(errorCode)));
    }

    public static List<Variant> toVariants(byte[] bytes, int offset, int length) {
        MendelianErrorSampleIndexEntryIterator iterator = new MendelianErrorSampleIndexEntryIterator(bytes, offset, length);
        ArrayList<Variant> list = new ArrayList<>(iterator.getApproxSize());
        iterator.forEachRemaining(list::add);
        return list;
    }

}
