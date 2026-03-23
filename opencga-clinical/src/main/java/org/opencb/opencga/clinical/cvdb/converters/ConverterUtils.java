package org.opencb.opencga.clinical.cvdb.converters;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.ClinicalComment;

import java.io.*;
import java.util.Arrays;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class ConverterUtils {

    protected static final String FIELD_SEPARATOR = " -- ";
    protected static final String KEY_VALUE_SEPARATOR = "=";

    private static final String DESCRIPTION_PREFIX = "DS";
    private static final String ANALYST_PREFIX = "AN";
    private static final String DEPENDENCY_PREFIX = "DP";
    private static final String FILTER_PREFIX = "FT";
    private static final String COMMENT_PREFIX = "CM";
    private static final String ATTRIBUTE_PREFIX = "AT";

    public static String encodeComent(ClinicalComment comment) {
        StringBuilder sb = new StringBuilder();

        sb.append(comment.getAuthor() == null ? " " : comment.getAuthor()).append(FIELD_SEPARATOR);
        sb.append(CollectionUtils.isEmpty(comment.getTags()) ? " " : StringUtils.join(comment.getTags(), ",")).append(FIELD_SEPARATOR);
        sb.append(comment.getMessage() == null ? " " : comment.getMessage()).append(FIELD_SEPARATOR);
        sb.append(comment.getDate() == null ? " " : comment.getDate());

        return sb.toString();
    }

    public static ClinicalComment decodeComment(String str) {
        ClinicalComment comment = new ClinicalComment();

        String[] fields = str.split(FIELD_SEPARATOR);
        if (fields[0].trim().length() > 0) {
            comment.setAuthor(fields[0].trim());
        }
        if (fields[1].trim().length() > 0) {
            comment.setTags(Arrays.asList(fields[1].trim().split(",")));
        }
        if (fields[2].trim().length() > 0) {
            comment.setMessage(fields[2].trim());
        }
        if (fields[3].trim().length() > 0) {
            comment.setDate(fields[3].trim());
        }

        return comment;
    }

    public static org.opencb.opencga.core.models.clinical.Interpretation clone(
            org.opencb.opencga.core.models.clinical.Interpretation input) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(input);
        oos.flush();
        oos.close();
        bos.close();

        byte[] byteData = bos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(byteData);

        return (org.opencb.opencga.core.models.clinical.Interpretation) new ObjectInputStream(bais).readObject();
    }

    public static String compressToBase64(String data) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream gzipOut = new GZIPOutputStream(baos);
        gzipOut.write(data.getBytes("UTF-8"));
        gzipOut.close();
        byte[] compressedBytes = baos.toByteArray();
        return Base64.getEncoder().encodeToString(compressedBytes);
    }

    // Método para descomprimir una cadena Base64 a un String
    public static String decompressFromBase64(String compressedData) throws IOException {
        byte[] compressedBytes = Base64.getDecoder().decode(compressedData);
        ByteArrayInputStream bais = new ByteArrayInputStream(compressedBytes);
        GZIPInputStream gzipIn = new GZIPInputStream(bais);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int len;
        while ((len = gzipIn.read(buffer)) > 0) {
            baos.write(buffer, 0, len);
        }
        gzipIn.close();
        return new String(baos.toByteArray(), "UTF-8");
    }
}
