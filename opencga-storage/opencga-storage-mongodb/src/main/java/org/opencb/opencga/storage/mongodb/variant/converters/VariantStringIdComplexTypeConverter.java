package org.opencb.opencga.storage.mongodb.variant.converters;

import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.commons.utils.CryptoUtils;

/**
 * Creates a sorted key for MongoDB.
 *
 * Format:
 * CHR:POS:REF:ALT
 *
 * Where CHR starts with " " if it's a single number chromosome, to sort 2 digits chromosomes.
 * Where POS has a left padding of 10 positions
 * Where REF and ALT are a SHA1 of the original allele if is bigger than {@link Variant#SV_THRESHOLD}
 *
 * Created on 12/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantStringIdComplexTypeConverter implements ComplexTypeConverter<Variant, Document> {

    public Variant convertToDataModelType(String object) {
        String[] split = object.split(":", -1);
        return new Variant(split[0].trim(), Integer.parseInt(split[1].trim()), split[2], split[3]);
    }

    @Override
    public Variant convertToDataModelType(Document object) {
        String[] split = object.getString("_id").split(":", -1);
        return new Variant(split[0].trim(), Integer.parseInt(split[1].trim()),
                object.getInteger("end"),
                object.getString("ref"),
                object.getString("alt"));
    }

    @Override
    public Document convertToStorageType(Variant variant) {
        return new Document("_id", buildId(variant))
                .append("ref", variant.getReference())
                .append("alt", variant.getAlternate())
                .append("end", variant.getEnd());
    }


    public String buildId(Variant variant) {
        return buildId(variant.getChromosome(), variant.getStart(), variant.getReference(), variant.getAlternate());
    }

    public String buildId(String chromosome, int start, String reference, String alternate) {
        StringBuilder stringBuilder = new StringBuilder();

        if (chromosome.length() == 1 && Character.isDigit(chromosome.charAt(0))) {
            stringBuilder.append(' ');
        }
        stringBuilder.append(chromosome)
                .append(':')
                .append(StringUtils.leftPad(Integer.toString(start), 10, " "))
                .append(':');

        if (alternate.length() > Variant.SV_THRESHOLD) {
            stringBuilder.append(new String(CryptoUtils.encryptSha1(alternate)));
        } else if (!alternate.equals("-")) {
            stringBuilder.append(alternate);
        }
        stringBuilder.append(':');
        if (reference.length() > Variant.SV_THRESHOLD) {
            stringBuilder.append(new String(CryptoUtils.encryptSha1(reference)));
        } else if (!reference.equals("-")) {
            stringBuilder.append(reference);
        }
        return stringBuilder.toString();
    }
}
