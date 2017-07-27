/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.mongodb.variant.converters;

import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.variant.Variant;
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
public class VariantStringIdConverter {

    public static final String SEPARATOR = ":";
    public static final char SEPARATOR_CHAR = ':';

    public Variant convertToDataModelType(String object) {
        String[] split = object.split(SEPARATOR, -1);
        return new Variant(split[0].trim(), Integer.parseInt(split[1].trim()), split[2], split[3]);
    }

    public Variant buildVariant(String variantId, int end, String reference, String alternate) {
        String[] split = variantId.split(SEPARATOR, -1);
        return new Variant(split[0].trim(), Integer.parseInt(split[1].trim()), end, reference, alternate);
    }

    public String buildId(Variant variant) {
        return buildId(variant.getChromosome(), variant.getStart(), variant.getReference(), variant.getAlternate());
    }

    public String buildId(String chromosome, int start, String reference, String alternate) {
        StringBuilder stringBuilder = buildId(chromosome, start, new StringBuilder());

        stringBuilder.append(SEPARATOR_CHAR);

        if (reference.length() > Variant.SV_THRESHOLD) {
            stringBuilder.append(new String(CryptoUtils.encryptSha1(reference)));
        } else if (!reference.equals("-")) {
            stringBuilder.append(reference);
        }
        stringBuilder.append(SEPARATOR_CHAR);
        if (alternate.length() > Variant.SV_THRESHOLD) {
            stringBuilder.append(new String(CryptoUtils.encryptSha1(alternate)));
        } else if (!alternate.equals("-")) {
            stringBuilder.append(alternate);
        }
        return stringBuilder.toString();
    }



    public static String buildId(String chromosome, int start) {
        return buildId(chromosome, start, new StringBuilder()).toString();
    }

    private static StringBuilder buildId(String chromosome, int start, StringBuilder stringBuilder) {

        appendChromosome(chromosome, stringBuilder)
                .append(SEPARATOR_CHAR)
                .append(StringUtils.leftPad(Integer.toString(start), 10, " "));
        return stringBuilder;
    }

    public static String convertChromosome(String chromosome) {
        return appendChromosome(chromosome, new StringBuilder()).toString();
    }

    protected static StringBuilder appendChromosome(String chromosome, StringBuilder stringBuilder) {
        if (chromosome.length() == 1 && Character.isDigit(chromosome.charAt(0))) {
            stringBuilder.append(' ');
        }
        return stringBuilder.append(chromosome);
    }
}
