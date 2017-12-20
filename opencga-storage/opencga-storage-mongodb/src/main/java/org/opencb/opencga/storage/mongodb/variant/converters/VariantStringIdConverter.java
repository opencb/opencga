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

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.biodata.models.variant.avro.StructuralVariantType;
import org.opencb.biodata.models.variant.avro.StructuralVariation;
import org.opencb.commons.utils.CryptoUtils;

import java.util.function.Supplier;

import static org.opencb.biodata.models.variant.avro.StructuralVariantType.COPY_NUMBER_GAIN;
import static org.opencb.biodata.models.variant.avro.StructuralVariantType.COPY_NUMBER_LOSS;

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
    protected static final int CHR = 0;
    protected static final int POS = 1;
    protected static final int REF = 2;
    protected static final int ALT = 3;
    protected static final int CI_POS_L = 4;
    protected static final int CI_POS_R = 5;
    protected static final int CI_END_L = 6;
    protected static final int CI_END_R = 7;
    protected static final int SV_SPLIT_LENGTH = CI_END_R + 1;
    protected static final char INS_SEQ_SEPARATOR = '_';

    @Deprecated
    public Variant convertToDataModelType(String object) {
        String[] split = object.split(SEPARATOR, -1);
        Variant variant = new Variant(split[CHR].trim(), Integer.parseInt(split[POS].trim()), split[REF], split[ALT]);
        StructuralVariation sv = buildSv(split);
        if (sv != null) {
            variant.setSv(sv);
        }
        return variant;
    }

    public Variant buildVariant(String variantId, int end, String reference, String alternate) {
        String[] split = variantId.split(SEPARATOR, -1);
        String chr = split[CHR].trim();
        int start = Integer.parseInt(split[POS].trim());
        StructuralVariation sv = buildSv(split);
        if (StringUtils.contains(alternate, INS_SEQ_SEPARATOR)) {
            String[] alternateSplit = StringUtils.splitPreserveAllTokens(alternate, INS_SEQ_SEPARATOR);
            alternate = alternateSplit[0];
            if (sv == null) {
                sv = new StructuralVariation();
            }
            sv.setLeftSvInsSeq(alternateSplit[1]);
            sv.setRightSvInsSeq(alternateSplit[2]);
        }
        Variant variant = new Variant(chr, start, end, reference, alternate);
        if (sv != null) {
            if (variant.getSv() != null && variant.getSv().getType() != null) {
                sv.setType(variant.getSv().getType());
            }
            variant.setSv(sv);
        }
        return variant;
    }

    private StructuralVariation buildSv(String[] split) {
        if (split.length == SV_SPLIT_LENGTH) {
            try {
                return new StructuralVariation(
                        getInt(split, CI_POS_L),
                        getInt(split, CI_POS_R),
                        getInt(split, CI_END_L),
                        getInt(split, CI_END_R),
                        VariantBuilder.getCopyNumberFromAlternate(split[ALT]),
                        null, null, null);
            } catch (RuntimeException e) {
                for (String s : split) {
                    // If any of the splits is non printable, the variantId had 4 colons in the SHA1
                    if (!StringUtils.isAsciiPrintable(s)) {
                        return null;
                    }
                }
                throw new IllegalArgumentException("Unable to build SV from " + String.join(SEPARATOR, split), e);
            }
        } else {
            return null;
        }
    }

    private Integer getInt(String[] split, int idx) {
        if (split.length > idx && StringUtils.isNotEmpty(split[idx])) {
            return Integer.valueOf(split[idx]);
        } else {
            return null;
        }
    }

    public String buildId(Variant variant) {
        return buildId(variant.getChromosome(), variant.getStart(), variant.getReference(), variant.getAlternate(), variant.getSv());
    }

    public String buildId(String chromosome, Integer start, String reference, String alternate) {
        return buildId(chromosome, start, reference, alternate, null);
    }

    private String buildId(String chromosome, int start, String reference, String alternate, StructuralVariation sv) {
        StringBuilder stringBuilder = buildId(chromosome, start, new StringBuilder());

        stringBuilder.append(SEPARATOR_CHAR);

        alternate = buildSVAlternate(alternate, sv);

        if (reference.length() > Variant.SV_THRESHOLD) {
            reduce(stringBuilder, reference, sv);
        } else if (!reference.equals("-")) {
            stringBuilder.append(reference);
        }
        stringBuilder.append(SEPARATOR_CHAR);
        if (alternate.length() > Variant.SV_THRESHOLD) {
            reduce(stringBuilder, alternate, sv);
        } else if (!alternate.equals("-")) {
            stringBuilder.append(alternate);
        }

        // All symbolic variants have a non null SV.
        if (validSV(sv)) {
            stringBuilder
                    .append(SEPARATOR_CHAR)
                    .append(get(sv::getCiStartLeft))
                    .append(SEPARATOR_CHAR)
                    .append(get(sv::getCiStartRight))
                    .append(SEPARATOR_CHAR)
                    .append(get(sv::getCiEndLeft))
                    .append(SEPARATOR_CHAR)
                    .append(get(sv::getCiEndRight));
//            if (sv.getCopyNumber() != null) {
//                stringBuilder
//                        .append(SEPARATOR_CHAR)
//                        .append(sv.getCopyNumber());
//            }
        }

        return stringBuilder.toString();
    }

    public String buildSVAlternate(String alternate, StructuralVariation sv) {
        if (sv != null) {
            if (StringUtils.isNotEmpty(sv.getLeftSvInsSeq()) || StringUtils.isNotEmpty(sv.getRightSvInsSeq())) {
                alternate = alternate + INS_SEQ_SEPARATOR + sv.getLeftSvInsSeq() + INS_SEQ_SEPARATOR + sv.getRightSvInsSeq();
            }
        }
        return alternate;
    }

    private void reduce(StringBuilder stringBuilder, String allele, StructuralVariation sv) {
        // FIXME: Use the same method to reduce long alleles from all variants
        if (!validSV(sv)) {
            stringBuilder.append(new String(CryptoUtils.encryptSha1(allele)));
        } else {
            stringBuilder.append(allele.charAt(0)).append('~').append(allele.hashCode()).append('~').append(allele.length());
        }
    }

    /**
     * Check if the structural variant information should be serialized.
     *
     * Checks if any field is non null, or the type is not COPY_NUMBER_GAIN nor COPY_NUMBER_LOSS
     * This two types are inferred from the Alternate
     *
     * @param sv StructuralVariation object
     * @return true if it should be serialized
     */
    private boolean validSV(StructuralVariation sv) {
        if (sv == null) {
            return false;
        } else {
            StructuralVariantType type = sv.getType();
            return type != null && !COPY_NUMBER_GAIN.equals(type) && !COPY_NUMBER_LOSS.equals(type)
                    || sv.getCiStartLeft() != null
                    || sv.getCiStartRight() != null
                    || sv.getCiEndLeft() != null
                    || sv.getCiEndRight() != null
                    || sv.getLeftSvInsSeq() != null
                    || sv.getRightSvInsSeq() != null;
        }
    }

    private <T> T get(Supplier<T> supplier, T defaultValue) {
        T t = supplier.get();
        return t == null ? defaultValue : t;
    }

    private <T> String get(Supplier<T> supplier) {
        T t = supplier.get();
        return t == null ? "" : t.toString();
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
        if (chromosome.length() == 1 && Character.isDigit(chromosome.charAt(CHR))) {
            stringBuilder.append(' ');
        }
        return stringBuilder.append(chromosome);
    }
}
