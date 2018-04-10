package org.opencb.opencga.storage.mongodb.variant.converters;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.utils.CryptoUtils;

import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.storage.mongodb.variant.converters.VariantStringIdConverter.SV_SPLIT_LENGTH;

/**
 * Created on 06/11/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantStringIdConverterTest {

    private final VariantStringIdConverter converter = new VariantStringIdConverter();

    @Test
    public void snv() {
        // SNV
        Variant v = new Variant("1", 1000, 1000, "A", "C");
        assertEquals(" 1:      1000:A:C", converter.buildId(v));
    }

    @Test
    public void indel() {
        // Indel
        Variant v = new Variant("1", 1000, 1002, "", "CA");
        assertEquals(" 1:      1000::CA", converter.buildId(v));

    }

    @Test
    public void sv() {
        // Structural
        String alt = "ACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGT";
        Variant v = new Variant("1", 1000, 1002, "TAG", alt);
        assertEquals(" 1:      1000:TAG:" + new String(CryptoUtils.encryptSha1(alt)), converter.buildId(v));

    }

    @Test
    public void cnv() {
        // CNV
        Variant v = new Variant("1", 1000, 2000, "A", "<CN5>");
        assertEquals(" 1:      1000:A:<CN5>", converter.buildId(v));
        assertEquals(v, converter.buildVariant(converter.buildId(v), 2000, "A", "<CN5>"));

        v.getSv().setCiStartLeft(999);
        v.getSv().setCiStartRight(1010);
        assertEquals(" 1:      1000:A:<CN5>:999:1010::", converter.buildId(v));
        assertEquals(v, converter.buildVariant(converter.buildId(v), 2000, "A", "<CN5>"));

        v.getSv().setCiEndLeft(1999);
        v.getSv().setCiEndRight(2010);
        assertEquals(" 1:      1000:A:<CN5>:999:1010:1999:2010", converter.buildId(v));
        assertEquals(v, converter.buildVariant(converter.buildId(v), 2000, "A", "<CN5>"));
    }

    @Test
    public void insertion() {
        // INS
        Variant v = new Variant("1:1000:-:AAAAA...TTTT");
        assertEquals(" 1:      1000::<INS>_AAAAA_TTTT::::", converter.buildId(v));
        assertEquals(v, converter.buildVariant(converter.buildId(v), 999, "", "<INS>_AAAAA_TTTT"));

        v.getSv().setCiStartLeft(999);
        v.getSv().setCiStartRight(1010);
        assertEquals(" 1:      1000::<INS>_AAAAA_TTTT:999:1010::", converter.buildId(v));
        assertEquals(v, converter.buildVariant(converter.buildId(v), 999, "", "<INS>_AAAAA_TTTT"));
    }

    @Test
    public void testAlleleSha1WithColons() {
        // This allele, in SHA1 mode has 4 colons, so the parser thinks it is a structural variant
        String alt = "CTATATTTTATATAAACTATATTTTATATGTATATGTGTGTATACTATATTTTATATACTATACTATAGA";
        Variant v = new Variant("1:1000:-:" + alt);
        assertEquals(SV_SPLIT_LENGTH - 1, StringUtils.countMatches(converter.buildId(v), ':'));
        assertEquals(v, converter.buildVariant(converter.buildId(v), 999, "", alt));
    }

    @Test
    public void bnd() {
        // INS
        Variant v = new Variant("1:1000:A:A[3:123[");
        String id = converter.buildId(v);
        assertEquals(" 1:      1000:A:A[3_123[::::", id);
        Variant v2 = converter.buildVariant(id, 999, "A", "A[3:123[");
        System.out.println("v.toJson()  = " + v.toJson());
        System.out.println("v2.toJson() = " + v2.toJson());
        assertEquals(v, v2);

        v.getSv().setCiStartLeft(999);
        v.getSv().setCiStartRight(1010);
        assertEquals(" 1:      1000:A:A[3_123[:999:1010::", converter.buildId(v));
        assertEquals(v, converter.buildVariant(converter.buildId(v), 999, "A", "A[3:123["));
    }
}
