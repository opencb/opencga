package org.opencb.opencga.storage.variant;

import com.google.common.base.Joiner;
import org.opencb.commons.bioformats.commons.SqliteSingletonConnection;
import org.opencb.commons.bioformats.feature.Genotype;
import org.opencb.commons.bioformats.feature.Genotypes;
import org.opencb.commons.bioformats.variant.VariantStudy;
import org.opencb.commons.bioformats.variant.utils.effect.VariantEffect;
import org.opencb.commons.bioformats.variant.utils.stats.*;
import org.opencb.commons.bioformats.variant.vcf4.VcfRecord;
import org.opencb.commons.bioformats.variant.vcf4.effect.EffectCalculator;
import org.opencb.commons.bioformats.variant.vcf4.io.VariantDBWriter;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: aaleman
 * Date: 10/30/13
 * Time: 3:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class VariantVcfSqliteWriter implements VariantDBWriter<VcfRecord> {

    private boolean createdSampleTable;
    private Statement stmt;
    private PreparedStatement pstmt;
    private SqliteSingletonConnection connection;


    public VariantVcfSqliteWriter(String dbName) {
        this.stmt = null;
        this.pstmt = null;
        this.createdSampleTable = false;
        this.connection = new SqliteSingletonConnection(dbName);
    }

    @Override
    public boolean writeHeader(String s) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean writeBatch(List<VcfRecord> vcfRecords) {
        String sql, sqlSampleInfo, sqlInfo;
        PreparedStatement pstmtSample, pstmtInfo;
        String sampleName;
        String sampleData;
        int allele_1, allele_2;
        Genotype g;
        int id;
        boolean res = true;
        List<List<VariantEffect>> batchEffect;

        PreparedStatement pstmt;
        if (!createdSampleTable && vcfRecords.size() > 0) {
            try {
                sql = "INSERT INTO sample (name) VALUES(?);";
                pstmt = SqliteSingletonConnection.getConnection().prepareStatement(sql);
                VcfRecord v = vcfRecords.get(0);
                for (String name : v.getSampleNames()) {
                    pstmt.setString(1, name);
                    pstmt.execute();
                }

                pstmt.close();
                SqliteSingletonConnection.getConnection().commit();
                createdSampleTable = true;
            } catch (SQLException e) {
                System.err.println("SAMPLE: " + e.getClass().getName() + ": " + e.getMessage());
                res = false;
            }
        }

        sql = "INSERT INTO variant (chromosome, position, id, ref, alt, qual, filter, info, format,genes,consequence_types, genotypes) VALUES(?,?,?,?,?,?,?,?,?,?,?,?);";
        sqlSampleInfo = "INSERT INTO sample_info(id_variant, sample_name, allele_1, allele_2, data) VALUES (?,?,?,?,?);";
        sqlInfo = "INSERT INTO variant_info(id_variant, key, value) VALUES (?,?,?);";


        batchEffect = EffectCalculator.getEffectPerVariant(vcfRecords);

        try {

            pstmt = SqliteSingletonConnection.getConnection().prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
            pstmtSample = SqliteSingletonConnection.getConnection().prepareStatement(sqlSampleInfo);
            pstmtInfo = SqliteSingletonConnection.getConnection().prepareStatement(sqlInfo);

            VcfRecord v;
            String genes, consecuenteTypes, genotypes;
            for (int i = 0; i < vcfRecords.size(); i++) {
                v = vcfRecords.get(i);

                pstmt.setString(1, v.getChromosome());
                pstmt.setInt(2, v.getPosition());
                pstmt.setString(3, v.getId());
                pstmt.setString(4, v.getReference());
                pstmt.setString(5, Joiner.on(",").join(v.getAltAlleles()));
                pstmt.setDouble(6, (v.getQuality().equals(".") ? 0 : Double.valueOf(v.getQuality())));
                pstmt.setString(7, v.getFilter());
                pstmt.setString(8, v.getInfo());
                pstmt.setString(9, v.getFormat());

                genes = parseGenes(batchEffect.get(i));
                consecuenteTypes = parseConsequenceTypes(batchEffect.get(i));
                genotypes = parseGenotypes(v);

                pstmt.setString(10, genes);
                pstmt.setString(11, consecuenteTypes);
                pstmt.setString(12, genotypes);

                pstmt.execute();
                ResultSet rs = pstmt.getGeneratedKeys();
                if (rs.next()) {
                    id = rs.getInt(1);
                    for (Map.Entry<String, String> entry : v.getSampleRawData().entrySet()) {
                        sampleName = entry.getKey();
                        sampleData = entry.getValue();

                        g = v.getSampleGenotype(sampleName);

                        allele_1 = (g.getAllele1() == null) ? -1 : g.getAllele1();
                        allele_2 = (g.getAllele2() == null) ? -1 : g.getAllele2();

                        pstmtSample.setInt(1, id);
                        pstmtSample.setString(2, sampleName);
                        pstmtSample.setInt(3, allele_1);
                        pstmtSample.setInt(4, allele_2);
                        pstmtSample.setString(5, sampleData);
                        pstmtSample.execute();

                    }

                    if (!v.getInfo().equals(".")) {
                        String[] infoFields = v.getInfo().split(";");
                        for (String elem : infoFields) {
                            String[] fields = elem.split("=");
                            pstmtInfo.setInt(1, id);
                            pstmtInfo.setString(2, fields[0]);
                            pstmtInfo.setString(3, fields[1]);
                            pstmtInfo.execute();
                        }
                    }
                } else {
                    res = false;
                }
            }

            pstmt.close();
            pstmtSample.close();
            pstmtInfo.close();
            SqliteSingletonConnection.getConnection().commit();

        } catch (SQLException e) {
            System.err.println("VARIANT/SAMPLE_INFO: " + e.getClass().getName() + ": " + e.getMessage());
            res = false;
        }

        return res;
    }

    private String parseGenotypes(VcfRecord r) {
        List<Genotype> list = new ArrayList<>();

        for (String sample : r.getSampleNames()) {
            Genotypes.addGenotypeToList(list, r.getSampleGenotype(sample));
        }

        return Joiner.on(",").join(list);
    }

    private String parseConsequenceTypes(List<VariantEffect> variantEffects) {
//        StringBuilder res = new StringBuilder();

        Set<String> cts = new HashSet<>();
        for (int i = 0; i < variantEffects.size(); i++) {
            if (!variantEffects.get(i).getConsequenceTypeObo().equals("")) {

                cts.add(variantEffects.get(i).getConsequenceTypeObo());
            }
        }

        return Joiner.on(",").join(cts);

    }

    private String parseGenes(List<VariantEffect> variantEffects) {

        Set<String> genes = new HashSet<>();

        for (int i = 0; i < variantEffects.size(); i++) {
            if (!variantEffects.get(i).getGeneName().equals("")) {

                genes.add(variantEffects.get(i).getGeneName());
            }
        }

        return Joiner.on(",").join(genes);
    }

    @Override
    public boolean writeVariantEffect(List<VariantEffect> variantEffects) {

        String sql = "INSERT INTO variant_effect(chromosome	, position , reference_allele , alternative_allele , " +
                "feature_id , feature_name , feature_type , feature_biotype , feature_chromosome , feature_start , " +
                "feature_end , feature_strand , snp_id , ancestral , alternative , gene_id , transcript_id , gene_name , " +
                "consequence_type , consequence_type_obo , consequence_type_desc , consequence_type_type , aa_position , " +
                "aminoacid_change , codon_change) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";

        boolean res = true;

        try {
            pstmt = SqliteSingletonConnection.getConnection().prepareStatement(sql);

            for (VariantEffect v : variantEffects) {
                pstmt.setString(1, v.getChromosome());
                pstmt.setInt(2, v.getPosition());
                pstmt.setString(3, v.getReferenceAllele());
                pstmt.setString(4, v.getAlternativeAllele());
                pstmt.setString(5, v.getFeatureId());
                pstmt.setString(6, v.getFeatureName());
                pstmt.setString(7, v.getFeatureType());
                pstmt.setString(8, v.getFeatureBiotype());
                pstmt.setString(9, v.getFeatureChromosome());
                pstmt.setInt(10, v.getFeatureStart());
                pstmt.setInt(11, v.getFeatureEnd());
                pstmt.setString(12, v.getFeatureStrand());
                pstmt.setString(13, v.getSnpId());
                pstmt.setString(14, v.getAncestral());
                pstmt.setString(15, v.getAlternative());
                pstmt.setString(16, v.getGeneId());
                pstmt.setString(17, v.getTranscriptId());
                pstmt.setString(18, v.getGeneName());
                pstmt.setString(19, v.getConsequenceType());
                pstmt.setString(20, v.getConsequenceTypeObo());
                pstmt.setString(21, v.getConsequenceTypeDesc());
                pstmt.setString(22, v.getConsequenceTypeType());
                pstmt.setInt(23, v.getAaPosition());
                pstmt.setString(24, v.getAminoacidChange());
                pstmt.setString(25, v.getCodonChange());

                pstmt.execute();

            }
            SqliteSingletonConnection.getConnection().commit();
            pstmt.close();
        } catch (SQLException e) {
            System.err.println("VARIANT_EFFECT: " + e.getClass().getName() + ": " + e.getMessage());
            res = false;
        }
        return res;
    }

    @Override
    public boolean writeVariantStats(List<VariantStats> vcfVariantStats) {
        String sql = "INSERT INTO variant_stats (chromosome, position, allele_ref, allele_alt, id, maf, mgf, allele_maf, genotype_maf, miss_allele, miss_gt, mendel_err, is_indel, cases_percent_dominant, controls_percent_dominant, cases_percent_recessive, controls_percent_recessive, genotypes) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
        boolean res = true;

        List<String> genotypes = new ArrayList<>(10);

        try {
            pstmt = SqliteSingletonConnection.getConnection().prepareStatement(sql);

            for (VariantStats v : vcfVariantStats) {
                pstmt.setString(1, v.getChromosome());
                pstmt.setLong(2, v.getPosition());
                pstmt.setString(3, v.getRefAlleles());
                pstmt.setString(4, Joiner.on(",").join(v.getAltAlleles()));
                pstmt.setString(5, v.getId());
                pstmt.setDouble(6, v.getMaf());
                pstmt.setDouble(7, v.getMgf());
                pstmt.setString(8, v.getMafAllele());
                pstmt.setString(9, v.getMgfAllele());
                pstmt.setInt(10, v.getMissingAlleles());
                pstmt.setInt(11, v.getMissingGenotypes());
                pstmt.setInt(12, v.getMendelinanErrors());
                pstmt.setInt(13, (v.isIndel() ? 1 : 0));
                pstmt.setDouble(14, v.getCasesPercentDominant());
                pstmt.setDouble(15, v.getControlsPercentDominant());
                pstmt.setDouble(16, v.getCasesPercentRecessive());
                pstmt.setDouble(17, v.getControlsPercentRecessive());

                for (Genotype g : v.getGenotypes()) {
                    genotypes.add(g.toString());
                }
                pstmt.setString(18, Joiner.on(",").join(genotypes));

                pstmt.execute();
                genotypes.clear();

            }
            SqliteSingletonConnection.getConnection().commit();
            pstmt.close();
        } catch (SQLException e) {
            System.err.println("VARIANT_STATS: " + e.getClass().getName() + ": " + e.getMessage());
            res = false;
        }


        return res;
    }

    @Override
    public boolean writeGlobalStats(VariantGlobalStats vcfGlobalStat) {
        boolean res = true;
        float titv = 0;
        float pass = 0;
        float avg = 0;
        try {
            String sql;
            stmt = SqliteSingletonConnection.getConnection().createStatement();

            sql = "INSERT INTO global_stats VALUES ('NUM_VARIANTS', 'Number of variants'," + vcfGlobalStat.getVariantsCount() + ");";
            stmt.executeUpdate(sql);
            sql = "INSERT INTO global_stats VALUES ('NUM_SAMPLES', 'Number of samples'," + vcfGlobalStat.getSamplesCount() + ");";
            stmt.executeUpdate(sql);
            sql = "INSERT INTO global_stats VALUES ('NUM_BIALLELIC', 'Number of biallelic variants'," + vcfGlobalStat.getBiallelicsCount() + ");";
            stmt.executeUpdate(sql);
            sql = "INSERT INTO global_stats VALUES ('NUM_MULTIALLELIC', 'Number of multiallelic variants'," + vcfGlobalStat.getMultiallelicsCount() + ");";
            stmt.executeUpdate(sql);
            sql = "INSERT INTO global_stats VALUES ('NUM_SNPS', 'Number of SNP'," + vcfGlobalStat.getSnpsCount() + ");";
            stmt.executeUpdate(sql);
            sql = "INSERT INTO global_stats VALUES ('NUM_INDELS', 'Number of indels'," + vcfGlobalStat.getIndelsCount() + ");";
            stmt.executeUpdate(sql);
            sql = "INSERT INTO global_stats VALUES ('NUM_TRANSITIONS', 'Number of transitions'," + vcfGlobalStat.getTransitionsCount() + ");";
            stmt.executeUpdate(sql);
            sql = "INSERT INTO global_stats VALUES ('NUM_TRANSVERSIONS', 'Number of transversions'," + vcfGlobalStat.getTransversionsCount() + ");";
            stmt.executeUpdate(sql);
            if (vcfGlobalStat.getTransversionsCount() > 0) {
                titv = vcfGlobalStat.getTransitionsCount() / (float) vcfGlobalStat.getTransversionsCount();
            }
            sql = "INSERT INTO global_stats VALUES ('TITV_RATIO', 'Ti/TV ratio'," + titv + ");";
            stmt.executeUpdate(sql);
            if (vcfGlobalStat.getVariantsCount() > 0) {
                pass = vcfGlobalStat.getPassCount() / (float) vcfGlobalStat.getVariantsCount();
                avg = vcfGlobalStat.getAccumQuality() / (float) vcfGlobalStat.getVariantsCount();
            }

            sql = "INSERT INTO global_stats VALUES ('PERCENT_PASS', 'Percentage of PASS'," + (pass * 100) + ");";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO global_stats VALUES ('AVG_QUALITY', 'Average quality'," + avg + ");";
            stmt.executeUpdate(sql);

            SqliteSingletonConnection.getConnection().commit();
            stmt.close();

        } catch (SQLException e) {
            System.err.println("GLOBAL_STATS: " + e.getClass().getName() + ": " + e.getMessage());
            res = false;
        }

        return res;
    }

    @Override
    public boolean writeSampleStats(VariantSampleStats vcfSampleStat) {
        String sql = "INSERT INTO sample_stats VALUES(?,?,?,?);";
        VariantSingleSampleStats s;
        String name;
        boolean res = true;
        try {
            pstmt = SqliteSingletonConnection.getConnection().prepareStatement(sql);

            for (Map.Entry<String, VariantSingleSampleStats> entry : vcfSampleStat.getSamplesStats().entrySet()) {
                s = entry.getValue();
                name = entry.getKey();

                pstmt.setString(1, name);
                pstmt.setInt(2, s.getMendelianErrors());
                pstmt.setInt(3, s.getMissingGenotypes());
                pstmt.setInt(4, s.getHomozygotesNumber());
                pstmt.execute();

            }
            SqliteSingletonConnection.getConnection().commit();
            pstmt.close();
        } catch (SQLException e) {
            System.err.println("SAMPLE_STATS: " + e.getClass().getName() + ": " + e.getMessage());
            res = false;
        }
        return res;
    }

    @Override
    public boolean writeSampleGroupStats(VariantSampleGroupStats vcfSampleGroupStat) throws IOException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean writeVariantGroupStats(VariantGroupStats vcfVariantGroupStat) throws IOException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean open() {
        return SqliteSingletonConnection.getConnection() != null;
    }

    @Override
    public boolean close() {
        boolean res = true;
        try {
            SqliteSingletonConnection.getConnection().close();
        } catch (SQLException e) {
            e.printStackTrace();
            res = false;
        }
        return res;
    }

    @Override
    public boolean pre() {

        boolean res = true;

        String variantEffectTable = "CREATE TABLE IF NOT EXISTS variant_effect(" +
                "id_variant_effect INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "chromosome	TEXT, " +
                "position INT64, " +
                "reference_allele TEXT, " +
                "alternative_allele TEXT, " +
                "feature_id TEXT, " +
                "feature_name TEXT, " +
                "feature_type TEXT, " +
                "feature_biotype TEXT, " +
                "feature_chromosome TEXT, " +
                "feature_start INT64, " +
                "feature_end INT64, " +
                "feature_strand TEXT, " +
                "snp_id TEXT, " +
                "ancestral TEXT, " +
                "alternative TEXT, " +
                "gene_id TEXT, " +
                "transcript_id TEXT, " +
                "gene_name TEXT, " +
                "consequence_type TEXT, " +
                "consequence_type_obo TEXT, " +
                "consequence_type_desc TEXT, " +
                "consequence_type_type TEXT, " +
                "aa_position INT64, " +
                "aminoacid_change TEXT, " +
                "codon_change TEXT); ";

        String variantTable = "CREATE TABLE IF NOT EXISTS variant (" +
                "id_variant INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "chromosome TEXT, " +
                "position INT64, " +
                "id TEXT, " +
                "ref TEXT, " +
                "alt TEXT, " +
                "qual DOUBLE, " +
                "filter TEXT, " +
                "info TEXT, " +
                "format TEXT, " +
                "genes TEXT, " +
                "consequence_types TEXT, " +
                "genotypes TEXT);";

        String sampleTable = "CREATE TABLE IF NOT EXISTS sample(" +
                "name TEXT PRIMARY KEY);";

        String sampleInfoTable = "CREATE TABLE IF NOT EXISTS sample_info(" +
                "id_sample_info INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "id_variant INTEGER, " +
                "sample_name TEXT, " +
                "allele_1 INTEGER, " +
                "allele_2 INTEGER, " +
                "data TEXT, " +
                "FOREIGN KEY(id_variant) REFERENCES variant(id_variant)," +
                "FOREIGN KEY(sample_name) REFERENCES sample(name));";
        String variantInfoTable = "CREATE TABLE IF NOT EXISTS variant_info(" +
                "id_variant_info INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "id_variant INTEGER, " +
                "key TEXT, " +
                "value TEXT, " +
                "FOREIGN KEY(id_variant) REFERENCES variant(id_variant));";

        String globalStatsTable = "CREATE TABLE IF NOT EXISTS global_stats (" +
                "name TEXT," +
                " title TEXT," +
                " value TEXT," +
                "PRIMARY KEY (name));";
        String variant_stats = "CREATE TABLE IF NOT EXISTS variant_stats (" +
                "id_variant INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "chromosome TEXT, " +
                "position INT64, " +
                "allele_ref TEXT, " +
                "allele_alt TEXT, " +
                "id TEXT, " +
                "maf DOUBLE, " +
                "mgf DOUBLE," +
                "allele_maf TEXT, " +
                "genotype_maf TEXT, " +
                "miss_allele INT, " +
                "miss_gt INT, " +
                "mendel_err INT, " +
                "is_indel INT, " +
                "cases_percent_dominant DOUBLE, " +
                "controls_percent_dominant DOUBLE, " +
                "cases_percent_recessive DOUBLE, " +
                "controls_percent_recessive DOUBLE, " +
                "genotypes TEXT);";
        String sample_stats = "CREATE TABLE IF NOT EXISTS sample_stats(" +
                "name TEXT, " +
                "mendelian_errors INT, " +
                "missing_genotypes INT, " +
                "homozygotesNumber INT, " +
                "PRIMARY KEY (name));";

        try {
            stmt = SqliteSingletonConnection.getConnection().createStatement();
            stmt.execute(variantEffectTable);
            stmt.execute(variantTable);
            stmt.execute(variantInfoTable);
            stmt.execute(sampleTable);
            stmt.execute(sampleInfoTable);
            stmt.execute(globalStatsTable);
            stmt.execute(variant_stats);
            stmt.execute(sample_stats);
            stmt.close();

            SqliteSingletonConnection.getConnection().commit();
        } catch (SQLException e) {
            System.err.println("PRE: " + e.getClass().getName() + ": " + e.getMessage());
            res = false;
        }

        return res;
    }

    @Override
    public boolean post() {
        boolean res = true;
        try {

            stmt = SqliteSingletonConnection.getConnection().createStatement();
            stmt.execute("CREATE INDEX IF NOT EXISTS variant_effect_chromosome_position_idx ON variant_effect (chromosome, position);");
            stmt.execute("CREATE INDEX IF NOT EXISTS variant_effect_feature_biotype_idx ON variant_effect (feature_biotype);");
            stmt.execute("CREATE INDEX IF NOT EXISTS variant_effect_consequence_type_obo_idx ON variant_effect (consequence_type_obo);");
            stmt.execute("CREATE INDEX IF NOT EXISTS variant_chromosome_position_idx ON variant (chromosome, position);");
            stmt.execute("CREATE INDEX IF NOT EXISTS variant_pass_idx ON variant (filter);");
            stmt.execute("CREATE INDEX IF NOT EXISTS variant_id_idx ON variant (id);");
            stmt.execute("CREATE INDEX IF NOT EXISTS sample_name_idx ON sample (name);");
            stmt.execute("CREATE INDEX IF NOT EXISTS sample_info_id_variant_idx ON sample_info (id_variant);");
            stmt.execute("CREATE INDEX IF NOT EXISTS variant_id_variant_idx ON variant (id_variant);");
            stmt.execute("CREATE INDEX IF NOT EXISTS variant_info_id_variant_key_idx ON variant_info (id_variant, key);");
            stmt.execute("CREATE INDEX IF NOT EXISTS variant_stats_chromosome_position_idx ON variant_stats (chromosome, position);");

            stmt.execute("REINDEX variant_effect_chromosome_position_idx;");
            stmt.execute("REINDEX variant_effect_feature_biotype_idx;");
            stmt.execute("REINDEX variant_effect_consequence_type_obo_idx;");
            stmt.execute("REINDEX variant_chromosome_position_idx;");
            stmt.execute("REINDEX variant_pass_idx;");
            stmt.execute("REINDEX variant_id_idx;");
            stmt.execute("REINDEX sample_name_idx;");
            stmt.execute("REINDEX sample_info_id_variant_idx;");
            stmt.execute("REINDEX variant_id_variant_idx;");
            stmt.execute("REINDEX variant_info_id_variant_key_idx;");
            stmt.execute("REINDEX variant_stats_chromosome_position_idx;");


            stmt.execute("CREATE TABLE IF NOT EXISTS consequence_type_count AS SELECT count(*) as count, consequence_type_obo from (select distinct chromosome, position, reference_allele, alternative_allele, consequence_type_obo from variant_effect) group by consequence_type_obo;");
            stmt.execute("CREATE TABLE IF NOT EXISTS biotype_count AS SELECT count(*) as count, feature_biotype from variant_effect group by feature_biotype order by feature_biotype ASC;  ");
            stmt.execute("CREATE TABLE IF NOT EXISTS chromosome_count AS SELECT count(*) as count, chromosome from variant group by chromosome order by chromosome ASC;");

            stmt.close();
            SqliteSingletonConnection.getConnection().commit();

        } catch (SQLException e) {
            System.err.println("POST: " + e.getClass().getName() + ": " + e.getMessage());
            res = false;
        }

        return res;
    }

    @Override
    public boolean writeStudy(VariantStudy study) {
        return true;
    }
}
