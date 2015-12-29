/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.variant.sqlite;

import com.google.common.base.Joiner;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFactory;
import org.opencb.biodata.models.variant.effect.VariantEffect;
import org.opencb.biodata.models.variant.stats.VariantStats;

import org.opencb.commons.db.SqliteSingletonConnection;
import org.opencb.opencga.core.auth.SqliteCredentials;

/**
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 * @TODO aaleman: Move writeEffect/writeStats/write*... to write(batch)
 */
public class VariantSqliteWriter implements VariantWriter {

    private boolean createdSampleTable;
    private Statement stmt;
    private SqliteSingletonConnection connection;

    private SqliteCredentials credentials;
    private boolean includeStats;
    private boolean includeSamples;
    private boolean includeEffect;


    public VariantSqliteWriter(SqliteCredentials credentials) {
        if (credentials == null) {
            throw new IllegalArgumentException("Credentials for accessing the database must be specified");
        }
        this.stmt = null;
        this.createdSampleTable = false;
        this.credentials = credentials;

    }

    @Deprecated
    public VariantSqliteWriter(String dbName) {
        this.stmt = null;
        this.createdSampleTable = false;
        this.connection = new SqliteSingletonConnection(dbName);
    }

    @Override
    public boolean write(List<Variant> variants) {

        boolean res = writeBatch(variants);
        if (res && this.includeStats) {
            res &= writeVariantStats(variants);
        }
        if (res && this.includeEffect) {
            res &= writeVariantEffect(variants);
        }
        return res;

    }

    public boolean writeBatch(List<Variant> vcfRecords) {
        String sql, sqlSampleInfo, sqlInfo;
        PreparedStatement pstmtSample, pstmtInfo;
        String sampleName;
        int allele_1, allele_2;
        Genotype g;
        int id;
        boolean res = true;

        PreparedStatement pstmt;
        if (!createdSampleTable && vcfRecords.size() > 0) {
            try {
                sql = "INSERT INTO sample (name) VALUES(?);";
                pstmt = SqliteSingletonConnection.getConnection().prepareStatement(sql);
                Variant v = vcfRecords.get(0);

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

        sql = "INSERT INTO variant (chromosome, position, id, ref, alt, qual, filter, info, format,genes,consequence_types, genotypes, polyphen_score, polyphen_effect, sift_score, sift_effect) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
        sqlSampleInfo = "INSERT INTO sample_info(id_variant, sample_name, allele_1, allele_2, data) VALUES (?,?,?,?,?);";
        sqlInfo = "INSERT INTO variant_info(id_variant, key, value) VALUES (?,?,?);";

        try {

            pstmt = SqliteSingletonConnection.getConnection().prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
            pstmtSample = SqliteSingletonConnection.getConnection().prepareStatement(sqlSampleInfo);
            pstmtInfo = SqliteSingletonConnection.getConnection().prepareStatement(sqlInfo);

            String genes, consequenceTypes, genotypes;
            Map<String, Object> polySift;
            for (int i = 0; i < vcfRecords.size(); i++) {
                Variant v = vcfRecords.get(i);
                String info = VariantFactory.getVcfInfo(v);
                List<VariantEffect> batchEffect = v.getEffect();

                pstmt.setString(1, v.getChromosome());
                pstmt.setLong(2, v.getStart());
                pstmt.setString(3, v.getId());
                pstmt.setString(4, v.getReference());
                pstmt.setString(5, v.getAlternate());
                pstmt.setDouble(6, (v.getAttribute("QUAL").equals(".") ? 0 : Double.valueOf(v.getAttribute("QUAL"))));
                pstmt.setString(7, v.getAttribute("FILTER"));
                pstmt.setString(8, info);
                pstmt.setString(9, v.getFormat());

                genes = parseGenes(batchEffect);
                consequenceTypes = parseConsequenceTypes(batchEffect);
                genotypes = parseGenotypes(v);

                pstmt.setString(10, genes);
                pstmt.setString(11, consequenceTypes);
                pstmt.setString(12, genotypes);
                polySift = parsePolyphenSift(batchEffect);


                if (polySift != null) {
                    pstmt.setDouble(13, (Double) polySift.get("ps"));
                    pstmt.setInt(14, (Integer) polySift.get("pe"));
                    pstmt.setDouble(15, (Double) polySift.get("ss"));
                    pstmt.setInt(16, (Integer) polySift.get("se"));
                }

                pstmt.execute();
                ResultSet rs = pstmt.getGeneratedKeys();
                if (rs.next()) {
                    id = rs.getInt(1);

                    if (this.includeSamples) {
                        for (Map.Entry<String, Map<String, String>> entry : v.getSamplesData().entrySet()) {
                            sampleName = entry.getKey();
                            Map<String, String> sampleData = entry.getValue();

                            g = new Genotype(sampleData.get("GT"));

                            allele_1 = (g.getAllele1() == null) ? -1 : g.getAllele1();
                            allele_2 = (g.getAllele2() == null) ? -1 : g.getAllele2();

                            pstmtSample.setInt(1, id);
                            pstmtSample.setString(2, sampleName);
                            pstmtSample.setInt(3, allele_1);
                            pstmtSample.setInt(4, allele_2);
                            pstmtSample.setString(5, VariantFactory.getVcfSampleRawData(v, sampleName));
                            pstmtSample.execute();

                        }
                    }

                    for (Map.Entry<String, String> entry : v.getAttributes().entrySet()) {
                        pstmtInfo.setInt(1, id);
                        pstmtInfo.setString(2, entry.getKey());
                        pstmtInfo.setString(3, entry.getValue());
                        pstmtInfo.execute();

                    }

                } else {
                    res = false;
                }
            }

            pstmt.close();
            if (this.includeSamples)
                pstmtSample.close();
            pstmtInfo.close();
            SqliteSingletonConnection.getConnection().commit();

        } catch (SQLException e) {
            System.err.println("VARIANT/SAMPLE_INFO: " + e.getClass().getName() + ": " + e.getMessage());
            res = false;
        }

        return res;
    }

    private Map<String, Object> parsePolyphenSift(List<VariantEffect> variantEffects) {
        Map<String, Object> map = new HashMap<>(4);

        for (VariantEffect effect : variantEffects) {
            if (effect.getAaPosition() != -1) {

                map.put("ss", effect.getSiftScore());
                map.put("se", effect.getSiftEffect());
                map.put("ps", effect.getPolyphenScore());
                map.put("pe", effect.getPolyphenEffect());
                return map;
            }
        }

        return null;
    }

    private String parseGenotypes(Variant variant) {
        List<Genotype> list = new ArrayList<>();

        for (String sample : variant.getSampleNames()) {
            Genotype g = new Genotype(variant.getSampleData(sample, "GT"));
            int index = list.indexOf(g);

            if (index >= 0) {
                Genotype auxG = list.get(index);
                auxG.setCount(auxG.getCount() + 1);
            } else {
                g.setCount(g.getCount() + 1);
                list.add(g);
            }
        }

        return Joiner.on(",").join(list);
    }

    private String parseConsequenceTypes(List<VariantEffect> variantEffects) {
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


    private boolean writeVariantEffect(List<Variant> variants) {

        String sql = "INSERT INTO variant_effect(chromosome	, position , reference_allele , alternative_allele , " +
                "feature_id , feature_name , feature_type , feature_biotype , feature_chromosome , feature_start , " +
                "feature_end , feature_strand , snp_id , ancestral , alternative , gene_id , transcript_id , gene_name , " +
                "consequence_type , consequence_type_obo , consequence_type_desc , consequence_type_type , aa_position , " +
                "aminoacid_change , codon_change," +
                "polyphen_score, polyphen_effect, sift_score, sift_effect) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";

        boolean res = true;

        try {
            PreparedStatement pstmt = SqliteSingletonConnection.getConnection().prepareStatement(sql);

            for (Variant variant : variants) {
                for (VariantEffect v : variant.getEffect()) {
                    pstmt.setString(1, v.getChromosome());
                    pstmt.setInt(2, v.getPosition());
                    pstmt.setString(3, v.getReferenceAllele());
                    pstmt.setString(4, v.getAlternateAllele());
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
                    pstmt.setDouble(26, v.getPolyphenScore());
                    pstmt.setInt(27, v.getPolyphenEffect());
                    pstmt.setDouble(28, v.getSiftScore());
                    pstmt.setInt(29, v.getSiftEffect());

                    pstmt.execute();

                }
            }
            SqliteSingletonConnection.getConnection().commit();
            pstmt.close();
        } catch (SQLException e) {
            System.err.println("VARIANT_EFFECT: " + e.getClass().getName() + ": " + e.getMessage());
            res = false;
        }
        return res;
    }


    public boolean writeVariantStats(List<Variant> variants) {
        String sql = "INSERT INTO variant_stats (chromosome, position, allele_ref, allele_alt, id, maf, mgf, allele_maf, genotype_maf, miss_allele, miss_gt, mendel_err, is_indel, cases_percent_dominant, controls_percent_dominant, cases_percent_recessive, controls_percent_recessive, genotypes) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
        boolean res = true;

        List<String> genotypes = new ArrayList<>(10);

        try {
            PreparedStatement pstmt = SqliteSingletonConnection.getConnection().prepareStatement(sql);

            for (Variant variant : variants) {
                VariantStats v = variant.getStats();

                pstmt.setString(1, v.getChromosome());
                pstmt.setLong(2, v.getPosition());
                pstmt.setString(3, v.getRefAllele());
                pstmt.setString(4, Joiner.on(",").join(v.getAltAlleles()));
                pstmt.setString(5, v.getId());
                pstmt.setDouble(6, v.getMaf());
                pstmt.setDouble(7, v.getMgf());
                pstmt.setString(8, v.getMafAllele());
                pstmt.setString(9, v.getMgfGenotype());
                pstmt.setInt(10, v.getMissingAlleles());
                pstmt.setInt(11, v.getMissingGenotypes());
                pstmt.setInt(12, v.getMendelianErrors());
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

    /*
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
        */
    @Override
    public boolean open() {
        System.out.println(this.credentials.getPath().toString());
        this.connection = new SqliteSingletonConnection(this.credentials.getPath().toString());
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
                "codon_change TEXT," +
                "polyphen_score DOUBLE, " +
                "polyphen_effect INT," +
                "sift_score DOUBLE," +
                "sift_effect INT); ";

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
                "genotypes TEXT, " +
                "polyphen_score DOUBLE, " +
                "polyphen_effect INT," +
                "sift_score DOUBLE," +
                "sift_effect INT); ";

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
    public boolean write(Variant variant) {
        return true;
    }

    @Override
    public void includeStats(boolean b) {

        this.includeStats = b;
    }

    @Override
    public void includeSamples(boolean b) {
        this.includeSamples = b;

    }

    @Override
    public void includeEffect(boolean b) {
        this.includeEffect = b;
    }
}
