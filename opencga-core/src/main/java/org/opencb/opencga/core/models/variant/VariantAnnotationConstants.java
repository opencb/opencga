package org.opencb.opencga.core.models.variant;

import org.opencb.biodata.models.variant.avro.ClinicalSignificance;
import org.opencb.biodata.models.variant.avro.DrugResponseClassification;
import org.opencb.biodata.models.variant.avro.TraitAssociation;

import java.util.HashMap;
import java.util.Map;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class VariantAnnotationConstants {

    public static final String THREEPRIME_OVERLAPPING_NCRNA = "3prime_overlapping_ncrna";
    // BioUtils.Biotype.IG_C_GENE;
    public static final String IG_C_GENE = "IG_C_gene";
    // BioUtils.Biotype.IG_C_PSEUDOGENE;
    public static final String IG_C_PSEUDOGENE = "IG_C_pseudogene";
    // BioUtils.Biotype.IG_D_GENE;
    public static final String IG_D_GENE = "IG_D_gene";
    // BioUtils.Biotype.IG_J_GENE;
    public static final String IG_J_GENE = "IG_J_gene";
    // BioUtils.Biotype.IG_J_PSEUDOGENE;
    public static final String IG_J_PSEUDOGENE = "IG_J_pseudogene";
    // BioUtils.Biotype.IG_V_GENE;
    public static final String IG_V_GENE = "IG_V_gene";
    // BioUtils.Biotype.IG_V_PSEUDOGENE;
    public static final String IG_V_PSEUDOGENE = "IG_V_pseudogene";
    // BioUtils.Biotype.MT_RRNA;
    public static final String MT_RRNA = "Mt_rRNA";
    // BioUtils.Biotype.MT_TRNA;
    public static final String MT_TRNA = "Mt_tRNA";
    // BioUtils.Biotype.TR_C_GENE;
    public static final String TR_C_GENE = "TR_C_gene";
    // BioUtils.Biotype.TR_D_GENE;
    public static final String TR_D_GENE = "TR_D_gene";
    // BioUtils.Biotype.TR_J_GENE;
    public static final String TR_J_GENE = "TR_J_gene";
    // BioUtils.Biotype.TR_J_PSEUDOGENE;
    public static final String TR_J_PSEUDOGENE = "TR_J_pseudogene";
    // BioUtils.Biotype.TR_V_GENE;
    public static final String TR_V_GENE = "TR_V_gene";
    // BioUtils.Biotype.TR_V_PSEUDOGENE;
    public static final String TR_V_PSEUDOGENE = "TR_V_pseudogene";
    public static final String ANTISENSE = "antisense";
    // BioUtils.Biotype.LINCRNA;
    public static final String LINCRNA = "lincRNA";
    // BioUtils.Biotype.MIRNA;
    public static final String MIRNA = "miRNA";
    // BioUtils.Biotype.MISC_RNA;
    public static final String MISC_RNA = "misc_RNA";
    // BioUtils.Biotype.POLYMORPHIC_PSEUDOGENE;
    public static final String POLYMORPHIC_PSEUDOGENE = "polymorphic_pseudogene";
    // BioUtils.Biotype.PROCESSED_PSEUDOGENE;
    public static final String PROCESSED_PSEUDOGENE = "processed_pseudogene";
    // BioUtils.Biotype.PROCESSED_TRANSCRIPT;
    public static final String PROCESSED_TRANSCRIPT = "processed_transcript";
    // BioUtils.Biotype.PROTEIN_CODING;
    public static final String PROTEIN_CODING = "protein_coding";
    // BioUtils.Biotype.PSEUDOGENE;
    public static final String PSEUDOGENE = "pseudogene";
    // BioUtils.Biotype.RRNA;
    public static final String RRNA = "rRNA";
    // BioUtils.Biotype.SENSE_INTRONIC;
    public static final String SENSE_INTRONIC = "sense_intronic";
    // BioUtils.Biotype.SENSE_OVERLAPPING;
    public static final String SENSE_OVERLAPPING = "sense_overlapping";
    // BioUtils.Biotype.SNRNA;
    public static final String SNRNA = "snRNA";
    // BioUtils.Biotype.SNORNA;
    public static final String SNORNA = "snoRNA";
    // BioUtils.Biotype.NONSENSE_MEDIATED_DECAY;
    public static final String NONSENSE_MEDIATED_DECAY = "nonsense_mediated_decay";
    public static final String NMD_TRANSCRIPT_VARIANT = "NMD_transcript_variant";
    // BioUtils.Biotype.UNPROCESSED_PSEUDOGENE;
    public static final String UNPROCESSED_PSEUDOGENE = "unprocessed_pseudogene";
    public static final String TRANSCRIBED_UNPROCESSED_PSEUDGENE = "transcribed_unprocessed_pseudogene";
    // BioUtils.Biotype.RETAINED_INTRON;
    public static final String RETAINED_INTRON = "retained_intron";
    // BioUtils.Biotype.NON_STOP_DECAY;
    public static final String NON_STOP_DECAY = "non_stop_decay";
    // BioUtils.Biotype.UNITARY_PSEUDOGENE;
    public static final String UNITARY_PSEUDOGENE = "unitary_pseudogene";
    // BioUtils.Biotype.TRANSLATED_PROCESSED_PSEUDOGENE;
    public static final String TRANSLATED_PROCESSED_PSEUDOGENE = "translated_processed_pseudogene";
    // BioUtils.Biotype.TRANSCRIBED_PROCESSED_PSEUDOGENE;
    public static final String TRANSCRIBED_PROCESSED_PSEUDOGENE = "transcribed_processed_pseudogene";
    // BioUtils.Biotype.TRNA_PSEUDOGENE;
    public static final String TRNA_PSEUDOGENE = "tRNA_pseudogene";
    // BioUtils.Biotype.SNORNA_PSEUDOGENE;
    public static final String SNORNA_PSEUDOGENE = "snoRNA_pseudogene";
    // BioUtils.Biotype.SNRNA_PSEUDOGENE;
    public static final String SNRNA_PSEUDOGENE = "snRNA_pseudogene";
    // BioUtils.Biotype.SCRNA_PSEUDOGENE;
    public static final String SCRNA_PSEUDOGENE = "scRNA_pseudogene";
    // BioUtils.Biotype.RRNA_PSEUDOGENE;
    public static final String RRNA_PSEUDOGENE = "rRNA_pseudogene";
    // BioUtils.Biotype.MISC_RNA_PSEUDOGENE;
    public static final String MISC_RNA_PSEUDOGENE = "misc_RNA_pseudogene";
    // BioUtils.Biotype.MIRNA_PSEUDOGENE;
    public static final String MIRNA_PSEUDOGENE = "miRNA_pseudogene";
    // BioUtils.Biotype.NON_CODING;
    public static final String NON_CODING = "non_coding";
    // BioUtils.Biotype.AMBIGUOUS_ORF;
    public static final String AMBIGUOUS_ORF = "ambiguous_orf";
    // BioUtils.Biotype.KNOWN_NCRNA;
    public static final String KNOWN_NCRNA = "known_ncrna";
    // BioUtils.Biotype.RETROTRANSPOSED;
    public static final String RETROTRANSPOSED = "retrotransposed";
    // BioUtils.Biotype.TRANSCRIBED_UNITARY_PSEUDOGENE;
    public static final String TRANSCRIBED_UNITARY_PSEUDOGENE = "transcribed_unitary_pseudogene";
    // BioUtils.Biotype.TRANSLATED_UNPROCESSED_PSEUDOGENE;
    public static final String TRANSLATED_UNPROCESSED_PSEUDOGENE = "translated_unprocessed_pseudogene";
    public static final String LRG_GENE = "LRG_gene";

    public static final String INTERGENIC_VARIANT = "intergenic_variant";
    public static final String REGULATORY_REGION_VARIANT = "regulatory_region_variant";
    public static final String TF_BINDING_SITE_VARIANT = "TF_binding_site_variant";
    public static final String UPSTREAM_GENE_VARIANT = "upstream_gene_variant";
    public static final String TWOKB_UPSTREAM_VARIANT = "2KB_upstream_variant";
    public static final String DOWNSTREAM_GENE_VARIANT = "downstream_gene_variant";
    public static final String TWOKB_DOWNSTREAM_VARIANT = "2KB_downstream_variant";
    public static final String SPLICE_DONOR_VARIANT = "splice_donor_variant";
    public static final String SPLICE_ACCEPTOR_VARIANT = "splice_acceptor_variant";
    public static final String INTRON_VARIANT = "intron_variant";
    public static final String SPLICE_REGION_VARIANT = "splice_region_variant";
    public static final String FIVE_PRIME_UTR_VARIANT = "5_prime_UTR_variant";
    public static final String THREE_PRIME_UTR_VARIANT = "3_prime_UTR_variant";
    public static final String INCOMPLETE_TERMINAL_CODON_VARIANT = "incomplete_terminal_codon_variant";
    public static final String STOP_RETAINED_VARIANT = "stop_retained_variant";
    public static final String START_RETAINED_VARIANT = "start_retained_variant";
    public static final String SYNONYMOUS_VARIANT = "synonymous_variant";
    public static final String INITIATOR_CODON_VARIANT = "initiator_codon_variant";
    public static final String START_LOST = "start_lost";
    public static final String STOP_GAINED = "stop_gained";
    public static final String STOP_LOST = "stop_lost";
    public static final String MISSENSE_VARIANT = "missense_variant";
    public static final String MATURE_MIRNA_VARIANT = "mature_miRNA_variant";
    public static final String NON_CODING_TRANSCRIPT_EXON_VARIANT = "non_coding_transcript_exon_variant";
    public static final String NON_CODING_TRANSCRIPT_VARIANT = "non_coding_transcript_variant";
    public static final String INFRAME_INSERTION = "inframe_insertion";
    public static final String INFRAME_VARIANT = "inframe_variant";
    public static final String FRAMESHIFT_VARIANT = "frameshift_variant";
    public static final String CODING_SEQUENCE_VARIANT = "coding_sequence_variant";
    public static final String TRANSCRIPT_ABLATION = "transcript_ablation";
    public static final String TRANSCRIPT_AMPLIFICATION = "transcript_amplification";
    public static final String COPY_NUMBER_CHANGE = "copy_number_change";
    public static final String TERMINATOR_CODON_VARIANT = "terminator_codon_variant";
    public static final String FEATURE_TRUNCATION = "feature_truncation";
    public static final String FEATURE_VARIANT = "feature_variant";
    public static final String STRUCTURAL_VARIANT = "structural_variant";
    public static final String INFRAME_DELETION = "inframe_deletion";


    public static final Map<Character, Character> COMPLEMENTARY_NT = new HashMap<>();
    public static final Map<String, ClinicalSignificance> CLINVAR_CLINSIG_TO_ACMG = new HashMap<>();
    public static final Map<String, TraitAssociation> CLINVAR_CLINSIG_TO_TRAIT_ASSOCIATION = new HashMap<>();
    // Currently left empty since the only item within DrugResponseClassification that seemed to match any clinvar
    // tag ("responsive") was removed at some point from the model
    public static final Map<String, DrugResponseClassification> CLINVAR_CLINSIG_TO_DRUG_RESPONSE = new HashMap<>();

    static {

        CLINVAR_CLINSIG_TO_ACMG.put("benign", ClinicalSignificance.benign);
        CLINVAR_CLINSIG_TO_ACMG.put("likely benign", ClinicalSignificance.likely_benign);
        CLINVAR_CLINSIG_TO_ACMG.put("conflicting interpretations of pathogenicity", ClinicalSignificance.uncertain_significance);
        CLINVAR_CLINSIG_TO_ACMG.put("likely pathogenic", ClinicalSignificance.likely_pathogenic);
        CLINVAR_CLINSIG_TO_ACMG.put("pathogenic", ClinicalSignificance.pathogenic);
        CLINVAR_CLINSIG_TO_ACMG.put("uncertain significance", ClinicalSignificance.uncertain_significance);
        CLINVAR_CLINSIG_TO_ACMG.put("conflicting data from submitters", ClinicalSignificance.uncertain_significance);

        CLINVAR_CLINSIG_TO_TRAIT_ASSOCIATION.put("risk factor", TraitAssociation.established_risk_allele);
        CLINVAR_CLINSIG_TO_TRAIT_ASSOCIATION.put("protective", TraitAssociation.protective);


        COMPLEMENTARY_NT.put('A', 'T');
        COMPLEMENTARY_NT.put('C', 'G');
        COMPLEMENTARY_NT.put('G', 'C');
        COMPLEMENTARY_NT.put('T', 'A');
        COMPLEMENTARY_NT.put('N', 'N');

    }

}
