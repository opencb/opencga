package org.opencb.opencga.storage.core.variant.query.filters;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.Score;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.KeyOpValue;
import org.opencb.opencga.storage.core.variant.query.KeyValues;
import org.opencb.opencga.storage.core.variant.query.ParsedQuery;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class VariantFilterBuilder {

    public VariantFilterBuilder() {
    }

    public Predicate<Variant> buildFilter(ParsedVariantQuery variantQuery) {

        List<Predicate<Variant>> filters = new LinkedList<>();

        addPositionalFilters(variantQuery, filters);
        addStudyFilters(variantQuery, filters);
        addAnnotationFilters(variantQuery, filters);
        addTypeFilter(variantQuery, filters);

        if (filters.isEmpty()) {
            return v -> true;
        } else {
            return mergeFilters(filters, VariantQueryUtils.QueryOperation.AND);
        }
    }

    private void addPositionalFilters(ParsedVariantQuery variantQuery, List<Predicate<Variant>> filters) {
        List<Predicate<Variant>> regionFilters = new LinkedList<>();

        List<Region> regions = variantQuery.getRegions();
        if (regions != null && !regions.isEmpty()) {
            List<Region> regionsMerged = VariantQueryUtils.mergeRegions(regions);
            regionFilters.add(v -> {
                for (Region region : regionsMerged) {
                    if (region.overlaps(v.getChromosome(), v.getStart(), v.getEnd())) {
                        return true;
                    }
                }
                return false;
            });
            for (Region region : regionsMerged) {
                regionFilters.add(variant -> region.overlaps(variant.getChromosome(), variant.getStart(), variant.getEnd()));
            }
        }
        ParsedVariantQuery.VariantQueryXref variantQueryXref = variantQuery.getXrefs();
        Predicate<Variant> geneFilter = getGeneFilter(variantQuery, variantQueryXref.getGenes());
        if (!variantQueryXref.getIds().isEmpty()) {
            Set<String> ids = new HashSet<>(variantQueryXref.getIds());
            regionFilters.add(variant -> ids.contains(variant.getAnnotation().getId()));
        }
//        if (!variantQueryXref.getOtherXrefs().isEmpty()) {
//
//        }
        if (!variantQueryXref.getVariants().isEmpty()) {
            Set<String> variants = variantQueryXref.getVariants().stream().map(Variant::toString).collect(Collectors.toSet());
            regionFilters.add(variant -> variants.contains(variant.getId()));
        }

        if (!regionFilters.isEmpty()) {
            Set<String> bts;
            if (!variantQuery.getBiotypes().isEmpty()) {
                bts = new HashSet<>(variantQuery.getBiotypes());
            } else {
                bts = null;
            }
            Set<String> cts;
            if (!variantQuery.getConsequenceTypes().isEmpty()) {
                cts = new HashSet<>(variantQuery.getConsequenceTypes());
            } else {
                cts = null;
            }
            if (cts != null || bts != null) {
                filters.add(variant -> {
                    if (variant.getAnnotation() != null && variant.getAnnotation().getConsequenceTypes() != null) {
                        for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
                            if (validCt(cts, ct) && validBt(bts, ct)) {
                                return true;
                            }
                        }
                    }
                    return false;
                });
            }
        }
        regionFilters.add(geneFilter);


        Predicate<Variant> predicate = mergeFilters(regionFilters, VariantQueryUtils.QueryOperation.OR);
        if (predicate != null) {
            filters.add(predicate);
        }
    }

    private Predicate<Variant> getGeneFilter(ParsedVariantQuery variantQuery, List<String> genes) {
        if (genes.isEmpty()) {
            return null;
        }

        List<Region> geneRegions = variantQuery.getGeneRegions();
        Predicate<Variant> geneRegionFilter;
        if (CollectionUtils.isEmpty(geneRegions)) {
            geneRegionFilter = null;
        } else {
            geneRegionFilter = variant -> geneRegions.stream().anyMatch(r -> r.contains(variant.getChromosome(), variant.getStart()));
        }

        Predicate<Variant> geneFilter;

        Set<String> bts;
        if (!variantQuery.getBiotypes().isEmpty()) {
            bts = new HashSet<>(variantQuery.getBiotypes());
        } else {
            bts = null;
        }
        Set<String> cts;
        if (!variantQuery.getConsequenceTypes().isEmpty()) {
            cts = new HashSet<>(variantQuery.getConsequenceTypes());
        } else {
            cts = null;
        }
        Set<String> genesSet = new HashSet<>(genes);
        geneFilter = variant -> {
            if (variant.getAnnotation() != null && variant.getAnnotation().getConsequenceTypes() != null) {
                for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
                    if (validGene(genesSet, ct) && (validCt(cts, ct)) && (validBt(bts, ct))) {
                        return true;
                    }
                }
            }
            return false;
        };

        if (geneRegionFilter == null) {
            // No gene region filter. Use only gene filter.
            return geneFilter;
        } else if (cts == null && bts == null) {
            // No CT not BT filter. Region filter is enough.
            return geneRegionFilter;
        } else {
            // Use both geneRegion and gene filter.
            return geneRegionFilter.and(geneFilter);
        }
    }

    private void addStudyFilters(ParsedVariantQuery variantQuery, List<Predicate<Variant>> filters) {

        if (variantQuery.getStudyQuery().getStudies() != null) {
            List<Predicate<Variant>> studyFilters = new LinkedList<>();
            variantQuery.getStudyQuery().getStudies().getValues().forEach(study -> {
                if (study.isNegated()) {
                    studyFilters.add(variant -> variant.getStudies().stream()
                            .noneMatch(variantStudy -> variantStudy.getStudyId().equals(study.getValue().getName())));
                } else {
                    studyFilters.add(variant -> variant.getStudies().stream()
                            .anyMatch(variantStudy -> variantStudy.getStudyId().equals(study.getValue().getName())));
                }
            });
            filters.add(mergeFilters(studyFilters, variantQuery.getStudyQuery().getStudies().getOperation()));
        }

        if (variantQuery.getStudyQuery().getFiles() != null) {
            List<Predicate<Variant>> fileFilters = new LinkedList<>();
            variantQuery.getStudyQuery().getFiles().getValues().forEach(file -> {
                if (file.isNegated()) {
                    fileFilters.add(variant -> variant.getStudies().stream()
                            .flatMap(variantStudy -> variantStudy.getFiles().stream())
                            .noneMatch(variantFile -> variantFile.getFileId().equals(file.getValue().getName())));
                } else {
                    fileFilters.add(variant -> variant.getStudies().stream()
                            .flatMap(variantStudy -> variantStudy.getFiles().stream())
                            .anyMatch(variantFile -> variantFile.getFileId().equals(file.getValue().getName())));
                }
            });
            filters.add(mergeFilters(fileFilters, variantQuery.getStudyQuery().getFiles().getOperation()));
        }

        if (variantQuery.getStudyQuery().getGenotypes() != null) {
            List<Predicate<Variant>> genotypeFilters = new LinkedList<>();
            variantQuery.getStudyQuery().getGenotypes().getValues().forEach(genotype -> {
                SampleMetadata sample = genotype.getKey();
                Set<String> values = new HashSet<>(genotype.getValue());
                String studyName = variantQuery.getStudyQuery().getDefaultStudyOrFail().getName();

                genotypeFilters.add(variant -> {
                    String gt = variant.getStudy(studyName).getSampleData(sample.getName(), "GT");
                    return values.contains(gt);
                });
            });
            filters.add(mergeFilters(genotypeFilters, variantQuery.getStudyQuery().getGenotypes().getOperation()));
        }

        addQualFilter(variantQuery, filters);
        addFilterFilter(variantQuery, filters);
        addScoreFilter(variantQuery, filters);
        addCohortStatsFilters(variantQuery, filters);
        addSampleDataFilters(variantQuery, filters);
        addFileDataFilters(variantQuery, filters);

    }

    private void addAnnotationFilters(ParsedVariantQuery variantQuery, List<Predicate<Variant>> filters) {
//        ParsedVariantQuery.VariantQueryXref variantQueryXref = variantQuery.getXrefs();
        addClinicalFilters(variantQuery, filters);

        ParsedQuery<KeyOpValue<String, Float>> freqQuery = variantQuery.getPopulationFrequencyAlt();
        if (!freqQuery.isEmpty()) {
            List<PopulationFrequencyVariantFilter.AltFreqFilter> freqFilters = freqQuery.mapValues(popFreq -> {
                String[] split = popFreq.getKey().split(VariantQueryUtils.STUDY_POP_FREQ_SEPARATOR);
                return new PopulationFrequencyVariantFilter.AltFreqFilter(split[0], split[1], popFreq.getOp(), popFreq.getValue());
            });
            filters.add(new PopulationFrequencyVariantFilter(freqQuery.getOperation(), freqFilters));
        }

        freqQuery = variantQuery.getPopulationFrequencyRef();
        if (!freqQuery.isEmpty()) {
            List<PopulationFrequencyVariantFilter.RefFreqFilter> freqFilters = freqQuery.mapValues(popFreq -> {
                String[] split = popFreq.getKey().split(VariantQueryUtils.STUDY_POP_FREQ_SEPARATOR);
                return new PopulationFrequencyVariantFilter.RefFreqFilter(split[0], split[1], popFreq.getOp(), popFreq.getValue());
            });
            filters.add(new PopulationFrequencyVariantFilter(freqQuery.getOperation(), freqFilters));
        }

        freqQuery = variantQuery.getPopulationFrequencyMaf();
        if (!freqQuery.isEmpty()) {
            List<PopulationFrequencyVariantFilter.MafFreqFilter> freqFilters = freqQuery.mapValues(popFreq -> {
                String[] split = popFreq.getKey().split(VariantQueryUtils.STUDY_POP_FREQ_SEPARATOR);
                return new PopulationFrequencyVariantFilter.MafFreqFilter(split[0], split[1], popFreq.getOp(), popFreq.getValue());
            });
            filters.add(new PopulationFrequencyVariantFilter(freqQuery.getOperation(), freqFilters));
        }

        if (variantQuery.getQuery().annotationExists() != null) {
            if (variantQuery.getQuery().annotationExists()) {
                filters.add(v -> v.getAnnotation() != null
                        && v.getAnnotation().getConsequenceTypes() != null
                        && !v.getAnnotation().getConsequenceTypes().isEmpty());
            } else {
                filters.add(v -> v.getAnnotation() == null
                        || v.getAnnotation().getConsequenceTypes() == null
                        || v.getAnnotation().getConsequenceTypes().isEmpty());
            }
        }
    }

    private void addClinicalFilters(ParsedVariantQuery variantQuery, List<Predicate<Variant>> filters) {
        if (variantQuery.getClinicalCombinations() == null) {
            return;
        }
        List<Set<String>> clinicalCombinations = variantQuery.getClinicalCombinations()
                .stream().map(HashSet::new).collect(Collectors.toList());
        if (clinicalCombinations.isEmpty()) {
            return;
        }
        if (clinicalCombinations.size() == 1) {
            Set<String> clinicalCombinationSet = clinicalCombinations.get(0);
            filters.add(variant -> {
                for (String c : VariantQueryUtils.buildClinicalCombinations(variant.getAnnotation())) {
                    if (clinicalCombinationSet.contains(c)) {
                        return true;
                    }
                }
                return false;
            });
        } else {
            filters.add(variant -> {
                for (Set<String> sourceCombinations : clinicalCombinations) {
                    boolean validSource = false;
                    for (String c : VariantQueryUtils.buildClinicalCombinations(variant.getAnnotation())) {
                        if (sourceCombinations.contains(c)) {
                            validSource = true;
                            break;
                        }
                    }
                    if (!validSource) {
                        return false;
                    }
                }
                // All sources were valid
                return true;
            });
        }

    }

    private boolean validGene(Set<String> genes, ConsequenceType ct) {
        return genes.contains(ct.getGeneId()) || genes.contains(ct.getGeneName()) || genes.contains(ct.getTranscriptId());
    }

    private boolean validCt(Set<String> acceptedCtValues, ConsequenceType ct) {
        if (acceptedCtValues == null) {
            return true;
        }
        for (SequenceOntologyTerm so : ct.getSequenceOntologyTerms()) {
            if (acceptedCtValues.contains(so.getName())) {
                return true;
            }
        }
        return false;
    }

    private boolean validBt(Set<String> acceptedBtValues, ConsequenceType ct) {
        if (acceptedBtValues == null) {
            return true;
        }
        return acceptedBtValues.contains(ct.getBiotype());
    }

    private Predicate<Variant> mergeFilters(List<Predicate<Variant>> filters, VariantQueryUtils.QueryOperation operator) {
        filters.removeIf(Objects::isNull);
        if (filters.isEmpty()) {
            return null;
        } else if (filters.size() == 1) {
            return filters.get(0);
        } else {
            Predicate<Variant> predicate = filters.get(0);
            for (int i = 1; i < filters.size(); i++) {
                if (operator == VariantQueryUtils.QueryOperation.OR) {
                    predicate = predicate.or(filters.get(i));
                } else {
                    predicate = predicate.and(filters.get(i));
                }
            }
            return predicate;
        }
    }


    private void addTypeFilter(ParsedVariantQuery variantQuery, List<Predicate<Variant>> filters) {
        if (variantQuery.getType() != null) {
            Set<VariantType> variantTypes = new HashSet<>(variantQuery.getType());
            filters.add(variant -> variantTypes.contains(variant.getType()));
        }
    }

    private void addQualFilter(ParsedVariantQuery variantQuery, List<Predicate<Variant>> filters) {
        String qual = variantQuery.getQuery().getString(VariantQueryParam.QUAL.key());
        if (qual != null && !qual.isEmpty()) {
            List<String> quals = VariantQueryUtils.splitValues(qual).getValues();
            filters.add(variant -> {
                for (String q : quals) {
                    // Simple parsing for now. A proper parser would handle operators.
                    // Assuming ">VALUE" or "<VALUE" or "VALUE"
                    Pattern pattern = Pattern.compile("^(.+?)(<=|>=|<|>|=|!=)(.+)$");
                    Matcher matcher = pattern.matcher(q);
                    String op;
                    String valueStr;
                    if (matcher.find()) {
                         op = matcher.group(2);
                         valueStr = matcher.group(3);
                    } else {
                         op = ">"; // Default? Or maybe "="
                         // If no operator, assume just value. But logic expects operator.
                         // Actually q might be "20" (meaning >= 20 usually implied)
                         // But here we need explicit parsing or assume.
                         // If checkOperator fails, it returns null.
                         // Let's assume ">" if just value, or parse simply.
                         // Actually let's assume valid operator is present or treat as >=.
                         if (q.matches("[\\.0-9]+")) {
                             op = ">=";
                             valueStr = q;
                         } else {
                             continue;
                         }
                    }

                    try {
                        double value = Double.parseDouble(valueStr);
                        // Check all files? or specific file? VariantQueryParam.QUAL usually applies to file.
                        // If includeFile is set, might filter on that.
                        // But usually checks if ANY file matches.

                        // For simplicity, checking if any file matches the criteria
                        // Need to check specific file if defined? Logic is complex.
                        // Adopting a simple "any file matches" strategy.
                        boolean match = false;
                        for (StudyEntry study : variant.getStudies()) {
                            for (FileEntry file : study.getFiles()) {
                                String qualStr = file.getData().get(StudyEntry.QUAL);
                                if (qualStr != null && !qualStr.equals(".")) {
                                    try {
                                        double fileQual = Double.parseDouble(qualStr);
                                        switch (op) {
                                            case ">":
                                                if (fileQual > value) {
                                                    match = true;
                                                }
                                                break;
                                            case ">=":
                                                if (fileQual >= value) {
                                                    match = true;
                                                }
                                                break;
                                            case "<":
                                                if (fileQual < value) {
                                                    match = true;
                                                }
                                                break;
                                            case "<=":
                                                if (fileQual <= value) {
                                                    match = true;
                                                }
                                                break;
                                            case "=":
                                                if (fileQual == value) {
                                                    match = true;
                                                }
                                                break;
                                            default:
                                                if (fileQual == value) {
                                                    match = true;
                                                }
                                                break;
                                        }
                                    } catch (NumberFormatException e) {
                                        // Ignore
                                    }
                                }
                                if (match) {
                                    break;
                                }
                            }
                            if (match) {
                                break;
                            }
                        }
                        if (!match) {
                            return false;
                        }
                    } catch (NumberFormatException e) {
                        return false;
                    }
                }
                return true;
            });
        }
    }

    private void addFilterFilter(ParsedVariantQuery variantQuery, List<Predicate<Variant>> filters) {
        String filterParam = variantQuery.getQuery().getString(VariantQueryParam.FILTER.key());
        if (filterParam != null && !filterParam.isEmpty()) {
            Set<String> acceptedFilters = new HashSet<>(VariantQueryUtils.splitValues(filterParam).getValues());
            filters.add(variant -> {
                 for (StudyEntry study : variant.getStudies()) {
                     for (FileEntry file : study.getFiles()) {
                         String fileFilter = file.getData().get(StudyEntry.FILTER);
                         if (fileFilter != null) {
                             // fileFilter can be "PASS;LowGQX"
                             for (String f : fileFilter.split(";")) {
                                 if (acceptedFilters.contains(f)) {
                                     return true;
                                 }
                             }
                         }
                     }
                 }
                 return false;
            });
        }
    }

    private void addScoreFilter(ParsedVariantQuery variantQuery, List<Predicate<Variant>> filters) {
        String scoreParam = variantQuery.getQuery().getString(VariantQueryParam.SCORE.key());
        if (scoreParam != null && !scoreParam.isEmpty()) {
            List<String> scores = VariantQueryUtils.splitValues(scoreParam).getValues();
             filters.add(variant -> {
                 if (variant.getAnnotation() == null || variant.getAnnotation().getFunctionalScore() == null) {
                     return false;
                 }
                 for (String s : scores) {
                     Pattern pattern = Pattern.compile("^(.+?)(<=|>=|<|>|=|!=)(.+)$");
                     Matcher matcher = pattern.matcher(s);
                     if (!matcher.find()) {
                         continue;
                     }
                     String key = matcher.group(1);
                     String op = matcher.group(2);
                     try {
                         double value = Double.parseDouble(matcher.group(3));
                         boolean match = false;
                         for (Score score : variant.getAnnotation().getFunctionalScore()) {
                             if (score.getSource().equals(key)) {
                                 Double scoreValue = score.getScore();
                                 if (scoreValue != null) {
                                     switch (op) {
                                          case ">":
                                              if (scoreValue > value) {
                                                  match = true;
                                              }
                                              break;
                                          case ">=":
                                              if (scoreValue >= value) {
                                                  match = true;
                                              }
                                              break;
                                          case "<":
                                              if (scoreValue < value) {
                                                  match = true;
                                              }
                                              break;
                                          case "<=":
                                              if (scoreValue <= value) {
                                                  match = true;
                                              }
                                              break;
                                          case "=":
                                              if (scoreValue == value) {
                                                  match = true;
                                              }
                                              break;
                                          default:
                                              if (scoreValue == value) {
                                                  match = true;
                                              }
                                              break;
                                      }
                                  }
                              }
                              if (match) {
                                  break;
                              }
                          }
                          if (!match) {
                              return false;
                          }
                     } catch (Exception e) {
                         return false;
                     }
                 }
                 return true;
             });
        }
    }

    private void addCohortStatsFilters(ParsedVariantQuery variantQuery, List<Predicate<Variant>> filters) {
        Map<VariantQueryParam, Function<VariantStats, Float>> statsParams = new HashMap<>();
        statsParams.put(VariantQueryParam.STATS_MAF, VariantStats::getMaf);
        statsParams.put(VariantQueryParam.STATS_MGF, VariantStats::getMgf);

        for (Map.Entry<VariantQueryParam, Function<VariantStats, Float>> entry : statsParams.entrySet()) {
             String paramValue = variantQuery.getQuery().getString(entry.getKey().key());
             if (paramValue != null && !paramValue.isEmpty()) {
             List<String> conditions = VariantQueryUtils.splitValues(paramValue).getValues();
                 filters.add(variant -> {
                     for (String condition : conditions) {
                         // Parse condition: "study:cohort<0.1" or "ALL<0.1"
                     // Parse condition: "study:cohort<0.1" or "ALL<0.1"
                     Pattern pattern = Pattern.compile("^(.+?)(<=|>=|<|>|=|!=)(.+)$");
                     Matcher matcher = pattern.matcher(condition);
                     if (!matcher.find()) {
                         // Invalid condition format
                         continue;
                     }
                     String key = matcher.group(1);
                     String op = matcher.group(2);
                     String valueStr = matcher.group(3);

                         try {
                             double value = Double.parseDouble(valueStr);
                             // Key can be "study:cohort" or "cohort" (default study)
                             // Assuming simple "cohort" or "study:cohort"
                             // Need to iterate studies in variant
                             boolean match = false;
                             for (StudyEntry studyEntry : variant.getStudies()) {
                                 // Check if study matches? If key has study.
                                 // For now, check all stats in studies.
                                 // This is a simplification. Real logic involves parsing study:cohort.
                                 for (VariantStats stats : studyEntry.getStats()) {
                                 String cohortName = stats.getCohortId();
                                 if (cohortName == null) {
                                     continue;
                                 }
                                 if (!key.equalsIgnoreCase("ALL") && !cohortName.equals(key)
                                             && !(studyEntry.getStudyId() + ":" + cohortName).equals(key)) {
                                         continue;
                                 }

                                     // VariantStats stats = studyEntry.getStats(cohortName); // Already have stats
                                     if (stats != null) {
                                         Float statValue = entry.getValue().apply(stats);
                                         if (statValue != null) {
                                              switch (op) {
                                                  case ">":
                                                      if (statValue > value) {
                                                          match = true;
                                                      }
                                                      break;
                                                  case ">=":
                                                      if (statValue >= value) {
                                                          match = true;
                                                      }
                                                      break;
                                                  case "<":
                                                      if (statValue < value) {
                                                          match = true;
                                                      }
                                                      break;
                                                  case "<=":
                                                      if (statValue <= value) {
                                                          match = true;
                                                      }
                                                      break;
                                                  case "=":
                                                      if (statValue == value) {
                                                          match = true;
                                                      }
                                                      break;
                                                  default:
                                                      if (statValue == value) {
                                                          match = true;
                                                      }
                                                      break;
                                              }
                                          }
                                      }
                                      if (match) {
                                          break;
                                      }
                                  }
                                  if (match) {
                                      break;
                                  }
                             }
                             if (!match) {
                                 return false;
                             }
                         } catch (Exception e) {
                             return false;
                         }
                     }
                     return true;
                 });
             }
        }
    }

    private void addSampleDataFilters(ParsedVariantQuery variantQuery, List<Predicate<Variant>> filters) {
        ParsedQuery<KeyValues<SampleMetadata, KeyOpValue<String, String>>> sampleDataQuery
                = variantQuery.getStudyQuery().getSampleDataQuery();

        if (sampleDataQuery != null && !sampleDataQuery.isEmpty()) {
            List<Predicate<Variant>> sampleFilters = new LinkedList<>();
            for (KeyValues<SampleMetadata, KeyOpValue<String, String>> entry
                    : sampleDataQuery.getValues()) {
                SampleMetadata sample = entry.getKey();
                String studyName = variantQuery.getStudyQuery().getDefaultStudyOrFail().getName();

                Predicate<Variant> p = variant -> {
                    StudyEntry studyEntry = variant.getStudy(studyName);
                    if (studyEntry == null) {
                        return false;
                    }

                    // Logic to check ALL conditions for this sample (AND logic usually)
                    for (KeyOpValue<String, String> condition : entry.getValues()) {
                        String formatKey = condition.getKey();
                        String op = condition.getOp();
                        String expectedValue = condition.getValue();

                        String sampleValue = studyEntry.getSampleData(sample.getName(), formatKey);
                        if (sampleValue == null || sampleValue.isEmpty() || sampleValue.equals(".")) {
                            // treat as missing? usually fails filter unless checking for missing
                            return false;
                        }

                        try {
                            double value = Double.parseDouble(expectedValue);
                            double sValue = Double.parseDouble(sampleValue);
                             switch (op) {
                                 case ">":
                                     if (!(sValue > value)) {
                                         return false;
                                     }
                                     break;
                                 case ">=":
                                     if (!(sValue >= value)) {
                                         return false;
                                     }
                                     break;
                                 case "<":
                                     if (!(sValue < value)) {
                                         return false;
                                     }
                                     break;
                                 case "<=":
                                     if (!(sValue <= value)) {
                                         return false;
                                     }
                                     break;
                                 case "=":
                                     if (!(sValue == value)) {
                                         return false;
                                     }
                                     break;
                                 case "!=":
                                     if (!(sValue != value)) {
                                         return false;
                                     }
                                     break;
                                 default:
                                     if (!(sValue == value)) {
                                         return false;
                                     }
                                     break;
                             }
                        } catch (NumberFormatException e) {
                            // String comparison
                            switch (op) {
                                case "=":
                                    if (!sampleValue.equals(expectedValue)) {
                                        return false;
                                    }
                                    break;
                                case "!=":
                                    if (sampleValue.equals(expectedValue)) {
                                        return false;
                                    }
                                    break;
                                // contains? matches?
                                default:
                                    if (!sampleValue.equals(expectedValue)) {
                                        return false;
                                    }
                                    break;
                            }
                        }
                    }
                    return true;
                };
                sampleFilters.add(p);
            }
            // Combine sample filters based on sampleDataQuery.getOperation() (AND/OR)
            filters.add(mergeFilters(sampleFilters, sampleDataQuery.getOperation()));
        }
    }

    private void addFileDataFilters(ParsedVariantQuery variantQuery, List<Predicate<Variant>> filters) {
        ParsedQuery<KeyValues<String, KeyOpValue<String, String>>> fileDataQuery
                = VariantQueryUtils.parseFileData(variantQuery.getQuery());

         if (fileDataQuery != null && !fileDataQuery.isEmpty()) {
            List<Predicate<Variant>> fileFilters = new LinkedList<>();
            for (KeyValues<String, KeyOpValue<String, String>> entry
                    : fileDataQuery.getValues()) {
                String fileName = entry.getKey();
                String studyName = variantQuery.getStudyQuery().getDefaultStudyOrFail().getName();

                Predicate<Variant> p = variant -> {
                    StudyEntry studyEntry = variant.getStudy(studyName);
                    if (studyEntry == null) {
                        return false;
                    }
                    FileEntry fileEntry = studyEntry.getFile(fileName);
                    if (fileEntry == null) {
                        return false;
                    }

                    for (KeyOpValue<String, String> condition : entry.getValues()) {
                        String infoKey = condition.getKey();
                        String op = condition.getOp();
                        String expectedValue = condition.getValue();

                        String infoValue = fileEntry.getData().get(infoKey);
                         if (infoValue == null || infoValue.isEmpty() || infoValue.equals(".")) {
                            return false;
                        }

                        try {
                            double value = Double.parseDouble(expectedValue);
                            double fValue = Double.parseDouble(infoValue);
                             switch (op) {
                                 case ">":
                                     if (!(fValue > value)) {
                                         return false;
                                     }
                                     break;
                                 case ">=":
                                     if (!(fValue >= value)) {
                                         return false;
                                     }
                                     break;
                                 case "<":
                                     if (!(fValue < value)) {
                                         return false;
                                     }
                                     break;
                                 case "<=":
                                     if (!(fValue <= value)) {
                                         return false;
                                     }
                                     break;
                                 case "=":
                                     if (!(fValue == value)) {
                                         return false;
                                     }
                                     break;
                                 case "!=":
                                     if (!(fValue != value)) {
                                         return false;
                                     }
                                     break;
                                 default:
                                     if (!(fValue == value)) {
                                         return false;
                                     }
                                     break;
                             }
                        } catch (NumberFormatException e) {
                             switch (op) {
                                case "=":
                                    if (!infoValue.equals(expectedValue)) {
                                        return false;
                                    }
                                    break;
                                case "!=":
                                    if (infoValue.equals(expectedValue)) {
                                        return false;
                                    }
                                    break;
                                default:
                                    if (!infoValue.equals(expectedValue)) {
                                        return false;
                                    }
                                    break;
                            }
                        }
                    }
                    return true;
                };
                fileFilters.add(p);
            }
            filters.add(mergeFilters(fileFilters, fileDataQuery.getOperation()));
         }
    }


}
