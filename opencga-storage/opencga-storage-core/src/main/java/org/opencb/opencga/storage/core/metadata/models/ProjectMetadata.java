package org.opencb.opencga.storage.core.metadata.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.opencb.cellbase.core.models.DataRelease;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.metadata.models.project.VariantSecondaryAnnotationIndexSets;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;

import java.util.*;

/**
 * Created on 02/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ProjectMetadata extends ResourceMetadata<ProjectMetadata> {

    // Last time (in millis from epoch) that a file was loaded. Timestamp at operation end!
    private static final String FILE_INDEX_LAST_TIMESTAMP = "file.index.last.timestamp";
    @Deprecated
    // This value was used by opencga-storage-hadoop internally for the same purpose. It is not used anymore.
    private static final String LAST_LOADED_FILE_TS = "lastLoadedFileTs";

    // Last time (in millis from epoch) that a variant stats index was executed for any cohort. Timestamp at operation end!
    private static final String STATS_INDEX_LAST_END_TIMESTAMP = "stats.index.last.timestamp";
    // Last time (in millis from epoch) that a variant annotation was executed. Timestamp at operation start!
    private static final String ANNOTATION_INDEX_LAST_UPDATE_START_TIMESTAMP = "annotation.index.last.timestamp";
    // Last time (in millis from epoch) that a variant annotation was executed. Timestamp at operation end!
    private static final String ANNOTATION_INDEX_LAST_UPDATE_END_TIMESTAMP = "annotation.index.last.end.timestamp";
    // Last time (in millis from epoch) that a full variant annotation was executed. Timestamp at operation start!
    private static final String ANNOTATION_INDEX_LAST_FULL_UPDATE_START_TIMESTAMP = "annotation.index.last.full.start.timestamp";

    private String species;
    private String assembly;
    private String dataRelease;

    private int release;
    // Timestamp of the last time the cache was valid. Anything older than this timestamp should be considered invalid.
    private int validCacheTimestamp;

    private VariantAnnotationSets annotation;
    private VariantSecondaryAnnotationIndexSets secondaryAnnotationIndex;

    private Map<String, Integer> counters;

    public static class VariantAnnotationSets {
        /**
         * The active annotation generation — variants are currently being stamped with
         * {@code current.id}.
         */
        private VariantAnnotationMetadata current;

        /**
         * Past generations whose variant data was preserved by an explicit
         * {@code saveAnnotation(snapshotId)} call (the per-id snapshot collection / column was
         * populated, possibly empty if no variants were stamped against this id at copy time).
         * Entries here are durable references that downstream queries can target.
         */
        private List<VariantAnnotationMetadata> saved;

        /**
         * Past generations recorded as audit-only — the project bumped past these ids (via an
         * annotator/config change or {@code --force-new-annotation-set}) but no
         * {@code saveAnnotation} call was made to preserve variant data under that id.
         * Entries here describe a transition that happened (annotator before/after, reason)
         * but cannot be queried back against — running {@code saveAnnotation(transitionId)}
         * promotes an entry from this list to {@link #saved}.
         */
        private List<VariantAnnotationMetadata> transitions;

        public VariantAnnotationSets() {
            saved = new ArrayList<>();
            transitions = new ArrayList<>();
        }

        public VariantAnnotationSets(VariantAnnotationMetadata current, List<VariantAnnotationMetadata> saved) {
            this(current, saved, new ArrayList<>());
        }

        public VariantAnnotationSets(VariantAnnotationMetadata current, List<VariantAnnotationMetadata> saved,
                                     List<VariantAnnotationMetadata> transitions) {
            this.current = current;
            this.saved = saved;
            this.transitions = transitions;
        }

        public VariantAnnotationSets(VariantAnnotationSets other) {
            this.current = other.current == null ? null : new VariantAnnotationMetadata(other.current);
            this.saved = new ArrayList<>(other.saved.size());
            for (VariantAnnotationMetadata saved : other.saved) {
                this.saved.add(new VariantAnnotationMetadata(saved));
            }
            // Defensive: pre-existing projects deserialized with no transitions field land with null
            // until Jackson catches up. Normalize to an empty list so callers can always iterate.
            List<VariantAnnotationMetadata> otherTransitions = other.transitions == null
                    ? Collections.emptyList() : other.transitions;
            this.transitions = new ArrayList<>(otherTransitions.size());
            for (VariantAnnotationMetadata t : otherTransitions) {
                this.transitions.add(new VariantAnnotationMetadata(t));
            }
        }

        public VariantAnnotationMetadata getCurrent() {
            return current;
        }

        public VariantAnnotationSets setCurrent(VariantAnnotationMetadata current) {
            this.current = current;
            return this;
        }

        public List<VariantAnnotationMetadata> getSaved() {
            return saved;
        }

        public VariantAnnotationMetadata getSaved(String name) {
            VariantAnnotationMetadata saved = getSavedOrNull(name);
            if (saved == null) {
                throw new VariantQueryException("Variant Annotation snapshot \"" + name + "\" not found!");
            }
            return saved;
        }

        /**
         * Non-throwing case-sensitive lookup of a {@code saved} entry by name. Returns {@code null}
         * if the name is not present.
         *
         * @param name snapshot name to find
         * @return the matching saved entry, or {@code null} if not present
         */
        public VariantAnnotationMetadata getSavedOrNull(String name) {
            for (VariantAnnotationMetadata annotation : getSaved()) {
                if (annotation.getName().equals(name)) {
                    return annotation;
                }
            }
            return null;
        }

        /**
         * Non-throwing case-sensitive lookup of a {@code transitions} entry by name. Returns
         * {@code null} if the name is not present.
         *
         * @param name transition auto-name to find
         * @return the matching transition entry, or {@code null} if not present
         */
        public VariantAnnotationMetadata getTransitionOrNull(String name) {
            for (VariantAnnotationMetadata annotation : getTransitions()) {
                if (annotation.getName().equals(name)) {
                    return annotation;
                }
            }
            return null;
        }

        /**
         * Remove a transition entry by case-sensitive name match. Returns the removed entry or
         * {@code null} if no entry matched.
         *
         * @param name transition auto-name to remove
         * @return the removed transition, or {@code null} if not present
         */
        public VariantAnnotationMetadata removeTransition(String name) {
            Iterator<VariantAnnotationMetadata> it = getTransitions().iterator();
            while (it.hasNext()) {
                VariantAnnotationMetadata t = it.next();
                if (t.getName().equals(name)) {
                    it.remove();
                    return t;
                }
            }
            return null;
        }

        /**
         * Remove a {@code saved} entry by case-sensitive name match. Returns the removed entry or
         * {@code null} if no entry matched.
         *
         * @param name snapshot name to remove
         * @return the removed saved entry, or {@code null} if not present
         */
        public VariantAnnotationMetadata removeSaved(String name) {
            Iterator<VariantAnnotationMetadata> it = getSaved().iterator();
            while (it.hasNext()) {
                VariantAnnotationMetadata s = it.next();
                if (s.getName().equals(name)) {
                    it.remove();
                    return s;
                }
            }
            return null;
        }

        /**
         * Look up a {@code transitions} entry by annotationSetId. Returns {@code null} if no entry
         * matches.
         *
         * @param id annotationSetId of the transition (the OLD generation's id, recorded at bump
         *           time)
         * @return the matching transition entry, or {@code null} if not present
         */
        public VariantAnnotationMetadata getTransitionByIdOrNull(int id) {
            for (VariantAnnotationMetadata t : getTransitions()) {
                if (t.getId() == id) {
                    return t;
                }
            }
            return null;
        }

        /**
         * The most recently appended transition entry, or {@code null} if {@link #transitions} is
         * empty. Used by bump-idempotency checks that need to peek at the tail of the list.
         *
         * @return the last transition entry or {@code null}
         */
        public VariantAnnotationMetadata getLastTransitionOrNull() {
            List<VariantAnnotationMetadata> list = getTransitions();
            return list.isEmpty() ? null : list.get(list.size() - 1);
        }

        /**
         * Find a recorded annotation generation by name across {@link #saved} and
         * {@link #transitions}. Callers that don't care whether the entry has preserved variant
         * data behind it (e.g. {@code getAnnotationMetadata} when the user asks for an arbitrary
         * generation by name) should use this rather than {@link #getSaved(String)} — the latter
         * is restricted to entries with preserved data and will throw on transition-only names.
         *
         * @param name annotation generation name (snapshot name from saved, or auto-transition name)
         * @return the matching annotation metadata
         */
        public VariantAnnotationMetadata findRecord(String name) {
            for (VariantAnnotationMetadata annotation : getSaved()) {
                if (annotation.getName().equals(name)) {
                    return annotation;
                }
            }
            for (VariantAnnotationMetadata annotation : getTransitions()) {
                if (annotation.getName().equals(name)) {
                    return annotation;
                }
            }
            throw new VariantQueryException("Variant Annotation \"" + name + "\" not found!");
        }

        public VariantAnnotationSets setSaved(List<VariantAnnotationMetadata> saved) {
            this.saved = saved;
            return this;
        }

        public List<VariantAnnotationMetadata> getTransitions() {
            // Defensive against pre-migration deserialized instances.
            if (transitions == null) {
                transitions = new ArrayList<>();
            }
            return transitions;
        }

        public VariantAnnotationSets setTransitions(List<VariantAnnotationMetadata> transitions) {
            this.transitions = transitions;
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            VariantAnnotationSets that = (VariantAnnotationSets) o;
            return Objects.equals(current, that.current)
                    && Objects.equals(saved, that.saved)
                    && Objects.equals(getTransitions(), that.getTransitions());
        }

        @Override
        public int hashCode() {
            return Objects.hash(current, saved, getTransitions());
        }
    }

    public static class VariantAnnotationMetadata {
        private int id;
        private String name;
        private Date creationDate;
        private VariantAnnotatorProgram annotator;
        private Map<String, ObjectMap> extensions;
        private List<ObjectMap> sourceVersion;
        private DataRelease dataRelease;
        private List<String> privateSources;
        /**
         * Free-form description of this annotation snapshot. Populated automatically when the
         * annotationSetId is bumped by {@code VariantAnnotationManager} to record why
         * (e.g. "annotator changed: ...; dataRelease changed: 4 -> 7"). Nullable for backwards
         * compatibility with snapshots created before this field existed.
         */
        private String description;

        public VariantAnnotationMetadata() {
            extensions = new HashMap<>();
            sourceVersion = new ArrayList<>();
        }

        public VariantAnnotationMetadata(int id, String name, Date creationDate, VariantAnnotatorProgram annotator,
                                         Map<String, ObjectMap> extensions, List<ObjectMap> sourceVersion, DataRelease dataRelease,
                                         List<String> privateSources) {
            this(id, name, creationDate, annotator, extensions, sourceVersion, dataRelease, privateSources, null);
        }

        public VariantAnnotationMetadata(int id, String name, Date creationDate, VariantAnnotatorProgram annotator,
                                         Map<String, ObjectMap> extensions, List<ObjectMap> sourceVersion, DataRelease dataRelease,
                                         List<String> privateSources, String description) {
            this.id = id;
            this.name = name;
            this.creationDate = creationDate;
            this.annotator = annotator;
            this.extensions = extensions;
            this.sourceVersion = sourceVersion != null ? sourceVersion : new ArrayList<>();
            this.dataRelease = dataRelease;
            this.privateSources = privateSources;
            this.description = description;
        }

        public VariantAnnotationMetadata(VariantAnnotationMetadata other) {
            this.id = other.id;
            this.name = other.name;
            this.creationDate = other.creationDate;
            this.annotator = other.annotator;
            this.extensions = other.extensions;
            this.sourceVersion = new ArrayList<>(other.sourceVersion.size());
            for (ObjectMap source : other.sourceVersion) {
                this.sourceVersion.add(new ObjectMap(source));
            }
            this.dataRelease = other.dataRelease == null ? null : JacksonUtils.copySafe(other.dataRelease, DataRelease.class);
            this.privateSources = other.privateSources != null ? new ArrayList<>(other.privateSources) : null;
            this.description = other.description;
        }

        public int getId() {
            return id;
        }

        public VariantAnnotationMetadata setId(int id) {
            this.id = id;
            return this;
        }

        public String getName() {
            return name;
        }

        public VariantAnnotationMetadata setName(String name) {
            this.name = name;
            return this;
        }

        public Date getCreationDate() {
            return creationDate;
        }

        public VariantAnnotationMetadata setCreationDate(Date creationDate) {
            this.creationDate = creationDate;
            return this;
        }

        public VariantAnnotatorProgram getAnnotator() {
            return annotator;
        }

        public VariantAnnotationMetadata setAnnotator(VariantAnnotatorProgram annotator) {
            this.annotator = annotator;
            return this;
        }

        public Map<String, ObjectMap> getExtensions() {
            return extensions;
        }

        public VariantAnnotationMetadata setExtensions(Map<String, ObjectMap> extensions) {
            this.extensions = extensions;
            return this;
        }

        public void addExtension(String id, ObjectMap metadata) {
            this.extensions.put(id, metadata);
        }

        public List<ObjectMap> getSourceVersion() {
            return sourceVersion;
        }

        public VariantAnnotationMetadata setSourceVersion(List<ObjectMap> sourceVersion) {
            this.sourceVersion = sourceVersion;
            return this;
        }

        public DataRelease getDataRelease() {
            return dataRelease;
        }

        public VariantAnnotationMetadata setDataRelease(DataRelease dataRelease) {
            this.dataRelease = dataRelease;
            return this;
        }

        public List<String> getPrivateSources() {
            return privateSources;
        }

        public VariantAnnotationMetadata setPrivateSources(List<String> privateSources) {
            this.privateSources = privateSources;
            return this;
        }

        public String getDescription() {
            return description;
        }

        public VariantAnnotationMetadata setDescription(String description) {
            this.description = description;
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            VariantAnnotationMetadata that = (VariantAnnotationMetadata) o;
            return id == that.id && Objects.equals(name, that.name)
                    && Objects.equals(creationDate, that.creationDate)
                    && Objects.equals(annotator, that.annotator)
                    && Objects.equals(extensions, that.extensions)
                    && Objects.equals(sourceVersion, that.sourceVersion)
                    && Objects.equals(dataRelease, that.dataRelease)
                    && Objects.equals(privateSources, that.privateSources)
                    && Objects.equals(description, that.description);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, creationDate, annotator, extensions, sourceVersion, dataRelease, privateSources,
                    description);
        }
    }

    public static class VariantAnnotatorProgram {
        private String name;
        private String version;
        // TODO: Add clientVersion;
        private String commit;

        public VariantAnnotatorProgram() {
        }

        public VariantAnnotatorProgram(String name, String version, String commit) {
            this.name = name;
            this.commit = commit;
            this.version = version;
        }

        public String getName() {
            return name;
        }

        public VariantAnnotatorProgram setName(String name) {
            this.name = name;
            return this;
        }

        public String getVersion() {
            return version;
        }

        public VariantAnnotatorProgram setVersion(String version) {
            this.version = version;
            return this;
        }

        public String getCommit() {
            return commit;
        }

        public VariantAnnotatorProgram setCommit(String commit) {
            this.commit = commit;
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof VariantAnnotatorProgram)) {
                return false;
            }
            VariantAnnotatorProgram that = (VariantAnnotatorProgram) o;
            return Objects.equals(name, that.name)
                    && Objects.equals(version, that.version)
                    && Objects.equals(commit, that.commit);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, version, commit);
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                    .append("name", name)
                    .append("version", version)
                    .append("commit", commit)
                    .toString();
        }
    }

    public ProjectMetadata() {
        release = 1;
        validCacheTimestamp = 0;
        dataRelease = "";
        annotation = new VariantAnnotationSets();
        secondaryAnnotationIndex = new VariantSecondaryAnnotationIndexSets();
        counters = new HashMap<>();
        setAttributes(new ObjectMap());
    }

    public ProjectMetadata(String species, String assembly, int release) {
        this(species, assembly, null, release, 0, null, null, null, null);
    }

    public ProjectMetadata(String species, String assembly, String dataRelease, int release, int validCacheTimestamp, ObjectMap attributes,
                           Map<String, Integer> counters, VariantAnnotationSets annotation,
                           VariantSecondaryAnnotationIndexSets secondaryAnnotationIndex) {
        this.species = species;
        this.assembly = assembly;
        this.dataRelease = dataRelease;
        this.release = release;
        this.validCacheTimestamp = validCacheTimestamp;
        setAttributes(attributes != null ? attributes : new ObjectMap());
        this.annotation = annotation != null ? annotation : new VariantAnnotationSets();
        this.secondaryAnnotationIndex = secondaryAnnotationIndex != null ? secondaryAnnotationIndex
                : new VariantSecondaryAnnotationIndexSets();
        this.counters = counters != null ? counters : new HashMap<>();
    }

    public ProjectMetadata(ProjectMetadata other) {
        super(other);
        this.species = other.species;
        this.assembly = other.assembly;
        this.dataRelease = other.dataRelease;
        this.release = other.release;
        this.validCacheTimestamp = other.validCacheTimestamp;
        setAttributes(other.getAttributes() != null ? new ObjectMap(other.getAttributes()) : new ObjectMap());
        this.annotation = other.annotation != null ? new VariantAnnotationSets(other.annotation) : new VariantAnnotationSets();
        this.counters = other.counters != null ? new HashMap<>(other.counters) : new HashMap<>();
        this.secondaryAnnotationIndex = other.secondaryAnnotationIndex;
    }

    public ProjectMetadata copy() {
        return new ProjectMetadata(this);
    }

    public String getSpecies() {
        return species;
    }

    public ProjectMetadata setSpecies(String species) {
        this.species = species;
        return this;
    }

    public String getAssembly() {
        return assembly;
    }

    public ProjectMetadata setAssembly(String assembly) {
        this.assembly = assembly;
        return this;
    }

    public String getDataRelease() {
        return dataRelease;
    }

    public ProjectMetadata setDataRelease(String dataRelease) {
        this.dataRelease = dataRelease;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public ProjectMetadata setRelease(int release) {
        this.release = release;
        return this;
    }

    public int getValidCacheTimestamp() {
        return validCacheTimestamp;
    }

    public ProjectMetadata setValidCacheTimestamp(int validCacheTimestamp) {
        this.validCacheTimestamp = validCacheTimestamp;
        return this;
    }

    public VariantAnnotationSets getAnnotation() {
        return annotation;
    }

    public ProjectMetadata setAnnotation(VariantAnnotationSets annotation) {
        this.annotation = annotation;
        return this;
    }

    public VariantSecondaryAnnotationIndexSets getSecondaryAnnotationIndex() {
        return secondaryAnnotationIndex;
    }

    public ProjectMetadata setSecondaryAnnotationIndex(VariantSecondaryAnnotationIndexSets secondaryAnnotationIndex) {
        this.secondaryAnnotationIndex = secondaryAnnotationIndex;
        return this;
    }

    public Map<String, Integer> getCounters() {
        return counters;
    }

    public ProjectMetadata setCounters(Map<String, Integer> counters) {
        this.counters = counters;
        return this;
    }

    @JsonIgnore
    public TaskMetadata.Status getAnnotationIndexStatus() {
        return getStatus("annotation");
    }

    @JsonIgnore
    public ProjectMetadata setAnnotationIndexStatus(TaskMetadata.Status annotationStatus) {
        return setStatus("annotation", annotationStatus);
    }

    /**
     * Last time (in millis from epoch) that a file was loaded. Timestamp at operation end!
     * @return this
     */
    @JsonIgnore
    public ProjectMetadata setVariantIndexLastTimestamp() {
        getAttributes().put(FILE_INDEX_LAST_TIMESTAMP, System.currentTimeMillis());
        return this;
    }

    /**
     * @return Last time (in millis from epoch) that a file was loaded. Timestamp at operation end!
     */
    @JsonIgnore
    public long getVariantIndexLastTimestamp() {
        long ts = getAttributes().getLong(FILE_INDEX_LAST_TIMESTAMP, 0);
        if (ts == 0) {
            // Old versions of the metadata may still use the old field
            return getAttributes().getLong(LAST_LOADED_FILE_TS, 0);
        }
        return ts;
    }

    /**
     *
     * @param annotationStartTimestamp The timestamp when the annotation index update started. Includes full or partial update.
     * @return this
     */
    @JsonIgnore
    public ProjectMetadata setAnnotationIndexLastUpdateStartTimestamp(long annotationStartTimestamp) {
        getAttributes().put(ANNOTATION_INDEX_LAST_UPDATE_START_TIMESTAMP, annotationStartTimestamp);
        return this;
    }

    /**
     * @return The timestamp when the annotation index was last updated. Includes full or partial update.
     */
    @JsonIgnore
    public long getAnnotationIndexLastUpdateStartTimestamp() {
        return getAttributes().getLong(ANNOTATION_INDEX_LAST_UPDATE_START_TIMESTAMP, 0);
    }

    @JsonIgnore
    public ProjectMetadata setAnnotationIndexLastUpdateEndTimestamp(long annotationEndTimestamp) {
        getAttributes().put(ANNOTATION_INDEX_LAST_UPDATE_END_TIMESTAMP, annotationEndTimestamp);
        return this;
    }

    @JsonIgnore
    public long getAnnotationIndexLastUpdateEndTimestamp() {
        return getAttributes().getLong(ANNOTATION_INDEX_LAST_UPDATE_END_TIMESTAMP, 0);
    }

    /**
     * @param annotationStartTimestamp The timestamp when the annotation index full update finished.
     * @return this
     */
    @JsonIgnore
    public ProjectMetadata setAnnotationIndexLastFullUpdateStartTimestamp(long annotationStartTimestamp) {
        getAttributes().put(ANNOTATION_INDEX_LAST_FULL_UPDATE_START_TIMESTAMP, annotationStartTimestamp);
        return this;
    }

    /**
     * @return The timestamp when the annotation index was last fully updated.
     */
    @JsonIgnore
    public long getAnnotationIndexLastFullUpdateStartTimestamp() {
        return getAttributes().getLong(ANNOTATION_INDEX_LAST_FULL_UPDATE_START_TIMESTAMP, 0);
    }


    @JsonIgnore
    public ProjectMetadata setStatsIndexLastEndTimestamp(long timeMillis) {
        getAttributes().put(STATS_INDEX_LAST_END_TIMESTAMP, timeMillis);
        return this;
    }

    @JsonIgnore
    public long getStatsLastEndTimestamp() {
        return getAttributes().getLong(STATS_INDEX_LAST_END_TIMESTAMP, 0);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProjectMetadata that = (ProjectMetadata) o;
        return release == that.release && Objects.equals(species, that.species)
                && Objects.equals(assembly, that.assembly)
                && Objects.equals(dataRelease, that.dataRelease)
                && Objects.equals(annotation, that.annotation)
                && Objects.equals(counters, that.counters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(species, assembly, dataRelease, release, annotation, counters);
    }

}
