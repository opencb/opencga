package org.opencb.opencga.storage.core.metadata.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.opencb.cellbase.core.models.DataRelease;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;

import java.util.*;

/**
 * Created on 02/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ProjectMetadata extends ResourceMetadata<ProjectMetadata> {

    private static final String FILE_INDEX_LAST_TIMESTAMP = "file.index.last.timestamp";
    @Deprecated
    // This value was used by opencga-storage-hadoop internally for the same purpose. It is not used anymore.
    private static final String LAST_LOADED_FILE_TS = "lastLoadedFileTs";
    private static final String STATS_INDEX_LAST_TIMESTAMP = "stats.index.last.timestamp";
    private static final String ANNOTATION_INDEX_LAST_TIMESTAMP = "annotation.index.last.timestamp";
    private static final String SEARCH_INDEX_LAST_TIMESTAMP = "search.index.last.timestamp";

    private String species;
    private String assembly;
    private String dataRelease;

    private int release;
    // Timestamp of the last time the cache was valid. Anything older than this timestamp should be considered invalid.
    private int validCacheTimestamp;

    private VariantAnnotationSets annotation;

    private Map<String, Integer> counters;

    public static class VariantAnnotationSets {
        private VariantAnnotationMetadata current;
        private List<VariantAnnotationMetadata> saved;

        public VariantAnnotationSets() {
            saved = new ArrayList<>();
        }

        public VariantAnnotationSets(VariantAnnotationMetadata current, List<VariantAnnotationMetadata> saved) {
            this.current = current;
            this.saved = saved;
        }

        public VariantAnnotationSets(VariantAnnotationSets other) {
            this.current = other.current == null ? null : new VariantAnnotationMetadata(other.current);
            this.saved = new ArrayList<>(other.saved.size());
            for (VariantAnnotationMetadata saved : other.saved) {
                this.saved.add(new VariantAnnotationMetadata(saved));
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
            ProjectMetadata.VariantAnnotationMetadata saved = null;
            for (ProjectMetadata.VariantAnnotationMetadata annotation : getSaved()) {
                if (annotation.getName().equals(name)) {
                    saved = annotation;
                }
            }
            if (saved == null) {
                throw new VariantQueryException("Variant Annotation snapshot \"" + name + "\" not found!");
            }
            return saved;
        }

        public VariantAnnotationSets setSaved(List<VariantAnnotationMetadata> saved) {
            this.saved = saved;
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
                    && Objects.equals(saved, that.saved);
        }

        @Override
        public int hashCode() {
            return Objects.hash(current, saved);
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

        public VariantAnnotationMetadata() {
            extensions = new HashMap<>();
            sourceVersion = new ArrayList<>();
        }

        public VariantAnnotationMetadata(int id, String name, Date creationDate, VariantAnnotatorProgram annotator,
                                         Map<String, ObjectMap> extensions, List<ObjectMap> sourceVersion, DataRelease dataRelease,
                                         List<String> privateSources) {
            this.id = id;
            this.name = name;
            this.creationDate = creationDate;
            this.annotator = annotator;
            this.extensions = extensions;
            this.sourceVersion = sourceVersion != null ? sourceVersion : new ArrayList<>();
            this.dataRelease = dataRelease;
            this.privateSources = privateSources;
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
                    && Objects.equals(privateSources, that.privateSources);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, creationDate, annotator, extensions, sourceVersion, dataRelease, privateSources);
        }
    }

    public static class VariantAnnotatorProgram {
        private String name;
        private String version;
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
        counters = new HashMap<>();
        setAttributes(new ObjectMap());
    }

    public ProjectMetadata(String species, String assembly, int release) {
        this(species, assembly, null, release, 0, null, null, null);
    }

    public ProjectMetadata(String species, String assembly, String dataRelease, int release, int validCacheTimestamp, ObjectMap attributes,
                           Map<String, Integer> counters, VariantAnnotationSets annotation) {
        this.species = species;
        this.assembly = assembly;
        this.dataRelease = dataRelease;
        this.release = release;
        this.validCacheTimestamp = validCacheTimestamp;
        setAttributes(attributes != null ? attributes : new ObjectMap());
        this.annotation = annotation != null ? annotation : new VariantAnnotationSets();
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

    @JsonIgnore
    public TaskMetadata.Status getSecondaryAnnotationIndexStatus() {
        return getStatus("secondaryAnnotationIndex");
    }

    @JsonIgnore
    public ProjectMetadata setSecondaryAnnotationIndexStatus(TaskMetadata.Status annotationStatus) {
        return setStatus("secondaryAnnotationIndex", annotationStatus);
    }

    @JsonIgnore
    public ProjectMetadata setVariantIndexLastTimestamp() {
        getAttributes().put(FILE_INDEX_LAST_TIMESTAMP, System.currentTimeMillis());
        return this;
    }

    @JsonIgnore
    public long getVariantIndexLastTimestamp() {
        long ts = getAttributes().getLong(FILE_INDEX_LAST_TIMESTAMP, 0);
        if (ts == 0) {
            // Old versions of the metadata may still use the old field
            return getAttributes().getLong(LAST_LOADED_FILE_TS, 0);
        }
        return ts;
    }

    @JsonIgnore
    public ProjectMetadata setAnnotationIndexLastTimestamp(long annotationStartTimestamp) {
        getAttributes().put(ANNOTATION_INDEX_LAST_TIMESTAMP, annotationStartTimestamp);
        return this;
    }

    @JsonIgnore
    public long getAnnotationIndexLastTimestamp() {
        return getAttributes().getLong(ANNOTATION_INDEX_LAST_TIMESTAMP, 0);
    }

    @JsonIgnore
    public ProjectMetadata setSecondaryAnnotationIndexLastTimestamp(long timestamp) {
        getAttributes().put(SEARCH_INDEX_LAST_TIMESTAMP, timestamp);
        return this;
    }

    @JsonIgnore
    public long getSecondaryAnnotationIndexLastTimestamp() {
        return getAttributes().getLong(SEARCH_INDEX_LAST_TIMESTAMP, 0);
    }

    @JsonIgnore
    public ProjectMetadata setStatsIndexLastTimestamp(long timeMillis) {
        getAttributes().put(STATS_INDEX_LAST_TIMESTAMP, timeMillis);
        return this;
    }

    @JsonIgnore
    public long getStatsLastTimestamp() {
        return getAttributes().getLong(STATS_INDEX_LAST_TIMESTAMP, 0);
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
