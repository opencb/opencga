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

package org.opencb.opencga.storage.core.variant.annotation;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.cellbase.core.models.DataRelease;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata.VariantAnnotationMetadata;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata.VariantAnnotatorProgram;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created on 23/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantAnnotationManager {

    @Deprecated
    public static final String ANNOTATION_SOURCE = "annotationSource";
    // File to load.
    public static final String CREATE = "annotation.create";
    public static final String LOAD_FILE = "annotation.load.file";
    public static final String CUSTOM_ANNOTATION_KEY = "custom_annotation_key";
    public static final String CURRENT = "CURRENT";

    /**
     * Marker prefix used in {@link VariantAnnotationMetadata#getName()} for snapshots created by
     * {@link #bumpAnnotationSetId}. Used as an idempotency marker to detect whether a transition
     * has already been bumped within the current annotate() call.
     */
    /**
     * Name prefix for entries recorded in {@link ProjectMetadata.VariantAnnotationSets#getTransitions()}.
     * Used by {@link #bumpAnnotationSetId} to tag implicit bump records (annotator change, overwrite,
     * forceNewAnnotationSet) and to recognise them for the in-call idempotency check. NOT a snapshot —
     * a snapshot implies preserved variant data, which transitions do not have.
     */
    private static final String AUTO_TRANSITION_PREFIX = "transition_";

    private static Logger logger = LoggerFactory.getLogger(VariantAnnotationManager.class);

    public abstract long annotate(Query query, ObjectMap options) throws VariantAnnotatorException, IOException, StorageEngineException;

    public abstract void saveAnnotation(String name, ObjectMap options) throws StorageEngineException, VariantAnnotatorException;

    public abstract void deleteAnnotation(String name, ObjectMap options) throws StorageEngineException, VariantAnnotatorException;

    /**
     * Apply the caller's pre-resolved annotator metadata to {@code current}, recording the OLD
     * state as a transition if anything actually changed. Cosmetic config changes that don't
     * affect annotation provenance (e.g. DNS CNAME repointing at the same physical CellBase
     * server) produce no transition because the metadata comparison comes out equal.
     *
     * <p>The caller (typically {@link VariantStorageEngine#updateCellbaseConfiguration}) resolves
     * the server-derived metadata BEFORE acquiring the project lock so that network failures
     * surface as {@link VariantAnnotatorException} without mutating engine state.
     *
     * <p>To defend against concurrent {@code updateCellbaseConfiguration} calls overwriting each
     * other's metadata (the classic TOCTOU on probe → lock-acquire), the caller also passes the
     * {@code current.annotator} value it observed at probe time; if {@code current.annotator} has
     * moved on by the time the lock is acquired the call aborts without mutating metadata (the
     * other writer's transition is the authoritative one for that window).
     *
     * @param preResolvedMetadata the server-derived annotator metadata captured by the caller
     *                            BEFORE acquiring the project lock — used as the new
     *                            {@code current.annotator} state when a bump is required.
     * @param expectedCurrentAnnotator the {@code current.annotator} value the caller observed at
     *                                 probe time. If {@code null}, no CAS guard is enforced.
     * @return the auto-name of the transition that was just recorded, or {@code null} if the
     *         comparison came out equal (no-op) or the CAS guard aborted. Callers that want to
     *         preserve the OLD state downstream (e.g. by submitting a saveAnnotation job with
     *         {@code fromAnnotationSet}) should pass this value through.
     * @throws StorageEngineException if the metadata mutation fails
     * @throws VariantAnnotatorException if the metadata cannot be applied
     */
    public abstract String synchroniseWithCurrentAnnotator(VariantAnnotationMetadata preResolvedMetadata,
                                                           VariantAnnotatorProgram expectedCurrentAnnotator)
            throws StorageEngineException, VariantAnnotatorException;

    protected final VariantAnnotationMetadata checkCurrentAnnotation(VariantAnnotator annotator, ProjectMetadata projectMetadata,
                                                                     boolean overwrite)
            throws VariantAnnotatorException {
        return checkCurrentAnnotation(annotator, projectMetadata, overwrite, false);
    }

    protected final VariantAnnotationMetadata checkCurrentAnnotation(VariantAnnotator annotator, ProjectMetadata projectMetadata,
                                                                     boolean overwrite, boolean forceNewAnnotationSet)
            throws VariantAnnotatorException {
        ProjectMetadata.VariantAnnotationMetadata newVariantAnnotationMetadata = annotator.getVariantAnnotationMetadata();
        return checkCurrentAnnotation(projectMetadata, overwrite, forceNewAnnotationSet, newVariantAnnotationMetadata);
    }

    protected final VariantAnnotationMetadata checkCurrentAnnotation(ProjectMetadata projectMetadata, boolean overwrite,
                                                                     VariantAnnotationMetadata newVariantAnnotationMetadata)
            throws VariantAnnotatorException {
        return checkCurrentAnnotation(projectMetadata, overwrite, false, newVariantAnnotationMetadata);
    }

    protected final VariantAnnotationMetadata checkCurrentAnnotation(ProjectMetadata projectMetadata, boolean overwrite,
                                                                     boolean forceNewAnnotationSet,
                                                                     VariantAnnotationMetadata newVariantAnnotationMetadata)
            throws VariantAnnotatorException {
        VariantAnnotationMetadata current = projectMetadata.getAnnotation().getCurrent();
        if (current == null) {
            current = new VariantAnnotationMetadata();
            projectMetadata.getAnnotation().setCurrent(current);
            current.setId(1);
            current.setName(CURRENT);
        }
        boolean firstAnnotation = current.getAnnotator() == null;

        // Reasons accumulated for bumping the annotationSetId. Each branch below that "would have
        // failed without overwrite" appends a human-readable reason. At the end of this method, if
        // the list is non-empty (or forceNewAnnotationSet is true), we snapshot current and bump.
        List<String> bumpReasons = new ArrayList<>();
        if (forceNewAnnotationSet && !firstAnnotation) {
            bumpReasons.add("forceNewAnnotationSet requested");
        }

        // Check using same annotator and same source version
        VariantAnnotatorProgram currentAnnotator = current.getAnnotator();
        VariantAnnotatorProgram newAnnotator = newVariantAnnotationMetadata.getAnnotator();
        if (!firstAnnotation && !currentAnnotator.equals(newAnnotator)) {
            String currentVersion = removePatchFromVersion(currentAnnotator.getVersion());
            String newVersion = removePatchFromVersion(newAnnotator.getVersion());
            if (!currentAnnotator.getName().equals(newAnnotator.getName())
                    || !currentVersion.equals(newVersion)) {
                String msg = "Using a different annotator! "
                        + "Existing annotation calculated with " + currentAnnotator.toString()
                        + ", attempting to annotate with " + newAnnotator.toString();
                if (overwrite) {
                    logger.info(msg);
                    bumpReasons.add("annotator changed: " + currentAnnotator + " -> " + newAnnotator);
                } else {
                    throw new VariantAnnotatorException(msg);
                }
            } else if (!currentAnnotator.getCommit().equals(newAnnotator.getCommit())) {
                String msg = "Using a different patch version for annotating variants. "
                        + "Existing annotation calculated with " + currentAnnotator.toString()
                        + ", attempting to annotate with " + newAnnotator.toString();
                if (overwrite) {
                    logger.info(msg);
                } else {
                    logger.warn(msg);
                }
                // Patch-only difference: not enough on its own to bump annotationSetId.
            }
        }

        if (current.getDataRelease() != null && newVariantAnnotationMetadata.getDataRelease() == null) {
            // Regression. DataRelease is lost.
            String msg = "DataRelease missing. "
                            + "Existing annotation calculated with dataRelease " + current.getDataRelease().getRelease()
                            + ", attempting to annotate without explicit dataRelease";

            if (overwrite) {
                logger.info(msg);
                bumpReasons.add("dataRelease dropped (was " + current.getDataRelease().getRelease() + ")");
            } else {
                throw new VariantAnnotatorException(msg);
            }
        }

        List<String> currentPrivateSources = current.getPrivateSources();
        List<String> newPrivateSources = newVariantAnnotationMetadata.getPrivateSources();
        if (currentPrivateSources == null) {
            currentPrivateSources = Collections.emptyList();
        }
        if (newPrivateSources == null) {
            newPrivateSources = Collections.emptyList();
        }
        if (!firstAnnotation && !new HashSet<>(currentPrivateSources).equals(new HashSet<>(newPrivateSources))) {
            String msg = "Private sources has changed. "
                    + "Existing annotation calculated with private sources " + currentPrivateSources
                    + ", attempting to annotate with " + newPrivateSources;

            if (overwrite) {
                logger.info(msg);
                bumpReasons.add("privateSources changed: " + currentPrivateSources + " -> " + newPrivateSources);
            } else {
                throw new VariantAnnotatorException(msg);
            }
        }

        if (newVariantAnnotationMetadata.getDataRelease() != null) {
            if (current.getDataRelease() == null) {
                // Missing current dataRelease. Continue.
            } else {
                if (!dataReleaseEquals(current.getDataRelease(), newVariantAnnotationMetadata.getDataRelease())) {
                    String msg = "DataRelease has changed. "
                            + "Existing annotation calculated with dataRelease " + current.getDataRelease().getRelease()
                            + ", attempting to annotate with " + newVariantAnnotationMetadata.getDataRelease().getRelease();

                    if (overwrite) {
                        logger.info(msg);
                        bumpReasons.add("dataRelease changed: " + current.getDataRelease().getRelease()
                                + " -> " + newVariantAnnotationMetadata.getDataRelease().getRelease());
                    } else {
                        throw new VariantAnnotatorException(msg);
                    }
                }
            }
        } else {
            // Check sources for old cellbase versions
            List<ObjectMap> currentSourceVersion = current.getSourceVersion();
            List<ObjectMap> newSourceVersion = newVariantAnnotationMetadata.getSourceVersion();

            if (newSourceVersion.isEmpty()) {
                throw new IllegalArgumentException("Missing annotator source version!");
            }

            if (CollectionUtils.isNotEmpty(currentSourceVersion) && !sameSourceVersion(newSourceVersion, currentSourceVersion)) {
                String msg = "Source version of the annotator has changed. "
                        + "Existing annotation calculated with "
                        + currentSourceVersion.stream().map(ObjectMap::toJson).collect(Collectors.joining(" , ", "[ ", " ]"))
                        + ", attempting to annotate with "
                        + newSourceVersion.stream().map(ObjectMap::toJson).collect(Collectors.joining(" , ", "[ ", " ]"));

                if (overwrite) {
                    logger.info(msg);
                    bumpReasons.add("annotator sourceVersion changed");
                } else {
                    // List of sources from cellbase 5.0.x is not reliable, and should
                    // not be taken into account to force a full annotation overwrite
                    if (newAnnotator.getName().toLowerCase().contains("cellbase") && newAnnotator.getVersion().startsWith("5.0")) {
                        logger.warn(msg);
                        logger.info("Ignore source version change at Cellbase v5.0.x");
                    } else {
                        throw new VariantAnnotatorException(msg);
                    }
                }
            }
        }

        // Check extensions
        Map<String, ObjectMap> currentExtensions = current.getExtensions();
        Map<String, ObjectMap> newExtensions = newVariantAnnotationMetadata.getExtensions();
        if (currentExtensions == null) {
            currentExtensions = Collections.emptyMap();
        }
        if (newExtensions == null) {
            newExtensions = Collections.emptyMap();
        }
        if (!currentExtensions.equals(newExtensions)) {
            String msg = "Annotator extensions has changed. "
                    + "Existing annotation calculated with extensions " + currentExtensions
                    + ", attempting to annotate with " + newExtensions;

            if (overwrite) {
                logger.info(msg);
                bumpReasons.add("annotator extensions changed");
            } else {
                throw new VariantAnnotatorException(msg);
            }
        }

        if (!bumpReasons.isEmpty()) {
            // Bump BEFORE the annotation pass so freshly written rows are stamped with the new id.
            // Idempotent within a single annotate() call (preflight + post-load both call here).
            bumpAnnotationSetId(projectMetadata, current, String.join("; ", bumpReasons));
        }

        return current;
    }

    /**
     * Check if two data releases are equal.
     *
     * Fields to compare:
     * - release
     * - date
     * - collections
     * - sources
     *
     * Ignored fields:
     * - active
     * - activeByDefaultIn
     *
     * @param current Current data release
     * @param other Other data release
     * @return true if both data release are equal
     */
    public static boolean dataReleaseEquals(DataRelease current, DataRelease other) {
        return current.getRelease() == other.getRelease()
                && Objects.equals(current.getDate(), other.getDate())
                && Objects.equals(current.getCollections(), other.getCollections())
                && Objects.equals(current.getSources(), other.getSources());
    }

    private static String removePatchFromVersion(String version) {
        String[] split = StringUtils.split(version, '.');
        if (split.length <= 1) {
            return version;
        }
        return split[0] + "." + split[1];
    }

    private boolean sameSourceVersion(List<ObjectMap> newSourceVersion, List<ObjectMap> currentSourceVersion) {
        if (currentSourceVersion.size() != newSourceVersion.size()) {
            return false;
        }
        Set<ObjectMap> newSourceVersionSet = new HashSet<>(newSourceVersion);
        return newSourceVersionSet.containsAll(currentSourceVersion);
    }

    protected final void updateCurrentAnnotation(VariantAnnotator annotator, ProjectMetadata projectMetadata,
                                                 boolean overwrite, boolean forceNewAnnotationSet,
                                                 VariantAnnotationMetadata newAnnotationMetadata)
            throws VariantAnnotatorException {
        List<ObjectMap> newSourceVersion = newAnnotationMetadata.getSourceVersion();
        VariantAnnotatorProgram newAnnotator = newAnnotationMetadata.getAnnotator();
        if (newSourceVersion == null) {
            newSourceVersion = Collections.emptyList();
        }
        if (newAnnotator == null) {
            throw new IllegalArgumentException("Missing annotator information for VariantAnnotator: " + annotator.getClass());
        }

        checkCurrentAnnotation(projectMetadata, overwrite, forceNewAnnotationSet, newAnnotationMetadata);

        VariantAnnotationMetadata current = projectMetadata.getAnnotation().getCurrent();
        current.setAnnotator(newAnnotator);
        current.setSourceVersion(newSourceVersion);
        current.setDataRelease(newAnnotationMetadata.getDataRelease());
        current.setPrivateSources(newAnnotationMetadata.getPrivateSources());
        current.setExtensions(newAnnotationMetadata.getExtensions());
    }

    /**
     * Record the outgoing {@link VariantAnnotationMetadata} into {@code transitions} and bump
     * {@code current.id}. The caller is responsible for verifying that something has actually
     * changed; the {@code reason} argument records why for human auditing.
     *
     * <p>The bumped id (the project-wide annotationSetId) lets per-sample SSI metadata, the
     * pending-annotation discovery and query-time SSI reads detect annotation drift after a
     * {@code variant-annotation-index --overwrite} run.
     *
     * <p>Entries land in {@code transitions} — audit-only records that no variant data was
     * preserved for. An explicit {@code saveAnnotation(snapshotId)} later promotes the entry to
     * {@code saved} and copies matching variants into the per-id snapshot collection.
     *
     * <p>This must run BEFORE the annotation pass so newly written variant rows are stamped with
     * the bumped id. {@link #checkCurrentAnnotation} can be called multiple times in a single
     * annotate() call (preflight + post-load); idempotency is preserved by detecting an
     * already-recorded auto-snapshot at the tail of {@code transitions} whose id is exactly
     * {@code current.id - 1} and whose annotator matches the outgoing one.
     *
     * @param projectMetadata Mutable project metadata. The {@code transitions} list and
     *                        {@code current.id} are modified in place.
     * @param current         The current annotation metadata (the snapshot of state before the bump).
     * @param reason          Human-readable description of why the bump happened. Stored on the
     *                        snapshot's {@link VariantAnnotationMetadata#getDescription()} for
     *                        auditability.
     */
    private VariantAnnotationMetadata bumpAnnotationSetId(ProjectMetadata projectMetadata, VariantAnnotationMetadata current,
                                                           String reason) {
        VariantAnnotatorProgram currentAnnotator = current.getAnnotator();
        // Transition entries are audit-only — they record that the project bumped past this id.
        // Variant data preservation is opt-in via saveAnnotation, which moves the matching variants
        // into a per-id snapshot collection on demand.
        VariantAnnotationMetadata last = projectMetadata.getAnnotation().getLastTransitionOrNull();
        if (last != null
                && last.getId() == current.getId() - 1
                && last.getName() != null
                && last.getName().startsWith(AUTO_TRANSITION_PREFIX)
                && Objects.equals(currentAnnotator, last.getAnnotator())) {
            // This transition has already been recorded (e.g. by an earlier preflight call within
            // the same annotate() run). Return the existing entry so callers that chain a
            // promote step (saveAnnotation) can still find it.
            return last;
        }
        VariantAnnotationMetadata transition = new VariantAnnotationMetadata(current);
        transition.setName(AUTO_TRANSITION_PREFIX + transition.getId() + "_" + System.currentTimeMillis());
        transition.setDescription(reason);
        projectMetadata.getAnnotation().getTransitions().add(transition);
        current.setId(current.getId() + 1);
        logger.info("Bumped annotationSetId to {} (previous {} recorded as transition '{}'): {}",
                current.getId(), transition.getId(), transition.getName(), reason);
        return transition;
    }

    /**
     * Move a transition entry from {@link ProjectMetadata.VariantAnnotationSets#getTransitions()}
     * into {@link ProjectMetadata.VariantAnnotationSets#getSaved()}, renaming it from its auto
     * name to {@code targetName}. The matching variant data is NOT copied here — the backend
     * saveAnnotation flow is responsible for that, using the returned entry's id to filter the copy.
     *
     * @param sourceTransitionName the auto-name of the transition to promote
     * @param targetName           the operator-given name to record in {@code saved}
     * @param projectMetadata      mutable project metadata
     * @return the promoted entry (now in {@code saved}, with the new name and original id)
     * @throws VariantAnnotatorException if no transition matches {@code sourceTransitionName} or
     *                                   if {@code targetName} collides with an existing saved entry
     */
    protected final VariantAnnotationMetadata promoteTransitionToSaved(String sourceTransitionName, String targetName,
                                                                       ProjectMetadata projectMetadata)
            throws VariantAnnotatorException {
        rejectIfSavedNameTaken(targetName, projectMetadata);
        VariantAnnotationMetadata t = projectMetadata.getAnnotation().removeTransition(sourceTransitionName);
        if (t == null) {
            throw new VariantAnnotatorException("Transition '" + sourceTransitionName
                    + "' not found in project annotation transitions");
        }
        t.setName(targetName);
        projectMetadata.getAnnotation().getSaved().add(t);
        return t;
    }

    private void rejectIfSavedNameTaken(String name, ProjectMetadata projectMetadata) throws VariantAnnotatorException {
        // CURRENT is a reserved magic value (the active generation, not a stored name) — compared
        // case-insensitively because user-facing CLI/REST may normalise casing. Saved-name
        // uniqueness, by contrast, is case-sensitive to stay consistent with getSaved(String).
        if (VariantAnnotationManager.CURRENT.equalsIgnoreCase(name)
                || projectMetadata.getAnnotation().getSavedOrNull(name) != null) {
            throw new VariantAnnotatorException("Annotation snapshot name '" + name + "' already exists!");
        }
    }

    protected final VariantAnnotationMetadata registerNewAnnotationSnapshot(String name, VariantAnnotator annotator,
                                                                            ProjectMetadata projectMetadata)
            throws VariantAnnotatorException {
        VariantAnnotationMetadata current = projectMetadata.getAnnotation().getCurrent();
        if (current == null) {
            // Should never enter here
            current = checkCurrentAnnotation(annotator, projectMetadata, true);
        }
        rejectIfSavedNameTaken(name, projectMetadata);

        // Routed through the transition mechanism: record current state as an auto-named transition
        // (this also bumps current.id), then immediately promote that transition to saved with the
        // operator-given name. External behavior unchanged from earlier — the entry lands in saved
        // and current.id is incremented by one — but the bump+promote primitives are now the single
        // shared path used by autobump-on-config-change and explicit save-from-transition as well.
        VariantAnnotationMetadata transition = bumpAnnotationSetId(projectMetadata, current,
                "explicit saveAnnotation requested ('" + name + "')");
        return promoteTransitionToSaved(transition.getName(), name, projectMetadata);
    }

    protected final VariantAnnotationMetadata removeAnnotationSnapshot(String name, ProjectMetadata projectMetadata)
            throws VariantAnnotatorException {

        if (VariantAnnotationManager.CURRENT.equalsIgnoreCase(name)) {
            throw new VariantAnnotatorException("Can not delete " + VariantAnnotationManager.CURRENT + " annotation");
        }

        VariantAnnotationMetadata annotation = projectMetadata.getAnnotation().removeSaved(name);
        if (annotation != null) {
            return annotation;
        } else {
            throw new VariantAnnotatorException("Variant Annotation snapshot \"" + name + "\" not found!");
        }
    }
}
