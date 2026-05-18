package org.opencb.opencga.storage.core.variant.annotation;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.EvidenceEntry;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.cellbase.core.models.DataRelease;
import org.opencb.cellbase.core.models.DataReleaseSource;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.operations.variant.VariantAnnotationExtensionConfigureParams;
import org.opencb.opencga.storage.core.StorageEngineTest;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotatorFactory;
import org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.CosmicVariantAnnotatorExtensionTaskTest;
import org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.cosmic.CosmicVariantAnnotatorExtensionTask;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantAnnotator;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.GROUP_NAME;
import static org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.CosmicVariantAnnotatorExtensionTaskTest.COSMIC_ASSEMBLY;
import static org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.CosmicVariantAnnotatorExtensionTaskTest.COSMIC_VERSION;

/**
 * Created on 24/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@StorageEngineTest
public abstract class VariantAnnotationManagerTest extends VariantStorageBaseTest {

    public static final String ASSEMBLY_38 = "GRCh38";
    public static final String ASSEMBLY_37 = "GRCh37";

    private URI annotatorExtensionInputUri;

    @Before
    public void setUp() throws Exception {
        annotatorExtensionInputUri = getResourceUri(ANNOTATOR_EXTENSION_VCF_TEST_FILE_NAME);
    }

    /**
     * The saved and transitions buckets must hold disjoint sets of names AND ids — a saved entry
     * carries preserved variant data, a transition is audit-only; the same generation cannot
     * legitimately appear in both at once. promoteTransitionToSaved moves entries between them
     * and must remove from the source bucket.
     */
    private static void assertSavedAndTransitionsDisjoint(ProjectMetadata.VariantAnnotationSets sets) {
        Set<String> savedNames = sets.getSaved().stream()
                .map(ProjectMetadata.VariantAnnotationMetadata::getName).collect(java.util.stream.Collectors.toSet());
        Set<Integer> savedIds = sets.getSaved().stream()
                .map(ProjectMetadata.VariantAnnotationMetadata::getId).collect(java.util.stream.Collectors.toSet());
        for (ProjectMetadata.VariantAnnotationMetadata t : sets.getTransitions()) {
            assertFalse("Transition name '" + t.getName() + "' must not also exist in saved",
                    savedNames.contains(t.getName()));
            assertFalse("Transition id " + t.getId() + " must not also exist in saved",
                    savedIds.contains(t.getId()));
        }
    }

    @Test
    public void testChangeAnnotator() throws Exception {
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        runETL(variantStorageEngine, smallInputUri, STUDY_NAME,
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false));

        variantStorageEngine.getOptions()
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), DummyVariantAnnotator.class.getName())
                .append(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER);

        // First annotation. Should run ok.
        variantStorageEngine.annotate(outputUri, new ObjectMap(DummyVariantAnnotator.ANNOT_VERSION, "v1"));
        assertEquals("v1", variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getAnnotator().getVersion());

        // Second annotation. New annotator. Overwrite. Should run ok.
        variantStorageEngine.annotate(outputUri, new ObjectMap(DummyVariantAnnotator.ANNOT_VERSION, "v2").append(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));
        assertEquals("v2", variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getAnnotator().getVersion());

        // Third annotation. New annotator. Do not overwrite. Should fail.
        thrown.expect(VariantAnnotatorException.class);
        thrown.expectMessage("Using a different annotator!");
        variantStorageEngine.annotate(outputUri, new ObjectMap(DummyVariantAnnotator.ANNOT_VERSION, "v3").append(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), false));
    }

    @Test
    public void testAnnotationSetIdBumpsOnAnnotatorChange() throws Exception {
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        runETL(variantStorageEngine, smallInputUri, STUDY_NAME,
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false));

        variantStorageEngine.getOptions()
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), DummyVariantAnnotator.class.getName())
                .append(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER);

        // First annotation. annotationSetId starts at 1.
        variantStorageEngine.annotate(outputUri, new ObjectMap(DummyVariantAnnotator.ANNOT_VERSION, "v1"));
        ProjectMetadata.VariantAnnotationSets sets =
                variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation();
        assertEquals(1, sets.getCurrent().getId());
        int savedBefore = sets.getSaved().size();
        int transitionsBefore = sets.getTransitions().size();

        // After the first annotation pass, every indexed sample/file must be stamped with id=1.
        int studyIdAfterFirst = variantStorageEngine.getMetadataManager().getStudyId(STUDY_NAME);
        for (Integer sampleId : variantStorageEngine.getMetadataManager().getIndexedSamples(studyIdAfterFirst)) {
            assertEquals("Sample annotationSetId must be stamped after a full annotate pass",
                    1, variantStorageEngine.getMetadataManager().getSampleMetadata(studyIdAfterFirst, sampleId)
                            .getAnnotationSetId());
        }
        variantStorageEngine.getMetadataManager().fileMetadataIterator(studyIdAfterFirst).forEachRemaining(fm -> {
            if (fm.isIndexed()) {
                assertEquals("File annotationSetId must be stamped after a full annotate pass",
                        1, fm.getAnnotationSetId());
            }
        });

        // Different annotator with overwrite. annotationSetId must bump, previous metadata is recorded
        // as an audit-only transition (no data preserved unless saveAnnotation is later invoked).
        variantStorageEngine.annotate(outputUri, new ObjectMap(DummyVariantAnnotator.ANNOT_VERSION, "v2")
                .append(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));
        sets = variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation();
        assertEquals(2, sets.getCurrent().getId());
        assertEquals("v2", sets.getCurrent().getAnnotator().getVersion());
        assertEquals("Implicit bump must not promote anything into saved",
                savedBefore, sets.getSaved().size());
        assertEquals("Implicit bump must record an audit-only transition",
                transitionsBefore + 1, sets.getTransitions().size());
        ProjectMetadata.VariantAnnotationMetadata transition = sets.getTransitions().get(sets.getTransitions().size() - 1);
        assertEquals(1, transition.getId());
        assertEquals("v1", transition.getAnnotator().getVersion());
        assertNotNull("Auto-snapshot must record a bump reason in description", transition.getDescription());
        assertThat(transition.getDescription(), containsString("annotator changed"));

        // After the second pass under overwrite, every indexed sample/file (newly-annotated AND
        // already-annotated re-stamped via alreadyAnnotatedFiles/alreadyAnnotatedSamples) must be at id=2.
        int studyId = variantStorageEngine.getMetadataManager().getStudyId(STUDY_NAME);
        for (Integer sampleId : variantStorageEngine.getMetadataManager().getIndexedSamples(studyId)) {
            assertEquals("Sample annotationSetId must advance to 2 after overwrite with annotator change",
                    2, variantStorageEngine.getMetadataManager().getSampleMetadata(studyId, sampleId)
                            .getAnnotationSetId());
        }
        variantStorageEngine.getMetadataManager().fileMetadataIterator(studyId).forEachRemaining(fm -> {
            if (fm.isIndexed()) {
                assertEquals("File annotationSetId must advance to 2 after overwrite with annotator change",
                        2, fm.getAnnotationSetId());
            }
        });

        // Same annotator again with overwrite. annotationSetId must NOT bump.
        variantStorageEngine.annotate(outputUri, new ObjectMap(DummyVariantAnnotator.ANNOT_VERSION, "v2")
                .append(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));
        sets = variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation();
        assertEquals("Same annotator must not bump annotationSetId",
                2, sets.getCurrent().getId());
        assertEquals(savedBefore, sets.getSaved().size());
        assertEquals(transitionsBefore + 1, sets.getTransitions().size());
        assertSavedAndTransitionsDisjoint(sets);
    }

    @Test
    public void testAnnotationSetIdBumpsOnForceNewAnnotationSet() throws Exception {
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        runETL(variantStorageEngine, smallInputUri, STUDY_NAME,
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false));

        variantStorageEngine.getOptions()
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), DummyVariantAnnotator.class.getName())
                .append(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER);

        // First annotation
        variantStorageEngine.annotate(outputUri, new ObjectMap(DummyVariantAnnotator.ANNOT_VERSION, "v1"));
        ProjectMetadata.VariantAnnotationSets sets =
                variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation();
        assertEquals(1, sets.getCurrent().getId());
        int savedBefore = sets.getSaved().size();
        int transitionsBefore = sets.getTransitions().size();

        // Same annotator + overwrite + forceNewAnnotationSet -> bumps even though nothing else changed
        variantStorageEngine.annotate(outputUri, new ObjectMap(DummyVariantAnnotator.ANNOT_VERSION, "v1")
                .append(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true)
                .append(VariantStorageOptions.ANNOTATION_FORCE_NEW_ANNOTATION_SET.key(), true));
        sets = variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation();
        assertEquals(2, sets.getCurrent().getId());
        assertEquals("Implicit bump must not promote anything into saved",
                savedBefore, sets.getSaved().size());
        assertEquals("Implicit bump must record an audit-only transition",
                transitionsBefore + 1, sets.getTransitions().size());
        ProjectMetadata.VariantAnnotationMetadata transition = sets.getTransitions().get(sets.getTransitions().size() - 1);
        assertNotNull(transition.getDescription());
        assertThat(transition.getDescription(), containsString("forceNewAnnotationSet"));
        assertSavedAndTransitionsDisjoint(sets);
    }

    @Test
    public void testSaveAnnotationIsIdempotentOnNameReuse() throws Exception {
        // Recovery scenario: a previous saveAnnotation call landed the metadata mutation (the
        // entry is in saved) but the data copy failed (MR crash, JVM death, network blip during
        // the Mongo aggregation). Re-running with the same name must NOT throw and must NOT
        // re-bump current.id — it should be a no-op on the metadata side, so the backend can just
        // re-run the data copy.
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        runETL(variantStorageEngine, smallInputUri, STUDY_NAME,
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false));

        variantStorageEngine.getOptions()
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), DummyVariantAnnotator.class.getName())
                .append(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER);

        variantStorageEngine.annotate(outputUri, new ObjectMap(DummyVariantAnnotator.ANNOT_VERSION, "v1"));

        // First save under name "snap1". bumps current.id (1 -> 2) and lands the OLD state as
        // saved["snap1"].
        variantStorageEngine.saveAnnotation("snap1", new ObjectMap());
        ProjectMetadata.VariantAnnotationSets sets =
                variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation();
        int currentIdAfterFirstSave = sets.getCurrent().getId();
        int savedCountAfterFirstSave = sets.getSaved().size();
        int transitionsCountAfterFirstSave = sets.getTransitions().size();
        ProjectMetadata.VariantAnnotationMetadata snap1 = sets.getSaved().stream()
                .filter(s -> s.getName().equals("snap1")).findFirst().orElseThrow(AssertionError::new);

        // Retry: same name, same options. Must succeed (no throw) and must not mutate metadata.
        variantStorageEngine.saveAnnotation("snap1", new ObjectMap());
        sets = variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation();
        assertEquals("Retry must not bump current.id",
                currentIdAfterFirstSave, sets.getCurrent().getId());
        assertEquals("Retry must not add a second saved entry",
                savedCountAfterFirstSave, sets.getSaved().size());
        assertEquals("Retry must not append another transition",
                transitionsCountAfterFirstSave, sets.getTransitions().size());
        ProjectMetadata.VariantAnnotationMetadata snap1Retry = sets.getSaved().stream()
                .filter(s -> s.getName().equals("snap1")).findFirst().orElseThrow(AssertionError::new);
        assertEquals("Retry must preserve the original saved id",
                snap1.getId(), snap1Retry.getId());
    }

    @Test
    public void testUpdateCellbaseConfigurationRollsBackOnProbeFailure() throws Exception {
        // Probe-before-commit invariant: if the candidate CellBase server is unreachable (probe
        // throws), the engine's cellbase config must remain on the OLD value and project
        // metadata must not be mutated. Without this guarantee the engine could end up pointing
        // at a server it can't reach, with current.annotator still reflecting the old server.
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        runETL(variantStorageEngine, smallInputUri, STUDY_NAME,
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false));

        variantStorageEngine.getOptions()
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), DummyVariantAnnotator.class.getName())
                .append(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER);

        // First annotation bootstraps current.annotator.
        variantStorageEngine.annotate(outputUri, new ObjectMap(DummyVariantAnnotator.ANNOT_VERSION, "v1"));
        ProjectMetadata.VariantAnnotationSets setsBefore =
                variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation();
        int currentIdBefore = setsBefore.getCurrent().getId();
        int savedBefore = setsBefore.getSaved().size();
        int transitionsBefore = setsBefore.getTransitions().size();
        org.opencb.opencga.core.config.storage.CellBaseConfiguration cellbaseBefore =
                variantStorageEngine.getConfiguration().getCellbase();

        // Arm the probe to throw: the dummy annotator now fails inside getVariantAnnotationMetadata.
        variantStorageEngine.getOptions().append(DummyVariantAnnotator.FAIL_METADATA, true);

        org.opencb.opencga.core.config.storage.CellBaseConfiguration newConfig =
                new org.opencb.opencga.core.config.storage.CellBaseConfiguration(
                        "http://updated.example.test/cellbase",
                        cellbaseBefore.getVersion(),
                        cellbaseBefore.getDataRelease(),
                        cellbaseBefore.getApiKey());

        try {
            variantStorageEngine.updateCellbaseConfiguration(newConfig);
            fail("Expected probe failure to throw VariantAnnotatorException");
        } catch (VariantAnnotatorException expected) {
            // expected
        } finally {
            variantStorageEngine.getOptions().remove(DummyVariantAnnotator.FAIL_METADATA);
        }

        // The engine's cellbase config must be restored to the previous value.
        assertSame("Probe failure must restore the engine cellbase config reference",
                cellbaseBefore, variantStorageEngine.getConfiguration().getCellbase());
        // Project metadata must not have been mutated.
        ProjectMetadata.VariantAnnotationSets setsAfter =
                variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation();
        assertEquals("current.id must not change on probe failure",
                currentIdBefore, setsAfter.getCurrent().getId());
        assertEquals("saved bucket must not change on probe failure",
                savedBefore, setsAfter.getSaved().size());
        assertEquals("transitions bucket must not change on probe failure",
                transitionsBefore, setsAfter.getTransitions().size());
    }

    @Test
    public void testSaveAnnotationFromMissingTransitionThrows() throws Exception {
        // Explicit fromAnnotationSet path: if the named transition doesn't exist AND no entry
        // with the target name exists in saved, saveAnnotation must fail loudly rather than
        // silently falling back to bump-current.
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        runETL(variantStorageEngine, smallInputUri, STUDY_NAME,
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false));

        variantStorageEngine.getOptions()
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), DummyVariantAnnotator.class.getName())
                .append(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER);

        variantStorageEngine.annotate(outputUri, new ObjectMap(DummyVariantAnnotator.ANNOT_VERSION, "v1"));

        ObjectMap saveOptions = new ObjectMap(
                org.opencb.opencga.core.models.operations.variant.VariantAnnotationSaveParams.FROM_ANNOTATION_SET,
                "transition_does_not_exist_999");
        try {
            variantStorageEngine.saveAnnotation("snapX", saveOptions);
            fail("Expected VariantAnnotatorException for missing transition");
        } catch (VariantAnnotatorException expected) {
            assertThat(expected.getMessage(), containsString("transition_does_not_exist_999"));
            assertThat(expected.getMessage(), containsString("not found"));
        }
    }

    @Test
    public void testSaveAnnotationFromExistingTransitionPromotesWithoutBump() throws Exception {
        // The fromAnnotationSet path on a real transition must MOVE the transition into saved
        // under the requested name WITHOUT bumping current.id again. Verifies the explicit
        // promote-from-transition flow used by setCellbaseConfiguration with annotationSaveId.
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        runETL(variantStorageEngine, smallInputUri, STUDY_NAME,
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false));

        variantStorageEngine.getOptions()
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), DummyVariantAnnotator.class.getName())
                .append(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER);

        variantStorageEngine.annotate(outputUri, new ObjectMap(DummyVariantAnnotator.ANNOT_VERSION, "v1"));
        // Force a bump so a transition exists.
        variantStorageEngine.annotate(outputUri, new ObjectMap(DummyVariantAnnotator.ANNOT_VERSION, "v2")
                .append(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));

        ProjectMetadata.VariantAnnotationSets setsBefore =
                variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation();
        assertFalse("Pre-condition: a transition must exist", setsBefore.getTransitions().isEmpty());
        ProjectMetadata.VariantAnnotationMetadata transition = setsBefore.getTransitions().get(setsBefore.getTransitions().size() - 1);
        int transitionId = transition.getId();
        String transitionName = transition.getName();
        int savedBefore = setsBefore.getSaved().size();
        int transitionsBefore = setsBefore.getTransitions().size();
        int currentIdBefore = setsBefore.getCurrent().getId();

        ObjectMap saveOptions = new ObjectMap(
                org.opencb.opencga.core.models.operations.variant.VariantAnnotationSaveParams.FROM_ANNOTATION_SET,
                transitionName);
        variantStorageEngine.saveAnnotation("preserved_v1", saveOptions);

        ProjectMetadata.VariantAnnotationSets setsAfter =
                variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation();
        assertEquals("Promote from existing transition must NOT bump current.id",
                currentIdBefore, setsAfter.getCurrent().getId());
        assertEquals("Transition count must drop by one (moved into saved)",
                transitionsBefore - 1, setsAfter.getTransitions().size());
        assertEquals("Saved count must grow by one",
                savedBefore + 1, setsAfter.getSaved().size());
        ProjectMetadata.VariantAnnotationMetadata promoted = setsAfter.getSaved().stream()
                .filter(s -> s.getName().equals("preserved_v1")).findFirst().orElseThrow(AssertionError::new);
        assertEquals("Promoted entry must preserve the original transition id",
                transitionId, promoted.getId());
        assertNull("The original transition name must no longer be in transitions",
                setsAfter.getTransitionOrNull(transitionName));
        assertSavedAndTransitionsDisjoint(setsAfter);
    }

    /**
     * Parser-level guard for the staleness-aware {@code ANNOTATION_EXISTS} predicate.
     *
     * <p>After a project annotationSetId bump that is NOT followed by a re-annotation pass, every
     * already-annotated variant is structurally stale: its stored {@code annotation.id} is older
     * than the project's current id. The query parser must report:
     * <ul>
     *   <li>{@code ANNOTATION_EXISTS=true} → variants whose annotation is *fresh* (id == current). </li>
     *   <li>{@code ANNOTATION_EXISTS=false} → variants that are *missing OR stale*. </li>
     * </ul>
     *
     * <p>Pre-fix (TASK-8120 first cut) the predicate only checked existence of the {@code annotation.id}
     * field, ignoring its value, so a bumped-but-not-re-annotated project would report ALL variants
     * as fresh — masking the staleness and skipping re-derivation. This test catches that regression
     * by simulating a bump and asserting the partition flips.
     */
    @Test
    public void testAnnotationExistsRespectsAnnotationSetIdStaleness() throws Exception {
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        runETL(variantStorageEngine, smallInputUri, STUDY_NAME,
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false));

        variantStorageEngine.getOptions()
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), DummyVariantAnnotator.class.getName())
                .append(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER);

        variantStorageEngine.annotate(outputUri, new ObjectMap(DummyVariantAnnotator.ANNOT_VERSION, "v1"));
        long total = variantStorageEngine.count(new Query()).first();
        assertTrue("Fixture should produce at least one variant", total > 0);

        // Sanity: post-load all variants are stamped with the project's current id (== 1).
        long annotated = variantStorageEngine.count(
                new Query(VariantQueryParam.ANNOTATION_EXISTS.key(), true)).first();
        long pending = variantStorageEngine.count(
                new Query(VariantQueryParam.ANNOTATION_EXISTS.key(), false)).first();
        assertEquals("Right after first annotation every variant must be fresh", total, annotated);
        assertEquals("Right after first annotation no variant should be missing/stale", 0L, pending);

        // Bump the project annotationSetId WITHOUT triggering a re-annotation pass. Every existing
        // variant now has stored annotation.id=1 < project current=2, i.e. structurally stale.
        variantStorageEngine.getMetadataManager().updateProjectMetadata(pm -> {
            pm.getAnnotation().getCurrent().setId(2);
            return pm;
        });

        annotated = variantStorageEngine.count(
                new Query(VariantQueryParam.ANNOTATION_EXISTS.key(), true)).first();
        pending = variantStorageEngine.count(
                new Query(VariantQueryParam.ANNOTATION_EXISTS.key(), false)).first();
        assertEquals("After bump, no variant has annotation.id == project current — all are stale",
                0L, annotated);
        assertEquals("After bump, every variant must show up as missing-or-stale",
                total, pending);
    }

    /**
     * End-to-end coverage of the {@code --force-new-annotation-set} loop. Reproduces the gap that
     * slipped past the original TASK-8120 suite: the bump propagated through project/file/sample
     * stamps, but the discovery query did not flag already-annotated variants as needing re-derivation,
     * so every variant kept its old {@code annotation.id} silently. This asserts the full cycle —
     * project bumps, every variant gets re-annotated, partition is fully fresh again.
     */
    @Test
    public void testForceNewAnnotationSetReannotatesExistingVariants() throws Exception {
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        runETL(variantStorageEngine, smallInputUri, STUDY_NAME,
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false));

        variantStorageEngine.getOptions()
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), DummyVariantAnnotator.class.getName())
                .append(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER);

        variantStorageEngine.annotate(outputUri, new ObjectMap(DummyVariantAnnotator.ANNOT_VERSION, "v1"));
        long total = variantStorageEngine.count(new Query()).first();
        assertEquals(1, variantStorageEngine.getMetadataManager().getProjectMetadata()
                .getAnnotation().getCurrent().getId());
        long annotatedAfterFirst = variantStorageEngine.count(
                new Query(VariantQueryParam.ANNOTATION_EXISTS.key(), true)).first();
        assertEquals("Pre-condition: every variant fresh against id=1", total, annotatedAfterFirst);

        // Same annotator + overwrite + forceNewAnnotationSet — the discovery loop must pick up every
        // already-annotated variant as stale (annotation.id=1 < new project current=2) and re-derive.
        variantStorageEngine.annotate(outputUri, new ObjectMap(DummyVariantAnnotator.ANNOT_VERSION, "v1")
                .append(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true)
                .append(VariantStorageOptions.ANNOTATION_FORCE_NEW_ANNOTATION_SET.key(), true));

        assertEquals("forceNewAnnotationSet must bump project id",
                2, variantStorageEngine.getMetadataManager().getProjectMetadata()
                        .getAnnotation().getCurrent().getId());
        long annotatedAfterBump = variantStorageEngine.count(
                new Query(VariantQueryParam.ANNOTATION_EXISTS.key(), true)).first();
        long pendingAfterBump = variantStorageEngine.count(
                new Query(VariantQueryParam.ANNOTATION_EXISTS.key(), false)).first();
        assertEquals("After force-new-annotation-set + overwrite, every variant must be re-annotated "
                        + "to the new id (i.e. fresh under the staleness-aware predicate)",
                total, annotatedAfterBump);
        assertEquals("After force-new-annotation-set + overwrite, no variant should remain stale or missing",
                0L, pendingAfterBump);
    }

    @Test
    public void testChangeAnnotatorFail() throws Exception {
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        runDefaultETL(smallInputUri, variantStorageEngine, newStudyMetadata(),
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false));

        variantStorageEngine.getOptions()
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), DummyVariantAnnotator.class.getName())
                .append(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER);

        // First annotation. Should run ok.
        variantStorageEngine.annotate(outputUri, new ObjectMap(DummyVariantAnnotator.ANNOT_KEY, "v1"));

        try {
            // Second annotation. New annotator. Overwrite. Fail annotation
            variantStorageEngine.annotate(outputUri, new ObjectMap(DummyVariantAnnotator.ANNOT_KEY, "v2")
                    .append(DummyVariantAnnotator.FAIL, true)
                    .append(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));
            fail("Expected to fail!");
        } catch (VariantAnnotatorException e) {
            e.printStackTrace();
            // Annotator information does not change
            assertEquals("v1", variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getAnnotator().getVersion());
        }


        // Second annotation bis. New annotator. Overwrite.
        variantStorageEngine.annotate(outputUri, new ObjectMap(DummyVariantAnnotator.ANNOT_VERSION, "v2")
                .append(DummyVariantAnnotator.FAIL, false)
                .append(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));
        assertEquals("v2", variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getAnnotator().getVersion());
    }

    @Test
    public void testApiKey() throws Exception {
        String cosmicApiKey = System.getenv("CELLBASE_COSMIC_APIKEY");
        String hgmdApiKey = System.getenv("CELLBASE_HGMD_APIKEY");
        Assume.assumeTrue(StringUtils.isNotEmpty(cosmicApiKey));
        Assume.assumeTrue(StringUtils.isNotEmpty(hgmdApiKey));

        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        variantStorageEngine.getConfiguration().getCellbase().setUrl(ParamConstants.CELLBASE_URL);
        variantStorageEngine.getConfiguration().getCellbase().setVersion("v5.4");
        variantStorageEngine.getConfiguration().getCellbase().setDataRelease("3");
        variantStorageEngine.getConfiguration().getCellbase().setApiKey(cosmicApiKey);
        variantStorageEngine.getOptions().put(VariantStorageOptions.ASSEMBLY.key(), ASSEMBLY_38);
        variantStorageEngine.reloadCellbaseConfiguration();
        variantStorageEngine.getCellBaseUtils().validate();

        runDefaultETL(smallInputUri, variantStorageEngine, newStudyMetadata(),
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false));

        variantStorageEngine.annotate(outputUri, new ObjectMap());

        variantStorageEngine.getConfiguration().getCellbase().setApiKey(hgmdApiKey);
        variantStorageEngine.reloadCellbaseConfiguration();
        variantStorageEngine.getCellBaseUtils().validate();

        try {
            variantStorageEngine.annotate(outputUri, new ObjectMap());
            fail("Expected to fail!");
        } catch (VariantAnnotatorException e) {
            assertTrue(e.getMessage().contains("Existing annotation calculated with private sources [cosmic], attempting to annotate with [hgmd]"));
        }
        variantStorageEngine.annotate(outputUri, new ObjectMap(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));
    }

    @Test
    public void testChangeDataRelease() throws Exception {
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        variantStorageEngine.getOptions()
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), DummyVariantAnnotator.class.getName())
                .append(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER)
                .append(DummyVariantAnnotator.ANNOT_KEY, "k1")
                .append(DummyVariantAnnotator.ANNOT_VERSION, "v1")
                .append(DummyVariantAnnotator.ANNOT_DATARELEASE, 2);
        variantStorageEngine.getOptions().put(VariantStorageOptions.ASSEMBLY.key(), ASSEMBLY_38);
        variantStorageEngine.getOptions().put(VariantStorageOptions.SPECIES.key(), "hsapiens");

        runETL(variantStorageEngine, smallInputUri, STUDY_NAME,
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false));

        // First annotation. Should run ok.
        variantStorageEngine.annotate(outputUri, new ObjectMap());
        assertEquals(2, variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getDataRelease().getRelease());
        assertEquals("k1", variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getAnnotator().getName());
        assertEquals("v1", variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getAnnotator().getVersion());

        variantStorageEngine.getOptions().append(DummyVariantAnnotator.ANNOT_KEY, "k2");

        // New annotator. Do not overwrite. Should fail.
        try {
            variantStorageEngine.annotate(outputUri, new ObjectMap(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), false));
            fail("Should fail");
        } catch (Exception e) {
            e.printStackTrace();
        }

        // New annotator. Overwrite. Should run ok.
        variantStorageEngine.annotate(outputUri, new ObjectMap(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));
        assertEquals(2, variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getDataRelease().getRelease());
        assertEquals("k2", variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getAnnotator().getName());
        assertEquals("v1", variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getAnnotator().getVersion());


        variantStorageEngine.getOptions().append(DummyVariantAnnotator.ANNOT_DATARELEASE, 3);

        // Same annotator, new datarelease. Do not overwrite. Should fail.
        try {
            variantStorageEngine.annotate(outputUri, new ObjectMap(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), false));
            fail("Should fail");
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals("DataRelease has changed. Existing annotation calculated with dataRelease 2, attempting to annotate with 3", e.getMessage());
        }

        // Same annotator, new datarelease. Overwrite. Should run ok.
        variantStorageEngine.annotate(outputUri, new ObjectMap(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));
        assertEquals(3, variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getDataRelease().getRelease());
        assertEquals("k2", variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getAnnotator().getName());
        assertEquals("v1", variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getAnnotator().getVersion());


        // Revert annotator. Do not overwrite. Should fail.
        variantStorageEngine.getOptions()
                .append(DummyVariantAnnotator.ANNOT_KEY, "k1")
                .append(DummyVariantAnnotator.ANNOT_VERSION, "v1")
                .append(DummyVariantAnnotator.ANNOT_DATARELEASE, 2);
        try {
            variantStorageEngine.annotate(outputUri, new ObjectMap(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), false));
            fail("Should fail");
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Overwrite. Should run ok.
        variantStorageEngine.annotate(outputUri, new ObjectMap(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));
        assertEquals(2, variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getDataRelease().getRelease());
        assertEquals("k1", variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getAnnotator().getName());
        assertEquals("v1", variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getAnnotator().getVersion());


        // Change data release "active" and "activeByDefaultIn". Do not overwrite. Should run ok.
        ProjectMetadata.VariantAnnotationMetadata current = variantStorageEngine.getMetadataManager().getProjectMetadata().copy()
                .getAnnotation().getCurrent();
        current.getDataRelease().setActive(!current.getDataRelease().isActive());
        current.getDataRelease().setActiveByDefaultIn(Arrays.asList("here", "there"));
        variantStorageEngine.getOptions()
                .append(DummyVariantAnnotator.ANNOT_METADATA, current);
        variantStorageEngine.annotate(outputUri, new ObjectMap(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), false));
    }

    @Test
    public void testDataReleaseEquals() throws IOException {
        DataRelease dr1 = new DataRelease(1, "date", Arrays.asList("here", "there"), new HashMap<String, String>(){{put("a", "b");}}, Arrays.asList(new DataReleaseSource("source", "v2", "so", "2020", null)));
        DataRelease dr2 = JacksonUtils.copy(dr1);
        assertTrue(VariantAnnotationManager.dataReleaseEquals(dr1, dr2));

        dr2 = JacksonUtils.copy(dr1).setActive(!dr2.isActive());
        assertTrue(VariantAnnotationManager.dataReleaseEquals(dr1, dr2));

        dr2 = JacksonUtils.copy(dr1).setActiveByDefaultIn(Arrays.asList("h", "d"));
        assertTrue(VariantAnnotationManager.dataReleaseEquals(dr1, dr2));

        dr2 = JacksonUtils.copy(dr1).setDate("otherDate");
        assertFalse(VariantAnnotationManager.dataReleaseEquals(dr1, dr2));

        dr2 = JacksonUtils.copy(dr1);
        dr2.getSources().get(0).setName("otherName");
        assertFalse(VariantAnnotationManager.dataReleaseEquals(dr1, dr2));
    }

    @Test
    public void testDataReleaseKnownFields() {
        Map<String, Class<?>> fields = JacksonUtils.getFields(DataRelease.class);
        fields.remove("active");
        fields.remove("activeByDefaultIn");
        // This test will fail if the DataRelease object is modified.
        // Modifications in this object will require to update the method VariantAnnotationManager::dataReleaseEquals
        assertEquals(new HashSet<>(Arrays.asList("date", "sources", "collections", "release")), fields.keySet());
    }

    @Test
    public void testMultiAnnotations() throws Exception {

        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        runDefaultETL(smallInputUri, variantStorageEngine, newStudyMetadata(),
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false));

        variantStorageEngine.getOptions()
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), DummyVariantAnnotator.class.getName())
                .append(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER);

        URI annotOutdir = outputUri.resolve("annot1");
        Files.createDirectories(Paths.get(annotOutdir));
        variantStorageEngine.saveAnnotation("v0", new ObjectMap());
        variantStorageEngine.annotate(annotOutdir, new ObjectMap(DummyVariantAnnotator.ANNOT_KEY, "v1").append(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));
        Collection<File> files = FileUtils.listFiles(Paths.get(annotOutdir).toFile(), null, false);
        assertNotEquals(0, files.size());

        annotOutdir = outputUri.resolve("annot2");
        Files.createDirectories(Paths.get(annotOutdir));
        variantStorageEngine.saveAnnotation("v1", new ObjectMap());
        variantStorageEngine.annotate(annotOutdir, new ObjectMap(DummyVariantAnnotator.ANNOT_KEY, "v2").append(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));
        files = FileUtils.listFiles(Paths.get(annotOutdir).toFile(), null, false);
        assertNotEquals(0, files.size());

        annotOutdir = outputUri.resolve("annot3");
        Files.createDirectories(Paths.get(annotOutdir));
        variantStorageEngine.saveAnnotation("v2", new ObjectMap());
        variantStorageEngine.annotate(annotOutdir, new Query(VariantQueryParam.REGION.key(), "1"), new ObjectMap(DummyVariantAnnotator.ANNOT_KEY, "v3")
                .append(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true)
                .append(VariantStorageOptions.ANNOTATION_FILE_DELETE_AFTER_LOAD.key(), true)
        );
        files = FileUtils.listFiles(Paths.get(annotOutdir).toFile(), null, false);
        assertEquals(0, files.size());

        assertEquals(0, variantStorageEngine.getAnnotation("v0", null, null).getResults().size());
        checkAnnotationSnapshot(variantStorageEngine, "v1", "v1");
        checkAnnotationSnapshot(variantStorageEngine, "v2", "v2");
        checkAnnotationSnapshot(variantStorageEngine, VariantAnnotationManager.CURRENT, VariantAnnotationManager.CURRENT, "v3", new Query(VariantQueryParam.REGION.key(), "1"));
        checkAnnotationSnapshot(variantStorageEngine, VariantAnnotationManager.CURRENT, "v2", "v2", new Query(VariantQueryParam.REGION.key(), "2"));

        variantStorageEngine.deleteAnnotation("v1", new ObjectMap());

        testQueries(variantStorageEngine);

    }

    @Test
    public void testCheckpointAnnotation() throws Exception {
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        variantStorageEngine.getOptions()
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), DummyVariantAnnotator.class.getName())
                .append(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER)
                .append(DummyVariantAnnotator.ANNOT_KEY, "k1")
                .append(DummyVariantAnnotator.ANNOT_VERSION, "v1")
                .append(DummyVariantAnnotator.ANNOT_DATARELEASE, 2)
                .append(VariantStorageOptions.ANNOTATION_BATCH_SIZE.key(), 5)
                .append(VariantStorageOptions.ANNOTATION_CHECKPOINT_SIZE.key(), 10);
        variantStorageEngine.getOptions().put(VariantStorageOptions.ASSEMBLY.key(), ASSEMBLY_38);
        variantStorageEngine.getOptions().put(VariantStorageOptions.SPECIES.key(), "hsapiens");

        runETL(variantStorageEngine, smallInputUri, STUDY_NAME,
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false));


        long numVariants = variantStorageEngine.count(new Query()).first();
        long annotatedVariants = variantStorageEngine.annotate(outputUri, new ObjectMap(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));
        assertEquals(2, variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getDataRelease().getRelease());
        assertEquals(numVariants, annotatedVariants);
        assertEquals("k1", variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getAnnotator().getName());
        assertEquals("v1", variantStorageEngine.getMetadataManager().getProjectMetadata().getAnnotation().getCurrent().getAnnotator().getVersion());


        variantStorageEngine.getOptions()
                .append(DummyVariantAnnotator.SKIP, "6:79656570:G:A,5:154271948:G:A");
        annotatedVariants = variantStorageEngine.annotate(outputUri, new ObjectMap(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));
        assertEquals(numVariants - 2, annotatedVariants);

        variantStorageEngine.getOptions()
                .append(DummyVariantAnnotator.SKIP, "6:79656570:G:A,5:154271948:G:A,4:69095197:T:C,X:48460314:A:G");
        annotatedVariants = variantStorageEngine.annotate(outputUri, new ObjectMap(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));
        assertEquals(numVariants - 4, annotatedVariants);

    }

    @Test
    public void testCosmicAnnotatorExtensionWithCosmicAnnotation() throws Exception {
        // Setup COSMIC directory
        URI annotOutdir = newOutputUri();
        Files.createDirectories(Paths.get(annotOutdir));
        Path cosmicFile = CosmicVariantAnnotatorExtensionTaskTest.initCosmicPath(Paths.get(annotOutdir));

        // Set up COSMIC annotator extension task
        VariantAnnotationExtensionConfigureParams params = new VariantAnnotationExtensionConfigureParams();
        params.setExtension(CosmicVariantAnnotatorExtensionTask.ID);
        params.setResources(Collections.singletonList(cosmicFile.toAbsolutePath().toString()));
        ObjectMap cosmicParams = new ObjectMap();
        cosmicParams.put(CosmicVariantAnnotatorExtensionTask.COSMIC_VERSION_KEY, COSMIC_VERSION);
        cosmicParams.put(CosmicVariantAnnotatorExtensionTask.COSMIC_ASSEMBLY_KEY, COSMIC_ASSEMBLY);
        params.setParams(cosmicParams);

        CosmicVariantAnnotatorExtensionTask task = new CosmicVariantAnnotatorExtensionTask(null);
        task.setup(params, null);

        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        runETL(variantStorageEngine, annotatorExtensionInputUri, STUDY_NAME,
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false)
                        .append(VariantStorageOptions.ASSEMBLY.key(), ASSEMBLY_38));

        variantStorageEngine.getOptions()
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), DummyVariantAnnotator.class.getName())
                .append(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER)
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_LIST.key(), CosmicVariantAnnotatorExtensionTask.ID)
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_ASSEMBLY.key(), COSMIC_ASSEMBLY)
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_VERSION.key(), COSMIC_VERSION)
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_FILE.key(), task.getOptions().getString(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_FILE.key()));

        variantStorageEngine.annotate(annotOutdir, new ObjectMap(DummyVariantAnnotator.ANNOT_KEY, "v1").append(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));
        // Check that cosmic variants are annotated
        DataResult<VariantAnnotation> annotationDataResult = variantStorageEngine.getAnnotation(new Query(), new QueryOptions());
        checkCosmicVariants(annotationDataResult, true);
    }

    @Test
    public void testCosmicAnnotatorExtensionWithoutCosmicAnnotation() throws Exception {
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        runETL(variantStorageEngine, annotatorExtensionInputUri, STUDY_NAME,
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false));

        variantStorageEngine.getOptions()
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), DummyVariantAnnotator.class.getName())
                .append(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER);

        URI annotOutdir = outputUri.resolve("annot1");
        Files.createDirectories(Paths.get(annotOutdir));
        variantStorageEngine.annotate(annotOutdir, new ObjectMap(DummyVariantAnnotator.ANNOT_KEY, "v1").append(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));

        // Check that cosmic variants are annotated
        DataResult<VariantAnnotation> annotationDataResult = variantStorageEngine.getAnnotation(new Query(), new QueryOptions());
        checkCosmicVariants(annotationDataResult, false);
    }

    @Test
    public void testCosmicAnnotatorExtensionInvalidCosmicFile() throws Exception {
        // Setup COSMIC directory
        Path cosmicFile = CosmicVariantAnnotatorExtensionTaskTest.initInvalidCosmicPath(Paths.get(newOutputUri("cosmic")));

        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        runETL(variantStorageEngine, annotatorExtensionInputUri, STUDY_NAME,
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false)
                        .append(VariantStorageOptions.ASSEMBLY.key(), ASSEMBLY_38));

        variantStorageEngine.getOptions()
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), DummyVariantAnnotator.class.getName())
                .append(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER)
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_LIST.key(), CosmicVariantAnnotatorExtensionTask.ID)
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_VERSION.key(), COSMIC_VERSION)
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_ASSEMBLY.key(), COSMIC_ASSEMBLY)
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_FILE.key(), cosmicFile);


        URI annotOutdir = outputUri.resolve("annot1");
        Files.createDirectories(Paths.get(annotOutdir));

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(containsString("COSMIC annotator extension is not available"));
        variantStorageEngine.annotate(annotOutdir, new ObjectMap(DummyVariantAnnotator.ANNOT_KEY, "v1").append(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));
    }

    @Test
    public void testCosmicAnnotatorExtensionMissingCosmicFile() throws Exception {
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        runETL(variantStorageEngine, annotatorExtensionInputUri, STUDY_NAME,
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false)
                        .append(VariantStorageOptions.ASSEMBLY.key(), ASSEMBLY_38));

        variantStorageEngine.getOptions()
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), DummyVariantAnnotator.class.getName())
                .append(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER)
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_LIST.key(), CosmicVariantAnnotatorExtensionTask.ID)
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_ASSEMBLY.key(), COSMIC_ASSEMBLY)
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_VERSION.key(), COSMIC_VERSION);

        URI annotOutdir = outputUri.resolve("annot1");
        Files.createDirectories(Paths.get(annotOutdir));

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Missing COSMIC file");
        variantStorageEngine.annotate(annotOutdir, new ObjectMap(DummyVariantAnnotator.ANNOT_KEY, "v1").append(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));
    }

    @Test
    public void testCosmicAnnotatorExtensionMissingCosmicVersion() throws Exception {
        // Setup COSMIC directory
        URI annotOutdir = newOutputUri();
        Files.createDirectories(Paths.get(annotOutdir));
        Path cosmicFile = CosmicVariantAnnotatorExtensionTaskTest.initCosmicPath(Paths.get(annotOutdir));

        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        runETL(variantStorageEngine, annotatorExtensionInputUri, STUDY_NAME,
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false)
                        .append(VariantStorageOptions.ASSEMBLY.key(), ASSEMBLY_38));

        variantStorageEngine.getOptions()
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), DummyVariantAnnotator.class.getName())
                .append(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER)
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_LIST.key(), CosmicVariantAnnotatorExtensionTask.ID)
                .append(VariantStorageOptions.ASSEMBLY.key(), COSMIC_ASSEMBLY)
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_FILE.key(), cosmicFile);

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Missing COSMIC version");
        variantStorageEngine.annotate(annotOutdir, new ObjectMap(DummyVariantAnnotator.ANNOT_KEY, "v1").append(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));
    }

    @Test
    public void testCosmicAnnotatorExtensionMissingAssembly() throws Exception {
        // Setup COSMIC directory
        URI annotOutdir = newOutputUri();
        Files.createDirectories(Paths.get(annotOutdir));
        Path cosmicFile = CosmicVariantAnnotatorExtensionTaskTest.initCosmicPath(Paths.get(annotOutdir));

        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        variantStorageEngine.getOptions()
                .remove(VariantStorageOptions.ASSEMBLY.key());
        runETL(variantStorageEngine, annotatorExtensionInputUri, STUDY_NAME,
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false));

        variantStorageEngine.getOptions()
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), DummyVariantAnnotator.class.getName())
                .append(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER)
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_LIST.key(), CosmicVariantAnnotatorExtensionTask.ID)
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_VERSION.key(), COSMIC_VERSION)
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_FILE.key(), cosmicFile);

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Missing COSMIC assembly");
        variantStorageEngine.annotate(annotOutdir, new ObjectMap(DummyVariantAnnotator.ANNOT_KEY, "v1").append(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));
    }

    @Test
    public void testCosmicAnnotatorExtensionMismatchAssembly() throws Exception {
        // Setup COSMIC directory
        URI annotOutdir = newOutputUri();
        Files.createDirectories(Paths.get(annotOutdir));
        Path cosmicFile = CosmicVariantAnnotatorExtensionTaskTest.initCosmicPath(Paths.get(annotOutdir));

        VariantStorageEngine variantStorageEngine = getVariantStorageEngine();
        runETL(variantStorageEngine, annotatorExtensionInputUri, STUDY_NAME,
                new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false)
                        .append(VariantStorageOptions.ASSEMBLY.key(), ASSEMBLY_37));

        variantStorageEngine.getOptions()
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), DummyVariantAnnotator.class.getName())
                .append(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER)
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_LIST.key(), CosmicVariantAnnotatorExtensionTask.ID)
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_ASSEMBLY.key(), COSMIC_ASSEMBLY)
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_VERSION.key(), COSMIC_VERSION)
                .append(VariantStorageOptions.ANNOTATOR_EXTENSION_COSMIC_FILE.key(), cosmicFile);

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(containsString("does not match"));
        variantStorageEngine.annotate(annotOutdir, new ObjectMap(DummyVariantAnnotator.ANNOT_KEY, "v1").append(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true));
    }

    public void checkCosmicVariants(DataResult<VariantAnnotation> annotationDataResult, boolean withCosmicAnnotation) {
        int cosmicCount = 0;
        int expectedCount = 0;
        for (VariantAnnotation va : annotationDataResult.getResults()) {
            String variantId = va.getChromosome() + ":" + va.getStart() + ":" + va.getReference() + ":" + va.getAlternate();
            if (COSMIC_VARIANTS.contains(variantId)) {
                expectedCount++;
                if (va.getTraitAssociation() != null) {
                    for (EvidenceEntry entry : va.getTraitAssociation()) {
                        if (CosmicVariantAnnotatorExtensionTask.ID.equals(entry.getSource().getName())) {
                            cosmicCount++;
                            break;
                        }
                    }
                }
            }
        }
        if (withCosmicAnnotation) {
            assertEquals(COSMIC_VARIANTS.size(), expectedCount);
            assertEquals(expectedCount, cosmicCount);
        } else {
            assertEquals(0, cosmicCount);
        }
    }

    public void testQueries(VariantStorageEngine variantStorageEngine) throws StorageEngineException {
        long count = variantStorageEngine.count(new Query()).first();
        long partialCount = 0;
        int batchSize = (int) Math.ceil(count / 10.0);
        for (int i = 0; i < 10; i++) {
            partialCount += variantStorageEngine.getAnnotation("v2", null, new QueryOptions(QueryOptions.LIMIT, batchSize)
                    .append(QueryOptions.SKIP, batchSize * i)).getResults().size();
        }
        assertEquals(count, partialCount);

        for (int chr = 1; chr < 22; chr += 2) {
            Query query = new Query(VariantQueryParam.REGION.key(), chr + "," + (chr + 1));
            count = variantStorageEngine.count(query).first();
            partialCount = variantStorageEngine.getAnnotation("v2", query, new QueryOptions()).getResults().size();
            assertEquals(count, partialCount);
        }

        String consequenceTypes = VariantField.ANNOTATION_CONSEQUENCE_TYPES.fieldName().replace(VariantField.ANNOTATION.fieldName() + ".", "");
        for (VariantAnnotation annotation : variantStorageEngine.getAnnotation("v2", null, new QueryOptions(QueryOptions.INCLUDE, consequenceTypes)).getResults()) {
            assertEquals(1, annotation.getConsequenceTypes().size());
        }
        for (VariantAnnotation annotation : variantStorageEngine.getAnnotation("v2", null, new QueryOptions(QueryOptions.EXCLUDE, consequenceTypes)).getResults()) {
            assertThat(annotation.getConsequenceTypes(), VariantMatchers.isEmpty());
        }

        for (Variant variant : variantStorageEngine) {
            Variant thisVariant = variantStorageEngine.getVariant(DummyVariantAnnotator.getRs(variant));
            assertThat(thisVariant, VariantMatchers.samePosition(variant));
        }

        // Get annotations from a deleted snapshot
        thrown.expectMessage("Variant Annotation snapshot \"v1\" not found!");
        assertEquals(0, variantStorageEngine.getAnnotation("v1", null, null).getResults().size());
    }

    public void checkAnnotationSnapshot(VariantStorageEngine variantStorageEngine, String annotationName, String expectedId) throws Exception {
        checkAnnotationSnapshot(variantStorageEngine, annotationName, annotationName, expectedId, null);
    }

    public void checkAnnotationSnapshot(VariantStorageEngine variantStorageEngine, String annotationName, String expectedAnnotationName, String expectedId, Query query) throws Exception {
        int count = 0;
        for (VariantAnnotation annotation: variantStorageEngine.getAnnotation(annotationName, query, new QueryOptions(QueryOptions.LIMIT, 5000)).getResults()) {
            assertEquals("an id -- " + expectedId, annotation.getId());
//            assertEquals("1", annotation.getAdditionalAttributes().get("opencga").getAttribute().getRelease());
            assertEquals(expectedAnnotationName, annotation.getAdditionalAttributes().get(GROUP_NAME.key())
                    .getAttribute().get(VariantField.AdditionalAttributes.ANNOTATION_ID.key()));
            count++;
        }
        assertEquals(count, variantStorageEngine.count(query).first().intValue());
    }

}
