/*
Copyright 2024 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package load

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"sigs.k8s.io/kind/pkg/cluster/nodes"
	"sigs.k8s.io/kind/pkg/log"
)

var (
	configApp   = []byte(`{"architecture":"amd64","os":"linux","config":{"Labels":{"app":"1"}}}`)
	configAppV2 = []byte(`{"architecture":"amd64","os":"linux","config":{"Labels":{"app":"2"}}}`)
	configBase  = []byte(`{"architecture":"amd64","os":"linux","config":{"Labels":{"base":"1"}}}`)
	layerApp    = []byte("app layer bytes")
	layerBase   = []byte("base layer bytes")
)

func idOf(config []byte) string { return sha256Digest(config) }

// recordingLogger captures everything so tests can assert log categories.
type recordingLogger struct {
	messages []string
}

func (l *recordingLogger) record(format string, args ...interface{}) {
	l.messages = append(l.messages, fmt.Sprintf(format, args...))
}
func (l *recordingLogger) Warn(m string)                     { l.record("%s", m) }
func (l *recordingLogger) Warnf(f string, a ...interface{})  { l.record(f, a...) }
func (l *recordingLogger) Error(m string)                    { l.record("%s", m) }
func (l *recordingLogger) Errorf(f string, a ...interface{}) { l.record(f, a...) }
func (l *recordingLogger) V(log.Level) log.InfoLogger        { return recordingInfo{l} }

type recordingInfo struct{ l *recordingLogger }

func (i recordingInfo) Enabled() bool                    { return true }
func (i recordingInfo) Info(m string)                    { i.l.record("%s", m) }
func (i recordingInfo) Infof(f string, a ...interface{}) { i.l.record(f, a...) }

func (l *recordingLogger) all() string { return strings.Join(l.messages, "\n") }

// buildArchive is the scenario-independent archive used by most tests.
func buildArchive(t *testing.T, dir string, oci bool) *Archive {
	t.Helper()
	images := []testImage{
		{config: configApp, layers: [][]byte{layerApp}, tags: []string{"docker.io/library/app:v1"}},
		{config: configBase, layers: [][]byte{layerBase}, tags: []string{"docker.io/library/base:1"}},
	}
	var path string
	if oci {
		path = ociArchive(t, dir, images)
	} else {
		path = dockerArchive(t, dir, images)
	}
	a, err := ParseArchive(path)
	if err != nil {
		t.Fatalf("ParseArchive: %v", err)
	}
	return a
}

func runNode(t *testing.T, a *Archive, node *fakeNode) applyResult {
	t.Helper()
	logger := &recordingLogger{}
	result, err := loadArchiveToNode(logger, a, node)
	if err != nil {
		t.Fatalf("loadArchiveToNode(%s): %v\nlog:\n%s", node.String(), err, logger.all())
	}
	return result
}

func TestFreshNodeImportsEachImageOnce(t *testing.T) {
	for _, oci := range []bool{false, true} {
		t.Run(fmt.Sprintf("oci=%v", oci), func(t *testing.T) {
			a := buildArchive(t, t.TempDir(), oci)
			rt := newFakeRuntime()
			node := &fakeNode{name: "fresh", rt: rt}

			result := runNode(t, a, node)
			if result.imported != 2 || result.unchanged != 0 || result.tagOnly != 0 {
				t.Fatalf("unexpected result: %+v", result)
			}

			imports, tags := rt.snapshot()
			if len(imports) != 1 {
				t.Fatalf("expected exactly one import call, got %d: %+v", len(imports), imports)
			}
			if len(tags) != 0 {
				t.Fatalf("expected no tag calls on a fresh node, got %+v", tags)
			}
			if got := sortedStrings(imports[0].tags); !reflect.DeepEqual(got, []string{"docker.io/library/app:v1", "docker.io/library/base:1"}) {
				t.Fatalf("unexpected imported tags: %v", got)
			}
			want := rt.taggedImages()
			if want["docker.io/library/app:v1"] != idOf(configApp) ||
				want["docker.io/library/base:1"] != idOf(configBase) {
				t.Fatalf("final state wrong: %v", want)
			}
			if got := rt.queries(); got != 1 {
				t.Fatalf("expected the image list to be queried exactly once, got %d", got)
			}
		})
	}
}

func TestAllPresentSkipsImport(t *testing.T) {
	for _, oci := range []bool{false, true} {
		t.Run(fmt.Sprintf("oci=%v", oci), func(t *testing.T) {
			a := buildArchive(t, t.TempDir(), oci)
			rt := newFakeRuntime()
			rt.seed(idOf(configApp), []string{"docker.io/library/app:v1"})
			rt.seed(idOf(configBase), []string{"docker.io/library/base:1"})
			node := &fakeNode{name: "n", rt: rt}

			logger := &recordingLogger{}
			result, err := loadArchiveToNode(logger, a, node)
			if err != nil {
				t.Fatal(err)
			}
			if result.imported != 0 || result.tagOnly != 0 || result.unchanged != 2 {
				t.Fatalf("unexpected result: %+v", result)
			}
			imports, tags := rt.snapshot()
			if len(imports) != 0 || len(tags) != 0 {
				t.Fatalf("expected no runtime mutation, got imports=%+v tags=%+v", imports, tags)
			}
			if !strings.Contains(logger.all(), "already present (content and tags)") {
				t.Fatalf("expected 'content and tags already present' log, got:\n%s", logger.all())
			}
			if strings.Contains(logger.all(), "importing") {
				t.Fatalf("did not expect an import log, got:\n%s", logger.all())
			}
		})
	}
}

func TestOnlyMissingImageIsImported(t *testing.T) {
	a := buildArchive(t, t.TempDir(), false)
	rt := newFakeRuntime()
	rt.seed(idOf(configBase), []string{"docker.io/library/base:1"})
	node := &fakeNode{name: "n", rt: rt}

	result := runNode(t, a, node)
	if result.imported != 1 || result.unchanged != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
	imports, tags := rt.snapshot()
	if len(imports) != 1 || !reflect.DeepEqual(imports[0].tags, []string{"docker.io/library/app:v1"}) {
		t.Fatalf("expected import of only app:v1, got %+v", imports)
	}
	if len(tags) != 0 {
		t.Fatalf("expected no tagging, got %+v", tags)
	}
	// The imported subset must not contain any of the other image's blobs.
	baseConfigMember := "blobs/sha256/" + stripPrefix(idOf(configBase))
	for _, member := range imports[0].members {
		if member == baseConfigMember {
			t.Fatalf("subset import included the other image's config %q", member)
		}
	}
	if got := rt.taggedImages()["docker.io/library/base:1"]; got != idOf(configBase) {
		t.Fatalf("base tag was disturbed: %v", got)
	}
}

func TestOneOfTwoTagsMissingTagsOnly(t *testing.T) {
	dir := t.TempDir()
	// One content, two names, in the archive.
	path := dockerArchive(t, dir, []testImage{
		{config: configApp, layers: [][]byte{layerApp}, tags: []string{"docker.io/library/app:v1", "docker.io/library/app:latest"}},
	})
	a, err := ParseArchive(path)
	if err != nil {
		t.Fatal(err)
	}
	// The node has the content under one name only.
	rt := newFakeRuntime()
	rt.seed(idOf(configApp), []string{"docker.io/library/app:v1"})
	node := &fakeNode{name: "n", rt: rt}

	logger := &recordingLogger{}
	result, err := loadArchiveToNode(logger, a, node)
	if err != nil {
		t.Fatal(err)
	}
	if result.imported != 0 || result.tagOnly != 1 || result.unchanged != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	imports, tags := rt.snapshot()
	if len(imports) != 0 {
		t.Fatalf("tag-only case must not import content, got %+v", imports)
	}
	if len(tags) != 1 {
		t.Fatalf("expected exactly one tag call, got %+v", tags)
	}
	if tags[0].target != "docker.io/library/app:latest" || tags[0].source != "docker.io/library/app:v1" {
		t.Fatalf("unexpected tag call: %+v", tags[0])
	}
	if got := rt.taggedImages()["docker.io/library/app:latest"]; got != idOf(configApp) {
		t.Fatalf("missing tag not added: %v", got)
	}
	if !strings.Contains(logger.all(), "content present") || !strings.Contains(logger.all(), "not importing content") {
		t.Fatalf("expected 'tag-only' log, got:\n%s", logger.all())
	}
}

func TestSameTagDifferentContentIsReimported(t *testing.T) {
	a := buildArchive(t, t.TempDir(), false)
	// Node has app:v1 pointing at old content.
	rt := newFakeRuntime()
	rt.seed(idOf(configAppV2), []string{"docker.io/library/app:v1"})
	rt.seed(idOf(configBase), []string{"docker.io/library/base:1"})
	node := &fakeNode{name: "n", rt: rt}

	result := runNode(t, a, node)
	if result.imported != 1 {
		t.Fatalf("expected app content to be imported, got %+v", result)
	}
	imports, _ := rt.snapshot()
	if len(imports) != 1 || !reflect.DeepEqual(imports[0].tags, []string{"docker.io/library/app:v1"}) {
		t.Fatalf("unexpected imports: %+v", imports)
	}
	// Crucially, the node must now use the NEW content, not the old one.
	if got := rt.taggedImages()["docker.io/library/app:v1"]; got != idOf(configApp) {
		t.Fatalf("stale content was kept: tag points at %q, want %q", got, idOf(configApp))
	}
}

func TestNodesDifferIndependently(t *testing.T) {
	a := buildArchive(t, t.TempDir(), false)

	// Node A has nothing.
	rtA := newFakeRuntime()
	nodeA := &fakeNode{name: "a", rt: rtA}

	// Node B has everything but a stale app tag.
	rtB := newFakeRuntime()
	rtB.seed(idOf(configAppV2), []string{"docker.io/library/app:v1"})
	rtB.seed(idOf(configBase), []string{"docker.io/library/base:1"})
	nodeB := &fakeNode{name: "b", rt: rtB}

	// Node C joined late and has only base.
	rtC := newFakeRuntime()
	rtC.seed(idOf(configBase), []string{"docker.io/library/base:1"})
	nodeC := &fakeNode{name: "c", rt: rtC}

	resA := runNode(t, a, nodeA)
	resB := runNode(t, a, nodeB)
	resC := runNode(t, a, nodeC)

	if resA.imported != 2 {
		t.Fatalf("node a: expected 2 imports, got %+v", resA)
	}
	if resB.imported != 1 || resB.unchanged != 1 {
		t.Fatalf("node b: expected 1 import + 1 unchanged, got %+v", resB)
	}
	if resC.imported != 1 || resC.unchanged != 1 {
		t.Fatalf("node c: expected 1 import + 1 unchanged, got %+v", resC)
	}

	// Node C must not have been told about node A's work.
	importsC, tagsC := rtC.snapshot()
	if len(importsC) != 1 || !reflect.DeepEqual(importsC[0].tags, []string{"docker.io/library/app:v1"}) {
		t.Fatalf("node c imported wrong images: %+v", importsC)
	}
	if len(tagsC) != 0 {
		t.Fatalf("node c should not have tagged: %+v", tagsC)
	}
}

func TestImageListQueryFailureDoesNotModifyNode(t *testing.T) {
	a := buildArchive(t, t.TempDir(), false)
	rt := newFakeRuntime()
	rt.listErr = fmt.Errorf("crictl exploded")
	node := &fakeNode{name: "n", rt: rt}

	logger := &recordingLogger{}
	if _, err := loadArchiveToNode(logger, a, node); err == nil {
		t.Fatal("expected an error when the image list cannot be queried")
	}
	imports, tags := rt.snapshot()
	if len(imports) != 0 || len(tags) != 0 {
		t.Fatalf("node was modified despite a failed query: imports=%+v tags=%+v", imports, tags)
	}
	if !strings.Contains(logger.all(), "not modifying it") {
		t.Fatalf("expected a clear failure log, got:\n%s", logger.all())
	}
}

func TestTruncatedArchiveFailsBeforeAnyNode(t *testing.T) {
	dir := t.TempDir()
	src := dockerArchive(t, dir, []testImage{
		{config: configApp, layers: [][]byte{layerApp}, tags: []string{"docker.io/library/app:v1"}},
	})
	broken := filepath.Join(dir, "broken.tar")
	truncateLastBytes(t, src, broken, 64)

	if _, err := ParseArchive(broken); err == nil {
		t.Fatal("expected a truncated archive to be rejected")
	}
	// And a totally empty/garbage archive is rejected too.
	garbage := filepath.Join(dir, "garbage.tar")
	if err := os.WriteFile(garbage, []byte("not a tar"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ParseArchive(garbage); err == nil {
		t.Fatal("expected a garbage archive to be rejected")
	}
}

func TestMultiPlatformComparesConfigDigestNotIndexDigest(t *testing.T) {
	dir := t.TempDir()
	path := ociArchive(t, dir, []testImage{
		{
			config:    defaultConfig(t, "multi"),
			layers:    [][]byte{layerApp},
			tags:      []string{"docker.io/library/multi:1"},
			platforms: []string{"amd64", "arm64"},
		},
	})
	a, err := ParseArchive(path)
	if err != nil {
		t.Fatal(err)
	}
	img := a.Images[0]
	if len(img.ConfigDigests) != 2 {
		t.Fatalf("expected both platform config digests, got %v", img.ConfigDigests)
	}
	if len(img.TargetDigests) != 1 {
		t.Fatalf("expected the index digest as the top level target, got %v", img.TargetDigests)
	}
	indexDigest := img.TargetDigests[0]

	// Node already has the amd64 config as its image ID and the index digest
	// as a repo digest (this is what containerd reports). The config digest
	// must match even though it differs from the index digest.
	rt := newFakeRuntime()
	rt.seed(img.ConfigDigests[0], []string{"docker.io/library/multi:1"}, indexDigest)
	node := &fakeNode{name: "n", rt: rt}

	result := runNode(t, a, node)
	if result.imported != 0 || result.unchanged != 1 {
		t.Fatalf("expected a skip, got %+v", result)
	}
	imports, tags := rt.snapshot()
	if len(imports) != 0 || len(tags) != 0 {
		t.Fatalf("expected no runtime mutation, got imports=%+v tags=%+v", imports, tags)
	}
}

func TestOCIArchiveWithoutRepositoryFallsBackToWholeImport(t *testing.T) {
	dir := t.TempDir()
	// Only the OCI ref.name annotation, which carries a bare tag.
	config := defaultConfig(t, "bare")
	configDigest := sha256Digest(config)
	manifest := ociManifest{
		SchemaVersion: 2,
		Config:        ociDescriptor{MediaType: "application/vnd.oci.image.config.v1+json", Digest: configDigest, Size: int64(len(config))},
	}
	manifestJSON, _ := jsonMarshal(manifest)
	manifestDigest := sha256Digest(manifestJSON)
	indexJSON, _ := jsonMarshal(map[string]interface{}{
		"schemaVersion": 2,
		"manifests": []map[string]interface{}{{
			"mediaType":   "application/vnd.oci.image.manifest.v1+json",
			"digest":      manifestDigest,
			"size":        len(manifestJSON),
			"annotations": map[string]string{"org.opencontainers.image.ref.name": "latest"},
		}},
	})
	layoutJSON, _ := jsonMarshal(map[string]string{"imageLayoutVersion": "1.0.0"})
	path := writeArchive(t, dir, "bare.tar", []tarEntry{
		{name: ociLayoutFileName, data: layoutJSON},
		{name: ociIndexFileName, data: indexJSON},
		{name: blobName(configDigest), data: config},
		{name: blobName(manifestDigest), data: manifestJSON},
	})

	a, err := ParseArchive(path)
	if err != nil {
		t.Fatalf("ParseArchive: %v", err)
	}
	if a.Comparable() {
		t.Fatal("expected the archive to be marked non-comparable")
	}
	if !strings.Contains(a.FallbackReason(), "repository name is unknown") {
		t.Fatalf("unexpected fallback reason: %q", a.FallbackReason())
	}

	// Fallback still imports content on a node which has it, because
	// correctness beats skipping when names are ambiguous.
	rt := newFakeRuntime()
	rt.seed(configDigest, nil)
	node := &fakeNode{name: "n", rt: rt}
	logger := &recordingLogger{}
	if _, err := importWholeArchive(logger, a, node); err != nil {
		t.Fatal(err)
	}
	imports, _ := rt.snapshot()
	if len(imports) != 1 {
		t.Fatalf("expected a whole-archive import, got %+v", imports)
	}
}

func TestParseArchiveRecordsUnnamedImage(t *testing.T) {
	dir := t.TempDir()
	path := dockerArchive(t, dir, []testImage{{config: configApp, layers: [][]byte{layerApp}}})
	a, err := ParseArchive(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Images) != 1 || len(a.Images[0].Tags) != 0 {
		t.Fatalf("expected one untagged image, got %+v", a.Images)
	}
}

func sortedStrings(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

func jsonMarshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func TestLoadNodesHandlesEachNodeIndependently(t *testing.T) {
	a := buildArchive(t, t.TempDir(), false)

	// node-a: empty. node-b: complete. node-c: base only.
	rtA, rtB, rtC := newFakeRuntime(), newFakeRuntime(), newFakeRuntime()
	rtB.seed(idOf(configApp), []string{"docker.io/library/app:v1"})
	rtB.seed(idOf(configBase), []string{"docker.io/library/base:1"})
	rtC.seed(idOf(configBase), []string{"docker.io/library/base:1"})

	logger := &recordingLogger{}
	if err := loadNodes(logger, a, []nodes.Node{
		&fakeNode{name: "node-a", rt: rtA},
		&fakeNode{name: "node-b", rt: rtB},
		&fakeNode{name: "node-c", rt: rtC},
	}); err != nil {
		t.Fatalf("loadNodes: %v", err)
	}

	importsA, _ := rtA.snapshot()
	importsB, _ := rtB.snapshot()
	importsC, _ := rtC.snapshot()

	if len(importsA) != 1 || len(importsA[0].tags) != 2 {
		t.Fatalf("node-a should import both images in one call: %+v", importsA)
	}
	if len(importsB) != 0 {
		t.Fatalf("node-b should need no imports: %+v", importsB)
	}
	if len(importsC) != 1 || !reflect.DeepEqual(importsC[0].tags, []string{"docker.io/library/app:v1"}) {
		t.Fatalf("node-c should import only the missing image: %+v", importsC)
	}
	// Each node queried once, and only once.
	for name, rt := range map[string]*fakeRuntime{"a": rtA, "b": rtB, "c": rtC} {
		if got := rt.queries(); got != 1 {
			t.Fatalf("node-%s queried %d times, want 1", name, got)
		}
	}
}

func TestLoadNodesReportsFailurePerNode(t *testing.T) {
	a := buildArchive(t, t.TempDir(), false)

	good := newFakeRuntime()
	bad := newFakeRuntime()
	bad.listErr = fmt.Errorf("runtime unavailable")

	logger := &recordingLogger{}
	err := loadNodes(logger, a, []nodes.Node{
		&fakeNode{name: "good", rt: good},
		&fakeNode{name: "bad", rt: bad},
	})
	if err == nil {
		t.Fatal("expected a failure from the bad node")
	}
	if !strings.Contains(err.Error(), "bad") {
		t.Fatalf("error should name the failing node, got: %v", err)
	}
	// The good node still got its work done; this is not a transaction.
	importsGood, _ := good.snapshot()
	if len(importsGood) != 1 {
		t.Fatalf("good node should still have been loaded: %+v", importsGood)
	}
	importsBad, _ := bad.snapshot()
	if len(importsBad) != 0 {
		t.Fatalf("bad node must not be modified: %+v", importsBad)
	}
}

func TestTagRepointedWhenContentExistsUnderAnotherName(t *testing.T) {
	a := buildArchive(t, t.TempDir(), false)
	rt := newFakeRuntime()
	// The archive's app content is on the node, but under a different name;
	// app:v1 is owned by other content.
	rt.seed(idOf(configApp), []string{"docker.io/library/app:latest"})
	rt.seed(idOf(configAppV2), []string{"docker.io/library/app:v1"})
	rt.seed(idOf(configBase), []string{"docker.io/library/base:1"})
	node := &fakeNode{name: "n", rt: rt}

	result := runNode(t, a, node)
	if result.imported != 0 || result.tagOnly != 1 || result.tagsAdded != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
	imports, tags := rt.snapshot()
	if len(imports) != 0 {
		t.Fatalf("content already present; must not import: %+v", imports)
	}
	if len(tags) != 1 || tags[0].target != "docker.io/library/app:v1" {
		t.Fatalf("expected a single re-tag of app:v1, got %+v", tags)
	}
	// The tag must now point at the archive's content, not the old one.
	if got := rt.taggedImages()["docker.io/library/app:v1"]; got != idOf(configApp) {
		t.Fatalf("tag points at %q, want %q", got, idOf(configApp))
	}
}
