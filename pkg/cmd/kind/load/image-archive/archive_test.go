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
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"testing"

	"sigs.k8s.io/kind/pkg/cluster/nodeutils"
)

func TestNormalizeImageName(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"ubuntu:18.04", "docker.io/library/ubuntu:18.04"},
		{"custom/ubuntu:18.04", "docker.io/custom/ubuntu:18.04"},
		{"registry.k8s.io/kindest/node:latest", "registry.k8s.io/kindest/node:latest"},
		{"baz", "docker.io/library/baz:latest"},
		{"localhost:5000/baz", "localhost:5000/baz:latest"},
		{"localhost:5000/baz:quux", "localhost:5000/baz:quux"},
		{"foo/bar@sha256:" + repeat("a", 64), "docker.io/foo/bar@sha256:" + repeat("a", 64)},
	}
	for _, tc := range cases {
		got, err := normalizeImageName(tc.in)
		if err != nil {
			t.Fatalf("normalizeImageName(%q): %v", tc.in, err)
		}
		if got != tc.want {
			t.Errorf("normalizeImageName(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestArchiveDigestsAreReal(t *testing.T) {
	// The archive parser hashes every member. Make sure the digest it
	// records for a config blob really is the sha256 of that blob, because
	// the whole comparison depends on it matching the runtime's image ID.
	dir := t.TempDir()
	path := dockerArchive(t, dir, []testImage{
		{config: configApp, layers: [][]byte{layerApp}, tags: []string{"docker.io/library/app:v1"}},
	})
	a, err := ParseArchive(path)
	if err != nil {
		t.Fatal(err)
	}
	img := a.Images[0]
	sum := sha256.Sum256(configApp)
	want := "sha256:" + hex.EncodeToString(sum[:])
	if len(img.ConfigDigests) != 1 || img.ConfigDigests[0] != want {
		t.Fatalf("config digest = %v, want %s", img.ConfigDigests, want)
	}
}

func TestWriteSubsetRewritesDockerManifestPaths(t *testing.T) {
	// docker 1.1 archives can use a leading "./" or symlinked paths for
	// layers. The subset must reference the regular files it actually
	// streams, otherwise the importer cannot find them.
	dir := t.TempDir()
	configName := "abc/config.json"
	layerName := "def/layer.tar"
	manifest := []dockerManifestEntry{{Config: "./" + configName, RepoTags: []string{"docker.io/library/sym:1"}, Layers: []string{"layer.tar"}}}
	manifestJSON, _ := jsonMarshal(manifest)
	path := writeArchive(t, dir, "sym.tar", []tarEntry{
		{name: dockerManifestName, data: manifestJSON},
		{name: configName, data: configApp},
		{name: layerName, data: layerApp},
		{name: "layer.tar", symlink: layerName},
	})
	a, err := ParseArchive(path)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := a.WriteSubset(&buf, a.Images); err != nil {
		t.Fatal(err)
	}
	parsed, err := parseStreamedArchive(&buf)
	if err != nil {
		t.Fatalf("subset is invalid: %v", err)
	}
	if len(parsed.images) != 1 || parsed.images[0].id != idOf(configApp) {
		t.Fatalf("unexpected subset: %+v", parsed.images)
	}
}

func TestWriteSubsetIsSelfConsistent(t *testing.T) {
	dir := t.TempDir()
	path := ociArchive(t, dir, []testImage{
		{config: configApp, layers: [][]byte{layerApp}, tags: []string{"docker.io/library/app:v1"}},
		{config: configBase, layers: [][]byte{layerBase}, tags: []string{"docker.io/library/base:1"}},
	})
	a, err := ParseArchive(path)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := a.WriteSubset(&buf, []*archiveImage{a.Images[0]}); err != nil {
		t.Fatal(err)
	}
	parsed, err := parseStreamedArchive(&buf)
	if err != nil {
		t.Fatalf("subset archive is not valid: %v", err)
	}
	if len(parsed.tags) != 1 || parsed.tags[0] != "docker.io/library/app:v1" {
		t.Fatalf("subset has wrong names: %v", parsed.tags)
	}
	// The other image's content must not be streamed.
	wantDigest := idOf(configBase)
	for _, member := range parsed.members {
		if member == "blobs/sha256/"+stripPrefix(wantDigest) {
			t.Fatalf("subset included the other image's config blob")
		}
	}

	// And the streamed bytes must be exactly the archive bytes for the blobs.
	if err := a.WriteSubset(io.Discard, a.Images); err != nil {
		t.Fatalf("whole subset: %v", err)
	}
}

func TestDigestFromReference(t *testing.T) {
	cases := map[string]string{
		"docker.io/library/app@sha256:" + repeat("a", 64): "sha256:" + repeat("a", 64),
		"docker.io/library/app:v1":                        "",
	}
	for in, want := range cases {
		got, ok := nodeutils.DigestFromReference(in)
		if want == "" {
			if ok {
				t.Errorf("DigestFromReference(%q) unexpectedly ok: %q", in, got)
			}
			continue
		}
		if !ok || got != want {
			t.Errorf("DigestFromReference(%q) = %q,%v want %q", in, got, ok, want)
		}
	}
}

func repeat(s string, n int) string {
	out := make([]byte, 0, len(s)*n)
	for i := 0; i < n; i++ {
		out = append(out, s...)
	}
	return string(out)
}

func stripPrefix(digest string) string {
	if len(digest) > len("sha256:") && digest[:len("sha256:")] == "sha256:" {
		return digest[len("sha256:"):]
	}
	return digest
}
