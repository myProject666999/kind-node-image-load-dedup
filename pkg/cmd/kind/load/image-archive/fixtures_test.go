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
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// Minimal but valid image archives built in the test's temp dir. They are
// small enough to reason about and contain real digests, so the parser and
// the subset writer are exercised exactly as they are in production.

// testImage describes one image to put into a fixture archive.
type testImage struct {
	// config is the image config JSON. Its digest becomes the image ID.
	config []byte
	// layers are layer contents; each becomes a gzip-less blob.
	layers [][]byte
	// tags are the repo tags in the archive.
	tags []string
	// platforms, when set, makes the archive contain a multi-platform index
	// whose platform manifests each reference the layers with a distinct
	// config. Only the first config is used as the image ID.
	platforms []string
}

func defaultConfig(t *testing.T, seed string) []byte {
	t.Helper()
	return []byte(`{"architecture":"amd64","os":"linux","config":{"Env":["SEED=` + seed + `"]}}`)
}

func sha256Digest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// writeArchive writes a tar archive to dir/name and returns its path.
func writeArchive(t *testing.T, dir, name string, entries []tarEntry) string {
	t.Helper()
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	tw := tar.NewWriter(f)
	for _, entry := range entries {
		hdr := &tar.Header{Name: entry.name, Mode: 0o644, Size: int64(len(entry.data)), Typeflag: tar.TypeReg, Format: tar.FormatPAX}
		if entry.symlink != "" {
			hdr.Typeflag = tar.TypeSymlink
			hdr.Linkname = entry.symlink
			hdr.Size = 0
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if entry.symlink == "" {
			if _, err := tw.Write(entry.data); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

type tarEntry struct {
	name    string
	data    []byte
	symlink string
}

// dockerArchive builds a docker save v1.2 style archive.
func dockerArchive(t *testing.T, dir string, images []testImage) string {
	t.Helper()
	var entries []tarEntry
	var manifest []dockerManifestEntry
	for i, img := range images {
		configName := filepath.ToSlash(filepath.Join("blobs", "sha256", strings.TrimPrefix(sha256Digest(img.config), "sha256:")))
		entries = append(entries, tarEntry{name: configName, data: img.config})
		entry := dockerManifestEntry{Config: configName, RepoTags: img.tags}
		for _, layer := range img.layers {
			layerName := filepath.ToSlash(filepath.Join("layer", "l"+itoa(i)+"-"+strings.TrimPrefix(sha256Digest(layer), "sha256:")))
			entries = append(entries, tarEntry{name: layerName, data: layer})
			entry.Layers = append(entry.Layers, layerName)
		}
		manifest = append(manifest, entry)
	}
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	entries = append([]tarEntry{{name: dockerManifestName, data: manifestJSON}}, entries...)
	return writeArchive(t, dir, "docker.tar", entries)
}

// ociArchive builds an OCI layout archive. Each testImage becomes a
// single-platform image (one manifest) unless platforms is set, in which
// case a multi-platform index is written.
func ociArchive(t *testing.T, dir string, images []testImage) string {
	t.Helper()
	var entries []tarEntry
	var indexManifests []map[string]interface{}
	for _, img := range images {
		configs := [][]byte{img.config}
		if len(img.platforms) > 1 {
			configs = configs[:0]
			for _, platform := range img.platforms {
				configs = append(configs, defaultConfig(t, "platform-"+platform))
			}
		}
		var platformManifests []ociDescriptor
		for j, config := range configs {
			configDigest := sha256Digest(config)
			var layerDescs []ociDescriptor
			for _, layer := range img.layers {
				layerDigest := sha256Digest(layer)
				entries = append(entries, tarEntry{name: blobName(layerDigest), data: layer})
				layerDescs = append(layerDescs, ociDescriptor{
					MediaType: "application/vnd.oci.image.layer.v1.tar",
					Digest:    layerDigest,
					Size:      int64(len(layer)),
				})
			}
			entries = append(entries, tarEntry{name: blobName(configDigest), data: config})
			manifest := ociManifest{
				SchemaVersion: 2,
				Config:        ociDescriptor{MediaType: "application/vnd.oci.image.config.v1+json", Digest: configDigest, Size: int64(len(config))},
				Layers:        layerDescs,
			}
			manifestJSON, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			manifestDigest := sha256Digest(manifestJSON)
			entries = append(entries, tarEntry{name: blobName(manifestDigest), data: manifestJSON})
			desc := ociDescriptor{
				MediaType: "application/vnd.oci.image.manifest.v1+json",
				Digest:    manifestDigest,
				Size:      int64(len(manifestJSON)),
			}
			if len(img.platforms) > 1 {
				desc.Platform = &ociPlatform{OS: "linux", Architecture: "amd64", Variant: itoa(j)}
			}
			platformManifests = append(platformManifests, desc)
		}

		top := platformManifests[0]
		if len(platformManifests) > 1 {
			index := ociIndex{SchemaVersion: 2, Manifests: platformManifests}
			indexJSON, err := json.Marshal(index)
			if err != nil {
				t.Fatal(err)
			}
			indexDigest := sha256Digest(indexJSON)
			entries = append(entries, tarEntry{name: blobName(indexDigest), data: indexJSON})
			top = ociDescriptor{
				MediaType: "application/vnd.oci.image.index.v1+json",
				Digest:    indexDigest,
				Size:      int64(len(indexJSON)),
			}
		}

		if len(img.tags) > 0 {
			for _, tag := range img.tags {
				desc := top
				desc.Annotations = map[string]string{"io.containerd.image.name": tag}
				indexManifests = append(indexManifests, descriptorMap(t, desc))
			}
		} else {
			indexManifests = append(indexManifests, descriptorMap(t, top))
		}
	}
	indexJSON, err := json.Marshal(map[string]interface{}{"schemaVersion": 2, "manifests": indexManifests})
	if err != nil {
		t.Fatal(err)
	}
	layoutJSON, err := json.Marshal(map[string]string{"imageLayoutVersion": "1.0.0"})
	if err != nil {
		t.Fatal(err)
	}
	entries = append([]tarEntry{
		{name: ociLayoutFileName, data: layoutJSON},
		{name: ociIndexFileName, data: indexJSON},
	}, entries...)
	return writeArchive(t, dir, "oci.tar", entries)
}

func descriptorMap(t *testing.T, desc ociDescriptor) map[string]interface{} {
	t.Helper()
	b, err := json.Marshal(desc)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func itoa(i int) string {
	return strconv.Itoa(i)
}

// truncateLastBytes copies src to dst with the final n bytes removed, to
// simulate a truncated download.
func truncateLastBytes(t *testing.T, src, dst string, n int64) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(data)) <= n {
		t.Fatalf("archive too small to truncate")
	}
	if err := os.WriteFile(dst, data[:int64(len(data))-n], 0o644); err != nil {
		t.Fatal(err)
	}
}

// sortedTags returns archive image tags in a stable order for assertions.
func sortedTags(imgs []*archiveImage) []string {
	var out []string
	for _, img := range imgs {
		out = append(out, img.Tags...)
	}
	sort.Strings(out)
	return out
}
