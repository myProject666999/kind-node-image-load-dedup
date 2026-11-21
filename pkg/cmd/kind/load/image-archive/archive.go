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
	"fmt"
	"hash"
	"io"
	"os"
	"path"
	"strings"

	"sigs.k8s.io/kind/pkg/errors"
)

// This file parses an image archive once, on the host, into a description
// that can be compared against each node's image list and used to stream an
// exact subset of the archive to nodes that need it.
//
// Two archive formats are understood, matching what containerd's importer
// accepts:
//   - docker save (v1.1/v1.2): a top level `manifest.json` plus config and
//     layer files.
//   - OCI layout: an `oci-layout` file and `index.json` referencing blobs
//     under `blobs/<algorithm>/<encoded>`.
//
// ParseArchive reads the whole archive exactly once. It records the offset
// and digest of every member so that a later subset import can seek directly
// to the members it needs instead of re-reading the archive per node.

const (
	ociLayoutFileName  = "oci-layout"
	ociIndexFileName   = "index.json"
	dockerManifestName = "manifest.json"
	// maxMetadataFileSize bounds how much of an archive's metadata we will
	// read into memory, to fail clearly rather than exhaust memory on a
	// malicious or corrupt input.
	maxMetadataFileSize = 32 << 20
)

// archiveMember is one regular file inside the archive. Only its position
// and digest are recorded during the single parse pass; metadata files are
// read on demand from the recorded offset, so a large archive is never
// buffered in memory.
type archiveMember struct {
	name   string // cleaned path inside the tar
	size   int64
	offset int64  // offset of the file data in the archive
	digest string // sha256 of the file contents
}

// dockerManifestEntry mirrors the docker save v1.1/v1.2 manifest.json entry.
type dockerManifestEntry struct {
	Config   string
	RepoTags []string
	Layers   []string
}

// ociDescriptor is the subset of an OCI descriptor we need.
type ociDescriptor struct {
	MediaType   string            `json:"mediaType"`
	Digest      string            `json:"digest"`
	Size        int64             `json:"size"`
	Platform    *ociPlatform      `json:"platform,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

type ociPlatform struct {
	Architecture string `json:"architecture,omitempty"`
	OS           string `json:"os,omitempty"`
	Variant      string `json:"variant,omitempty"`
}

type ociIndex struct {
	SchemaVersion int             `json:"schemaVersion"`
	Manifests     []ociDescriptor `json:"manifests"`
}

type ociManifest struct {
	SchemaVersion int             `json:"schemaVersion"`
	Config        ociDescriptor   `json:"config"`
	Layers        []ociDescriptor `json:"layers"`
}

// archiveImage is one image in the archive: a set of names sharing the same
// content, identified by the digests of its per-platform configs.
type archiveImage struct {
	// Tags are normalized repo:tag names which should reference this image.
	Tags []string
	// ConfigDigests are the digests of the image's config blobs. The runtime
	// reports the config digest of the node's platform as the image ID, so
	// membership in this set is how content identity is compared.
	ConfigDigests []string
	// TargetDigests are digests of the archive's top level targets. They are
	// informational: an index digest and a manifest digest live at different
	// levels and are not used for comparison.
	TargetDigests []string
	// Members are the archive entries needed to import this image.
	Members []string
	// OCITopDigests are the top level index.json descriptor digests which
	// name this image, used to rewrite index.json for a subset import.
	OCITopDigests []string
	// dockerConfigs is the set of docker manifest.json entries (by config
	// digest) belonging to this image.
	dockerConfigs []string
}

// Archive is a parsed image archive.
type Archive struct {
	path    string
	members map[string]*archiveMember
	// symlinks maps a symlink name to its resolved target name.
	symlinks map[string]string

	// Exactly one of dockerEntries / oci is populated.
	dockerEntries []dockerManifestEntry
	ociLayout     []byte
	ociTops       []ociDescriptor
	ociIndexExtra map[string]json.RawMessage

	Images []*archiveImage

	// fallbackReason, when non-empty, means the archive cannot be safely
	// compared image by image (e.g. its names are ambiguous). Callers must
	// fall back to importing the whole archive on every node.
	fallbackReason string
}

// Comparable reports whether per-image decisions can be made for this
// archive.
func (a *Archive) Comparable() bool { return a.fallbackReason == "" }

// FallbackReason returns why the archive cannot be compared, if it cannot.
func (a *Archive) FallbackReason() string { return a.fallbackReason }

// Path returns the path the archive was parsed from.
func (a *Archive) Path() string { return a.path }

// ParseArchive reads and validates an image archive from disk. Any error
// means the archive is malformed or truncated, and it is returned before
// any node is modified.
func ParseArchive(archivePath string) (*Archive, error) {
	f, err := os.Open(archivePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	a := &Archive{
		path:     archivePath,
		members:  map[string]*archiveMember{},
		symlinks: map[string]string{},
	}

	counted := &countingReader{r: f}
	tr := tar.NewReader(counted)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read image archive %q", archivePath)
		}
		name := path.Clean(strings.TrimPrefix(hdr.Name, "./"))
		// hdr.Name is inside the archive; reject absolute/parent paths.
		if strings.HasPrefix(name, "../") || path.IsAbs(name) {
			return nil, errors.Errorf("image archive %q contains unsafe path %q", archivePath, hdr.Name)
		}
		switch hdr.Typeflag {
		case tar.TypeSymlink:
			a.symlinks[name] = resolveArchiveLink(name, hdr.Linkname)
			continue
		case tar.TypeDir, tar.TypeXHeader, tar.TypeXGlobalHeader:
			continue
		}
		if hdr.Typeflag != tar.TypeReg && hdr.Typeflag != tar.TypeRegA { //nolint:staticcheck
			continue
		}

		offset := counted.n
		hasher := sha256.New()
		n, err := io.Copy(hasher, tr)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read %q from image archive %q", name, archivePath)
		}
		if n != hdr.Size {
			return nil, errors.Errorf("image archive %q is truncated: %q has %d of %d bytes", archivePath, name, n, hdr.Size)
		}

		a.members[name] = &archiveMember{
			name:   name,
			size:   hdr.Size,
			offset: offset,
			digest: digestOf(hasher),
		}
	}

	// Parse the archive's metadata files. They may appear anywhere in the
	// tar, so this happens after the single sequential pass.
	manifestMember, hasManifest := a.memberFor(dockerManifestName)
	layoutMember, hasLayout := a.memberFor(ociLayoutFileName)
	indexMember, hasIndex := a.memberFor(ociIndexFileName)
	if hasManifest {
		data, err := a.readMemberData(manifestMember)
		if err != nil {
			return nil, err
		}
		var entries []dockerManifestEntry
		if err := json.Unmarshal(data, &entries); err != nil {
			return nil, errors.Wrapf(err, "failed to parse %s in image archive %q", dockerManifestName, archivePath)
		}
		a.dockerEntries = entries
	}
	if hasLayout {
		data, err := a.readMemberData(layoutMember)
		if err != nil {
			return nil, err
		}
		a.ociLayout = data
	}
	if hasIndex {
		data, err := a.readMemberData(indexMember)
		if err != nil {
			return nil, err
		}
		var index ociIndex
		if err := json.Unmarshal(data, &index); err != nil {
			return nil, errors.Wrapf(err, "failed to parse %s in image archive %q", ociIndexFileName, archivePath)
		}
		a.ociTops = index.Manifests
		// Keep the raw bytes so a subset index.json can preserve any
		// top-level fields (annotations, subject) we do not model.
		if err := json.Unmarshal(data, &a.ociIndexExtra); err != nil {
			return nil, errors.Wrapf(err, "failed to parse %s in image archive %q", ociIndexFileName, archivePath)
		}
	}

	switch {
	case hasLayout || hasIndex:
		if !hasLayout || !hasIndex {
			return nil, errors.Errorf("image archive %q has an incomplete OCI layout", archivePath)
		}
		if err := a.buildOCIImages(); err != nil {
			return nil, errors.Wrapf(err, "invalid OCI image archive %q", archivePath)
		}
	case hasManifest:
		if err := a.buildDockerImages(); err != nil {
			return nil, errors.Wrapf(err, "invalid image archive %q", archivePath)
		}
	default:
		return nil, errors.Errorf("image archive %q does not contain a manifest.json or an OCI layout", archivePath)
	}

	a.verifyBlobs()
	// A tag claimed by two different contents is ambiguous: which one wins
	// depends on import order. Import everything rather than guess.
	if a.fallbackReason == "" {
		seen := map[string]bool{}
		for _, img := range a.Images {
			for _, tag := range img.Tags {
				if seen[tag] {
					a.fallbackReason = fmt.Sprintf("tag %q is used by more than one image in the archive", tag)
					break
				}
				seen[tag] = true
			}
			if a.fallbackReason != "" {
				break
			}
		}
	}
	if len(a.Images) == 0 {
		return nil, errors.Errorf("image archive %q contains no images", archivePath)
	}
	return a, nil
}

func (a *Archive) buildDockerImages() error {
	byConfig := map[string]*archiveImage{}
	order := []string{}
	for _, entry := range a.dockerEntries {
		configMember, ok := a.memberFor(entry.Config)
		if !ok {
			return errors.Errorf("config %q referenced by %s is missing", entry.Config, dockerManifestName)
		}
		tags := make([]string, 0, len(entry.RepoTags))
		for _, raw := range entry.RepoTags {
			name, err := normalizeImageName(raw)
			if err != nil {
				return errors.Wrapf(err, "invalid repo tag %q", raw)
			}
			tags = appendUnique(tags, name)
		}

		img, ok := byConfig[configMember.digest]
		if !ok {
			img = &archiveImage{
				ConfigDigests: []string{configMember.digest},
				dockerConfigs: []string{configMember.digest},
			}
			byConfig[configMember.digest] = img
			order = append(order, configMember.digest)
		}
		for _, tag := range tags {
			img.Tags = appendUnique(img.Tags, tag)
		}
		img.Members = appendUnique(img.Members, configMember.name)
		for _, layer := range entry.Layers {
			layerMember, ok := a.memberFor(layer)
			if !ok {
				return errors.Errorf("layer %q referenced by %s is missing", layer, dockerManifestName)
			}
			img.Members = appendUnique(img.Members, layerMember.name)
		}
	}
	for _, key := range order {
		a.Images = append(a.Images, byConfig[key])
	}
	return nil
}

func (a *Archive) buildOCIImages() error {
	const (
		imageNameAnnotation = "io.containerd.image.name"
		refNameAnnotation   = "org.opencontainers.image.ref.name"
	)
	byTop := map[string]*archiveImage{}
	order := []string{}

	for _, desc := range a.ociTops {
		var names []string
		for _, key := range []string{imageNameAnnotation, refNameAnnotation} {
			value := desc.Annotations[key]
			if value == "" {
				continue
			}
			if key == refNameAnnotation && !strings.ContainsAny(value, "/:") {
				// The OCI ref.name annotation only carries the tag; without a
				// repository there is no way to know the image's name. Do not
				// guess: mark the archive as non-comparable so callers import
				// the whole archive on every node instead of comparing.
				if a.fallbackReason == "" {
					a.fallbackReason = fmt.Sprintf("OCI annotation %q holds only a tag (%q); the repository name is unknown", key, value)
				}
				continue
			}
			name, err := normalizeImageName(value)
			if err != nil {
				return errors.Wrapf(err, "invalid image name annotation %q", value)
			}
			names = appendUnique(names, name)
		}

		img, ok := byTop[desc.Digest]
		if !ok {
			img = &archiveImage{TargetDigests: []string{desc.Digest}, OCITopDigests: []string{desc.Digest}}
			byTop[desc.Digest] = img
			order = append(order, desc.Digest)
		}
		for _, name := range names {
			img.Tags = appendUnique(img.Tags, name)
		}
		if err := a.collectOCIContent(desc, img, 0); err != nil {
			return err
		}
	}

	for _, key := range order {
		a.Images = append(a.Images, byTop[key])
	}
	return nil
}

// collectOCIContent walks an OCI descriptor and records the config digests
// and archive members needed to import it.
func (a *Archive) collectOCIContent(desc ociDescriptor, img *archiveImage, depth int) error {
	if depth > 4 {
		return errors.Errorf("OCI descriptor nesting is too deep at %s", desc.Digest)
	}
	member, ok := a.memberFor(blobName(desc.Digest))
	if !ok {
		return errors.Errorf("blob %s is missing from the archive", desc.Digest)
	}
	img.Members = appendUnique(img.Members, member.name)
	data, err := a.readMemberData(member)
	if err != nil {
		return err
	}

	switch desc.MediaType {
	case "application/vnd.oci.image.manifest.v1+json",
		"application/vnd.docker.distribution.manifest.v2+json":
		var manifest ociManifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			return errors.Wrapf(err, "failed to parse manifest %s", desc.Digest)
		}
		configMember, ok := a.memberFor(blobName(manifest.Config.Digest))
		if !ok {
			return errors.Errorf("config blob %s is missing from the archive", manifest.Config.Digest)
		}
		img.Members = appendUnique(img.Members, configMember.name)
		img.ConfigDigests = appendUnique(img.ConfigDigests, configMember.digest)
		for _, layer := range manifest.Layers {
			layerMember, ok := a.memberFor(blobName(layer.Digest))
			if !ok {
				return errors.Errorf("layer blob %s is missing from the archive", layer.Digest)
			}
			img.Members = appendUnique(img.Members, layerMember.name)
		}
		return nil
	case "application/vnd.oci.image.index.v1+json",
		"application/vnd.docker.distribution.manifest.list.v2+json":
		var index ociIndex
		if err := json.Unmarshal(data, &index); err != nil {
			return errors.Wrapf(err, "failed to parse image index %s", desc.Digest)
		}
		if len(index.Manifests) == 0 {
			return errors.Errorf("image index %s has no manifests", desc.Digest)
		}
		for _, child := range index.Manifests {
			if err := a.collectOCIContent(child, img, depth+1); err != nil {
				return err
			}
		}
		return nil
	default:
		return errors.Errorf("unsupported OCI media type %q for blob %s", desc.MediaType, desc.Digest)
	}
}

// verifyBlobs checks that every OCI blob name encodes the digest and size
// we observed, catching corruption beyond simple truncation.
func (a *Archive) verifyBlobs() {
	if len(a.ociLayout) == 0 {
		return
	}
	for name, member := range a.members {
		if !strings.HasPrefix(name, "blobs/") {
			continue
		}
		parts := strings.Split(strings.TrimPrefix(name, "blobs/"), "/")
		if len(parts) != 2 {
			continue
		}
		expected := parts[0] + ":" + parts[1]
		if member.digest != expected && a.fallbackReason == "" {
			a.fallbackReason = fmt.Sprintf("blob %q has digest %s, expected %s", name, member.digest, expected)
		}
	}
}

// readMemberData reads a (small) member's contents from the archive, using
// the offset recorded during the parse pass.
func (a *Archive) readMemberData(member *archiveMember) ([]byte, error) {
	if member.size > maxMetadataFileSize {
		return nil, errors.Errorf("image archive %q: file %q is too large to inspect (%d bytes)", a.path, member.name, member.size)
	}
	f, err := os.Open(a.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if _, err := f.Seek(member.offset, io.SeekStart); err != nil {
		return nil, err
	}
	data := make([]byte, member.size)
	if _, err := io.ReadFull(f, data); err != nil {
		return nil, errors.Wrapf(err, "failed to read %q from image archive %q", member.name, a.path)
	}
	return data, nil
}

// memberFor returns the regular file with the given name, following
// symlinks.
func (a *Archive) memberFor(name string) (*archiveMember, bool) {
	name = path.Clean(strings.TrimPrefix(name, "./"))
	for i := 0; i < 8; i++ {
		if link, ok := a.symlinks[name]; ok {
			name = link
			continue
		}
		member, ok := a.members[name]
		return member, ok
	}
	return nil, false
}

func resolveArchiveLink(name, linkname string) string {
	if path.IsAbs(linkname) {
		return path.Clean(linkname)
	}
	return path.Clean(path.Join(path.Dir(name), linkname))
}

func blobName(dgst string) string {
	parts := strings.SplitN(dgst, ":", 2)
	if len(parts) != 2 {
		return dgst
	}
	return path.Join("blobs", parts[0], parts[1])
}

func digestOf(h hash.Hash) string {
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

func appendUnique(list []string, value string) []string {
	for _, existing := range list {
		if existing == value {
			return list
		}
	}
	return append(list, value)
}

// countingReader tracks how many bytes have been consumed, so the data
// offset of each tar entry can be recorded.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
