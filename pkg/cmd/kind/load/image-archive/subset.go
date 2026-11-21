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
	"bytes"
	"encoding/json"
	"io"
	"os"
	"time"

	"sigs.k8s.io/kind/pkg/errors"
)

// SubsetMembers returns the distinct archive member names needed to import
// the given images.
func (a *Archive) SubsetMembers(images []*archiveImage) []string {
	seen := map[string]bool{}
	var names []string
	for _, img := range images {
		for _, name := range img.Members {
			if seen[name] {
				continue
			}
			seen[name] = true
			names = append(names, name)
		}
	}
	return names
}

// WriteSubset writes a valid archive containing only the given images to w.
//
// This is used when the runtime cannot be told to import a subset of an
// archive directly: the archive is rewritten on the fly, streaming only the
// members in use, so no per-node temporary copy is needed.
func (a *Archive) WriteSubset(w io.Writer, images []*archiveImage) error {
	members := a.SubsetMembers(images)
	tw := tar.NewWriter(w)

	if len(a.ociLayout) > 0 {
		// OCI layout: oci-layout, a rewritten index.json, and blobs.
		if err := writeTarFile(tw, ociLayoutFileName, a.ociLayout); err != nil {
			return err
		}
		index, err := a.subsetOCIIndex(images)
		if err != nil {
			return err
		}
		if err := writeTarFile(tw, ociIndexFileName, index); err != nil {
			return err
		}
	} else {
		// docker save: manifest.json (rewritten) plus configs and layers.
		manifest, err := a.subsetDockerManifest(images)
		if err != nil {
			return err
		}
		if err := writeTarFile(tw, dockerManifestName, manifest); err != nil {
			return err
		}
	}

	// Stream the blob bodies from a fresh handle per call so that concurrent
	// per-node imports never share a read offset. Members are written in the
	// order collected, which preserves the archive's own ordering.
	f, err := os.Open(a.path)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, name := range members {
		member, ok := a.members[name]
		if !ok {
			return errors.Errorf("internal error: missing archive member %q", name)
		}
		hdr := &tar.Header{
			Name:     member.name,
			Mode:     0o644,
			Size:     member.size,
			Typeflag: tar.TypeReg,
			Format:   tar.FormatPAX,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if _, err := f.Seek(member.offset, io.SeekStart); err != nil {
			return err
		}
		if _, err := io.CopyN(tw, f, member.size); err != nil {
			return errors.Wrapf(err, "failed to copy %q from image archive", member.name)
		}
	}
	return tw.Close()
}

func writeTarFile(tw *tar.Writer, name string, data []byte) error {
	hdr := &tar.Header{
		Name:     name,
		Mode:     0o644,
		Size:     int64(len(data)),
		Typeflag: tar.TypeReg,
		ModTime:  time.Unix(0, 0),
		Format:   tar.FormatPAX,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	_, err := tw.Write(data)
	return err
}

// subsetOCIIndex builds an index.json referencing only the given images.
func (a *Archive) subsetOCIIndex(images []*archiveImage) ([]byte, error) {
	wanted := map[string]bool{}
	for _, img := range images {
		for _, digest := range img.OCITopDigests {
			wanted[digest] = true
		}
	}
	index := ociIndex{SchemaVersion: 2}
	for _, desc := range a.ociTops {
		if !wanted[desc.Digest] {
			continue
		}
		index.Manifests = append(index.Manifests, desc)
	}
	if len(index.Manifests) == 0 {
		return nil, errors.New("internal error: no OCI manifests selected")
	}
	// Preserve top-level fields we do not model (annotations and the like)
	// from the source index, replacing only the manifest list.
	if a.ociIndexExtra != nil {
		indexBytes, err := json.Marshal(index)
		if err != nil {
			return nil, err
		}
		var merged map[string]json.RawMessage
		if err := json.Unmarshal(indexBytes, &merged); err != nil {
			return nil, err
		}
		for key, value := range a.ociIndexExtra {
			if _, ok := merged[key]; ok {
				continue
			}
			merged[key] = value
		}
		return json.Marshal(merged)
	}
	return json.Marshal(index)
}

// subsetDockerManifest builds a manifest.json for only the given images.
//
// Paths are rewritten to the archive members actually streamed: docker
// archives may reference configs/layers through non-canonical names (a
// leading "./", or a v1.1 symlink), and the subset streams regular files
// only. The importer looks these paths up by name, so the rewrite is what
// keeps the subset importable.
func (a *Archive) subsetDockerManifest(images []*archiveImage) ([]byte, error) {
	wanted := map[string]bool{}
	for _, img := range images {
		for _, config := range img.dockerConfigs {
			wanted[config] = true
		}
	}
	var entries []dockerManifestEntry
	for _, entry := range a.dockerEntries {
		member, ok := a.memberFor(entry.Config)
		if !ok || !wanted[member.digest] {
			continue
		}
		rewritten := dockerManifestEntry{
			Config:   member.name,
			RepoTags: entry.RepoTags,
		}
		for _, layer := range entry.Layers {
			layerMember, ok := a.memberFor(layer)
			if !ok {
				return nil, errors.Errorf("internal error: missing archive member %q", layer)
			}
			rewritten.Layers = append(rewritten.Layers, layerMember.name)
		}
		entries = append(entries, rewritten)
	}
	if len(entries) == 0 {
		return nil, errors.New("internal error: no docker manifest entries selected")
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	if err := enc.Encode(entries); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}
