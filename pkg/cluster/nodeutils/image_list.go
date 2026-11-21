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

package nodeutils

import (
	"bytes"
	"encoding/json"
	"strings"

	"sigs.k8s.io/kind/pkg/cluster/nodes"
	"sigs.k8s.io/kind/pkg/errors"
)

// Image is one image reported by a node's container runtime.
//
// ID is the runtime's image ID. For containerd this is the digest of the
// image config, i.e. per-platform content, not the digest of a
// multi-platform index. RepoTags are the names the image is referenced by
// (normalized to include the registry and tag), and RepoDigests are
// immutable digest references to the runtime's top level target, which may
// be a manifest or an index digest depending on how the image was imported
// or pulled.
type Image struct {
	ID          string
	RepoTags    []string
	RepoDigests []string
}

// ImageList is a snapshot of the images known to a node's runtime.
//
// Callers should query it once per node and keep it up to date from the
// results of their own operations instead of re-querying the runtime.
type ImageList struct {
	Images []Image
}

// FindByTag returns the image carrying the given repo tag, or nil.
func (l *ImageList) FindByTag(tag string) *Image {
	for i := range l.Images {
		for _, t := range l.Images[i].RepoTags {
			if t == tag {
				return &l.Images[i]
			}
		}
	}
	return nil
}

// AddTag records that tag now points at the image with ID id, without
// querying the runtime. If the image is already known its entry is updated,
// otherwise a minimal entry is added.
func (l *ImageList) AddTag(tag, id string) {
	for i := range l.Images {
		img := &l.Images[i]
		if img.ID == id {
			if !containsString(img.RepoTags, tag) {
				img.RepoTags = append(img.RepoTags, tag)
			}
			return
		}
	}
	l.Images = append(l.Images, Image{ID: id, RepoTags: []string{tag}})
}

// AddImported records the result of importing an image. configDigests are
// the archive's per-platform config digests for the image; the first is used
// as the runtime image ID the runtime will report for the node's platform,
// which is what later tag decisions compare against. tags are the names the
// import created.
func (l *ImageList) AddImported(tags []string, configDigests []string) {
	id := ""
	if len(configDigests) > 0 {
		id = configDigests[0]
	}
	for i := range l.Images {
		img := &l.Images[i]
		if img.ID != id {
			continue
		}
		for _, tag := range tags {
			if !containsString(img.RepoTags, tag) {
				img.RepoTags = append(img.RepoTags, tag)
			}
		}
		return
	}
	l.Images = append(l.Images, Image{
		ID:       id,
		RepoTags: append([]string(nil), tags...),
	})
}

// DigestFromReference returns the digest part of a `repo@digest` reference.
func DigestFromReference(ref string) (string, bool) {
	if i := strings.LastIndex(ref, "@"); i >= 0 {
		return ref[i+1:], true
	}
	return "", false
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// ListImages queries the node runtime once and returns its complete image
// list. An error here means the caller cannot know what the node already
// has; it must not be treated as an empty list.
func ListImages(n nodes.Node) (*ImageList, error) {
	var out bytes.Buffer
	if err := n.Command("crictl", "images", "--output=json").SetStdout(&out).Run(); err != nil {
		return nil, errors.Wrapf(err, "failed to list images on node %q", n.String())
	}
	// The CRI JSON encoding uses lowerCamelCase field names today, but
	// older/derived tools have emitted snake_case, so accept both.
	raw := struct {
		Images []json.RawMessage `json:"images"`
	}{}
	if err := json.Unmarshal(out.Bytes(), &raw); err != nil {
		return nil, errors.Wrapf(err, "failed to parse image list from node %q", n.String())
	}
	list := &ImageList{Images: make([]Image, 0, len(raw.Images))}
	for _, entry := range raw.Images {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(entry, &fields); err != nil {
			return nil, errors.Wrapf(err, "failed to parse image entry from node %q", n.String())
		}
		img := Image{
			ID:          firstJSONString(fields, "id", "ID"),
			RepoTags:    firstJSONStringSlice(fields, "repoTags", "repo_tags", "RepoTags"),
			RepoDigests: firstJSONStringSlice(fields, "repoDigests", "repo_digests", "RepoDigests"),
		}
		list.Images = append(list.Images, img)
	}
	return list, nil
}

func firstJSONString(fields map[string]json.RawMessage, keys ...string) string {
	for _, key := range keys {
		raw, ok := fields[key]
		if !ok {
			continue
		}
		var s string
		if err := json.Unmarshal(raw, &s); err == nil {
			return s
		}
	}
	return ""
}

func firstJSONStringSlice(fields map[string]json.RawMessage, keys ...string) []string {
	for _, key := range keys {
		raw, ok := fields[key]
		if !ok {
			continue
		}
		var s []string
		if err := json.Unmarshal(raw, &s); err == nil {
			return s
		}
	}
	return nil
}
