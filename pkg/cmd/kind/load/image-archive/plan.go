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
	"strings"

	"sigs.k8s.io/kind/pkg/cluster/nodeutils"
	"sigs.k8s.io/kind/pkg/errors"
)

// A tag fix points an archive name at content the node already has.
type tagFix struct {
	// tag is the normalized archive name that should exist.
	tag string
	// source is a reference already known to the node runtime that resolves
	// to the desired content.
	source string
	// contentID is the runtime image ID the tag will point at.
	contentID string
}

// nodePlan is the per-node work derived from one archive.
type nodePlan struct {
	// imports are images whose content is missing and must be imported.
	imports []*archiveImage
	// tagOnly are images whose content is present but whose names are not all
	// correct. They are fixed without importing.
	tagOnly []*archiveImage
	// unchanged are images whose content and tags are both already present.
	unchanged []*archiveImage
	// fixes are the individual tag operations to perform.
	fixes []tagFix
}

// decide compares one archive with one node's image list and returns the
// work required. It is pure: it never talks to the node.
//
// Content and names are judged separately:
//   - content is present if any node image has a config digest from the
//     archive, or carries a digest reference into the archive's content.
//   - a tag is correct if the node has that name and it resolves to the
//     archive's content for that image.
//
// A tag that exists but points at other content needs a re-tag (name
// ownership changes); a missing tag with content present needs only a tag.
// Either way the content need not be imported.
func decide(a *Archive, list *nodeutils.ImageList) nodePlan {
	plan := nodePlan{}
	for _, img := range a.Images {
		source, contentID := findContentReference(list, img)
		contentPresent := contentID != ""

		if !contentPresent {
			// One import covers all of this image's tags; the importer
			// creates or repoints them.
			plan.imports = append(plan.imports, img)
			continue
		}

		var imageFixes []tagFix
		for _, tag := range img.Tags {
			existing := list.FindByTag(tag)
			if existing != nil && imageMatchesContent(existing, img) {
				continue
			}
			imageFixes = append(imageFixes, tagFix{
				tag:       tag,
				source:    source,
				contentID: contentID,
			})
		}

		if len(imageFixes) == 0 && len(img.Tags) > 0 {
			plan.unchanged = append(plan.unchanged, img)
			continue
		}
		if len(imageFixes) == 0 {
			// No names at all in the archive and the content exists.
			plan.unchanged = append(plan.unchanged, img)
			continue
		}
		plan.tagOnly = append(plan.tagOnly, img)
		plan.fixes = append(plan.fixes, imageFixes...)
	}
	// Drop tag fixes for images that are going to be imported anyway; the
	// import sets their names. This can only happen when the same tag is
	// reachable from two archive entries, which is not expected, but keeping
	// the invariant avoids surprise double work.
	if len(plan.imports) > 0 {
		imported := map[string]bool{}
		for _, img := range plan.imports {
			for _, tag := range img.Tags {
				imported[tag] = true
			}
		}
		kept := plan.fixes[:0]
		for _, fix := range plan.fixes {
			if !imported[fix.tag] {
				kept = append(kept, fix)
			}
		}
		plan.fixes = kept
		// An image is tag-only only if it still has a fix to perform.
		remaining := map[string]bool{}
		for _, fix := range plan.fixes {
			remaining[fix.tag] = true
		}
		keptImages := plan.tagOnly[:0]
		for _, img := range plan.tagOnly {
			for _, tag := range img.Tags {
				if remaining[tag] {
					keptImages = append(keptImages, img)
					break
				}
			}
		}
		plan.tagOnly = keptImages
	}
	return plan
}

// findContentReference returns a reference on the node that resolves to the
// archive image's content, plus the runtime image ID of that content.
func findContentReference(list *nodeutils.ImageList, img *archiveImage) (reference, contentID string) {
	wanted := map[string]bool{}
	for _, digest := range img.ConfigDigests {
		wanted[digest] = true
	}
	for _, digest := range img.TargetDigests {
		wanted[digest] = true
	}
	for i := range list.Images {
		entry := &list.Images[i]
		if !imageMatchesContent(entry, img) {
			continue
		}
		id := entry.ID
		if id == "" {
			id = digestMatching(entry, wanted)
		}
		if ref := nodeReference(entry); ref != "" {
			return ref, id
		}
		// Content is present but has no name; we cannot refer to it to tag.
		// Treat as missing so the import creates a usable name.
		return "", ""
	}
	return "", ""
}

// imageMatchesContent reports whether a node image holds the archive image's
// content. Only same-level digests are compared: the node's image ID is a
// config digest, and repo digests may be top level manifest or index
// digests. Comparing a config digest to an index digest would never match.
func imageMatchesContent(entry *nodeutils.Image, img *archiveImage) bool {
	for _, digest := range img.ConfigDigests {
		if entry.ID == digest {
			return true
		}
	}
	for _, ref := range entry.RepoDigests {
		digest, ok := nodeutils.DigestFromReference(ref)
		if !ok {
			continue
		}
		for _, configDigest := range img.ConfigDigests {
			if digest == configDigest {
				return true
			}
		}
		for _, targetDigest := range img.TargetDigests {
			if digest == targetDigest {
				return true
			}
		}
	}
	return false
}

func digestMatching(entry *nodeutils.Image, wanted map[string]bool) string {
	for _, ref := range entry.RepoDigests {
		if digest, ok := nodeutils.DigestFromReference(ref); ok && wanted[digest] {
			return digest
		}
	}
	return ""
}

// nodeReference returns a name the runtime can resolve back to the image.
func nodeReference(entry *nodeutils.Image) string {
	if len(entry.RepoTags) > 0 {
		return entry.RepoTags[0]
	}
	if len(entry.RepoDigests) > 0 {
		return entry.RepoDigests[0]
	}
	return ""
}

// normalizeImageName normalizes a docker or OCI reference the same way the
// node runtime does, so names from the archive and names reported by the node
// are directly comparable. Digest references are kept as `repo@digest`.
func normalizeImageName(image string) (string, error) {
	const (
		defaultDomain    = "docker.io/"
		officialRepoName = "library"
	)
	if image == "" {
		return "", errors.Errorf("empty image name")
	}
	name, digest := image, ""
	if i := strings.Index(name, "@"); i >= 0 {
		name, digest = name[:i], name[i:]
	}

	normalized := name
	if !strings.ContainsRune(normalized, '/') {
		normalized = officialRepoName + "/" + normalized
	}
	if i := strings.IndexRune(normalized, '/'); i == -1 ||
		(!strings.ContainsAny(normalized[:i], ".:") && normalized[:i] != "localhost") {
		normalized = defaultDomain + normalized
	}
	if digest != "" {
		return normalized + digest, nil
	}
	if !hasTag(normalized) {
		normalized += ":latest"
	}
	return normalized, nil
}

func hasTag(image string) bool {
	lastSlash := strings.LastIndex(image, "/")
	lastColon := strings.LastIndex(image, ":")
	return lastColon > lastSlash
}
