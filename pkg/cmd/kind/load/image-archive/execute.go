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
	"fmt"
	"io"
	"os"
	"strings"

	"sigs.k8s.io/kind/pkg/cluster/nodes"
	"sigs.k8s.io/kind/pkg/cluster/nodeutils"
	"sigs.k8s.io/kind/pkg/errors"
	"sigs.k8s.io/kind/pkg/log"
)

// applyResult summarizes what was done to one node.
type applyResult struct {
	imported  int
	tagOnly   int
	unchanged int
	tagsAdded int
}

// loadArchiveToNode reconciles one node with one archive:
//
//  1. query the node's image list exactly once,
//  2. decide, from that snapshot, which images need importing and which need
//     only tag repairs,
//  3. import the missing content as one streamed subset, then perform tag
//     repairs,
//  4. update the local snapshot from the operations performed.
//
// It never re-queries the node. A query failure is returned as an error and
// the node is left untouched: it is neither assumed empty nor assumed done.
func loadArchiveToNode(logger log.Logger, archive *Archive, node nodes.Node) (applyResult, error) {
	var result applyResult

	list, err := nodeutils.ListImages(node)
	if err != nil {
		logger.Errorf("failed to list images on node %q, not modifying it: %v", node.String(), err)
		return result, errors.Wrapf(err, "failed to inspect images on node %q", node.String())
	}

	plan := decide(archive, list)
	result.imported = len(plan.imports)
	result.tagOnly = len(plan.tagOnly)
	result.unchanged = len(plan.unchanged)
	result.tagsAdded = len(plan.fixes)

	for _, img := range plan.unchanged {
		logger.V(0).Infof("Node %q: image %s already present (content and tags); skipping", node.String(), describeImage(img))
	}

	if len(plan.imports) > 0 {
		logger.V(0).Infof("Node %q: importing %d image(s) not fully present: %s", node.String(), len(plan.imports), describeImages(plan.imports))
		importer := nodeutils.NewImageImporter(node)
		if err := importer.ImportStream(func(w io.Writer) error {
			return archive.WriteSubset(w, plan.imports)
		}); err != nil {
			// Tag repairs cannot make up for missing content. Stop touching
			// this node and report; other nodes are unaffected.
			return result, errors.Wrapf(err, "failed to import images into node %q", node.String())
		}
		for _, img := range plan.imports {
			list.AddImported(img.Tags, img.ConfigDigests)
		}
	}

	for _, img := range plan.tagOnly {
		logger.V(0).Infof("Node %q: image %s content present, adding missing tag(s); not importing content", node.String(), describeImage(img))
	}

	for _, fix := range plan.fixes {
		if err := nodeutils.TagImage(node, fix.source, fix.tag); err != nil {
			return result, errors.Wrapf(err, "failed to tag %q on node %q", fix.tag, node.String())
		}
		list.AddTag(fix.tag, fix.contentID)
	}

	return result, nil
}

// importWholeArchive is the conservative fallback used when per-image
// comparison is not safe for this archive. Content is still fully imported;
// only the skipping optimization is disabled.
//
// The original file is streamed byte for byte, so the fallback behaves
// exactly like the historical implementation.
func importWholeArchive(logger log.Logger, archive *Archive, node nodes.Node) (applyResult, error) {
	var result applyResult
	logger.V(0).Infof("Node %q: importing the whole archive (%s)", node.String(), archive.FallbackReason())
	importer := nodeutils.NewImageImporter(node)
	if err := importer.ImportStream(func(w io.Writer) error {
		f, err := os.Open(archive.Path())
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(w, f)
		return err
	}); err != nil {
		return result, errors.Wrapf(err, "failed to import images into node %q", node.String())
	}
	result.imported = len(archive.Images)
	return result, nil
}

func describeImage(img *archiveImage) string {
	return describeImages([]*archiveImage{img})
}

func describeImages(images []*archiveImage) string {
	names := make([]string, 0, len(images))
	for _, img := range images {
		if len(img.Tags) == 0 {
			names = append(names, "<untagged>")
			continue
		}
		names = append(names, strings.Join(img.Tags, ","))
	}
	return strings.Join(names, "; ")
}

// summarize renders a human readable per-node summary for the final log line.
func summarize(node string, result applyResult) string {
	return fmt.Sprintf(
		"node %q: %d image(s) imported, %d image(s) tag-only (%d tag(s) added), %d image(s) content and tags already present",
		node, result.imported, result.tagOnly, result.tagsAdded, result.unchanged,
	)
}
