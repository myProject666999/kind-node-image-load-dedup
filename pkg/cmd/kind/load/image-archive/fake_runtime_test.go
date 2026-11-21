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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"sync"

	"sigs.k8s.io/kind/pkg/cluster/nodes"
	"sigs.k8s.io/kind/pkg/exec"
)

// fakeRuntime is an in-memory stand-in for a node's containerd + crictl.
//
// It models content and name ownership separately (deleting a name does not
// delete content), records every import and tag call, and exposes its image
// list in the shape crictl reports. Tests assert on the calls, not just the
// final state, so a regression that re-imports everything cannot pass.
type fakeRuntime struct {
	mu sync.Mutex

	images map[string]*fakeImage // keyed by runtime image ID
	order  []string
	names  map[string]string // repo tag -> image ID

	imports []importCall
	tags    []tagCall
	// listCalls counts image list queries, so tests can assert the list is
	// queried exactly once per node.
	listCalls int
	// listErr makes the image list query fail.
	listErr error
}

type fakeImage struct {
	id string
	// topDigests are exposed as repo digests (index/manifest level).
	topDigests []string
}

type importCall struct {
	node    string
	tags    []string
	members []string
}

type tagCall struct {
	node   string
	source string
	target string
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{
		images: map[string]*fakeImage{},
		names:  map[string]string{},
	}
}

// seed adds content with the given tags and top-level digest references.
// No import call is recorded; this models pre-existing node state.
func (r *fakeRuntime) seed(id string, tags []string, topDigests ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	img, ok := r.images[id]
	if !ok {
		img = &fakeImage{id: id}
		r.images[id] = img
		r.order = append(r.order, id)
	}
	for _, d := range topDigests {
		img.topDigests = appendUnique(img.topDigests, d)
	}
	for _, tag := range tags {
		r.names[tag] = id
	}
}

func (r *fakeRuntime) snapshot() (imports []importCall, tags []tagCall) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]importCall(nil), r.imports...), append([]tagCall(nil), r.tags...)
}

func (r *fakeRuntime) queries() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.listCalls
}

func (r *fakeRuntime) taggedImages() map[string]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[string]string{}
	for tag, id := range r.names {
		out[tag] = id
	}
	return out
}

// fakeNode implements nodes.Node backed by a fakeRuntime.
type fakeNode struct {
	name string
	rt   *fakeRuntime
}

var _ nodes.Node = (*fakeNode)(nil)

func (n *fakeNode) String() string               { return n.name }
func (n *fakeNode) Role() (string, error)        { return "control-plane", nil }
func (n *fakeNode) IP() (string, string, error)  { return "127.0.0.1", "", nil }
func (n *fakeNode) SerialLogs(w io.Writer) error { return nil }

func (n *fakeNode) Command(name string, args ...string) exec.Cmd {
	return &fakeCmd{node: n, name: name, args: args}
}

func (n *fakeNode) CommandContext(_ context.Context, name string, args ...string) exec.Cmd {
	return &fakeCmd{node: n, name: name, args: args}
}

type fakeCmd struct {
	node   *fakeNode
	name   string
	args   []string
	stdin  io.Reader
	stdout io.Writer
}

var _ exec.Cmd = (*fakeCmd)(nil)

func (c *fakeCmd) SetEnv(...string) exec.Cmd { return c }
func (c *fakeCmd) SetStdin(r io.Reader) exec.Cmd {
	c.stdin = r
	return c
}
func (c *fakeCmd) SetStdout(w io.Writer) exec.Cmd {
	c.stdout = w
	return c
}
func (c *fakeCmd) SetStderr(w io.Writer) exec.Cmd { return c }

func (c *fakeCmd) Run() error {
	rt := c.node.rt
	rt.mu.Lock()
	defer rt.mu.Unlock()

	switch {
	case c.name == "containerd" && len(c.args) == 2 && c.args[0] == "config":
		_, err := io.WriteString(c.stdout, fakeContainerdConfig)
		return err
	case c.name == "crictl" && len(c.args) == 2 && c.args[0] == "images":
		rt.listCalls++
		if rt.listErr != nil {
			return rt.listErr
		}
		return c.writeImageList()
	case c.name == "ctr" && hasArgs(c.args, "images", "import"):
		return c.runImport()
	case c.name == "ctr" && hasArgs(c.args, "images", "tag"):
		return c.runTag()
	}
	return fmt.Errorf("fake runtime: unsupported command %q %v", c.name, c.args)
}

func (c *fakeCmd) writeImageList() error {
	rt := c.node.rt
	type jsonImage struct {
		ID          string   `json:"id"`
		RepoTags    []string `json:"repoTags"`
		RepoDigests []string `json:"repoDigests"`
	}
	images := []jsonImage{}
	for _, id := range rt.order {
		img := rt.images[id]
		tags := []string{}
		for tag, owner := range rt.names {
			if owner == id {
				tags = append(tags, tag)
			}
		}
		sort.Strings(tags)
		digests := make([]string, 0, len(img.topDigests))
		for _, digest := range img.topDigests {
			digests = append(digests, "docker.io/library/img@"+digest)
		}
		images = append(images, jsonImage{ID: id, RepoTags: tags, RepoDigests: digests})
	}
	b, err := json.Marshal(map[string]interface{}{"images": images})
	if err != nil {
		return err
	}
	_, err = c.stdout.Write(b)
	return err
}

// runImport consumes the streamed archive, validates it the way a real
// importer would, applies it and records the call.
func (c *fakeCmd) runImport() error {
	rt := c.node.rt
	parsed, err := parseStreamedArchive(c.stdin)
	if err != nil {
		return fmt.Errorf("fake runtime: invalid import archive: %w", err)
	}
	rt.imports = append(rt.imports, importCall{
		node:    c.node.name,
		tags:    parsed.tags,
		members: parsed.members,
	})
	for _, imported := range parsed.images {
		img, ok := rt.images[imported.id]
		if !ok {
			img = &fakeImage{id: imported.id}
			rt.images[imported.id] = img
			rt.order = append(rt.order, imported.id)
		}
		for _, d := range imported.topDigests {
			img.topDigests = appendUnique(img.topDigests, d)
		}
		for _, tag := range imported.tags {
			rt.names[tag] = imported.id
		}
	}
	return nil
}

func (c *fakeCmd) runTag() error {
	rt := c.node.rt
	// ctr --namespace=k8s.io images tag --force <source> <target>
	if len(c.args) < 4 {
		return fmt.Errorf("fake runtime: malformed tag command %v", c.args)
	}
	source := c.args[len(c.args)-2]
	target := c.args[len(c.args)-1]
	rt.tags = append(rt.tags, tagCall{node: c.node.name, source: source, target: target})

	id, ok := rt.names[source]
	if !ok {
		for existingID, img := range rt.images {
			if existingID == source {
				id, ok = existingID, true
				break
			}
			for _, d := range img.topDigests {
				if d == source {
					id, ok = existingID, true
					break
				}
			}
			if ok {
				break
			}
		}
	}
	if !ok {
		return fmt.Errorf("fake runtime: no such image %q", source)
	}
	rt.names[target] = id
	return nil
}

func hasArgs(args []string, needles ...string) bool {
	for _, needle := range needles {
		found := false
		for _, arg := range args {
			if arg == needle {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// streamedArchive is what the fake importer extracted from an import stream.
type streamedArchive struct {
	tags    []string
	members []string
	images  []streamedImage
}

type streamedImage struct {
	id         string
	tags       []string
	topDigests []string
}

// parseStreamedArchive validates a streamed subset archive: every referenced
// blob must be present, manifests must be parseable, and names must be
// attached to content. This is what makes a wrongly-built subset fail.
func parseStreamedArchive(r io.Reader) (*streamedArchive, error) {
	tr := tar.NewReader(r)
	contents := map[string][]byte{}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			return nil, err
		}
		contents[hdr.Name] = data
	}

	out := &streamedArchive{}
	for name := range contents {
		out.members = append(out.members, name)
	}
	sort.Strings(out.members)

	if _, ok := contents[ociLayoutFileName]; ok {
		return out, parseStreamedOCI(contents, out)
	}
	return out, parseStreamedDocker(contents, out)
}

func parseStreamedDocker(contents map[string][]byte, out *streamedArchive) error {
	manifestData, ok := contents[dockerManifestName]
	if !ok {
		return fmt.Errorf("missing %s", dockerManifestName)
	}
	var entries []dockerManifestEntry
	if err := json.Unmarshal(manifestData, &entries); err != nil {
		return err
	}
	if len(entries) == 0 {
		return fmt.Errorf("empty %s", dockerManifestName)
	}
	for _, entry := range entries {
		config, ok := contents[entry.Config]
		if !ok {
			return fmt.Errorf("manifest references missing config %q", entry.Config)
		}
		for _, layer := range entry.Layers {
			if _, ok := contents[layer]; !ok {
				return fmt.Errorf("manifest references missing layer %q", layer)
			}
		}
		out.tags = append(out.tags, entry.RepoTags...)
		out.images = append(out.images, streamedImage{
			id:         digestBytes(config),
			tags:       entry.RepoTags,
			topDigests: []string{digestBytes(manifestData)},
		})
	}
	return nil
}

func parseStreamedOCI(contents map[string][]byte, out *streamedArchive) error {
	indexData, ok := contents[ociIndexFileName]
	if !ok {
		return fmt.Errorf("missing %s", ociIndexFileName)
	}
	var index ociIndex
	if err := json.Unmarshal(indexData, &index); err != nil {
		return err
	}
	if len(index.Manifests) == 0 {
		return fmt.Errorf("empty %s", ociIndexFileName)
	}
	for _, top := range index.Manifests {
		var tags []string
		for _, key := range []string{"io.containerd.image.name", "org.opencontainers.image.ref.name"} {
			if value := top.Annotations[key]; value != "" {
				tags = append(tags, value)
			}
		}
		configs, err := streamedConfigDigests(contents, top, 0)
		if err != nil {
			return err
		}
		if len(configs) == 0 {
			return fmt.Errorf("descriptor %s has no config", top.Digest)
		}
		out.tags = append(out.tags, tags...)
		out.images = append(out.images, streamedImage{
			// The runtime reports the config digest of its platform as the
			// image ID; tests use single-platform images or the first
			// platform, so take the first.
			id:         configs[0],
			tags:       tags,
			topDigests: []string{top.Digest},
		})
	}
	return nil
}

func streamedConfigDigests(contents map[string][]byte, desc ociDescriptor, depth int) ([]string, error) {
	if depth > 4 {
		return nil, fmt.Errorf("descriptor nesting too deep")
	}
	name := blobName(desc.Digest)
	data, ok := contents[name]
	if !ok {
		return nil, fmt.Errorf("missing blob %q", name)
	}
	switch desc.MediaType {
	case "application/vnd.oci.image.manifest.v1+json",
		"application/vnd.docker.distribution.manifest.v2+json":
		var manifest ociManifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			return nil, err
		}
		configName := blobName(manifest.Config.Digest)
		config, ok := contents[configName]
		if !ok {
			return nil, fmt.Errorf("missing config blob %q", configName)
		}
		for _, layer := range manifest.Layers {
			if _, ok := contents[blobName(layer.Digest)]; !ok {
				return nil, fmt.Errorf("missing layer blob %q", blobName(layer.Digest))
			}
		}
		return []string{digestBytes(config)}, nil
	case "application/vnd.oci.image.index.v1+json",
		"application/vnd.docker.distribution.manifest.list.v2+json":
		var child ociIndex
		if err := json.Unmarshal(data, &child); err != nil {
			return nil, err
		}
		var out []string
		for _, c := range child.Manifests {
			digests, err := streamedConfigDigests(contents, c, depth+1)
			if err != nil {
				return nil, err
			}
			out = append(out, digests...)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported media type %q", desc.MediaType)
	}
}

func digestBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

const fakeContainerdConfig = `version = 2

[plugins]
  [plugins."io.containerd.grpc.v1.cri"]
    [plugins."io.containerd.grpc.v1.cri".containerd]
      snapshotter = "overlayfs"
`
