# `kind load image-archive` — per-node image load deduplication

## Summary

`kind load image-archive` no longer imports the entire archive into every
node on every invocation. The archive is parsed once on the host, each node's
existing image list is queried once, and each image is judged **separately
for content and for names**. Images that are already fully present are
skipped; images whose content is present but whose tags are missing or point
at other content are repaired without re-importing layers. Content that is
genuinely missing is imported as one streamed subset of the archive.

Correctness is preferred over skipping: a node is never left using stale
content. If a tag on the node points at other content, the archive's content
is imported so the tag resolves to the new image.

## Behaviour changes visible to users

- **Output.** Per-image decisions are logged at the default verbosity (`V(0)`)
  with explicit categories:
  - `already present (content and tags); skipping`
  - `content present, adding missing tag(s); not importing content` (tag-only)
  - `importing N image(s) not fully present: ...` (full or partial import)
  - a per-node summary line:
    `node "x": N image(s) imported, M image(s) tag-only (K tag(s) added), L image(s) content and tags already present`.

  Previously a single `Loading Docker Image from archive ... to node ...` line
  was printed at `V(2)`. That line is gone; the new messages are at `V(0)`.
  Scripts that parsed `-v=2` output for the old line will need updating.
- **Nodes are no longer modified when their image list cannot be read.** The
  command fails for that node with a clear error instead of assuming the node
  is empty (silent full import) or complete (silent skip).
- **Archive validation happens before any node is touched.** A malformed or
  truncated archive is rejected up front with no node modified.
- **Per-node failures are aggregated and all nodes are attempted.** One node
  failing no longer prevents the others from being reconciled; the returned
  error names each failing node.
- **A streaming import means no per-node temp copy of the archive on the
  host.** Previously each node opened the archive and streamed it whole; now
  the host reads the archive's metadata once and streams only the members in
  use to each node.

## How content and names are compared

Names and content are distinct:

- A **name** (repo tag, or a `repo@digest` reference) can be re-pointed
  without deleting the content behind it.
- **Content** is shared: several names can refer to one content, and deleting
  a name does not delete content. Deleting/re-pointing the *last* name would
  make the old content unreachable (a future garbage collection could then
  remove it), which is why tag re-pointing is treated as a real mutation and
  logged as such.

Comparison uses digests, never tag names alone (a local rebuild can move a
tag to new content):

- The **archive** records, for each image, the sha256 of its **config blob(s)**
  — one per platform — plus the digests of its **top-level targets** (image
  manifest, or image index/manifest list).
- The **node** reports, via `crictl images --output=json`, an image ID and
  repo digests. For containerd the image ID is the **config digest** of the
  platform the node runs. Repo digests are the **top-level target** digests
  (an index digest for a multi-platform image, a manifest digest otherwise).

**Same-level comparison only.** An index digest and a per-platform manifest
or config digest live at different levels and will *never* be equal. So:

- content is considered present if the node image ID equals one of the
  archive's config digests, or a node repo digest equals one of the archive's
  config digests, or a node repo digest equals one of the archive's top-level
  target digests.

This is why multi-platform archives do not cause a full re-import on every
run: the node's config digest matches one of the archive's per-platform
config digests even though the node's repo digest (the index digest) is not
comparable to a manifest digest.

### When comparison is unavailable or unsafe

These conditions make per-image skipping unsafe. In each case the command
falls back to importing the **whole archive file, byte for byte, on every
node** — the historical behaviour — and logs a warning. It never guesses:

- An OCI index entry names an image only through the
  `org.opencontainers.image.ref.name` annotation with a bare tag (no
  repository), so there is no unambiguous full name to compare or create.
- A tag inside the archive names more than one distinct content.
- An OCI blob's content hash does not equal the digest in its path
  (`blobs/<alg>/<encoded>`), indicating corruption.
- Any archive that is neither a docker save archive nor an OCI layout, or
  that is missing metadata or referenced blobs, is rejected as an error
  (not a fallback).

The design deliberately prefers over-importing to wrongly skipping: using
stale content is more expensive than loading twice.

## Host cost

- The archive is read **once** on the host: one pass records each member's
  offset, size and sha256, and small metadata (manifest.json, index.json,
  configs, layer descriptors' manifests) is read from the recorded offset on
  demand. Large layer blobs are hashed during that single pass but never
  buffered in memory.
- For each node, the recorded members are streamed directly to `ctr` via its
  stdin. No host-side repacking to a temp file occurs. `ctr images import`
  cannot import a subset of a tar directly, so the subset is a rewritten tar
  stream produced on the fly (with a rewritten `manifest.json`/`index.json`);
  this preserves importability without a disk copy.
- If an archive cannot be compared, the fallback streams the original file
  unchanged.

## Node-side operations

- **Image list query:** exactly once per node per archive
  (`crictl images --output=json`). After operations, the local view is updated
  from what was done; the node is not queried again.
- **Imports:** at most one `ctr images import` per node per archive, carrying
  only the images whose content is missing. The snapshotter is detected lazily,
  so a node requiring no import does not run `containerd config dump`.
- **Tag repairs:** `ctr images tag --force <existing-name-or-digest> <archive-tag>`
  per tag that is missing or points at other content. A tag repair requires
  the content to be reachable by *some* name or digest on the node; if the
  content has no name at all it is imported instead, which creates a usable
  name.
- If a tag needs repair because it exists but points at other content, and
  the archive's content is also missing, the content is imported; the import
  repoints the tag and no separate tag call is made.

## Tests

Tests construct minimal but valid docker-save and OCI-layout archives in a
temp dir, and use an in-memory fake node runtime that records every import and
tag call, models content and name ownership separately, validates every
streamed subset archive, and reports image lists in `crictl` JSON shape.
Coverage includes:

- fresh node (one import, both images, subset contains both);
- everything already present (zero calls);
- only one image missing (one import, subset excludes the other's blobs);
- one of two names for the same content missing (tag-only, no import);
- same tag pointing at other content (import, tag ends on new content);
- nodes with different pre-existing images (each decided independently);
- image list query failure (node untouched, clear error);
- truncated and garbage archives (rejected before any node);
- multi-platform OCI archive with an index digest on the node (skipped, no
  re-import);
- OCI archive with only a bare-tag annotation (whole-archive fallback);
- docker archive with `./`-prefixed and symlinked paths (subset paths
  rewritten so the importer can find them).

Run them with:

```bash
go test ./pkg/cmd/kind/load/image-archive/ ./pkg/cluster/nodeutils/
```

## Files changed

- `pkg/cmd/kind/load/image-archive/archive.go` — single-pass archive parser,
  docker save and OCI layout support, per-image content/member recording,
  comparability/fallback determination.
- `pkg/cmd/kind/load/image-archive/plan.go` — name normalization and the pure
  per-node content/tag decision.
- `pkg/cmd/kind/load/image-archive/subset.go` — streamed subset archive
  writer (rewritten `manifest.json` / `index.json`).
- `pkg/cmd/kind/load/image-archive/execute.go` — per-node reconcile: one
  query, imports, tag repairs, local view updates, logging.
- `pkg/cmd/kind/load/image-archive/image-archive.go` — orchestration, archive
  parsing before nodes, per-node results and aggregated errors.
- `pkg/cluster/nodeutils/image_list.go` — `crictl images` parsing and a
  locally-maintainable image list.
- `pkg/cluster/nodeutils/util.go` — `ImageImporter` (lazy snapshotter,
  streaming import) and `TagImage`; `LoadImageArchive` now delegates to it.
- `site/content/docs/user/quick-start.md` — user-facing note on deduplication,
  fallback, and non-transactional per-node reporting.
