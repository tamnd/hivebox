//! The `Snapshots` service for container cells. A snapshot is the cell's image with one more layer
//! holding what the cell wrote, stored in the node's image store, and its id is that image's id.
//! A restore is a create with the snapshot as the source, and a commit gives a scrubbed snapshot a
//! name in the caller's project. A fork copies what a cell wrote on its own node while the cell
//! runs, freezes it only to bring the copy up to date, and starts children on the cell's image with
//! the copy as their upper.
//!
//! A snapshot can squash git repositories first, for a testbed made from an upstream clone: the
//! repository is rebuilt with one commit holding what `HEAD` holds, so the fix a task asks for
//! cannot be read from a later commit, another branch, a tag, the reflog or a stash.

use super::verify::{file_error, git_sh};
use super::{Api, Args, Call, invalid, parse_id, status};
use futures::stream::BoxStream;
use hive_nectar::BlobId;
use hive_nectar::upper::Scrub;
use hive_proto::v1;
use hive_proto::v1::snapshots_server::Snapshots;
use tonic::{Request, Response, Status};

/// Rebuilds the git repository whose work tree is the current directory with one commit, which
/// holds the tree `HEAD` holds, on the branch `HEAD` is on. The new `.git` gets only that tree's
/// objects, in one pack, and the old one's `info/exclude`, and takes its place, so other commits, branches, tags,
/// remotes, the reflog, stashes, hooks and the packs fetched from upstream are gone. The work tree
/// is not touched, and the index is made again from `HEAD`. A repository with submodules, or
/// whose `.git` is not a directory in the work tree, is refused.
const SQUASH_GIT: &str = r#"set -eu
top=$(git rev-parse --show-toplevel)
if [ "$top" != "$(pwd -P)" ] || [ ! -d "$top/.git" ] || [ -L "$top/.git" ]; then
  echo "$PWD is not the top of a work tree with its .git in it" >&2; exit 1
fi
if git ls-tree -r HEAD | grep -q '^160000 '; then
  echo "$top has submodules, which are not squashed" >&2; exit 1
fi
tree=$(git rev-parse --verify 'HEAD^{tree}')
when=$(git log -1 --format=%cI HEAD)
branch=$(git symbolic-ref -q --short HEAD || echo main)
new=$(mktemp -d "$top/.git-squash.XXXXXX")
trap 'rm -rf "$new"' EXIT
git init -q --bare "$new"
rm -rf "$new/hooks"
{ echo "$tree"; git ls-tree -r -t HEAD | awk '{ print $3 }'; } | git pack-objects -q "$new/objects/pack/pack" > /dev/null
commit=$(GIT_AUTHOR_NAME=hivebox GIT_AUTHOR_EMAIL=hivebox@localhost GIT_AUTHOR_DATE="$when" GIT_COMMITTER_NAME=hivebox GIT_COMMITTER_EMAIL=hivebox@localhost GIT_COMMITTER_DATE="$when" git --git-dir="$new" commit-tree -m base "$tree")
git --git-dir="$new" update-ref "refs/heads/$branch" "$commit"
git --git-dir="$new" symbolic-ref HEAD "refs/heads/$branch"
git --git-dir="$new" config core.bare false
git --git-dir="$new" config core.logAllRefUpdates true
if [ -f "$top/.git/info/exclude" ]; then cp "$top/.git/info/exclude" "$new/info/exclude"; fi
# The old .git is most likely in a lower layer, and overlay would copy it whole to rename it.
rm -rf "$top/.git"
mv "$new" "$top/.git"
git read-tree HEAD
git update-index -q --refresh > /dev/null || true
[ "$(git rev-list --all | wc -l)" -eq 1 ] && [ "$(git rev-parse 'HEAD^{tree}')" = "$tree" ]
echo "$top $commit""#;

/// The most children one fork makes.
const MAX_FORK: u32 = 16;

fn snapshot_name(r: Option<&v1::SnapshotRef>) -> String {
    r.map(|r| r.id.clone()).unwrap_or_default()
}

fn snapshot_id(r: Option<v1::SnapshotRef>) -> Result<BlobId, Status> {
    let id = r.map(|r| r.id).unwrap_or_default();
    if id.is_empty() {
        return Err(invalid("name the snapshot"));
    }
    id.parse().map_err(|_| invalid(format!("{id:?} is not a snapshot id")))
}

#[tonic::async_trait]
impl Snapshots for Api {
    type RestoreStream = BoxStream<'static, Result<v1::CreateEvent, Status>>;
    type ForkStream = BoxStream<'static, Result<v1::CreateEvent, Status>>;

    async fn snapshot(
        &self,
        req: Request<v1::SnapshotRequest>,
    ) -> Result<Response<v1::SnapshotRef>, Status> {
        let call = Call::new(self, &req, "snapshot.create")?;
        let r = req.into_inner();
        let cell = r.cell_id.clone();
        let args = Args::default()
            .num(u64::try_from(r.kind).unwrap_or(0))
            .map(&r.labels)
            .num(r.scrub.into())
            .strs(&r.allow)
            .strs(&r.squash_git);
        let snap = async {
            let id = parse_id(&r.cell_id)?;
            self.owned(&call.project, id).map_err(status)?;
            match v1::SnapshotKind::try_from(r.kind) {
                Ok(v1::SnapshotKind::Unspecified | v1::SnapshotKind::Disk) => {}
                _ => return Err(Status::unimplemented("only disk snapshots are in yet")),
            }
            if !r.labels.is_empty() {
                return Err(invalid("snapshot labels are not kept yet, so send none"));
            }
            let scrub = match (r.scrub, r.allow.is_empty()) {
                (true, _) => Some(Scrub { allow: r.allow }),
                (false, true) => None,
                (false, false) => return Err(invalid("allow only means something with scrub on")),
            };
            if let Some(p) = r.squash_git.iter().find(|p| !p.starts_with('/')) {
                return Err(invalid(format!("{p:?} is not an absolute path")));
            }
            if !r.squash_git.is_empty() {
                let drone = self.comb.drone(id).await.map_err(status)?;
                for path in &r.squash_git {
                    let out = git_sh(&drone, path, SQUASH_GIT, 0).await.map_err(status)?;
                    if out.exit_code != 0 {
                        let e =
                            file_error(&format!("squashing the git repository in {path}"), &out);
                        return Err(status(e));
                    }
                }
            }
            self.comb.snapshot(id, scrub).await.map_err(status)
        }
        .await;
        let snap = call.check(&cell, &args, snap)?;
        call.record(&cell, &args, format!("ok snapshot={snap}"));
        Ok(Response::new(v1::SnapshotRef { id: snap.to_string() }))
    }

    async fn restore(
        &self,
        req: Request<v1::RestoreRequest>,
    ) -> Result<Response<Self::RestoreStream>, Status> {
        let call = Call::new(self, &req, "snapshot.restore")?;
        let (meta, ext, r) = req.into_parts();
        let args = Args::default().str(&snapshot_name(r.snapshot.as_ref()));
        let id = call.check("", &args, snapshot_id(r.snapshot))?;
        let mut spec =
            r.spec.ok_or_else(|| invalid("a restore needs the spec of the new cells"))?;
        spec.source = Some(v1::cell_spec::Source::Snapshot(v1::SnapshotRef { id: id.to_string() }));
        let create = v1::CreateRequest {
            spec: Some(spec),
            count: r.count,
            idempotency_key: r.idempotency_key,
            placement: None,
        };
        self.make(Request::from_parts(meta, ext, create), "snapshot.restore").await
    }

    async fn fork(
        &self,
        req: Request<v1::ForkRequest>,
    ) -> Result<Response<Self::ForkStream>, Status> {
        let call = Call::new(self, &req, "snapshot.fork")?;
        let r = req.into_inner();
        let count = r.count.max(1);
        let args = Args::default().num(count.into()).map(&r.labels).str(&r.idempotency_key);
        let keys: Vec<_> = (0..count)
            .map(|i| {
                (!r.idempotency_key.is_empty()).then(|| format!("{}/fork/{i}", r.idempotency_key))
            })
            .collect();
        let forked = async {
            if count > MAX_FORK {
                return Err(invalid(format!(
                    "count is {count}, and one fork makes at most {MAX_FORK}"
                )));
            }
            let id = parse_id(&r.cell_id)?;
            let parent = self.owned(&call.project, id).map_err(status)?;
            // A retry that finds every child made gets them back without the parent frozen again.
            if keys.iter().all(|k| k.as_ref().is_some_and(|k| self.comb.made(&call.project, k))) {
                return Ok((parent.spec, None));
            }
            let forked = self.comb.fork(id).await.map_err(status)?;
            Ok((forked.spec, Some(forked.seed)))
        }
        .await;
        let (mut spec, seed) = call.check(&r.cell_id, &args, forked)?;
        spec.labels.extend(r.labels);
        Ok(Response::new(self.creates(&call, &args, &spec, keys, false, seed)))
    }

    async fn commit(
        &self,
        req: Request<v1::CommitRequest>,
    ) -> Result<Response<v1::ImageRef>, Status> {
        let call = Call::new(self, &req, "snapshot.commit")?;
        let r = req.into_inner();
        let args = Args::default().str(&snapshot_name(r.snapshot.as_ref())).str(&r.name);
        let committed = async {
            let id = snapshot_id(r.snapshot)?;
            self.comb.commit(&call.project, id, &r.name).await.map_err(status)
        }
        .await;
        call.done("", &args, &committed);
        committed.map(|()| Response::new(v1::ImageRef { r#ref: r.name }))
    }

    async fn delete(&self, req: Request<v1::SnapshotRef>) -> Result<Response<v1::Empty>, Status> {
        let call = Call::new(self, &req, "snapshot.delete")?;
        let args = Args::default().str(&req.get_ref().id);
        call.check(
            "",
            &args,
            Err(Status::unimplemented(
                "snapshots are images in the store, which has no garbage collection yet",
            )),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::SQUASH_GIT;
    use std::path::Path;
    use std::process::Command;

    fn git(dir: &Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .args(["-c", "user.name=t", "-c", "user.email=t@t"])
            .args(args)
            .current_dir(dir)
            .output()
            .unwrap();
        assert!(out.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&out.stderr));
        String::from_utf8(out.stdout).unwrap()
    }

    #[test]
    fn a_squashed_repository_keeps_the_tree_and_none_of_the_history() {
        if Command::new("git").arg("--version").output().is_err() {
            return;
        }
        let dir = std::env::temp_dir().join(format!("hive-squash-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("src")).unwrap();
        git(&dir, &["init", "-q", "-b", "dev"]);
        std::fs::write(dir.join("src/calc.py"), "def add(a, b):\n    return a - b\n").unwrap();
        std::fs::write(dir.join(".gitignore"), "*.pyc\n").unwrap();
        git(&dir, &["add", "-A"]);
        git(&dir, &["commit", "-qm", "base"]);
        let base = git(&dir, &["rev-parse", "HEAD"]);
        // The upstream fix, then everything that could still point at it.
        std::fs::write(dir.join("src/calc.py"), "def add(a, b):\n    return a + b  # FIX\n")
            .unwrap();
        git(&dir, &["commit", "-qam", "fix add"]);
        let fix = git(&dir, &["rev-parse", "HEAD"]);
        git(&dir, &["tag", "v1.1"]);
        git(&dir, &["branch", "upstream"]);
        git(&dir, &["reset", "-q", "--hard", base.trim()]);
        std::fs::write(dir.join("src/calc.py"), "def add(a, b):\n    return a + b  # FIX\n")
            .unwrap();
        git(&dir, &["stash", "-q"]);
        git(&dir, &["gc", "-q"]);
        std::fs::write(dir.join(".git/info/exclude"), "secret.txt\n").unwrap();
        std::fs::write(dir.join("notes.txt"), "work in progress\n").unwrap();

        let squash = |at: &Path| {
            Command::new("sh").args(["-c", SQUASH_GIT]).current_dir(at).output().unwrap()
        };
        let below = squash(&dir.join("src"));
        assert!(!below.status.success(), "only the top of a work tree is squashed");
        let tree = git(&dir, &["rev-parse", "HEAD^{tree}"]);
        let out = squash(&dir);
        assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));

        assert_eq!(git(&dir, &["rev-list", "--all", "--reflog"]).lines().count(), 1);
        assert_eq!(git(&dir, &["for-each-ref", "--format=%(refname)"]), "refs/heads/dev\n");
        assert_eq!(git(&dir, &["rev-parse", "HEAD^{tree}"]), tree);
        let all = git(&dir, &["log", "--all", "-p"]);
        assert!(!all.contains("FIX") && !all.contains("fix add"), "{all}");
        let fsck =
            Command::new("git").args(["fsck", "--strict"]).current_dir(&dir).output().unwrap();
        assert!(fsck.status.success(), "{}", String::from_utf8_lossy(&fsck.stderr));
        let lost = Command::new("git")
            .args(["cat-file", "-e", fix.trim()])
            .current_dir(&dir)
            .status()
            .unwrap();
        assert!(!lost.success(), "the fix commit is still there");
        assert_eq!(git(&dir, &["status", "--porcelain"]), "?? notes.txt\n");
        assert_eq!(git(&dir, &["check-ignore", "secret.txt"]), "secret.txt\n");
        assert!(!dir.join(".git/hooks").exists());
        let left: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with(".git-squash"))
            .collect();
        assert!(left.is_empty(), "{left:?}");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
