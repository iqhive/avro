#!/usr/bin/env bash
# sync-iskorotkov.sh merges a commit of iskorotkov/avro (FETCH_HEAD unless
# another commit is named) into the branch checked out here. The merge commit's
# second parent is the upstream commit itself, so the next run finds it as the
# merge base and brings in only what is newer.
#
# Upstream imports its own packages as github.com/iskorotkov/avro/v2, and this
# repository as github.com/iqhive/avro/v2, so a plain merge would see the same
# import lines changed on both sides. This script merges against rewritten
# trees instead: in the merge base and in the upstream commit, the module path
# is renamed in every text file (the rename the "Rename the module" commit
# made) and the .github directory is left out. Only real changes are left on
# either side, and upstream's workflows never replace this repository's.
#
# It exits 0 when it has merged or there is nothing new, and 1 on a conflict.
# On a conflict it lists the conflicted files and leaves the branch as it was;
# merge by hand then, and the next run carries on from that merge.
set -euo pipefail
shopt -s inherit_errexit

upstream=${1:-FETCH_HEAD}

# rewrite prints the tree of commit $1 with the module path renamed and the
# .github directory removed.
rewrite() {
	local commit=$1 index entry mode path blob
	index=$(mktemp)
	GIT_INDEX_FILE=$index git read-tree "$commit"
	GIT_INDEX_FILE=$index git rm -r -q -f --cached --ignore-unmatch -- .github
	while IFS= read -r -d '' path; do
		path=${path#"$commit":}
		entry=$(git ls-tree "$commit" -- "$path")
		mode=${entry%% *}
		[ "$mode" = 100644 ] || [ "$mode" = 100755 ] || continue
		blob=$(git cat-file blob "$commit:$path" |
			sed 's#github\.com/iskorotkov/avro/#github.com/iqhive/avro/#g' |
			git hash-object -w --stdin)
		GIT_INDEX_FILE=$index git update-index --cacheinfo "$mode,$blob,$path"
	done < <(git grep -I -l -z -F 'github.com/iskorotkov/avro/' "$commit" -- . ':!.github')
	GIT_INDEX_FILE=$index git write-tree
	rm -f "$index"
}

head=$(git rev-parse HEAD)
theirs=$(git rev-parse "$upstream^{commit}")
short=$(git rev-parse --short "$theirs")
if ! base=$(git merge-base "$head" "$theirs"); then
	echo "HEAD and $short share no history" >&2
	exit 1
fi
if [ "$base" = "$theirs" ]; then
	echo "already up to date with iskorotkov/avro $short"
	exit 0
fi

base_tree=$(rewrite "$base")
theirs_tree=$(rewrite "$theirs")
set +e
result=$(git merge-tree --write-tree --name-only --merge-base="$base_tree" "$head" "$theirs_tree")
status=$?
set -e
case $status in
0) ;;
1)
	echo "merging iskorotkov/avro $short conflicts in:" >&2
	# The first line is the tree; the conflicted paths follow up to a blank line.
	sed -n '2,/^$/p' <<<"$result" >&2
	exit 1
	;;
*) exit "$status" ;;
esac

count=$(git rev-list --count "$base..$theirs")
commit=$(git commit-tree "${result%%$'\n'*}" -p "$head" -p "$theirs" \
	-m "Merge iskorotkov/avro main at $short ($count new commits)")
git merge -q --ff-only "$commit"
echo "merged iskorotkov/avro $short ($count new commits) as $(git rev-parse --short HEAD)"
