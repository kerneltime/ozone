#!/usr/bin/env python3
"""
lint_spec.py — mechanical consistency gate for the Leader-Side Execution spec suite.

Automated checks (this is the FLOOR, not a full review):
  1. ref-resolution    — every id referenced in a YAML ref field resolves to a definition
  2. coverage          — every I-n is exercised by >=1 T-n (via T-n covers: or I-n tests:)
  3. fence-balance      — every file has an even number of ``` fences (no unclosed code block)
  4. anchor-existence   — every `File.java:NNN` evidence anchor resolves (file exists, line<=len)
  5. projection (§28)   — every I-row in the §28 traceability table is derivable from the sources

NOT automated (run the on-demand agent review for these): semantic correctness, design
completeness, phase-safety, prose-id integrity beyond the families below, and full
regenerate-and-diff of §22/§28. See `leader-exec-spec-review-prompt.md`.

Exit 0 = GREEN. Non-zero = findings printed.
"""
import re, os, sys, glob
from collections import Counter, defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, *(['..'] * 5)))   # .../leader-execution -> repo root
DOCS = ['leader-planned-execution.md', 'leader-execution-locking.md',
        'leader-execution-components.md', 'leader-execution-test-plan.md',
        'leader-execution-phasing.md']
FAM = r'(?:D|ALT|RC|I|B|C|P|T|F|A|EXC|Q|R)'
REF_FIELDS = ['depends_on', 'enables', 'rejects', 'deferred_alternatives', 'addresses',
              'killed_by', 'deferred_by', 'resolved_by', 'implements', 'covers', 'tests',
              'must_satisfy', 'must_pass', 'supersedes']

def docpath(d): return os.path.join(HERE, d)
fails = []

# ---- parse defined + referenced ids ----
defined, referenced = {}, defaultdict(set)
for d in DOCS:
    txt = open(docpath(d)).read()
    body = txt.split('# PART I', 1)[1] if d == 'leader-planned-execution.md' else txt  # skip §A schema placeholders
    for m in re.findall(r'(?:^|[{\s])id:\s*(' + FAM + r'-[A-Za-z0-9\-]+)', body, re.M):
        defined.setdefault(m, d)
    for m in re.findall(r'^\s*[-*]?\s*\*\*(' + FAM + r'-[A-Za-z0-9\-]+)', body, re.M):  # prose `- **I-1 ...**`
        defined.setdefault(m, d)
    for fld in REF_FIELDS:
        for blk in re.findall(fld + r':\s*(\[[^\]]*\]|' + FAM + r'-[A-Za-z0-9\-]+)', body):
            for tok in re.findall(FAM + r'-[A-Za-z0-9\-]+', blk):
                referenced[tok].add(d)

# 1. ref-resolution
dangling = sorted(r for r in referenced if r not in defined and r not in ('D-n', 'I-n', 'T-n', 'C-n', 'P-n', 'B-n'))
if dangling:
    fails.append('REF-RESOLUTION: dangling -> ' + ', '.join(dangling))

# 2. coverage
i_def = {k for k in defined if k.startswith('I-')}
i_cov = set()
for d in DOCS:
    for blk in re.findall(r'(?:covers|tests):\s*(\[[^\]]*\])', open(docpath(d)).read()):
        i_cov |= set(re.findall(r'I-[A-Za-z0-9\-]+', blk))
uncov = sorted(i_def - i_cov)
if uncov:
    fails.append('COVERAGE: invariants with no test -> ' + ', '.join(uncov))

# 3. fence-balance
for d in DOCS:
    n = sum(1 for l in open(docpath(d)) if l.startswith('```'))
    if n % 2:
        fails.append(f'FENCE-BALANCE: {d} has {n} fences (odd = unclosed code block)')

# 4. anchor-existence
javafiles = {}
for root in ('hadoop-ozone', 'hadoop-hdds'):
    for p in glob.glob(os.path.join(REPO, root, '**', '*.java'), recursive=True):
        javafiles.setdefault(os.path.basename(p), p)   # first wins; Java basenames ~unique
bad_anchors = []
seen = set()
for d in DOCS:
    for f, ln in re.findall(r'([A-Za-z0-9_]+\.java):(\d+)', open(docpath(d)).read()):
        key = (f, int(ln))
        if key in seen:
            continue
        seen.add(key)
        p = javafiles.get(f)
        if p is None:
            bad_anchors.append(f'{f}:{ln} (file not found in worktree)')
        else:
            total = sum(1 for _ in open(p))
            if int(ln) > total:
                bad_anchors.append(f'{f}:{ln} (> {total} lines)')
if bad_anchors:
    fails.append('ANCHOR-EXISTENCE: ' + '; '.join(bad_anchors[:20]) + (' ...' if len(bad_anchors) > 20 else ''))

# 5. §28 projection-freshness (master traceability matrix rows must be source-derivable)
master = open(docpath('leader-planned-execution.md')).read()
src = defaultdict(set)  # invariant -> tests, from T-n covers: and I-n tests:
for blk in re.finditer(r'id:\s*(T-[A-Za-z0-9\-]+)(.*?)(?=\nid:|\n```|\n## |\Z)', master + open(docpath('leader-execution-test-plan.md')).read(), re.S):
    for iv in re.findall(r'covers:\s*\[([^\]]*)\]', blk.group(2)):
        for t in re.findall(r'I-[A-Za-z0-9\-]+', iv):
            src[t].add(blk.group(1))
for blk in re.finditer(r'id:\s*(I-[A-Za-z0-9\-]+)(.*?)(?=\nid:|\n```|\n## |\Z)', master, re.S):
    for tv in re.findall(r'tests:\s*\[([^\]]*)\]', blk.group(2)):
        for t in re.findall(r'T-[A-Za-z0-9\-]+', tv):
            src[blk.group(1)].add(t)
m = re.search(r'## 28\. Traceability matrix\n(.*?)(\n# PART|\n## )', master, re.S)
if m:
    for row in re.findall(r'^\|\s*(I-[A-Za-z0-9\-]+)\s*\|([^|]*)\|', m.group(1), re.M):
        inv, cell = row[0], row[1]
        listed = set(re.findall(r'T-[A-Za-z0-9\-]+', cell))
        stale = listed - src.get(inv, set())
        if stale:
            fails.append(f'PROJECTION(§28): {inv} row lists {sorted(stale)} not derivable from sources')

# 6. bidirectional projection: every derived (I -> T) must be LISTED in §28 AND test-plan §6.2
tp = open(docpath('leader-execution-test-plan.md')).read()
matrices = [('§28', m.group(1) if m else None),
            ('§6.2', (lambda mm: mm.group(1) if mm else None)(re.search(r'#+\s*6\.2[^\n]*\n(.*?)(\n#+\s|\Z)', tp, re.S)))]
for label, body in matrices:
    if not body:
        continue
    for inv, tests in src.items():
        row = re.search(r'^\|\s*`?' + re.escape(inv) + r'`?\s*\|([^|]*)\|', body, re.M)
        if not row:
            continue
        missing = tests - set(re.findall(r'T-[A-Za-z0-9\-]+', row.group(1)))
        if missing:
            fails.append(f'PROJECTION({label} under-listing): {inv} row omits {sorted(missing)} (derived from a covers:/tests: source)')

# 7. P-n must_pass SET must be identical across all single-line {id: P-n ... must_pass:[...]} representations
pn = defaultdict(set)
for d in DOCS:
    for mm in re.finditer(r'\{id:\s*(P-\d)\b[^}]*?must_pass:\s*\[([^\]]*)\][^}]*\}', open(docpath(d)).read()):
        pn[mm.group(1)].add(frozenset(re.findall(r'T-[A-Za-z0-9\-]+', mm.group(2))))
for ph in sorted(pn):
    if len(pn[ph]) > 1:
        fails.append(f'P-n DIVERGENCE: {ph} must_pass set differs across representations -> {[sorted(s) for s in pn[ph]]}')

# 8. a T-/I- id defined more than once must carry the SAME covers: set (aliases with divergent covers forbidden)
cov_by_id = defaultdict(set)
for d in DOCS:
    for mm in re.finditer(r'\bid:\s*([IT]-[A-Za-z0-9\-]+)(.*?)(?=\n\s*id:|\n#+\s|\Z)', open(docpath(d)).read(), re.S):
        cv = re.findall(r'covers:\s*\[([^\]]*)\]', mm.group(2))
        if cv:
            cov_by_id[mm.group(1)].add(frozenset(re.findall(r'I-[A-Za-z0-9\-]+', cv[0])))
for idn in sorted(cov_by_id):
    if len(cov_by_id[idn]) > 1:
        fails.append(f'DUPLICATE-ID divergent covers: {idn} -> {[sorted(s) for s in cov_by_id[idn]]}')

# ---- report ----
print(f"DEFINED by family: {dict(sorted(Counter(k.split('-')[0] for k in defined).items()))}")
print(f"anchors checked: {len(seen)} | invariants: {len(i_def)} defined, {len(i_def & i_cov)} covered")
if fails:
    print("\nRESULT: FINDINGS")
    for f in fails:
        print("  - " + f)
    sys.exit(1)
print("\nRESULT: GREEN")
sys.exit(0)
