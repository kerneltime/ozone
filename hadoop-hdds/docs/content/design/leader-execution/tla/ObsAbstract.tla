---------------------------- MODULE ObsAbstract ----------------------------
\* Abstract atomic specification of OBS (flat) namespace operations.
\* Sequential oracle: every operation is a single atomic step. Quota is enforced
\* EXACTLY here (a new committed key requires room). The implementation (ObsImpl)
\* must refine this; if its locking allows quota over-commit, refinement fails.
EXTENDS Naturals, FiniteSets

CONSTANTS KEYS, CLIENTS, PROPS

VARIABLES aopen, acommitted, aprop, aused
AVars == << aopen, acommitted, aprop, aused >>

ATypeOK ==
  /\ aopen \subseteq (KEYS \X CLIENTS)
  /\ acommitted \subseteq KEYS
  /\ aprop \in PROPS
  /\ aused \in Nat

AInit ==
  /\ aopen = {}
  /\ acommitted = {}
  /\ aprop = 0
  /\ aused = 0

ACreateKey(n, c) ==
  /\ aopen' = aopen \cup {<<n, c>>}
  /\ UNCHANGED << acommitted, aprop, aused >>

\* SOFT quota (accepted limitation -- see EXC-3 and the over-commit counterexample
\* recorded in QuotaOvercommit.cfg): the counter `aused` is tracked EXACTLY, but the
\* limit is deliberately NOT a linearizability gate here. Concurrent commits under the
\* shared bucket lock can transiently over-commit; exact enforcement is delegated to a
\* separate mechanism (background QuotaRepair / a leader-local reservation), out of
\* scope for the namespace-concurrency proof. `UsedConsistent` still proves the counter
\* never loses an update.
ACommitKey(n, c) ==
  /\ <<n, c>> \in aopen
  /\ aopen' = aopen \ {<<n, c>>}
  /\ acommitted' = acommitted \cup {n}
  /\ aused' = IF n \in acommitted THEN aused ELSE aused + 1
  /\ UNCHANGED aprop

ADeleteKey(n) ==
  /\ n \in acommitted
  /\ acommitted' = acommitted \ {n}
  /\ aused' = aused - 1
  /\ UNCHANGED << aopen, aprop >>

ARenameKey(s, d) ==
  /\ s \in acommitted
  /\ d \notin acommitted
  /\ acommitted' = (acommitted \ {s}) \cup {d}
  /\ UNCHANGED << aopen, aprop, aused >>   \* -1 for s, +1 for d => net 0

ASetProp(val) ==
  /\ aprop' = val
  /\ UNCHANGED << aopen, acommitted, aused >>

ANext ==
  \/ \E n \in KEYS, c \in CLIENTS : ACreateKey(n, c)
  \/ \E n \in KEYS, c \in CLIENTS : ACommitKey(n, c)
  \/ \E n \in KEYS : ADeleteKey(n)
  \/ \E s \in KEYS, d \in KEYS : (s # d) /\ ARenameKey(s, d)
  \/ \E val \in PROPS : ASetProp(val)

ASpec == AInit /\ [][ANext]_AVars
=============================================================================
