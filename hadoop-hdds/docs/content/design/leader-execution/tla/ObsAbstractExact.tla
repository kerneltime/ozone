-------------------------- MODULE ObsAbstractExact --------------------------
\* EXACT-quota variant of the OBS oracle, kept SOLELY to document the quota
\* over-commit limitation as a reproducible counterexample. ObsImpl does NOT
\* refine this oracle -- run QuotaOvercommit.cfg to reproduce the over-commit
\* trace on demand. The shipped/accepted oracle is ObsAbstract (soft quota).
EXTENDS Naturals, FiniteSets

CONSTANTS KEYS, CLIENTS, PROPS, QUOTA_LIMIT

VARIABLES aopen, acommitted, aprop, aused
AVars == << aopen, acommitted, aprop, aused >>

AInit ==
  /\ aopen = {}
  /\ acommitted = {}
  /\ aprop = 0
  /\ aused = 0

ACreateKey(n, c) ==
  /\ aopen' = aopen \cup {<<n, c>>}
  /\ UNCHANGED << acommitted, aprop, aused >>

ACommitKey(n, c) ==
  /\ <<n, c>> \in aopen
  /\ (n \in acommitted) \/ (aused + 1 <= QUOTA_LIMIT)   \* EXACT enforcement gate
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
  /\ UNCHANGED << aopen, aprop, aused >>

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
