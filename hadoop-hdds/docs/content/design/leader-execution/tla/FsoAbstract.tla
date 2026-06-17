---------------------------- MODULE FsoAbstract ----------------------------
\* Abstract atomic spec of FSO namespace operations. Scope: M2a + M2b + M3.
\*   create/commit/delete file, deleteDir(empty), rename(file+dir),
\*   rmDir (recursive: tombstone regardless of children), purge (reclaim one
\*   orphaned childless node from a tombstoned subtree).
\* Per-node-transition oracle (the linearizable unit is one transition, NOT a
\* whole client request). FsoImpl must refine this. Root (bucket) = oid 0.
EXTENDS Naturals, FiniteSets

CONSTANTS NAMES, CLIENTS, MAXOID

VARIABLES anodes, aopenf, anext, apurging
AVars == << anodes, aopenf, anext, apurging >>

Root == 0
IsLiveDir(P)        == (P = Root) \/ (\E e \in anodes : e.oid = P /\ e.isDir)
ExistsChild(P, nm)  == \E e \in anodes : e.parent = P /\ e.name = nm
HasChildren(oid)    == (\E e \in anodes : e.parent = oid) \/ (\E o \in aopenf : o.parent = oid)
TheChild(P, nm)     == CHOOSE e \in anodes : e.parent = P /\ e.name = nm

ParentOf(nds, x)          == IF \E e \in nds : e.oid = x THEN (CHOOSE e \in nds : e.oid = x).parent ELSE 0
RECURSIVE AncestorsUp(_, _, _)
AncestorsUp(nds, x, fuel) == IF (fuel = 0) \/ (x = 0) THEN {x}
                             ELSE {x} \cup AncestorsUp(nds, ParentOf(nds, x), fuel - 1)
IsAncestorOf(nds, a, x)   == a \in AncestorsUp(nds, x, MAXOID + 1)
\* node `oid` sits under an in-progress recursive delete iff its (stored) parent
\* chain reaches a tombstoned/purging oid.
UnderPurge(nds, prg, oid) == (AncestorsUp(nds, oid, MAXOID + 1) \cap prg) # {}

\* 3-level seed: /dirA(1)/dirB(2)/fileC(3), so rmDir(dirA) leaves dirB orphaned-but-present.
AInit ==
  /\ anodes = { [oid |-> 1, parent |-> 0, name |-> 0, isDir |-> TRUE],
                [oid |-> 2, parent |-> 1, name |-> 0, isDir |-> TRUE],
                [oid |-> 3, parent |-> 2, name |-> 0, isDir |-> FALSE] }
  /\ aopenf = {}
  /\ anext = 4
  /\ apurging = {}

ACreateDir(P, nm) ==
  /\ anext <= MAXOID
  /\ IsLiveDir(P)
  /\ ~ExistsChild(P, nm)
  /\ anodes' = anodes \cup {[oid |-> anext, parent |-> P, name |-> nm, isDir |-> TRUE]}
  /\ anext'  = anext + 1
  /\ UNCHANGED << aopenf, apurging >>

ACreateFile(P, nm, cl) ==
  /\ IsLiveDir(P)
  /\ [parent |-> P, name |-> nm, client |-> cl] \notin aopenf
  /\ aopenf' = aopenf \cup {[parent |-> P, name |-> nm, client |-> cl]}
  /\ UNCHANGED << anodes, anext, apurging >>

ACommitFile(P, nm, cl) ==
  /\ [parent |-> P, name |-> nm, client |-> cl] \in aopenf
  /\ IsLiveDir(P)
  /\ ~ExistsChild(P, nm)
  /\ anext <= MAXOID
  /\ aopenf' = aopenf \ {[parent |-> P, name |-> nm, client |-> cl]}
  /\ anodes' = anodes \cup {[oid |-> anext, parent |-> P, name |-> nm, isDir |-> FALSE]}
  /\ anext'  = anext + 1
  /\ UNCHANGED apurging

ADeleteFile(P, nm) ==
  /\ \E e \in anodes : e.parent = P /\ e.name = nm /\ ~e.isDir
  /\ anodes' = anodes \ {TheChild(P, nm)}
  /\ UNCHANGED << aopenf, anext, apurging >>

ADeleteDir(P, nm) ==
  /\ \E e \in anodes : e.parent = P /\ e.name = nm /\ e.isDir /\ ~HasChildren(e.oid)
  /\ anodes' = anodes \ {TheChild(P, nm)}
  /\ UNCHANGED << aopenf, anext, apurging >>

ARename(P1, n1, P2, n2) ==
  /\ \E e \in anodes : e.parent = P1 /\ e.name = n1
  /\ IsLiveDir(P2)
  /\ ~ExistsChild(P2, n2)
  /\ LET e == TheChild(P1, n1) IN
        /\ (~e.isDir) \/ (~IsAncestorOf(anodes, e.oid, P2))
        /\ anodes' = (anodes \ {e}) \cup {[oid |-> e.oid, parent |-> P2, name |-> n2, isDir |-> e.isDir]}
  /\ UNCHANGED << aopenf, anext, apurging >>

\* recursive delete: tombstone the directory regardless of children (its subtree is
\* reclaimed by APurge -- EXC-2 eventual). Adds the dir's oid to the purging set.
ARmDir(P, nm) ==
  /\ \E e \in anodes : e.parent = P /\ e.name = nm /\ e.isDir
  /\ LET e == TheChild(P, nm) IN
        /\ anodes'   = anodes \ {e}
        /\ apurging' = apurging \cup {e.oid}
  /\ UNCHANGED << aopenf, anext >>

\* background purge: reclaim one orphaned, childless node. Removing a dir adds its
\* oid to the purging set so deeper descendants stay accounted-for.
APurge(oid) ==
  /\ \E e \in anodes : e.oid = oid
  /\ UnderPurge(anodes, apurging, oid)
  /\ ~HasChildren(oid)
  /\ LET e == CHOOSE x \in anodes : x.oid = oid IN
        /\ anodes'   = anodes \ {e}
        /\ apurging' = IF e.isDir THEN apurging \cup {oid} ELSE apurging
  /\ UNCHANGED << aopenf, anext >>

ANext ==
  \/ \E P \in 0..MAXOID, nm \in NAMES               : ACreateDir(P, nm)
  \/ \E P \in 0..MAXOID, nm \in NAMES, cl \in CLIENTS : ACreateFile(P, nm, cl)
  \/ \E P \in 0..MAXOID, nm \in NAMES, cl \in CLIENTS : ACommitFile(P, nm, cl)
  \/ \E P \in 0..MAXOID, nm \in NAMES               : ADeleteFile(P, nm)
  \/ \E P \in 0..MAXOID, nm \in NAMES               : ADeleteDir(P, nm)
  \/ \E P1 \in 0..MAXOID, n1 \in NAMES, P2 \in 0..MAXOID, n2 \in NAMES :
        ((P1 # P2) \/ (n1 # n2)) /\ ARename(P1, n1, P2, n2)
  \/ \E P \in 0..MAXOID, nm \in NAMES               : ARmDir(P, nm)
  \/ \E oid \in 1..MAXOID                           : APurge(oid)

ASpec == AInit /\ [][ANext]_AVars
=============================================================================
