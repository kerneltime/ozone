------------------------------ MODULE FsoImpl ------------------------------
\* FSO locking IMPLEMENTATION. Scope: M2a (tree + file ops + deleteDir) +
\* M2b (directory rename, rename-stability + cycle prevention) +
\* M3 (recursive delete: rmDir tombstone + decomposed per-node-locked purge).
\* Two lock kinds: CONTAINER(objectID) and SLOT(parentObjectID,name); deduped to
\* strongest, acquired in a uniform total order, held across the Commit step.
\* Refines FsoAbstract (per-node linearizability). 3-level seeded tree.
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS NAMES, CLIENTS, MAXOID, Procs, NoProc, MAX_OPS

\* ---- lock keys and uniform total order (design Q4/I-11) ----
ContainerKeys == { [kind |-> "C", a |-> o, b |-> 0]  : o \in 0..MAXOID }
SlotKeys      == { [kind |-> "S", a |-> p, b |-> nm] : p \in 0..MAXOID, nm \in NAMES }
AllLockKeys   == ContainerKeys \cup SlotKeys

MkC(o, md)     == [kind |-> "C", a |-> o,  b |-> 0,  mode |-> md]
MkS(p, nm, md) == [kind |-> "S", a |-> p,  b |-> nm, mode |-> md]
KeyOf(e)       == [kind |-> e.kind, a |-> e.a, b |-> e.b]
RankOf(e)      == e.a * 100 + (IF e.kind = "C" THEN 0 ELSE 1) * 10 + e.b

RawLocks(op, P, nm, P2, n2, D) ==
  CASE op = "createDir"  -> { MkC(0,"S"), MkC(P,"S"), MkS(P,nm,"X") }
    [] op = "createFile" -> { MkC(0,"S"), MkC(P,"S") }
    [] op = "commitFile" -> { MkC(0,"S"), MkC(P,"S"), MkS(P,nm,"X") }
    [] op = "deleteFile" -> { MkC(0,"S"), MkC(P,"S"), MkS(P,nm,"X") }
    [] op = "deleteDir"  -> { MkC(0,"S"), MkC(P,"S"), MkS(P,nm,"X"), MkC(D,"X") }
    [] op = "rename"     -> { MkC(0,"S"), MkC(P,"S"), MkC(P2,"S"), MkS(P,nm,"X"), MkS(P2,n2,"X") }
    [] op = "rmDir"      -> { MkC(0,"S"), MkC(P,"S"), MkS(P,nm,"X") }
    [] op = "purge"      -> { MkC(0,"S"), MkC(D,"X") }

KeysOf(raw)     == { KeyOf(e) : e \in raw }
ModeFor(raw, k) == IF \E e \in raw : KeyOf(e) = k /\ e.mode = "X" THEN "X" ELSE "S"
Deduped(raw)    == { [kind |-> k.kind, a |-> k.a, b |-> k.b, mode |-> ModeFor(raw, k)] : k \in KeysOf(raw) }

RECURSIVE SortByRank(_)
SortByRank(S) ==
  IF S = {} THEN << >>
  ELSE LET m == CHOOSE e \in S : \A f \in S : RankOf(e) <= RankOf(f)
       IN  <<m>> \o SortByRank(S \ {m})

\* cycle-prevention + orphan detection (pure, fuel-bounded upward walk).
ParentOf(nds, x)          == IF \E e \in nds : e.oid = x THEN (CHOOSE e \in nds : e.oid = x).parent ELSE 0
RECURSIVE AncestorsUp(_, _, _)
AncestorsUp(nds, x, fuel) == IF (fuel = 0) \/ (x = 0) THEN {x}
                             ELSE {x} \cup AncestorsUp(nds, ParentOf(nds, x), fuel - 1)
IsAncestorOf(nds, a, x)   == a \in AncestorsUp(nds, x, MAXOID + 1)
UnderPurge(nds, prg, oid) == (AncestorsUp(nds, oid, MAXOID + 1) \cap prg) # {}

SEED == { [oid |-> 1, parent |-> 0, name |-> 0, isDir |-> TRUE],
          [oid |-> 2, parent |-> 1, name |-> 0, isDir |-> TRUE],
          [oid |-> 3, parent |-> 2, name |-> 0, isDir |-> FALSE] }

(*--algorithm fso

variables
  nodes = SEED,
  openf = {},
  nextOid = 4,
  purging = {},
  lockShared = [ k \in AllLockKeys |-> {} ],
  lockExcl   = [ k \in AllLockKeys |-> NoProc ];

define
  IsLiveDir(P)       == (P = 0) \/ (\E e \in nodes : e.oid = P /\ e.isDir)
  ExistsChild(P, nm) == \E e \in nodes : e.parent = P /\ e.name = nm
  HasChildren(o)     == (\E e \in nodes : e.parent = o) \/ (\E f \in openf : f.parent = o)
  LiveDirOids        == {0} \cup { e.oid : e \in {x \in nodes : x.isDir} }
  DirOidAt(P, nm)    == IF \E e \in nodes : e.parent = P /\ e.name = nm /\ e.isDir
                        THEN (CHOOSE e \in nodes : e.parent = P /\ e.name = nm /\ e.isDir).oid
                        ELSE P
  PurgeableOids      == { x.oid : x \in {y \in nodes : UnderPurge(nodes, purging, y.oid) /\ ~HasChildren(y.oid)} }
end define;

process Worker \in Procs
variables
  op = "none",
  P = 0, nm = 0, cl = 0, P2 = 0, n2 = 0, D = 0, pt = 0,
  reqLocks = << >>,
  i = 1,
  opsLeft = MAX_OPS;
begin
  Loop:
    while opsLeft > 0 do
      Pick:
        either
          op := "createDir";
          with PP \in LiveDirOids, nn \in NAMES do P := PP; nm := nn; end with;
        or
          op := "createFile";
          with PP \in LiveDirOids, nn \in NAMES, ccl \in CLIENTS do P := PP; nm := nn; cl := ccl; end with;
        or
          op := "commitFile";
          with PP \in LiveDirOids, nn \in NAMES, ccl \in CLIENTS do P := PP; nm := nn; cl := ccl; end with;
        or
          op := "deleteFile";
          with PP \in LiveDirOids, nn \in NAMES do P := PP; nm := nn; end with;
        or
          op := "deleteDir";
          with PP \in LiveDirOids, nn \in NAMES do P := PP; nm := nn; end with;
        or
          op := "rename";
          with PP \in LiveDirOids, nn \in NAMES, PP2 \in LiveDirOids, nn2 \in NAMES do
            P := PP; nm := nn; P2 := PP2; n2 := nn2;
          end with;
        or
          op := "rmDir";
          with PP \in LiveDirOids, nn \in NAMES do P := PP; nm := nn; end with;
        or
          op := "purge";
          with oo \in PurgeableOids do pt := oo; end with;
        end either;
        D := IF op = "purge" THEN pt ELSE DirOidAt(P, nm);
        reqLocks := SortByRank(Deduped(RawLocks(op, P, nm, P2, n2, D)));
        i := 1;

      AcquireLoop:
        while i <= Len(reqLocks) do
          if reqLocks[i].mode = "S" then
            await lockExcl[KeyOf(reqLocks[i])] = NoProc;
            lockShared[KeyOf(reqLocks[i])] := lockShared[KeyOf(reqLocks[i])] \cup {self};
          else
            await (lockExcl[KeyOf(reqLocks[i])] = NoProc) /\ (lockShared[KeyOf(reqLocks[i])] = {});
            lockExcl[KeyOf(reqLocks[i])] := self;
          end if;
          i := i + 1;
        end while;

      Commit:  \* linearization point; FSO-REVAL re-checks under the held locks
        if op = "createDir" then
          if (nextOid <= MAXOID) /\ IsLiveDir(P) /\ ~ExistsChild(P, nm) then
            nodes := nodes \cup {[oid |-> nextOid, parent |-> P, name |-> nm, isDir |-> TRUE]};
            nextOid := nextOid + 1;
          end if;
        elsif op = "createFile" then
          if IsLiveDir(P) /\ ([parent |-> P, name |-> nm, client |-> cl] \notin openf) then
            openf := openf \cup {[parent |-> P, name |-> nm, client |-> cl]};
          end if;
        elsif op = "commitFile" then
          if ([parent |-> P, name |-> nm, client |-> cl] \in openf) /\ IsLiveDir(P)
             /\ ~ExistsChild(P, nm) /\ (nextOid <= MAXOID) then
            openf := openf \ {[parent |-> P, name |-> nm, client |-> cl]};
            nodes := nodes \cup {[oid |-> nextOid, parent |-> P, name |-> nm, isDir |-> FALSE]};
            nextOid := nextOid + 1;
          end if;
        elsif op = "deleteFile" then
          if \E e \in nodes : e.parent = P /\ e.name = nm /\ ~e.isDir then
            nodes := nodes \ {CHOOSE e \in nodes : e.parent = P /\ e.name = nm /\ ~e.isDir};
          end if;
        elsif op = "deleteDir" then
          if \E e \in nodes : e.parent = P /\ e.name = nm /\ e.isDir /\ e.oid = D /\ ~HasChildren(D) then
            nodes := nodes \ {CHOOSE e \in nodes : e.parent = P /\ e.name = nm /\ e.isDir /\ e.oid = D};
          end if;
        elsif op = "rename" then
          if (\E e \in nodes : e.parent = P /\ e.name = nm)
             /\ IsLiveDir(P2) /\ ~ExistsChild(P2, n2) then
            with e = CHOOSE ee \in nodes : ee.parent = P /\ ee.name = nm do
              if (~e.isDir) \/ (~IsAncestorOf(nodes, e.oid, P2)) then
                nodes := (nodes \ {e}) \cup {[oid |-> e.oid, parent |-> P2, name |-> n2, isDir |-> e.isDir]};
              end if;
            end with;
          end if;
        elsif op = "rmDir" then
          if \E e \in nodes : e.parent = P /\ e.name = nm /\ e.isDir then
            with e = CHOOSE ee \in nodes : ee.parent = P /\ ee.name = nm /\ ee.isDir do
              nodes := nodes \ {e};
              purging := purging \cup {e.oid};
            end with;
          end if;
        elsif op = "purge" then
          if (\E e \in nodes : e.oid = D) /\ UnderPurge(nodes, purging, D) /\ ~HasChildren(D) then
            with e = CHOOSE ee \in nodes : ee.oid = D do
              nodes := nodes \ {e};
              purging := IF e.isDir THEN purging \cup {D} ELSE purging;
            end with;
          end if;
        end if;

      Release:
        lockShared := [ k \in AllLockKeys |-> lockShared[k] \ {self} ];
        lockExcl   := [ k \in AllLockKeys |-> IF lockExcl[k] = self THEN NoProc ELSE lockExcl[k] ];
        opsLeft := opsLeft - 1;
    end while;
end process;

end algorithm; *)

\* BEGIN TRANSLATION
VARIABLES nodes, openf, nextOid, purging, lockShared, lockExcl, pc

(* define statement *)
IsLiveDir(P)       == (P = 0) \/ (\E e \in nodes : e.oid = P /\ e.isDir)
ExistsChild(P, nm) == \E e \in nodes : e.parent = P /\ e.name = nm
HasChildren(o)     == (\E e \in nodes : e.parent = o) \/ (\E f \in openf : f.parent = o)
LiveDirOids        == {0} \cup { e.oid : e \in {x \in nodes : x.isDir} }
DirOidAt(P, nm)    == IF \E e \in nodes : e.parent = P /\ e.name = nm /\ e.isDir
                      THEN (CHOOSE e \in nodes : e.parent = P /\ e.name = nm /\ e.isDir).oid
                      ELSE P
PurgeableOids      == { x.oid : x \in {y \in nodes : UnderPurge(nodes, purging, y.oid) /\ ~HasChildren(y.oid)} }

VARIABLES op, P, nm, cl, P2, n2, D, pt, reqLocks, i, opsLeft

vars == << nodes, openf, nextOid, purging, lockShared, lockExcl, pc, op, P, 
           nm, cl, P2, n2, D, pt, reqLocks, i, opsLeft >>

ProcSet == (Procs)

Init == (* Global variables *)
        /\ nodes = SEED
        /\ openf = {}
        /\ nextOid = 4
        /\ purging = {}
        /\ lockShared = [ k \in AllLockKeys |-> {} ]
        /\ lockExcl = [ k \in AllLockKeys |-> NoProc ]
        (* Process Worker *)
        /\ op = [self \in Procs |-> "none"]
        /\ P = [self \in Procs |-> 0]
        /\ nm = [self \in Procs |-> 0]
        /\ cl = [self \in Procs |-> 0]
        /\ P2 = [self \in Procs |-> 0]
        /\ n2 = [self \in Procs |-> 0]
        /\ D = [self \in Procs |-> 0]
        /\ pt = [self \in Procs |-> 0]
        /\ reqLocks = [self \in Procs |-> << >>]
        /\ i = [self \in Procs |-> 1]
        /\ opsLeft = [self \in Procs |-> MAX_OPS]
        /\ pc = [self \in ProcSet |-> "Loop"]

Loop(self) == /\ pc[self] = "Loop"
              /\ IF opsLeft[self] > 0
                    THEN /\ pc' = [pc EXCEPT ![self] = "Pick"]
                    ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
              /\ UNCHANGED << nodes, openf, nextOid, purging, lockShared, 
                              lockExcl, op, P, nm, cl, P2, n2, D, pt, reqLocks, 
                              i, opsLeft >>

Pick(self) == /\ pc[self] = "Pick"
              /\ \/ /\ op' = [op EXCEPT ![self] = "createDir"]
                    /\ \E PP \in LiveDirOids:
                         \E nn \in NAMES:
                           /\ P' = [P EXCEPT ![self] = PP]
                           /\ nm' = [nm EXCEPT ![self] = nn]
                    /\ UNCHANGED <<cl, P2, n2, pt>>
                 \/ /\ op' = [op EXCEPT ![self] = "createFile"]
                    /\ \E PP \in LiveDirOids:
                         \E nn \in NAMES:
                           \E ccl \in CLIENTS:
                             /\ P' = [P EXCEPT ![self] = PP]
                             /\ nm' = [nm EXCEPT ![self] = nn]
                             /\ cl' = [cl EXCEPT ![self] = ccl]
                    /\ UNCHANGED <<P2, n2, pt>>
                 \/ /\ op' = [op EXCEPT ![self] = "commitFile"]
                    /\ \E PP \in LiveDirOids:
                         \E nn \in NAMES:
                           \E ccl \in CLIENTS:
                             /\ P' = [P EXCEPT ![self] = PP]
                             /\ nm' = [nm EXCEPT ![self] = nn]
                             /\ cl' = [cl EXCEPT ![self] = ccl]
                    /\ UNCHANGED <<P2, n2, pt>>
                 \/ /\ op' = [op EXCEPT ![self] = "deleteFile"]
                    /\ \E PP \in LiveDirOids:
                         \E nn \in NAMES:
                           /\ P' = [P EXCEPT ![self] = PP]
                           /\ nm' = [nm EXCEPT ![self] = nn]
                    /\ UNCHANGED <<cl, P2, n2, pt>>
                 \/ /\ op' = [op EXCEPT ![self] = "deleteDir"]
                    /\ \E PP \in LiveDirOids:
                         \E nn \in NAMES:
                           /\ P' = [P EXCEPT ![self] = PP]
                           /\ nm' = [nm EXCEPT ![self] = nn]
                    /\ UNCHANGED <<cl, P2, n2, pt>>
                 \/ /\ op' = [op EXCEPT ![self] = "rename"]
                    /\ \E PP \in LiveDirOids:
                         \E nn \in NAMES:
                           \E PP2 \in LiveDirOids:
                             \E nn2 \in NAMES:
                               /\ P' = [P EXCEPT ![self] = PP]
                               /\ nm' = [nm EXCEPT ![self] = nn]
                               /\ P2' = [P2 EXCEPT ![self] = PP2]
                               /\ n2' = [n2 EXCEPT ![self] = nn2]
                    /\ UNCHANGED <<cl, pt>>
                 \/ /\ op' = [op EXCEPT ![self] = "rmDir"]
                    /\ \E PP \in LiveDirOids:
                         \E nn \in NAMES:
                           /\ P' = [P EXCEPT ![self] = PP]
                           /\ nm' = [nm EXCEPT ![self] = nn]
                    /\ UNCHANGED <<cl, P2, n2, pt>>
                 \/ /\ op' = [op EXCEPT ![self] = "purge"]
                    /\ \E oo \in PurgeableOids:
                         pt' = [pt EXCEPT ![self] = oo]
                    /\ UNCHANGED <<P, nm, cl, P2, n2>>
              /\ D' = [D EXCEPT ![self] = IF op'[self] = "purge" THEN pt'[self] ELSE DirOidAt(P'[self], nm'[self])]
              /\ reqLocks' = [reqLocks EXCEPT ![self] = SortByRank(Deduped(RawLocks(op'[self], P'[self], nm'[self], P2'[self], n2'[self], D'[self])))]
              /\ i' = [i EXCEPT ![self] = 1]
              /\ pc' = [pc EXCEPT ![self] = "AcquireLoop"]
              /\ UNCHANGED << nodes, openf, nextOid, purging, lockShared, 
                              lockExcl, opsLeft >>

AcquireLoop(self) == /\ pc[self] = "AcquireLoop"
                     /\ IF i[self] <= Len(reqLocks[self])
                           THEN /\ IF reqLocks[self][i[self]].mode = "S"
                                      THEN /\ lockExcl[KeyOf(reqLocks[self][i[self]])] = NoProc
                                           /\ lockShared' = [lockShared EXCEPT ![KeyOf(reqLocks[self][i[self]])] = lockShared[KeyOf(reqLocks[self][i[self]])] \cup {self}]
                                           /\ UNCHANGED lockExcl
                                      ELSE /\ (lockExcl[KeyOf(reqLocks[self][i[self]])] = NoProc) /\ (lockShared[KeyOf(reqLocks[self][i[self]])] = {})
                                           /\ lockExcl' = [lockExcl EXCEPT ![KeyOf(reqLocks[self][i[self]])] = self]
                                           /\ UNCHANGED lockShared
                                /\ i' = [i EXCEPT ![self] = i[self] + 1]
                                /\ pc' = [pc EXCEPT ![self] = "AcquireLoop"]
                           ELSE /\ pc' = [pc EXCEPT ![self] = "Commit"]
                                /\ UNCHANGED << lockShared, lockExcl, i >>
                     /\ UNCHANGED << nodes, openf, nextOid, purging, op, P, nm, 
                                     cl, P2, n2, D, pt, reqLocks, opsLeft >>

Commit(self) == /\ pc[self] = "Commit"
                /\ IF op[self] = "createDir"
                      THEN /\ IF (nextOid <= MAXOID) /\ IsLiveDir(P[self]) /\ ~ExistsChild(P[self], nm[self])
                                 THEN /\ nodes' = (nodes \cup {[oid |-> nextOid, parent |-> P[self], name |-> nm[self], isDir |-> TRUE]})
                                      /\ nextOid' = nextOid + 1
                                 ELSE /\ TRUE
                                      /\ UNCHANGED << nodes, nextOid >>
                           /\ UNCHANGED << openf, purging >>
                      ELSE /\ IF op[self] = "createFile"
                                 THEN /\ IF IsLiveDir(P[self]) /\ ([parent |-> P[self], name |-> nm[self], client |-> cl[self]] \notin openf)
                                            THEN /\ openf' = (openf \cup {[parent |-> P[self], name |-> nm[self], client |-> cl[self]]})
                                            ELSE /\ TRUE
                                                 /\ openf' = openf
                                      /\ UNCHANGED << nodes, nextOid, purging >>
                                 ELSE /\ IF op[self] = "commitFile"
                                            THEN /\ IF ([parent |-> P[self], name |-> nm[self], client |-> cl[self]] \in openf) /\ IsLiveDir(P[self])
                                                       /\ ~ExistsChild(P[self], nm[self]) /\ (nextOid <= MAXOID)
                                                       THEN /\ openf' = openf \ {[parent |-> P[self], name |-> nm[self], client |-> cl[self]]}
                                                            /\ nodes' = (nodes \cup {[oid |-> nextOid, parent |-> P[self], name |-> nm[self], isDir |-> FALSE]})
                                                            /\ nextOid' = nextOid + 1
                                                       ELSE /\ TRUE
                                                            /\ UNCHANGED << nodes, 
                                                                            openf, 
                                                                            nextOid >>
                                                 /\ UNCHANGED purging
                                            ELSE /\ IF op[self] = "deleteFile"
                                                       THEN /\ IF \E e \in nodes : e.parent = P[self] /\ e.name = nm[self] /\ ~e.isDir
                                                                  THEN /\ nodes' = nodes \ {CHOOSE e \in nodes : e.parent = P[self] /\ e.name = nm[self] /\ ~e.isDir}
                                                                  ELSE /\ TRUE
                                                                       /\ nodes' = nodes
                                                            /\ UNCHANGED purging
                                                       ELSE /\ IF op[self] = "deleteDir"
                                                                  THEN /\ IF \E e \in nodes : e.parent = P[self] /\ e.name = nm[self] /\ e.isDir /\ e.oid = D[self] /\ ~HasChildren(D[self])
                                                                             THEN /\ nodes' = nodes \ {CHOOSE e \in nodes : e.parent = P[self] /\ e.name = nm[self] /\ e.isDir /\ e.oid = D[self]}
                                                                             ELSE /\ TRUE
                                                                                  /\ nodes' = nodes
                                                                       /\ UNCHANGED purging
                                                                  ELSE /\ IF op[self] = "rename"
                                                                             THEN /\ IF (\E e \in nodes : e.parent = P[self] /\ e.name = nm[self])
                                                                                        /\ IsLiveDir(P2[self]) /\ ~ExistsChild(P2[self], n2[self])
                                                                                        THEN /\ LET e == CHOOSE ee \in nodes : ee.parent = P[self] /\ ee.name = nm[self] IN
                                                                                                  IF (~e.isDir) \/ (~IsAncestorOf(nodes, e.oid, P2[self]))
                                                                                                     THEN /\ nodes' = ((nodes \ {e}) \cup {[oid |-> e.oid, parent |-> P2[self], name |-> n2[self], isDir |-> e.isDir]})
                                                                                                     ELSE /\ TRUE
                                                                                                          /\ nodes' = nodes
                                                                                        ELSE /\ TRUE
                                                                                             /\ nodes' = nodes
                                                                                  /\ UNCHANGED purging
                                                                             ELSE /\ IF op[self] = "rmDir"
                                                                                        THEN /\ IF \E e \in nodes : e.parent = P[self] /\ e.name = nm[self] /\ e.isDir
                                                                                                   THEN /\ LET e == CHOOSE ee \in nodes : ee.parent = P[self] /\ ee.name = nm[self] /\ ee.isDir IN
                                                                                                             /\ nodes' = nodes \ {e}
                                                                                                             /\ purging' = (purging \cup {e.oid})
                                                                                                   ELSE /\ TRUE
                                                                                                        /\ UNCHANGED << nodes, 
                                                                                                                        purging >>
                                                                                        ELSE /\ IF op[self] = "purge"
                                                                                                   THEN /\ IF (\E e \in nodes : e.oid = D[self]) /\ UnderPurge(nodes, purging, D[self]) /\ ~HasChildren(D[self])
                                                                                                              THEN /\ LET e == CHOOSE ee \in nodes : ee.oid = D[self] IN
                                                                                                                        /\ nodes' = nodes \ {e}
                                                                                                                        /\ purging' = (IF e.isDir THEN purging \cup {D[self]} ELSE purging)
                                                                                                              ELSE /\ TRUE
                                                                                                                   /\ UNCHANGED << nodes, 
                                                                                                                                   purging >>
                                                                                                   ELSE /\ TRUE
                                                                                                        /\ UNCHANGED << nodes, 
                                                                                                                        purging >>
                                                 /\ UNCHANGED << openf, 
                                                                 nextOid >>
                /\ pc' = [pc EXCEPT ![self] = "Release"]
                /\ UNCHANGED << lockShared, lockExcl, op, P, nm, cl, P2, n2, D, 
                                pt, reqLocks, i, opsLeft >>

Release(self) == /\ pc[self] = "Release"
                 /\ lockShared' = [ k \in AllLockKeys |-> lockShared[k] \ {self} ]
                 /\ lockExcl' = [ k \in AllLockKeys |-> IF lockExcl[k] = self THEN NoProc ELSE lockExcl[k] ]
                 /\ opsLeft' = [opsLeft EXCEPT ![self] = opsLeft[self] - 1]
                 /\ pc' = [pc EXCEPT ![self] = "Loop"]
                 /\ UNCHANGED << nodes, openf, nextOid, purging, op, P, nm, cl, 
                                 P2, n2, D, pt, reqLocks, i >>

Worker(self) == Loop(self) \/ Pick(self) \/ AcquireLoop(self)
                   \/ Commit(self) \/ Release(self)

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == (\E self \in Procs: Worker(self))
           \/ Terminating

Spec == Init /\ [][Next]_vars

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION

\* ---- refinement mapping: tree + purging set IS the abstract state ----
ABS == INSTANCE FsoAbstract WITH anodes <- nodes, aopenf <- openf, anext <- nextOid, apurging <- purging
Refinement == ABS!ASpec

\* ---- safety invariants ----
LockInv == \A k \in AllLockKeys : (lockExcl[k] # NoProc) => (lockShared[k] = {})

NoLeak ==
  \A p \in Procs :
    (pc[p] = "Done") =>
      /\ \A k \in AllLockKeys : p \notin lockShared[k]
      /\ \A k \in AllLockKeys : lockExcl[k] # p

\* M3 safety: no PERMANENT, unaccounted orphan. Every node's parent is the root,
\* still present, or in the purging set (an in-progress recursive delete).
\* (Strict NoOrphan does NOT hold mid-purge -- transient orphans are by design, EXC-2.)
Accounted ==
  \A e \in nodes : (e.parent = 0) \/ (\E p \in nodes : p.oid = e.parent) \/ (e.parent \in purging)
=============================================================================
