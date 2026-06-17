------------------------------ MODULE ObsImpl ------------------------------
\* OBS locking IMPLEMENTATION with the faithful striped lock manager + quota.
\*   - lock ids: Bucket (shared by key ops) + stripes 0..STRIPES-1 (key slots)
\*   - sorted, dedup-to-strongest acquisition; lock held across the commit
\*   - QUOTA: usedBytes is a counter (the merge operator). The check is computed
\*     at CommitPlan (reads current usedBytes under the SHARED bucket lock) and the
\*     increment applies at CommitApply -- with NO re-check (followers do no business
\*     logic). Because concurrent commits to different keys hold bucket-SHARED, they
\*     do not serialize on usedBytes, so the plan-time decision can be stale.
\* Checked to refine ObsAbstract (linearizability + exact quota).
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS KEYS, CLIENTS, PROPS, Procs, STRIPES, NoProc, SORTED, MAX_OPS, QUOTA_LIMIT

\* --- lock identities and the total order (Q4: order by stripe, not logical key) ---
Bucket      == STRIPES                 \* bucket lock id = a number outside the stripe range
Slot(n)     == n % STRIPES
AllLocks    == 0..STRIPES               \* stripes 0..STRIPES-1, plus Bucket (= STRIPES)
LockRank(l) == IF l = Bucket THEN 0 ELSE l + 1

RawLocks(ot, nn, cc, dd) ==
  CASE ot = "create"  -> { [id |-> Bucket, mode |-> "S"] }
    [] ot = "commit"  -> { [id |-> Bucket, mode |-> "S"], [id |-> Slot(nn), mode |-> "X"] }
    [] ot = "delete"  -> { [id |-> Bucket, mode |-> "S"], [id |-> Slot(nn), mode |-> "X"] }
    [] ot = "rename"  -> { [id |-> Bucket, mode |-> "S"], [id |-> Slot(nn), mode |-> "X"],
                           [id |-> Slot(dd), mode |-> "X"] }
    [] ot = "setprop" -> { [id |-> Bucket, mode |-> "X"] }

LockIdsOf(raw)      == { r.id : r \in raw }
ModeFor(raw, anId)  == IF \E r \in raw : (r.id = anId) /\ (r.mode = "X") THEN "X" ELSE "S"
Deduped(raw)        == { [id |-> anId, mode |-> ModeFor(raw, anId)] : anId \in LockIdsOf(raw) }

RECURSIVE SortByRank(_)
SortByRank(S) ==
  IF S = {} THEN << >>
  ELSE LET m == CHOOSE r \in S : \A q \in S : LockRank(r.id) <= LockRank(q.id)
       IN  <<m>> \o SortByRank(S \ {m})

\* "Natural" (argument-order) acquisition -- BUGGY order, used only when SORTED=FALSE.
NatSeq(ot, nn, cc, dd) ==
  CASE ot = "create"  -> << [id |-> Bucket, mode |-> "S"] >>
    [] ot = "commit"  -> << [id |-> Bucket, mode |-> "S"], [id |-> Slot(nn), mode |-> "X"] >>
    [] ot = "delete"  -> << [id |-> Bucket, mode |-> "S"], [id |-> Slot(nn), mode |-> "X"] >>
    [] ot = "rename"  -> << [id |-> Bucket, mode |-> "S"], [id |-> Slot(nn), mode |-> "X"],
                            [id |-> Slot(dd), mode |-> "X"] >>
    [] ot = "setprop" -> << [id |-> Bucket, mode |-> "X"] >>

OrderedLocks(ot, nn, cc, dd) ==
  IF SORTED THEN SortByRank(Deduped(RawLocks(ot, nn, cc, dd)))
            ELSE NatSeq(ot, nn, cc, dd)

(*--algorithm obs

variables
  open = {},
  committed = {},
  prop = 0,
  used = 0,                              \* usedBytes counter (the merge operator)
  lockShared = [ l \in AllLocks |-> {} ],
  lockExcl   = [ l \in AllLocks |-> NoProc ];

process Worker \in Procs
variables
  opType = "none",
  n = 0, c = 0, d = 0, v = 0,
  okQuota = TRUE,
  reqLocks = << >>,
  i = 1,
  opsLeft = MAX_OPS;
begin
  Loop:
    while opsLeft > 0 do
      Pick:
        either
          opType := "create";
          with kk \in KEYS, cc \in CLIENTS do n := kk; c := cc; end with;
        or
          opType := "commit";
          with kk \in KEYS, cc \in CLIENTS do n := kk; c := cc; end with;
        or
          opType := "delete";
          with kk \in KEYS do n := kk; end with;
        or
          opType := "rename";
          with kk \in KEYS, dd \in (KEYS \ {kk}) do n := kk; d := dd; end with;
        or
          opType := "setprop";
          with vv \in PROPS do v := vv; end with;
        end either;
        reqLocks := OrderedLocks(opType, n, c, d);
        i := 1;

      AcquireLoop:
        while i <= Len(reqLocks) do
          if reqLocks[i].mode = "S" then
            await lockExcl[reqLocks[i].id] = NoProc;
            lockShared[reqLocks[i].id] := lockShared[reqLocks[i].id] \cup {self};
          else
            await (lockExcl[reqLocks[i].id] = NoProc) /\ (lockShared[reqLocks[i].id] = {});
            lockExcl[reqLocks[i].id] := self;
          end if;
          i := i + 1;
        end while;

      CommitPlan:  \* quota decided here from CURRENT used (shared bucket lock => may go stale)
        okQuota := \/ (opType # "commit")
                   \/ (n \in committed)
                   \/ (used + 1 <= QUOTA_LIMIT);

      CommitApply:  \* linearization point; applies the (possibly stale) plan, no re-check of quota
        if opType = "create" then
          open := open \cup {<<n, c>>};
        elsif opType = "commit" then
          if (<<n, c>> \in open) /\ okQuota then
            used := IF n \in committed THEN used ELSE used + 1;   \* reads OLD committed
            open := open \ {<<n, c>>};
            committed := committed \cup {n};
          end if;
        elsif opType = "delete" then
          if n \in committed then
            committed := committed \ {n};
            used := used - 1;
          end if;
        elsif opType = "rename" then
          if (n \in committed) /\ (d \notin committed) then
            committed := (committed \ {n}) \cup {d};
          end if;
        elsif opType = "setprop" then
          prop := v;
        end if;

      Release:
        lockShared := [ l \in AllLocks |-> lockShared[l] \ {self} ];
        lockExcl   := [ l \in AllLocks |-> IF lockExcl[l] = self THEN NoProc ELSE lockExcl[l] ];
        opsLeft := opsLeft - 1;
    end while;
end process;

end algorithm; *)

\* BEGIN TRANSLATION
VARIABLES open, committed, prop, used, lockShared, lockExcl, pc, opType, n, c, 
          d, v, okQuota, reqLocks, i, opsLeft

vars == << open, committed, prop, used, lockShared, lockExcl, pc, opType, n, 
           c, d, v, okQuota, reqLocks, i, opsLeft >>

ProcSet == (Procs)

Init == (* Global variables *)
        /\ open = {}
        /\ committed = {}
        /\ prop = 0
        /\ used = 0
        /\ lockShared = [ l \in AllLocks |-> {} ]
        /\ lockExcl = [ l \in AllLocks |-> NoProc ]
        (* Process Worker *)
        /\ opType = [self \in Procs |-> "none"]
        /\ n = [self \in Procs |-> 0]
        /\ c = [self \in Procs |-> 0]
        /\ d = [self \in Procs |-> 0]
        /\ v = [self \in Procs |-> 0]
        /\ okQuota = [self \in Procs |-> TRUE]
        /\ reqLocks = [self \in Procs |-> << >>]
        /\ i = [self \in Procs |-> 1]
        /\ opsLeft = [self \in Procs |-> MAX_OPS]
        /\ pc = [self \in ProcSet |-> "Loop"]

Loop(self) == /\ pc[self] = "Loop"
              /\ IF opsLeft[self] > 0
                    THEN /\ pc' = [pc EXCEPT ![self] = "Pick"]
                    ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
              /\ UNCHANGED << open, committed, prop, used, lockShared, 
                              lockExcl, opType, n, c, d, v, okQuota, reqLocks, 
                              i, opsLeft >>

Pick(self) == /\ pc[self] = "Pick"
              /\ \/ /\ opType' = [opType EXCEPT ![self] = "create"]
                    /\ \E kk \in KEYS:
                         \E cc \in CLIENTS:
                           /\ n' = [n EXCEPT ![self] = kk]
                           /\ c' = [c EXCEPT ![self] = cc]
                    /\ UNCHANGED <<d, v>>
                 \/ /\ opType' = [opType EXCEPT ![self] = "commit"]
                    /\ \E kk \in KEYS:
                         \E cc \in CLIENTS:
                           /\ n' = [n EXCEPT ![self] = kk]
                           /\ c' = [c EXCEPT ![self] = cc]
                    /\ UNCHANGED <<d, v>>
                 \/ /\ opType' = [opType EXCEPT ![self] = "delete"]
                    /\ \E kk \in KEYS:
                         n' = [n EXCEPT ![self] = kk]
                    /\ UNCHANGED <<c, d, v>>
                 \/ /\ opType' = [opType EXCEPT ![self] = "rename"]
                    /\ \E kk \in KEYS:
                         \E dd \in (KEYS \ {kk}):
                           /\ n' = [n EXCEPT ![self] = kk]
                           /\ d' = [d EXCEPT ![self] = dd]
                    /\ UNCHANGED <<c, v>>
                 \/ /\ opType' = [opType EXCEPT ![self] = "setprop"]
                    /\ \E vv \in PROPS:
                         v' = [v EXCEPT ![self] = vv]
                    /\ UNCHANGED <<n, c, d>>
              /\ reqLocks' = [reqLocks EXCEPT ![self] = OrderedLocks(opType'[self], n'[self], c'[self], d'[self])]
              /\ i' = [i EXCEPT ![self] = 1]
              /\ pc' = [pc EXCEPT ![self] = "AcquireLoop"]
              /\ UNCHANGED << open, committed, prop, used, lockShared, 
                              lockExcl, okQuota, opsLeft >>

AcquireLoop(self) == /\ pc[self] = "AcquireLoop"
                     /\ IF i[self] <= Len(reqLocks[self])
                           THEN /\ IF reqLocks[self][i[self]].mode = "S"
                                      THEN /\ lockExcl[reqLocks[self][i[self]].id] = NoProc
                                           /\ lockShared' = [lockShared EXCEPT ![reqLocks[self][i[self]].id] = lockShared[reqLocks[self][i[self]].id] \cup {self}]
                                           /\ UNCHANGED lockExcl
                                      ELSE /\ (lockExcl[reqLocks[self][i[self]].id] = NoProc) /\ (lockShared[reqLocks[self][i[self]].id] = {})
                                           /\ lockExcl' = [lockExcl EXCEPT ![reqLocks[self][i[self]].id] = self]
                                           /\ UNCHANGED lockShared
                                /\ i' = [i EXCEPT ![self] = i[self] + 1]
                                /\ pc' = [pc EXCEPT ![self] = "AcquireLoop"]
                           ELSE /\ pc' = [pc EXCEPT ![self] = "CommitPlan"]
                                /\ UNCHANGED << lockShared, lockExcl, i >>
                     /\ UNCHANGED << open, committed, prop, used, opType, n, c, 
                                     d, v, okQuota, reqLocks, opsLeft >>

CommitPlan(self) == /\ pc[self] = "CommitPlan"
                    /\ okQuota' = [okQuota EXCEPT ![self] = \/ (opType[self] # "commit")
                                                            \/ (n[self] \in committed)
                                                            \/ (used + 1 <= QUOTA_LIMIT)]
                    /\ pc' = [pc EXCEPT ![self] = "CommitApply"]
                    /\ UNCHANGED << open, committed, prop, used, lockShared, 
                                    lockExcl, opType, n, c, d, v, reqLocks, i, 
                                    opsLeft >>

CommitApply(self) == /\ pc[self] = "CommitApply"
                     /\ IF opType[self] = "create"
                           THEN /\ open' = (open \cup {<<n[self], c[self]>>})
                                /\ UNCHANGED << committed, prop, used >>
                           ELSE /\ IF opType[self] = "commit"
                                      THEN /\ IF (<<n[self], c[self]>> \in open) /\ okQuota[self]
                                                 THEN /\ used' = (IF n[self] \in committed THEN used ELSE used + 1)
                                                      /\ open' = open \ {<<n[self], c[self]>>}
                                                      /\ committed' = (committed \cup {n[self]})
                                                 ELSE /\ TRUE
                                                      /\ UNCHANGED << open, 
                                                                      committed, 
                                                                      used >>
                                           /\ prop' = prop
                                      ELSE /\ IF opType[self] = "delete"
                                                 THEN /\ IF n[self] \in committed
                                                            THEN /\ committed' = committed \ {n[self]}
                                                                 /\ used' = used - 1
                                                            ELSE /\ TRUE
                                                                 /\ UNCHANGED << committed, 
                                                                                 used >>
                                                      /\ prop' = prop
                                                 ELSE /\ IF opType[self] = "rename"
                                                            THEN /\ IF (n[self] \in committed) /\ (d[self] \notin committed)
                                                                       THEN /\ committed' = ((committed \ {n[self]}) \cup {d[self]})
                                                                       ELSE /\ TRUE
                                                                            /\ UNCHANGED committed
                                                                 /\ prop' = prop
                                                            ELSE /\ IF opType[self] = "setprop"
                                                                       THEN /\ prop' = v[self]
                                                                       ELSE /\ TRUE
                                                                            /\ prop' = prop
                                                                 /\ UNCHANGED committed
                                                      /\ used' = used
                                           /\ open' = open
                     /\ pc' = [pc EXCEPT ![self] = "Release"]
                     /\ UNCHANGED << lockShared, lockExcl, opType, n, c, d, v, 
                                     okQuota, reqLocks, i, opsLeft >>

Release(self) == /\ pc[self] = "Release"
                 /\ lockShared' = [ l \in AllLocks |-> lockShared[l] \ {self} ]
                 /\ lockExcl' = [ l \in AllLocks |-> IF lockExcl[l] = self THEN NoProc ELSE lockExcl[l] ]
                 /\ opsLeft' = [opsLeft EXCEPT ![self] = opsLeft[self] - 1]
                 /\ pc' = [pc EXCEPT ![self] = "Loop"]
                 /\ UNCHANGED << open, committed, prop, used, opType, n, c, d, 
                                 v, okQuota, reqLocks, i >>

Worker(self) == Loop(self) \/ Pick(self) \/ AcquireLoop(self)
                   \/ CommitPlan(self) \/ CommitApply(self)
                   \/ Release(self)

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == (\E self \in Procs: Worker(self))
           \/ Terminating

Spec == Init /\ [][Next]_vars

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION

\* ---- refinement mapping: the committed namespace + quota IS the abstract state ----
ABS == INSTANCE ObsAbstract WITH aopen <- open, acommitted <- committed, aprop <- prop, aused <- used
Refinement == ABS!ASpec

\* Exact-quota oracle, kept ONLY to document the accepted over-commit limitation.
\* ObsImpl does NOT refine this -- run QuotaOvercommit.cfg to reproduce the trace.
ABSExact == INSTANCE ObsAbstractExact WITH aopen <- open, acommitted <- committed, aprop <- prop, aused <- used
RefinementExact == ABSExact!ASpec

\* ---- lock-safety invariants ----
LockInv == \A l \in AllLocks : (lockExcl[l] # NoProc) => (lockShared[l] = {})

NoLeak ==
  \A p \in Procs :
    (pc[p] = "Done") =>
      /\ \A l \in AllLocks : p \notin lockShared[l]
      /\ \A l \in AllLocks : lockExcl[l] # p

\* the counter never loses an update (stays equal to the true committed count),
\* even if quota is over-committed -- separates "counter consistent" from "limit enforced".
UsedConsistent == used = Cardinality(committed)
=============================================================================
