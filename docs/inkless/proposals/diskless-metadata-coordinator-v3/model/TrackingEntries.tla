--------------------------- MODULE TrackingEntries ---------------------------
(***************************************************************************)
(* PROTOTYPE, throwaway. Bounded model of the tracking entries from ticket *)
(* 05 for one broker, one leased range 1..MaxSeq at tracking partition t,   *)
(* and participants P. Fences, fence answers, release reports, and late     *)
(* attempts are separate messages that any interleaving can reorder.        *)
(*                                                                         *)
(* Abstractions:                                                           *)
(*  - Leases, switches, and floors across partitions belong to             *)
(*    FencesFloors. The lease ends once, after the broker stops.           *)
(*  - A finish report is computed when it's delivered. Finished is         *)
(*    monotone, so a later report only claims true facts.                  *)
(*  - t isn't a participant, and fences aren't hull-merged.                *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS
  P,              \* participants, as model values
  t,              \* the tracking partition, a model value
  MaxSeq,         \* sequences in the leased range
  FixTerminal,    \* 05: released is terminal
  FixStoreEarly,  \* 05: a report for an unrecorded sequence is stored
  FixResolvedFloor, \* 17: no entry at or below t's own floor means resolved
  MaxFaults,      \* lost-tail faults in one behavior
  MaxLate         \* late attempts in flight at once

Nodes == P \cup {t}
Seqs == 1..MaxSeq
Max2(a, b) == IF a > b THEN a ELSE b
Min(X) == CHOOSE x \in X : \A y \in X : x <= y
DataSets == (SUBSET P) \ {{}}
Small(X) == IF MaxLate = 0 THEN {{}} ELSE {{}} \cup {{x} : x \in X}

VARIABLES
  nextSeq,  \* broker: the next sequence it uses
  ds,       \* broker: the data set of each used sequence
  part,     \* broker: part[s][n] is "open", "ok", or "exc"
  stopped,  \* broker: paused or restarted, so the lease can end
  net,      \* late attempts in flight, as <<s, n>>
  T,        \* tracking partition state
  Q,        \* participant states
  ans,      \* fence answers in flight
  deleted, everRel, hist, faults

vars == <<nextSeq, ds, part, stopped, net, T, Q, ans, deleted, everRel, hist, faults>>

T0 == [rec |-> {}, hiRec |-> 0, pset |-> <<>>, ent |-> <<>>,
       rep |-> [ft |-> 0, exc |-> {}], floor |-> 0, jobs |-> {}, queue |-> {}]
Q0 == [floor |-> 0, held |-> {}, rel |-> {}, relPend |-> {}, done |-> {}]

(* Broker-side view *)
Used == 1..(nextSeq - 1)
Finished(s) == \A n \in DOMAIN part[s] : part[s][n] # "open"
FT ==
  LET un == {s \in Used : ~Finished(s)}
  IN IF un = {} THEN nextSeq - 1 ELSE Min(un) - 1
Report ==
  [ft |-> FT,
   exc |-> {ps \in P \X Seqs : /\ ps[2] <= FT /\ ps[2] \in Used
                                /\ ps[1] \in ds[ps[2]] /\ part[ps[2]][ps[1]] = "exc"}]

(* Tracking partition helpers *)
State(st, s, p) ==
  IF s \in DOMAIN st.ent /\ p \in DOMAIN st.ent[s] THEN st.ent[s][p] ELSE "none"
Fencing(st, s) == [p \in P |-> IF State(st, s, p) = "released" THEN "released" ELSE "fencing"]

(* The tracking partition skips a fence to a participant whose entries in *)
(* the range are all held or released.                                    *)
NewJobs(st, a, b) ==
  {[p |-> p, a |-> a, b |-> b] :
     p \in {p2 \in P : \E s \in a..b : State(st, s, p2) \in {"pending", "fencing"}}}

ReportTrig(st) ==
  LET Moves(s, p) ==
        /\ s \in st.rec /\ s <= st.rep.ft
        /\ State(st, s, p) = "pending"
      NS(s, p) == IF Moves(s, p)
                  THEN IF <<p, s>> \in st.rep.exc THEN "fencing" ELSE "held"
                  ELSE st.ent[s][p]
      exc == {ps \in P \X Seqs : Moves(ps[2], ps[1]) /\ ps \in st.rep.exc}
  IN [st EXCEPT !.ent = [s \in DOMAIN st.ent |-> [p \in DOMAIN st.ent[s] |-> NS(s, p)]],
                !.jobs = @ \cup {[p |-> ps[1], a |-> ps[2], b |-> ps[2]] : ps \in exc}]

MissedTrig(st) ==
  LET Cand == {s \in 1..st.rep.ft : s \notin st.rec /\ s \notin DOMAIN st.pset}
      st2 == [st EXCEPT !.pset = [s \in Cand |-> P] @@ @,
                        !.ent = [s \in Cand |-> Fencing(st, s)] @@ @,
                        !.floor = IF Cand = {} THEN @ ELSE Max2(@, st.rep.ft)]
  IN [st2 EXCEPT !.jobs = @ \cup UNION {NewJobs(st2, s, s) : s \in Cand}]

QueueTrig(st) ==
  LET Done == {s \in DOMAIN st.ent :
                 /\ s \in DOMAIN st.pset
                 /\ st.pset[s] \subseteq DOMAIN st.ent[s]
                 /\ \A p \in DOMAIN st.ent[s] : st.ent[s][p] = "released"}
  IN [st EXCEPT !.queue = @ \cup Done,
                !.ent = [s \in DOMAIN @ \ Done |-> @[s]]]

Settle(st) == QueueTrig(MissedTrig(ReportTrig(st)))
MergeRep(st, r) == IF r.ft >= st.rep.ft THEN [st EXCEPT !.rep = r] ELSE st

(* Every action that changes T or Q goes through Upd. Storage deletes a  *)
(* key in the same step that queues it, which is the worst case.         *)
Upd(newT, newQ, ns) ==
  /\ T' = newT
  /\ Q' = newQ
  /\ deleted' = deleted \cup newT.queue
  /\ everRel' = everRel \cup {ps \in P \X Seqs : State(newT, ps[2], ps[1]) = "released"}
  /\ hist' = IF MaxFaults = 0 THEN hist
             ELSE [n \in Nodes |-> IF n \notin ns THEN hist[n]
                                   ELSE Append(hist[n], IF n = t THEN newT ELSE newQ[n])]

-----------------------------------------------------------------------------
Init ==
  /\ nextSeq = 1
  /\ ds = <<>>
  /\ part = <<>>
  /\ stopped = FALSE
  /\ net = {}
  /\ T = T0
  /\ Q = [p \in P |-> Q0]
  /\ ans = {}
  /\ deleted = {}
  /\ everRel = {}
  /\ hist = [n \in Nodes |-> IF MaxFaults = 0 THEN <<>> ELSE <<IF n = t THEN T0 ELSE Q0>>]
  /\ faults = {}

OpenParts == {sn \in Used \X Nodes : sn[2] \in DOMAIN part[sn[1]] /\ part[sn[1]][sn[2]] = "open"}

(* Broker actions *)
CloseFile(d) ==
  /\ ~stopped
  /\ nextSeq <= MaxSeq
  /\ ds' = ds @@ (nextSeq :> d)
  /\ part' = part @@ (nextSeq :> [n \in d \cup {t} |-> "open"])
  /\ nextSeq' = nextSeq + 1
  /\ UNCHANGED <<stopped, net, T, Q, ans, deleted, everRel, hist, faults>>

(* A deadline passes only after every earlier file finished. *)
GiveUp(s, L) ==
  /\ ~Finished(s)
  /\ \A s2 \in Used : s2 < s => Finished(s2)
  /\ L \subseteq {<<s, n>> : n \in {n2 \in DOMAIN part[s] : part[s][n2] = "open"}}
  /\ Cardinality(net \cup L) <= MaxLate
  /\ part' = [part EXCEPT ![s] = [n \in DOMAIN @ |-> IF @[n] = "open" THEN "exc" ELSE @[n]]]
  /\ net' = net \cup L
  /\ UNCHANGED <<nextSeq, ds, stopped, T, Q, ans, deleted, everRel, hist, faults>>

(* A pause, cut-off, or restart. Every part has finished or given up. *)
Stop ==
  /\ ~stopped
  /\ OpenParts = {}
  /\ stopped' = TRUE
  /\ UNCHANGED <<nextSeq, ds, part, net, T, Q, ans, deleted, everRel, hist, faults>>

SendReport ==
  LET st == Settle(MergeRep(T, Report))
  IN /\ ~stopped
     /\ st # T
     /\ Upd(st, Q, {t})
     /\ UNCHANGED <<nextSeq, ds, part, stopped, net, ans, faults>>

(* Coordinator actions *)
RecordAt(s, withRep) ==
  LET st == T
      early == IF s \in DOMAIN st.ent THEN st.ent[s] ELSE <<>>
      fresh == [p \in ds[s] |-> IF p \in DOMAIN early /\ early[p] = "released"
                               THEN "released" ELSE "pending"]
      st2 == [st EXCEPT !.rec = @ \cup {s}, !.hiRec = Max2(@, s),
                        !.pset = (s :> ds[s]) @@ @,
                        !.ent = (s :> (fresh @@ early)) @@ @]
  IN Settle(IF withRep THEN MergeRep(st2, Report) ELSE st2)

(* A partition accepts only a sequence above its floor and above the last *)
(* one it committed for the broker, as in FencesFloors.                   *)
RowP(p) == IF Q[p].done = {} THEN 0 ELSE CHOOSE x \in Q[p].done : \A y \in Q[p].done : x >= y
AcceptsT(s) == s > Max2(T.floor, T.hiRec)
AcceptsP(s, p) == s > Max2(Q[p].floor, RowP(p))

CommitP(s, p, allFail) ==
  [Q EXCEPT ![p].done = @ \cup {s},
            ![p].held = IF allFail THEN @ ELSE @ \cup {s},
            ![p].rel = IF allFail THEN @ \cup {s} ELSE @,
            ![p].relPend = IF allFail THEN @ \cup {s} ELSE @]

Commit(s, n) ==
  /\ part[s][n] = "open"
  /\ ~\E s2 \in Used : s2 < s /\ n \in DOMAIN part[s2] /\ part[s2][n] = "open"
  /\ IF n = t
     THEN /\ IF AcceptsT(s) THEN Upd(RecordAt(s, TRUE), Q, {t})
             ELSE UNCHANGED <<T, Q, deleted, everRel, hist>>
          /\ part' = [part EXCEPT ![s][t] = IF AcceptsT(s) THEN "ok" ELSE "exc"]
     ELSE \E allFail \in BOOLEAN :
          /\ IF AcceptsP(s, n) THEN Upd(T, CommitP(s, n, allFail), {n})
             ELSE UNCHANGED <<T, Q, deleted, everRel, hist>>
          /\ part' = [part EXCEPT ![s][n] = IF AcceptsP(s, n) THEN "ok" ELSE "exc"]
  /\ UNCHANGED <<nextSeq, ds, stopped, net, ans, faults>>

(* A rejection before the producer checks, such as a full overlay. *)
Reject(s, n) ==
  /\ part[s][n] = "open"
  /\ part' = [part EXCEPT ![s][n] = "exc"]
  /\ UNCHANGED <<nextSeq, ds, stopped, net, T, Q, ans, deleted, everRel, hist, faults>>

(* A late attempt carries no report. *)
Late(s, n) ==
  /\ <<s, n>> \in net
  /\ net' = net \ {<<s, n>>}
  /\ IF n = t
     THEN IF AcceptsT(s) THEN Upd(RecordAt(s, FALSE), Q, {t})
          ELSE UNCHANGED <<T, Q, deleted, everRel, hist>>
     ELSE \E allFail \in BOOLEAN :
          IF AcceptsP(s, n) THEN Upd(T, CommitP(s, n, allFail), {n})
          ELSE UNCHANGED <<T, Q, deleted, everRel, hist>>
  /\ UNCHANGED <<nextSeq, ds, part, stopped, ans, faults>>

(* The idle end or the epoch end, from the recorded finishedThrough. *)
LeaseEnd ==
  LET st == T
      lo == st.rep.ft + 1
      newS == {s \in lo..MaxSeq : s \notin DOMAIN st.pset}
      NS(s, p) == IF st.ent[s][p] = "pending" THEN "fencing" ELSE st.ent[s][p]
      st2 == [st EXCEPT !.pset = [s \in newS |-> P] @@ @,
                        !.ent = [s \in newS \cup DOMAIN @ |->
                                   IF s \in newS THEN Fencing(st, s)
                                   ELSE IF s >= lo THEN [p \in DOMAIN @[s] |-> NS(s, p)]
                                   ELSE @[s]],
                        !.floor = MaxSeq]
      st3 == [st2 EXCEPT !.jobs = @ \cup (IF lo <= MaxSeq THEN NewJobs(st2, lo, MaxSeq) ELSE {})]
  IN /\ stopped
     /\ T.floor < MaxSeq
     /\ Upd(Settle(st3), Q, {t})
     /\ UNCHANGED <<nextSeq, ds, part, stopped, net, ans, faults>>

(* Participant p applies fence j and sends its answer. *)
FenceApply(j) ==
  LET p == j.p
      refs == {s \in j.a..j.b : s \in Q[p].held}
  IN /\ j \in T.jobs
     /\ ~\E x \in ans : x.j = j
     /\ Upd(T, [Q EXCEPT ![p].floor = Max2(@, j.b),
                         ![p].rel = @ \cup ((j.a..j.b) \ refs)], {p})
     /\ ans' = ans \cup {[j |-> j, refs |-> refs]}
     /\ UNCHANGED <<nextSeq, ds, part, stopped, net, faults>>

AnswerApply(x) ==
  LET st == T
      p == x.j.p
      NS(s) == IF FixTerminal /\ st.ent[s][p] = "released" THEN "released"
               ELSE IF s \in x.refs THEN "held" ELSE "released"
      st2 == [st EXCEPT !.jobs = @ \ {x.j},
                        !.ent = [s \in DOMAIN @ |->
                                   IF s \in x.j.a..x.j.b /\ p \in DOMAIN @[s]
                                   THEN [@[s] EXCEPT ![p] = NS(s)] ELSE @[s]]]
  IN /\ x \in ans
     /\ ans' = ans \ {x}
     /\ Upd(Settle(st2), Q, {t})
     /\ UNCHANGED <<nextSeq, ds, part, stopped, net, faults>>

RemoveBatch(p, s) ==
  /\ s \in Q[p].held
  /\ Upd(T, [Q EXCEPT ![p].held = @ \ {s}, ![p].rel = @ \cup {s},
                      ![p].relPend = @ \cup {s}], {p})
  /\ UNCHANGED <<nextSeq, ds, part, stopped, net, ans, faults>>

(* A release report reaches t. Without ack, the reply is lost, or the *)
(* participant's leader changes before release-accepted applies.     *)
DeliverReport(p, s, ack) ==
  LET st == T
      bound == IF FixResolvedFloor THEN Max2(st.hiRec, st.floor) ELSE st.hiRec
      st2 == IF s \in DOMAIN st.ent
             THEN [st EXCEPT !.ent[s] = (p :> "released") @@ @]
             ELSE IF s > bound /\ FixStoreEarly
             THEN [st EXCEPT !.ent = (s :> (p :> "released")) @@ @]
             ELSE st
      newQ == IF ack THEN [Q EXCEPT ![p].relPend = @ \ {s}] ELSE Q
  IN /\ s \in Q[p].relPend
     /\ Upd(Settle(st2), newQ, {t} \cup (IF ack THEN {p} ELSE {}))
     /\ UNCHANGED <<nextSeq, ds, part, stopped, net, ans, faults>>

(* Drops a suffix of acknowledged records at n. Messages in flight and *)
(* storage deletes stay.                                                *)
LostTail(n) ==
  /\ Cardinality(faults) < MaxFaults
  /\ \E i \in 1..(Len(hist[n]) - 1) :
       /\ IF n = t THEN T' = hist[n][i] /\ Q' = Q
          ELSE Q' = [Q EXCEPT ![n] = hist[n][i]] /\ T' = T
       /\ hist' = [hist EXCEPT ![n] = SubSeq(@, 1, i)]
  /\ faults' = faults \cup {n}
  /\ UNCHANGED <<nextSeq, ds, part, stopped, net, ans, deleted, everRel>>

Next ==
  \/ \E d \in DataSets : CloseFile(d)
  \/ \E s \in Used : \E L \in Small({s} \X Nodes) : GiveUp(s, L)
  \/ Stop
  \/ SendReport
  \/ \E sn \in OpenParts : Commit(sn[1], sn[2]) \/ Reject(sn[1], sn[2])
  \/ \E sn \in net : Late(sn[1], sn[2])
  \/ LeaseEnd
  \/ \E j \in T.jobs : FenceApply(j)
  \/ \E x \in ans : AnswerApply(x)
  \/ \E p \in P : \E s \in Q[p].held : RemoveBatch(p, s)
  \/ \E p \in P : \E s \in Q[p].relPend : \E ack \in BOOLEAN : DeliverReport(p, s, ack)
  \/ \E n \in Nodes : LostTail(n)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Invariants *)

(* No object is deleted or queued while a batch references it. A partition *)
(* that lost its tail is exempt, as in FencesFloors.                       *)
NoDeleteReferenced ==
  \A s \in deleted \cup T.queue : \A p \in P \ faults : s \notin Q[p].held

(* No event moves an entry out of released. *)
ReleasedTerminal ==
  faults = {} => \A ps \in everRel : State(T, ps[2], ps[1]) \in {"released", "none"}

(* A participant never holds a batch of a sequence it has released. *)
NoHoldAfterRelease == \A p \in P \ faults : Q[p].rel \cap Q[p].held = {}

(* No entry outlives its deleted object, which would stay stuck. *)
NoEntryForDeleted == faults = {} => \A s \in deleted : s \notin DOMAIN T.ent

(* No fence raises a floor past a part the broker still sends. *)
NoCutLive ==
  faults = {} =>
    \A sn \in OpenParts :
      sn[1] > (IF sn[2] = t THEN T.floor ELSE Q[sn[2]].floor)

Quiet ==
  /\ stopped /\ T.floor = MaxSeq /\ net = {} /\ ans = {} /\ T.jobs = {}
  /\ \A p \in P : Q[p].relPend = {}

(* Once the lease ended and nothing is in flight, every used key that no *)
(* participant holds is deleted.                                          *)
NoLeak ==
  (faults = {} /\ Quiet) =>
    \A s \in Used : (\A p \in P : s \notin Q[p].held) => s \in deleted

Perms == Permutations(P)
=============================================================================
