---------------------------- MODULE FencesFloors ----------------------------
(***************************************************************************)
(* PROTOTYPE, throwaway. Bounded model of leases, commits, fences, floors, *)
(* releases, lease ends, and the deletion queue for one uploading broker   *)
(* and the coordinator partitions P. Each v3 fix is a flag, so a config    *)
(* with one fix off reproduces the v2 defect that fix closes.              *)
(*                                                                         *)
(* Abstractions:                                                           *)
(*  - One append is atomic with its apply and its reply. The overlay isn't *)
(*    modeled.                                                             *)
(*  - A finish report is computed when it's delivered, not when it's sent. *)
(*    Finished is monotone, so a later report only claims true facts.      *)
(*  - Time is untimed. A file's deadline passes only after every earlier   *)
(*    file finished. With FixLease, the idle timer fires only for a paused *)
(*    or cut-off broker, and the pause first finishes every open part.     *)
(*  - Fences are per range, with no hull merge across requests.            *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS
  P,              \* coordinator partitions, as model values
  R,              \* lease range length
  MaxSeq,         \* highest sequence in an epoch
  MaxEpoch,       \* 1 or 2
  MaxFiles,       \* files closed in one behavior
  FixFinish,      \* 01: finish reports replace the silent-participant trigger
  FixLease,       \* 02: keepalives, switch on confirm, scoped lease release
  FixBrokerWide,  \* 06: finishedThrough covers every file of the epoch
  FixPlannedWait, \* 17: a release waits until no planned sequence is at or below b
  FixMissedFirst, \* 17: a release's report marks missed sequences before the lease end
  MaxFaults,      \* lost-tail faults in one behavior; faults is the set of partitions that lost one
  MaxLate         \* late attempts in flight at once

ASSUME MaxEpoch \in {1, 2}
ASSUME MaxLate \in {0, 1}

None == [tp |-> "none"]
Pair0 == <<0, 0>>
GT(a, b) == a[1] > b[1] \/ (a[1] = b[1] /\ a[2] > b[2])
GE(a, b) == GT(a, b) \/ a = b
MaxPair(a, b) == IF GT(a, b) THEN a ELSE b
Max(X) == CHOOSE x \in X : \A y \in X : x >= y
Min(X) == CHOOSE x \in X : \A y \in X : x <= y
Seqs == 1..MaxSeq
DataSets == (SUBSET P) \ {{}}
Small(X) == IF MaxLate = 0 THEN {{}} ELSE {{}} \cup {{x} : x \in X}

VARIABLES
  epoch, ranges, reqHi, nextSeq, renewal, files, toRelease, paused, pausedEver,
  net, S, deleted, hist, faults

vars == <<epoch, ranges, reqHi, nextSeq, renewal, files, toRelease, paused,
          pausedEver, net, S, deleted, hist, faults>>

EmptyRep == [ft |-> 0, exc |-> {}]

S0 == [row |-> Pair0, fenced |-> Pair0, held |-> {}, relPend |-> {},
       leases |-> {}, rec |-> <<>>, known |-> {},
       rep |-> [e \in 1..MaxEpoch |-> EmptyRep],
       ent |-> <<>>, jobs |-> {}, queue |-> {}]

Floor(st) == MaxPair(st.row, st.fenced)
InRanges(st, e, s) == \E r \in st.leases : r.e = e /\ r.a <= s /\ s <= r.b

(* Broker-side view *)
Finished(k) == \A q \in DOMAIN files[k].parts : files[k].parts[q] # "open"
FilesOf(e) == {k \in DOMAIN files : k[1] = e}

FT(q, e) ==
  LET considered == {k \in FilesOf(e) : FixBrokerWide \/ files[k].tp = q}
      unfinished == {k[2] : k \in {k2 \in considered : ~Finished(k2)}}
      hi == IF e = epoch THEN nextSeq - 1 ELSE MaxSeq
  IN IF unfinished = {} THEN hi ELSE Min(unfinished) - 1

Exc(q, e) ==
  LET ft == FT(q, e)
      ks == {k \in FilesOf(e) : files[k].tp = q /\ k[2] <= ft}
  IN UNION {{<<p, k[2]>> : p \in {p2 \in DOMAIN files[k].parts \ {q} :
                                     files[k].parts[p2] = "exc"}} : k \in ks}

Report(q, e) == [ft |-> FT(q, e), exc |-> Exc(q, e)]
MergeRep(old, new) == IF new.ft >= old.ft THEN new ELSE old
WithReport(q, st, e) ==
  IF FixFinish THEN [st EXCEPT !.rep[e] = MergeRep(st.rep[e], Report(q, e))]
  ELSE st

(* Triggers that run in the apply of every record at a tracking partition *)
ExcTrig(st) ==
  IF ~FixFinish THEN st ELSE
  LET NS(k, p) ==
        IF k \in DOMAIN st.rec /\ st.ent[k][p] = "pending"
        THEN IF <<p, k[2]>> \in st.rep[k[1]].exc THEN "fencing"
             ELSE IF k[2] <= st.rep[k[1]].ft THEN "held" ELSE "pending"
        ELSE st.ent[k][p]
      newJobs == {<<kp[2], kp[1][1], kp[1][2], kp[1][2]>> :
                    kp \in {x \in (DOMAIN st.ent) \X P :
                              /\ x[2] \in DOMAIN st.ent[x[1]]
                              /\ st.ent[x[1]][x[2]] = "pending"
                              /\ NS(x[1], x[2]) = "fencing"}}
  IN [st EXCEPT !.ent = [k \in DOMAIN st.ent |->
                           [p \in DOMAIN st.ent[k] |-> NS(k, p)]],
                !.jobs = @ \cup newJobs]

Fencing(st, k) ==
  [p \in P |-> IF k \in DOMAIN st.ent /\ p \in DOMAIN st.ent[k]
                  /\ st.ent[k][p] = "released"
               THEN "released" ELSE "fencing"]

MissedTrig(st) ==
  LET Cand == {k \in (1..MaxEpoch) \X Seqs :
                 /\ \E r \in st.leases : r.e = k[1] /\ r.live
                                         /\ r.a <= k[2] /\ k[2] <= r.b
                 /\ k \notin DOMAIN st.rec
                 /\ k \notin st.known
                 /\ IF FixFinish THEN k[2] <= st.rep[k[1]].ft
                    ELSE \E s2 \in Seqs : s2 > k[2] /\ <<k[1], s2>> \in DOMAIN st.rec}
  IN [st EXCEPT !.ent = [k \in DOMAIN st.ent \cup Cand |->
                           IF k \in Cand THEN Fencing(st, k) ELSE st.ent[k]],
                !.known = @ \cup Cand,
                !.jobs = @ \cup {<<p, k[1], k[2], k[2]>> : k \in Cand, p \in P}]

SilentTrig(q, st) ==
  IF FixFinish THEN st ELSE
  LET Trig == {k \in DOMAIN st.rec :
                 /\ (q \notin st.rec[k] \/ st.ent[k][q] = "released")
                 /\ \A p \in DOMAIN st.ent[k] : st.ent[k][p] # "held"
                 /\ \E p \in DOMAIN st.ent[k] : st.ent[k][p] = "pending"}
      newJobs == {<<kp[2], kp[1][1], kp[1][2], kp[1][2]>> :
                    kp \in {x \in Trig \X P : x[2] \in DOMAIN st.ent[x[1]]
                                              /\ st.ent[x[1]][x[2]] = "pending"}}
  IN [st EXCEPT !.ent = [k \in DOMAIN st.ent |->
                           IF k \in Trig
                           THEN [p \in DOMAIN st.ent[k] |->
                                   IF st.ent[k][p] = "pending" THEN "fencing"
                                   ELSE st.ent[k][p]]
                           ELSE st.ent[k]],
                !.jobs = @ \cup newJobs]

QueueTrig(st) ==
  [st EXCEPT !.queue = @ \cup {k \in st.known :
                                 /\ k \in DOMAIN st.ent
                                 /\ \A p \in DOMAIN st.ent[k] :
                                      st.ent[k][p] = "released"}]

Settle(q, st) == QueueTrig(SilentTrig(q, MissedTrig(ExcTrig(st))))

(* A lease end at q for epoch e: up to b, or every range of e when all. *)
EndLease(q, st, e, b, all) ==
  LET atE == {r \in st.leases : r.e = e}
      ending == {r \in atE : r.live /\ (all \/ r.b <= b)}
      hiB == IF all THEN (IF atE = {} THEN 0 ELSE Max({r.b : r \in atE})) ELSE b
      recd == {s \in Seqs : <<e, s>> \in DOMAIN st.rec
                            /\ \E r \in ending : r.a <= s /\ s <= r.b}
      lo == IF FixFinish THEN st.rep[e].ft + 1
            ELSE IF recd # {} THEN Max(recd) + 1
            ELSE IF ending = {} THEN hiB + 1
            ELSE Min({r.a : r \in ending})
      own == {s \in lo..hiB : InRanges(st, e, s)}
      newKnown == {<<e, s>> : s \in own} \ DOMAIN st.rec
      st2 == [st EXCEPT
               !.fenced = MaxPair(@, <<e, hiB>>),
               !.leases = {IF r \in ending THEN [r EXCEPT !.live = FALSE] ELSE r
                             : r \in @},
               !.ent = [k \in DOMAIN st.ent \cup newKnown |->
                          IF k \in newKnown THEN Fencing(st, k)
                          ELSE IF FixFinish /\ k[1] = e /\ k[2] \in own
                          THEN [p \in DOMAIN st.ent[k] |->
                                  IF st.ent[k][p] = "pending" THEN "fencing"
                                  ELSE st.ent[k][p]]
                          ELSE st.ent[k]],
               !.known = @ \cup newKnown,
               !.jobs = @ \cup (IF lo <= hiB
                                THEN {<<p, e, lo, hiB>> : p \in P} ELSE {})]
  IN Settle(q, st2)

Hist(Q) ==
  IF MaxFaults = 0 THEN hist
  ELSE [q \in P |-> IF q \in Q THEN Append(hist[q], S'[q]) ELSE hist[q]]

(* Every action that changes S goes through Upd. Storage deletes a key in *)
(* the same step that queues it, which is the worst case for safety.      *)
Upd(newS, Q) ==
  /\ S' = newS
  /\ hist' = Hist(Q)
  /\ deleted' = deleted \cup UNION {newS[q].queue : q \in P}

Broker == <<epoch, ranges, reqHi, nextSeq, renewal, files, toRelease, paused,
            pausedEver>>

-----------------------------------------------------------------------------
Init ==
  /\ epoch = 1
  /\ ranges = {}
  /\ reqHi = 0
  /\ nextSeq = 1
  /\ renewal = None
  /\ files = <<>>
  /\ toRelease = {}
  /\ paused = FALSE
  /\ pausedEver = FALSE
  /\ net = {}
  /\ S = [q \in P |-> S0]
  /\ deleted = {}
  /\ hist = [q \in P |-> IF MaxFaults = 0 THEN <<>> ELSE <<S0>>]
  /\ faults = {}

OpenParts == {kq \in (DOMAIN files) \X P :
                kq[2] \in DOMAIN files[kq[1]].parts
                /\ files[kq[1]].parts[kq[2]] = "open"}

CloseAll(L) ==
  /\ files' = [k \in DOMAIN files |->
                 [files[k] EXCEPT !.parts = [q \in DOMAIN @ |->
                                               IF @[q] = "open" THEN "exc" ELSE @[q]]]]
  /\ net' = net \cup L

(* Broker actions *)
RequestLease(q) ==
  /\ renewal = None
  /\ ~paused
  /\ reqHi + R <= MaxSeq
  /\ renewal' = [tp |-> q, a |-> reqHi + 1, b |-> reqHi + R]
  /\ reqHi' = reqHi + R
  /\ UNCHANGED <<epoch, ranges, nextSeq, files, toRelease, paused, pausedEver,
                 net, S, deleted, hist, faults>>

GrantLease(lost) ==
  LET q == renewal.tp
      nr == [tp |-> q, a |-> renewal.a, b |-> renewal.b]
      st == S[q]
      inUse == {r.tp : r \in ranges}
      st2 == Settle(q, WithReport(q,
               [st EXCEPT !.leases = @ \cup {[e |-> epoch, a |-> nr.a,
                                              b |-> nr.b, live |-> TRUE]}],
               epoch))
  IN
  /\ renewal # None
  /\ Upd([S EXCEPT ![q] = st2], {q})
  /\ renewal' = None
  /\ IF lost
     THEN /\ ranges' = ranges
          /\ toRelease' = IF FixLease
                          THEN toRelease \cup {[tp |-> q, e |-> epoch, b |-> nr.b]}
                          ELSE toRelease
     ELSE IF ~FixLease \/ inUse \subseteq {q}
     THEN /\ ranges' = ranges \cup {nr}
          /\ toRelease' = toRelease
     ELSE /\ ranges' = {nr}
          /\ toRelease' = toRelease \cup
               {[tp |-> t2, e |-> epoch,
                 b |-> Max({r.b : r \in {r2 \in ranges : r2.tp = t2}})]
                  : t2 \in inUse \ {q}}
  /\ UNCHANGED <<epoch, reqHi, nextSeq, files, paused, pausedEver, net,
                 faults>>

GiveUpRenewal ==
  /\ renewal # None
  /\ renewal' = None
  /\ toRelease' = IF FixLease
                  THEN toRelease \cup {[tp |-> renewal.tp, e |-> epoch, b |-> renewal.b]}
                  ELSE toRelease
  /\ UNCHANGED <<epoch, ranges, reqHi, nextSeq, files, paused, pausedEver,
                 net, S, deleted, hist, faults>>

CloseFile(ds) ==
  LET avail == {s \in nextSeq..MaxSeq : \E r \in ranges : r.a <= s /\ s <= r.b}
      s == Min(avail)
      tp == (CHOOSE r \in ranges : r.a <= s /\ s <= r.b).tp
      k == <<epoch, s>>
  IN
  /\ ~paused
  /\ Cardinality(DOMAIN files) < MaxFiles
  /\ avail # {}
  /\ files' = files @@ (k :> [tp |-> tp, ds |-> ds,
                              parts |-> [q \in ds \cup {tp} |-> "open"]])
  /\ nextSeq' = s + 1
  /\ UNCHANGED <<epoch, ranges, reqHi, renewal, toRelease, paused, pausedEver,
                 net, S, deleted, hist, faults>>

(* A file's deadline passes only after every earlier file finished. Each *)
(* open part can leave a late attempt in flight.                         *)
GiveUpFile(k, L) ==
  /\ ~Finished(k)
  /\ Cardinality(net \cup L) <= MaxLate
  /\ \A k2 \in FilesOf(k[1]) : k2[2] < k[2] => Finished(k2)
  /\ L \subseteq {<<k, q>> : q \in {q2 \in DOMAIN files[k].parts :
                                       files[k].parts[q2] = "open"}}
  /\ files' = [files EXCEPT ![k].parts =
                 [q \in DOMAIN @ |-> IF @[q] = "open" THEN "exc" ELSE @[q]]]
  /\ net' = net \cup L
  /\ UNCHANGED <<epoch, ranges, reqHi, nextSeq, renewal, toRelease, paused,
                 pausedEver, S, deleted, hist, faults>>

Restart(L) ==
  /\ epoch < MaxEpoch
  /\ Cardinality(net \cup L) <= MaxLate
  /\ L \subseteq OpenParts
  /\ CloseAll(L)
  /\ epoch' = epoch + 1
  /\ ranges' = {}
  /\ reqHi' = 0
  /\ nextSeq' = 1
  /\ renewal' = None
  /\ toRelease' = {}
  /\ paused' = FALSE
  /\ UNCHANGED <<pausedEver, S, deleted, hist, faults>>

(* A pause or a cut-off longer than the idle timeout, at most once. *)
Pause(L) ==
  /\ ~pausedEver
  /\ Cardinality(net \cup L) <= MaxLate
  /\ L \subseteq OpenParts
  /\ CloseAll(L)
  /\ paused' = TRUE
  /\ pausedEver' = TRUE
  /\ UNCHANGED <<epoch, ranges, reqHi, nextSeq, renewal, toRelease,
                 S, deleted, hist, faults>>

Resume ==
  /\ paused
  /\ paused' = FALSE
  /\ UNCHANGED <<epoch, ranges, reqHi, nextSeq, renewal, files, toRelease,
                 pausedEver, net, S, deleted, hist, faults>>

ReleaseReady(rl) ==
  /\ \A k \in FilesOf(rl.e) :
       (IF FixBrokerWide THEN k[2] <= rl.b ELSE files[k].tp = rl.tp) => Finished(k)
  /\ FixPlannedWait =>
       \A r \in ranges : \A s \in r.a..r.b : s >= nextSeq => s > rl.b

SendRelease(rl) ==
  LET q == rl.tp
      st0 == WithReport(q, S[q], rl.e)
      st == IF FixMissedFirst THEN Settle(q, st0) ELSE st0
      st2 == IF GT(<<rl.e, rl.b>>, st.fenced) THEN EndLease(q, st, rl.e, rl.b, FALSE)
             ELSE Settle(q, st)
  IN
  /\ rl.e = epoch
  /\ ReleaseReady(rl)
  /\ toRelease' = toRelease \ {rl}
  /\ Upd([S EXCEPT ![q] = st2], {q})
  /\ UNCHANGED <<epoch, ranges, reqHi, nextSeq, renewal, files, paused,
                 pausedEver, net, faults>>

(* A keepalive or an early flush that carries a newer finish report. *)
SendReport(q) ==
  LET st2 == Settle(q, WithReport(q, S[q], epoch))
  IN
  /\ FixFinish /\ FixLease
  /\ ~paused
  /\ q \in {r.tp : r \in ranges}
  /\ st2.rep # S[q].rep
  /\ Upd([S EXCEPT ![q] = st2], {q})
  /\ UNCHANGED <<epoch, ranges, reqHi, nextSeq, renewal, files, toRelease,
                 paused, pausedEver, net, faults>>

(* Coordinator actions *)

(* The leader's acceptance rules for the part of file k at q. *)
Accepts(k, q) ==
  LET st == S[q]
      leaseOK == q # files[k].tp \/ InRanges(st, k[1], k[2])
  IN k[1] >= st.row[1] /\ GT(k, Floor(st)) /\ leaseOK

(* A late attempt carries the report from its send time, which the model *)
(* doesn't keep, so it carries none. Reports only merge upward.         *)
Committed(k, q, withRep) ==
  LET st == S[q]
      f == files[k]
      c == [st EXCEPT !.row = k, !.held = IF q \in f.ds THEN @ \cup {k} ELSE @]
      old == IF k \in DOMAIN st.ent THEN st.ent[k] ELSE <<>>
      fresh == [p \in f.ds |-> IF p = q THEN "held"
                               ELSE IF p \in DOMAIN old /\ old[p] = "released"
                               THEN "released" ELSE "pending"]
  IN IF q # f.tp THEN c
     ELSE LET t == [c EXCEPT !.rec = (k :> f.ds) @@ @,
                             !.known = @ \cup {k},
                             !.ent = (k :> fresh) @@ @]
          IN Settle(q, IF withRep THEN WithReport(q, t, k[1]) ELSE t)

(* The broker sends the part in queue order, and the leader answers it. *)
Commit(k, q) ==
  /\ k[1] = epoch
  /\ ~paused
  /\ files[k].parts[q] = "open"
  /\ ~\E k2 \in FilesOf(k[1]) : k2[2] < k[2] /\ q \in DOMAIN files[k2].parts
                                /\ files[k2].parts[q] = "open"
  /\ IF Accepts(k, q)
     THEN Upd([S EXCEPT ![q] = Committed(k, q, TRUE)], {q})
     ELSE UNCHANGED <<S, hist, deleted>>
  /\ files' = [files EXCEPT ![k].parts[q] =
                 IF Accepts(k, q) \/ k = S[q].row THEN "ok" ELSE "exc"]
  /\ UNCHANGED <<epoch, ranges, reqHi, nextSeq, renewal, toRelease, paused,
                 pausedEver, net, faults>>

(* A rejection before the producer checks, such as a full overlay. *)
Reject(k, q) ==
  /\ k[1] = epoch
  /\ ~paused
  /\ files[k].parts[q] = "open"
  /\ files' = [files EXCEPT ![k].parts[q] = "exc"]
  /\ UNCHANGED <<epoch, ranges, reqHi, nextSeq, renewal, toRelease, paused,
                 pausedEver, net, S, deleted, hist, faults>>

(* A late attempt reaches the leader after the broker gave up on the part. *)
Late(k, q) ==
  /\ <<k, q>> \in net
  /\ net' = net \ {<<k, q>>}
  /\ IF Accepts(k, q)
     THEN Upd([S EXCEPT ![q] = Committed(k, q, FALSE)], {q})
     ELSE UNCHANGED <<S, hist, deleted>>
  /\ UNCHANGED <<epoch, ranges, reqHi, nextSeq, renewal, files, toRelease,
                 paused, pausedEver, faults>>

Retire(q) ==
  LET e == epoch - 1
      st == S[q]
      st1 == [st EXCEPT !.row = MaxPair(@, <<epoch, 0>>)]
  IN
  /\ epoch > 1
  /\ \/ st.row[1] < epoch
     \/ \E r \in st.leases : r.e = e /\ r.live
  /\ Upd([S EXCEPT ![q] = EndLease(q, st1, e, 0, TRUE)], {q})
  /\ UNCHANGED <<Broker, net, faults>>

(* The partition whose range holds the broker's next sequence gets a part *)
(* on every file, so its idle timer never fires while the broker is busy.  *)
InUse ==
  LET avail == {s \in nextSeq..MaxSeq : \E r \in ranges : r.a <= s /\ s <= r.b}
  IN IF avail = {} THEN {}
     ELSE {(CHOOSE r \in ranges : r.a <= Min(avail) /\ Min(avail) <= r.b).tp}

IdleEnd(q) ==
  /\ \E r \in S[q].leases : r.e = epoch /\ r.live
  /\ \/ paused
     \/ /\ ~FixLease
        /\ q \notin InUse
        /\ ~\E k \in FilesOf(epoch) : files[k].tp = q /\ files[k].parts[q] = "open"
        /\ (renewal = None \/ renewal.tp # q)
  /\ Upd([S EXCEPT ![q] = EndLease(q, S[q], epoch, 0, TRUE)], {q})
  /\ UNCHANGED <<Broker, net, faults>>

(* One step: participant p applies fence j from q, and q records the *)
(* answer. The late-answer races belong to the tracking-entries spec. *)
Fence(q, j) ==
  LET p == j[1]  e == j[2]  a == j[3]  b == j[4]
      raise == GT(<<e, b>>, Floor(S[p]))
      refs == {s \in a..b : <<e, s>> \in S[p].held}
      S1 == IF raise THEN [S EXCEPT ![p].fenced = <<e, b>>] ELSE S
      st == S1[q]
      NS(k) == IF st.ent[k][p] = "released" THEN "released"
               ELSE IF k[2] \in refs THEN "held" ELSE "released"
      st2 == [st EXCEPT
               !.jobs = @ \ {j},
               !.ent = [k \in DOMAIN @ |->
                          IF k[1] = e /\ a <= k[2] /\ k[2] <= b
                             /\ p \in DOMAIN st.ent[k]
                          THEN [st.ent[k] EXCEPT ![p] = NS(k)]
                          ELSE st.ent[k]]]
  IN
  /\ Upd([S1 EXCEPT ![q] = Settle(q, st2)], {q} \cup (IF raise THEN {p} ELSE {}))
  /\ UNCHANGED <<Broker, net, faults>>

(* Retention or another path removes a participant's batches of k. *)
RemoveBatches(p, k) ==
  LET st == S[p]
      isTp == p = files[k].tp
      st1 == [st EXCEPT !.held = @ \ {k},
                        !.relPend = IF isTp THEN @ ELSE @ \cup {k}]
      st2 == IF isTp /\ k \in DOMAIN st.ent /\ p \in DOMAIN st.ent[k]
             THEN Settle(p, [st1 EXCEPT !.ent[k][p] = "released"])
             ELSE st1
  IN
  /\ Upd([S EXCEPT ![p] = st2], {p})
  /\ UNCHANGED <<Broker, net, faults>>

DeliverRelease(p, k) ==
  LET q == files[k].tp
      st == S[q]
      old == IF k \in DOMAIN st.ent THEN st.ent[k] ELSE <<>>
      newEnt ==
        IF p \in DOMAIN old THEN [old EXCEPT ![p] = "released"]
        ELSE IF k \in st.known THEN old
        ELSE (p :> "released") @@ old
      st2 == Settle(q, [st EXCEPT !.ent = (k :> newEnt) @@ @])
  IN
  /\ Upd([S EXCEPT ![q] = st2, ![p].relPend = @ \ {k}], {p, q})
  /\ UNCHANGED <<Broker, net, faults>>

(* Drops a suffix of acknowledged records at q. Everyone else keeps what *)
(* they observed, and storage keeps its deletes.                         *)
LostTail(q) ==
  /\ Cardinality(faults) < MaxFaults
  /\ \E i \in 1..(Len(hist[q]) - 1) :
       /\ S' = [S EXCEPT ![q] = hist[q][i]]
       /\ hist' = [hist EXCEPT ![q] = SubSeq(@, 1, i)]
  /\ faults' = faults \cup {q}
  /\ UNCHANGED <<Broker, net, deleted>>

Next ==
  \/ \E q \in P : RequestLease(q)
  \/ \E lost \in BOOLEAN : GrantLease(lost)
  \/ GiveUpRenewal
  \/ \E ds \in DataSets : CloseFile(ds)
  \/ \E k \in DOMAIN files : \E L \in Small({k} \X P) : GiveUpFile(k, L)
  \/ \E L \in Small(OpenParts) : Restart(L) \/ Pause(L)
  \/ Resume
  \/ \E rl \in toRelease : SendRelease(rl)
  \/ \E q \in P : SendReport(q)
  \/ \E kq \in OpenParts : Commit(kq[1], kq[2]) \/ Reject(kq[1], kq[2])
  \/ \E kq \in net : Late(kq[1], kq[2])
  \/ \E q \in P : Retire(q)
  \/ \E q \in P : IdleEnd(q)
  \/ \E q \in P : \E j \in S[q].jobs : Fence(q, j)
  \/ \E p \in P : \E k \in S[p].held : RemoveBatches(p, k)
  \/ \E p \in P : \E k \in S[p].relPend : DeliverRelease(p, k)
  \/ \E q \in P : LostTail(q)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Invariants *)
Queued == UNION {S[q].queue : q \in P}

(* No object is deleted or queued while a batch references it, and no part *)
(* commits into a queued or deleted key. A partition that lost its tail is *)
(* exempt: release inversion and resurrection there are accepted risks in  *)
(* ticket 10, which claims no other partition can be hit.                  *)
NoDeleteReferenced ==
  \A k \in deleted \cup Queued : \A p \in P \ faults : k \notin S[p].held

(* No fence raises a floor past a part the broker still sends, or past a *)
(* sequence it still plans to use.                                       *)
NoCutLive ==
  (~pausedEver /\ faults = {}) =>
    /\ \A k \in FilesOf(epoch) : \A q \in DOMAIN files[k].parts :
         (files[k].parts[q] = "open" /\ k \notin S[q].held /\ k # S[q].row)
           => ~GE(S[q].fenced, k)
    /\ \A r \in ranges : \A s \in r.a..r.b :
         s >= nextSeq => \A q \in P : ~GE(S[q].fenced, <<epoch, s>>)

Quiet ==
  /\ net = {}
  /\ \A q \in P : /\ S[q].jobs = {} /\ S[q].relPend = {}
                  /\ S[q].row[1] = epoch
                  /\ ~\E r \in S[q].leases : r.e < epoch /\ r.live

(* Once an epoch ended and the coordinator is quiet, every uploaded key of *)
(* that epoch that no partition holds is queued.                           *)
NoLeak ==
  (faults = {} /\ Quiet) =>
    \A k \in DOMAIN files :
      (k[1] < epoch /\ \A p \in P : k \notin S[p].held) => k \in Queued

(* Once the lease covering a finished file ends, its tracking partition  *)
(* knows the key, so a fence or a record resolves it. NoLeak fails later *)
(* for the same gap, but only after the epoch ends and every fence ends.  *)
EndedKnown ==
  faults = {} =>
    \A k \in DOMAIN files :
      LET q == files[k].tp
      IN (Finished(k) /\ ~\E r \in S[q].leases : r.e = k[1] /\ r.live
                                               /\ r.a <= k[2] /\ k[2] <= r.b)
           => k \in S[q].known

Perms == Permutations(P)

(* Compact state for counterexample traces *)
View ==
  [epoch |-> epoch, ranges |-> ranges, nextSeq |-> nextSeq, renewal |-> renewal,
   files |-> [k \in DOMAIN files |-> [tp |-> files[k].tp, parts |-> files[k].parts]],
   toRelease |-> toRelease, paused |-> paused, net |-> net,
   fenced |-> [q \in P |-> S[q].fenced], row |-> [q \in P |-> S[q].row],
   held |-> [q \in P |-> S[q].held],
   leases |-> [q \in P |-> S[q].leases],
   ft |-> [q \in P |-> [e \in 1..MaxEpoch |-> S[q].rep[e]]],
   ent |-> [q \in P |-> S[q].ent], jobs |-> [q \in P |-> S[q].jobs],
   queue |-> [q \in P |-> S[q].queue], deleted |-> deleted, faults |-> faults]
=============================================================================
