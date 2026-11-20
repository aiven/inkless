--------------------------- MODULE EpochRetirement ---------------------------
(***************************************************************************)
(* PROTOTYPE, throwaway. Bounded model of broker epoch retirement from     *)
(* ticket 03 for one broker ID, tracking partition t, and participant p.   *)
(* Each partition's leader has its own metadata image, which can lag the   *)
(* metadata log, and a new leader's image can be older than the last one.  *)
(*                                                                         *)
(* A broker epoch is the metadata offset of the first record its           *)
(* registration writes. A restart first writes records that remove the     *)
(* old incarnation from ISRs, so its registration record can sit above the *)
(* epoch's offset. With no prior registration, the broker is in no ISR or  *)
(* ELR, and the registration record sits at the epoch's offset.            *)
(*                                                                         *)
(* Abstractions:                                                           *)
(*  - Sequences aren't modeled. Each epoch has one leased range, and a     *)
(*    lease end fences the whole epoch.                                    *)
(*  - Each epoch uploads one object. If its tracking part didn't commit    *)
(*    when the lease ends, it's a leftover, deleted once p applies the     *)
(*    fence without holding it.                                            *)
(*  - An append is atomic with its apply and reply. No overlay.            *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS
  t, p,           \* the tracking partition and the participant
  Rule,           \* "v2", "fable", "v3", or "none" to never retire
  Guard,          \* 03: append only when retiredThrough covers the newest held epoch
  PreWhenUnreg,   \* what-if: records precede a registration with no prior one
  MaxLog          \* metadata log length

Parts == {t, p}
Max2(a, b) == IF a > b THEN a ELSE b
MaxOf(S) == IF S = {} THEN 0 ELSE CHOOSE x \in S : \A y \in S : y <= x

VARIABLES
  mlog,       \* metadata log: [k |-> "pre"], [k |-> "reg", e |-> E], [k |-> "unreg"]
  live,       \* the broker's registered epoch, 0 when unregistered
  granted,    \* epochs whose lease request t accepted
  net,        \* every request sent; each can arrive, again, at any later time
  img,        \* last contained offset of each partition leader's image
  row,        \* per partition: epoch e, retiredThrough rt, v2 marker retired
  fenced,     \* per partition: highest fully fenced epoch
  leases,     \* live lease epochs at t
  ended,      \* epochs whose lease ended at t
  committed,  \* per partition: epochs whose part committed
  deleted     \* epochs whose leftover object was deleted

vars == <<mlog, live, granted, net, img, row, fenced, leases, ended, committed, deleted>>

Init ==
  /\ mlog = <<>>
  /\ live = 0
  /\ granted = {}
  /\ net = {}
  /\ img = [x \in Parts |-> 0]
  /\ row = [x \in Parts |-> [e |-> 0, rt |-> 0, retired |-> FALSE]]
  /\ fenced = [x \in Parts |-> 0]
  /\ leases = {}
  /\ ended = {}
  /\ committed = [x \in Parts |-> {}]
  /\ deleted = {}

(* What an image at offset o shows: the epoch of the newest registration, *)
(* or 0 for none.                                                          *)
RECURSIVE ViewAt(_)
ViewAt(o) ==
  IF o = 0 THEN 0
  ELSE CASE mlog[o].k = "reg" -> mlog[o].e
         [] mlog[o].k = "unreg" -> 0
         [] OTHER -> ViewAt(o - 1)
View(x) == ViewAt(img[x])

Newest(x) == Max2(row[x].e, IF x = t THEN MaxOf(leases) ELSE 0)

Stale(x, e) ==
  \/ e < row[x].e
  \/ e <= fenced[x]
  \/ Rule = "v2" /\ row[x].retired
  \/ Rule # "v2" /\ e <= row[x].rt

(* Ends the leases in es at t, raises t's floor, and fences p for each    *)
(* epoch whose tracking part didn't commit.                                *)
EndLeases(es) ==
  /\ leases' = leases \ es
  /\ ended' = ended \cup es
  /\ fenced' = [fenced EXCEPT ![t] = Max2(@, MaxOf(es))]
  /\ net' = net \cup {[k |-> "fence", e |-> e] : e \in es \ committed[t]}

(* Broker and controller *)
Register ==
  \E pre \in IF live > 0 \/ PreWhenUnreg THEN {0, 1} ELSE {0} :
    LET e == Len(mlog) + 1
    IN /\ Len(mlog) + pre + 1 <= MaxLog
       /\ mlog' = mlog \o [i \in 1..pre |-> [k |-> "pre"]] \o <<[k |-> "reg", e |-> e]>>
       /\ live' = e
       /\ UNCHANGED <<granted, net, img, row, fenced, leases, ended, committed, deleted>>

Unregister ==
  /\ live > 0
  /\ Len(mlog) < MaxLog
  /\ mlog' = Append(mlog, [k |-> "unreg"])
  /\ live' = 0
  /\ UNCHANGED <<granted, net, img, row, fenced, leases, ended, committed, deleted>>

SendLease ==
  /\ live > 0
  /\ net' = net \cup {[k |-> "lease", e |-> live]}
  /\ UNCHANGED <<mlog, live, granted, img, row, fenced, leases, ended, committed, deleted>>

SendPart(x) ==
  /\ live \in granted
  /\ net' = net \cup {[k |-> "part", to |-> x, e |-> live]}
  /\ UNCHANGED <<mlog, live, granted, img, row, fenced, leases, ended, committed, deleted>>

(* Leaders *)
ImageAdvance(x) ==
  /\ img[x] < Len(mlog)
  /\ img' = [img EXCEPT ![x] = @ + 1]
  /\ UNCHANGED <<mlog, live, granted, net, row, fenced, leases, ended, committed, deleted>>

Elect(x) ==
  /\ \E o \in 0..Len(mlog) : o # img[x] /\ img' = [img EXCEPT ![x] = o]
  /\ UNCHANGED <<mlog, live, granted, net, row, fenced, leases, ended, committed, deleted>>

(* An image newer than the request's epoch rejects it as stale. *)
ImageRejects(x, e) == View(x) > e

DeliverLease(m) ==
  /\ m \in net /\ m.k = "lease"
  /\ ~ImageRejects(t, m.e) /\ ~Stale(t, m.e) /\ m.e > MaxOf(leases \cup ended)
  /\ LET old == {e \in leases : e < m.e}
     IN /\ granted' = granted \cup {m.e}
        /\ leases' = (leases \ old) \cup {m.e}
        /\ ended' = ended \cup old
        /\ fenced' = [fenced EXCEPT ![t] = Max2(@, MaxOf(old))]
        /\ net' = net \cup {[k |-> "fence", e |-> e] : e \in old \ committed[t]}
  /\ UNCHANGED <<mlog, live, img, row, committed, deleted>>

DeliverPart(m) ==
  /\ m \in net /\ m.k = "part"
  /\ LET x == m.to
     IN /\ m.e \notin committed[x]
        /\ ~ImageRejects(x, m.e)
        /\ ~Stale(x, m.e)
        /\ x = t => m.e \in leases
        /\ committed' = [committed EXCEPT ![x] = @ \cup {m.e}]
        /\ row' = [row EXCEPT ![x].e = Max2(@, m.e)]
  /\ UNCHANGED <<mlog, live, granted, net, img, fenced, leases, ended, deleted>>

(* p applies a lease-end fence, and its answer says whether p holds the   *)
(* epoch's object.                                                         *)
DeliverFence(m) ==
  /\ m \in net /\ m.k = "fence"
  /\ fenced' = [fenced EXCEPT ![p] = Max2(@, m.e)]
  /\ deleted' = IF m.e \in committed[p] THEN deleted ELSE deleted \cup {m.e}
  /\ UNCHANGED <<mlog, live, granted, net, img, row, leases, ended, committed>>

Retire(x) ==
  LET v == View(x)
      endT(es) == IF x = t THEN EndLeases(es)
                  ELSE UNCHANGED <<leases, ended, fenced, net>>
  IN /\ Rule # "none"
     /\ Newest(x) > 0
     /\ IF Rule = "v3" THEN
          LET rt == IF v > 0 THEN v - 1 ELSE img[x]
          IN /\ rt > row[x].rt
             /\ Guard => rt >= Newest(x)
             /\ row' = [row EXCEPT ![x].rt = rt]
             /\ endT({e \in leases : e <= rt})
        ELSE IF v > 0 THEN
          /\ v > Newest(x)
          /\ row' = [row EXCEPT ![x].e = v]
          /\ endT({e \in leases : e < v})
        ELSE IF Rule = "v2" THEN
          /\ ~row[x].retired
          /\ row' = [row EXCEPT ![x].retired = TRUE]
          /\ endT(leases)
        ELSE
          LET rt == Newest(x)
          IN /\ rt > row[x].rt
             /\ row' = [row EXCEPT ![x].rt = rt]
             /\ endT({e \in leases : e <= rt})
     /\ UNCHANGED <<mlog, live, granted, img, committed, deleted>>

Next ==
  \/ Register \/ Unregister \/ SendLease
  \/ \E x \in Parts : SendPart(x) \/ ImageAdvance(x) \/ Elect(x) \/ Retire(x)
  \/ \E m \in net : DeliverLease(m) \/ DeliverPart(m) \/ DeliverFence(m)

Spec == Init /\ [][Next]_vars

(* Invariants *)
TypeOK ==
  /\ live \in 0..MaxLog
  /\ \A x \in Parts : row[x].rt \in 0..MaxLog /\ row[x].e \in 0..MaxLog

(* A registered epoch is never stale anywhere, and its lease never ends.  *)
LiveNotRetired == live > 0 => (\A x \in Parts : ~Stale(x, live)) /\ live \notin ended

(* Once t's image is current and t has nothing left to retire, no lease   *)
(* of a dead epoch remains.                                                *)
RetireComplete == (img[t] = Len(mlog) /\ ~ENABLED Retire(t)) => leases \subseteq {live}

(* No object is deleted while a partition holds its batch. *)
NoDeleteReferenced == \A e \in deleted : \A x \in Parts : e \notin committed[x]
=============================================================================
