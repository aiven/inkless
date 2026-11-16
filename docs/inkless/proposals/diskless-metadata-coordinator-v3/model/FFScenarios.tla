---------------------------- MODULE FFScenarios ----------------------------
(* PROTOTYPE. FencesFloors with the broker's choices pinned to a scenario.  *)
(* lease[i] is the partition of the i-th lease request in an epoch, and     *)
(* file[n] is the data set of the n-th file. Everything else stays          *)
(* nondeterministic, and TLC checks every interleaving within the scenario. *)
(* p1 is the tracking partition t, p2 is t + 1, p3 is a plain participant.  *)
EXTENDS FencesFloors
CONSTANTS Scenario, p1, p2, p3

Plans ==
  [A |-> [lease |-> <<p1>>,         file |-> <<{p2}, {p2}>>],              \* empty tracking part
   B |-> [lease |-> <<p1>>,         file |-> <<{p2}, {p1}>>],              \* missed sequence
   C |-> [lease |-> <<p1, p2>>,     file |-> <<{p3}, {p3}, {p3}>>],        \* fallback, low rate
   D |-> [lease |-> <<p1, p2, p1>>, file |-> <<{p3}, {p3}, {p3}>>],        \* switch back to t
   E |-> [lease |-> <<p1, p2, p1>>, file |-> <<{p1}, {p3}, {p1}, {p1}>>],  \* cross-partition race
   F |-> [lease |-> <<p1, p2>>,     file |-> <<{p2}, {p2}>>],              \* lost lease reply
   G |-> [lease |-> <<p1>>,         file |-> <<{p2, p3}, {p2}>>],          \* epoch end
   H |-> [lease |-> <<p1, p2>>,     file |-> <<{p1}>>]]                    \* abandoned-range leak

LeasePlan == Plans[Scenario].lease
FilePlan == Plans[Scenario].file

ScenarioOK ==
  /\ (renewal' # None /\ renewal' # renewal) =>
       /\ reqHi' \div R <= Len(LeasePlan)
       /\ renewal'.tp = LeasePlan[reqHi' \div R]
  /\ (DOMAIN files' # DOMAIN files) =>
       LET n == Cardinality(DOMAIN files')
       IN /\ n <= Len(FilePlan)
          /\ files'[<<epoch, nextSeq' - 1>>].ds = FilePlan[n]
=============================================================================
