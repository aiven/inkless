---- MODULE TrackingEntries_TTrace_1794905107 ----
EXTENDS Sequences, TLCExt, TrackingEntries, Toolbox, TrackingEntries_TEConstants, Naturals, TLC

_expression ==
    LET TrackingEntries_TEExpression == INSTANCE TrackingEntries_TEExpression
    IN TrackingEntries_TEExpression!expression
----

_trace ==
    LET TrackingEntries_TETrace == INSTANCE TrackingEntries_TETrace
    IN TrackingEntries_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        Q = ((p1 :> [floor |-> 0, held |-> {1}, rel |-> {2}, relPend |-> {}, done |-> {1, 2}] @@ p2 :> [floor |-> 0, held |-> {}, rel |-> {}, relPend |-> {}, done |-> {}]))
        /\
        stopped = (TRUE)
        /\
        hist = ((p1 :> <<>> @@ p2 :> <<>> @@ t :> <<>>))
        /\
        deleted = ({})
        /\
        T = ([rec |-> {1, 2}, hiRec |-> 2, pset |-> <<{p1}, {p1}>>, ent |-> <<(p1 :> "held"), (p1 :> "held")>>, rep |-> [ft |-> 2, exc |-> {}], floor |-> 2, jobs |-> {}, queue |-> {}])
        /\
        ans = ({})
        /\
        part = (<<(p1 :> "ok" @@ t :> "ok"), (p1 :> "ok" @@ t :> "ok")>>)
        /\
        nextSeq = (3)
        /\
        net = ({})
        /\
        faults = ({})
        /\
        everRel = ({})
        /\
        ds = (<<{p1}, {p1}>>)
    )
----

_init ==
    /\ Q = _TETrace[1].Q
    /\ T = _TETrace[1].T
    /\ stopped = _TETrace[1].stopped
    /\ deleted = _TETrace[1].deleted
    /\ ds = _TETrace[1].ds
    /\ faults = _TETrace[1].faults
    /\ net = _TETrace[1].net
    /\ hist = _TETrace[1].hist
    /\ ans = _TETrace[1].ans
    /\ part = _TETrace[1].part
    /\ nextSeq = _TETrace[1].nextSeq
    /\ everRel = _TETrace[1].everRel
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ Q  = _TETrace[i].Q
        /\ Q' = _TETrace[j].Q
        /\ T  = _TETrace[i].T
        /\ T' = _TETrace[j].T
        /\ stopped  = _TETrace[i].stopped
        /\ stopped' = _TETrace[j].stopped
        /\ deleted  = _TETrace[i].deleted
        /\ deleted' = _TETrace[j].deleted
        /\ ds  = _TETrace[i].ds
        /\ ds' = _TETrace[j].ds
        /\ faults  = _TETrace[i].faults
        /\ faults' = _TETrace[j].faults
        /\ net  = _TETrace[i].net
        /\ net' = _TETrace[j].net
        /\ hist  = _TETrace[i].hist
        /\ hist' = _TETrace[j].hist
        /\ ans  = _TETrace[i].ans
        /\ ans' = _TETrace[j].ans
        /\ part  = _TETrace[i].part
        /\ part' = _TETrace[j].part
        /\ nextSeq  = _TETrace[i].nextSeq
        /\ nextSeq' = _TETrace[j].nextSeq
        /\ everRel  = _TETrace[i].everRel
        /\ everRel' = _TETrace[j].everRel

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("TrackingEntries_TTrace_1794905107.json", _TETrace)

=============================================================================

 Note that you can extract this module `TrackingEntries_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `TrackingEntries_TEExpression.tla` file takes precedence 
  over the module `TrackingEntries_TEExpression` below).

---- MODULE TrackingEntries_TEExpression ----
EXTENDS Sequences, TLCExt, TrackingEntries, Toolbox, TrackingEntries_TEConstants, Naturals, TLC

expression == 
    [
        \* To hide variables of the `TrackingEntries` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        Q |-> Q
        ,T |-> T
        ,stopped |-> stopped
        ,deleted |-> deleted
        ,ds |-> ds
        ,faults |-> faults
        ,net |-> net
        ,hist |-> hist
        ,ans |-> ans
        ,part |-> part
        ,nextSeq |-> nextSeq
        ,everRel |-> everRel
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_QUnchanged |-> Q = Q'
        
        \* Format the `Q` variable as Json value.
        \* ,_QJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(Q)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_QModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].Q # _TETrace[s-1].Q
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE TrackingEntries_TETrace ----
\*EXTENDS IOUtils, TrackingEntries, TrackingEntries_TEConstants, TLC
\*
\*trace == IODeserialize("TrackingEntries_TTrace_1794905107.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE TrackingEntries_TETrace ----
EXTENDS TrackingEntries, TrackingEntries_TEConstants, TLC

trace == 
    <<
    ([Q |-> (p1 :> [floor |-> 0, held |-> {}, rel |-> {}, relPend |-> {}, done |-> {}] @@ p2 :> [floor |-> 0, held |-> {}, rel |-> {}, relPend |-> {}, done |-> {}]),stopped |-> FALSE,hist |-> (p1 :> <<>> @@ p2 :> <<>> @@ t :> <<>>),deleted |-> {},T |-> [rec |-> {}, hiRec |-> 0, pset |-> <<>>, ent |-> <<>>, rep |-> [ft |-> 0, exc |-> {}], floor |-> 0, jobs |-> {}, queue |-> {}],ans |-> {},part |-> <<>>,nextSeq |-> 1,net |-> {},faults |-> {},everRel |-> {},ds |-> <<>>]),
    ([Q |-> (p1 :> [floor |-> 0, held |-> {}, rel |-> {}, relPend |-> {}, done |-> {}] @@ p2 :> [floor |-> 0, held |-> {}, rel |-> {}, relPend |-> {}, done |-> {}]),stopped |-> FALSE,hist |-> (p1 :> <<>> @@ p2 :> <<>> @@ t :> <<>>),deleted |-> {},T |-> [rec |-> {}, hiRec |-> 0, pset |-> <<>>, ent |-> <<>>, rep |-> [ft |-> 0, exc |-> {}], floor |-> 0, jobs |-> {}, queue |-> {}],ans |-> {},part |-> <<(p1 :> "open" @@ t :> "open")>>,nextSeq |-> 2,net |-> {},faults |-> {},everRel |-> {},ds |-> <<{p1}>>]),
    ([Q |-> (p1 :> [floor |-> 0, held |-> {}, rel |-> {}, relPend |-> {}, done |-> {}] @@ p2 :> [floor |-> 0, held |-> {}, rel |-> {}, relPend |-> {}, done |-> {}]),stopped |-> FALSE,hist |-> (p1 :> <<>> @@ p2 :> <<>> @@ t :> <<>>),deleted |-> {},T |-> [rec |-> {}, hiRec |-> 0, pset |-> <<>>, ent |-> <<>>, rep |-> [ft |-> 0, exc |-> {}], floor |-> 0, jobs |-> {}, queue |-> {}],ans |-> {},part |-> <<(p1 :> "open" @@ t :> "open"), (p1 :> "open" @@ t :> "open")>>,nextSeq |-> 3,net |-> {},faults |-> {},everRel |-> {},ds |-> <<{p1}, {p1}>>]),
    ([Q |-> (p1 :> [floor |-> 0, held |-> {1}, rel |-> {}, relPend |-> {}, done |-> {1}] @@ p2 :> [floor |-> 0, held |-> {}, rel |-> {}, relPend |-> {}, done |-> {}]),stopped |-> FALSE,hist |-> (p1 :> <<>> @@ p2 :> <<>> @@ t :> <<>>),deleted |-> {},T |-> [rec |-> {}, hiRec |-> 0, pset |-> <<>>, ent |-> <<>>, rep |-> [ft |-> 0, exc |-> {}], floor |-> 0, jobs |-> {}, queue |-> {}],ans |-> {},part |-> <<(p1 :> "ok" @@ t :> "open"), (p1 :> "open" @@ t :> "open")>>,nextSeq |-> 3,net |-> {},faults |-> {},everRel |-> {},ds |-> <<{p1}, {p1}>>]),
    ([Q |-> (p1 :> [floor |-> 0, held |-> {1}, rel |-> {}, relPend |-> {}, done |-> {1}] @@ p2 :> [floor |-> 0, held |-> {}, rel |-> {}, relPend |-> {}, done |-> {}]),stopped |-> FALSE,hist |-> (p1 :> <<>> @@ p2 :> <<>> @@ t :> <<>>),deleted |-> {},T |-> [rec |-> {1}, hiRec |-> 1, pset |-> <<{p1}>>, ent |-> <<(p1 :> "pending")>>, rep |-> [ft |-> 0, exc |-> {}], floor |-> 0, jobs |-> {}, queue |-> {}],ans |-> {},part |-> <<(p1 :> "ok" @@ t :> "ok"), (p1 :> "open" @@ t :> "open")>>,nextSeq |-> 3,net |-> {},faults |-> {},everRel |-> {},ds |-> <<{p1}, {p1}>>]),
    ([Q |-> (p1 :> [floor |-> 0, held |-> {1}, rel |-> {2}, relPend |-> {2}, done |-> {1, 2}] @@ p2 :> [floor |-> 0, held |-> {}, rel |-> {}, relPend |-> {}, done |-> {}]),stopped |-> FALSE,hist |-> (p1 :> <<>> @@ p2 :> <<>> @@ t :> <<>>),deleted |-> {},T |-> [rec |-> {1}, hiRec |-> 1, pset |-> <<{p1}>>, ent |-> <<(p1 :> "pending")>>, rep |-> [ft |-> 0, exc |-> {}], floor |-> 0, jobs |-> {}, queue |-> {}],ans |-> {},part |-> <<(p1 :> "ok" @@ t :> "ok"), (p1 :> "ok" @@ t :> "open")>>,nextSeq |-> 3,net |-> {},faults |-> {},everRel |-> {},ds |-> <<{p1}, {p1}>>]),
    ([Q |-> (p1 :> [floor |-> 0, held |-> {1}, rel |-> {2}, relPend |-> {}, done |-> {1, 2}] @@ p2 :> [floor |-> 0, held |-> {}, rel |-> {}, relPend |-> {}, done |-> {}]),stopped |-> FALSE,hist |-> (p1 :> <<>> @@ p2 :> <<>> @@ t :> <<>>),deleted |-> {},T |-> [rec |-> {1}, hiRec |-> 1, pset |-> <<{p1}>>, ent |-> <<(p1 :> "pending")>>, rep |-> [ft |-> 0, exc |-> {}], floor |-> 0, jobs |-> {}, queue |-> {}],ans |-> {},part |-> <<(p1 :> "ok" @@ t :> "ok"), (p1 :> "ok" @@ t :> "open")>>,nextSeq |-> 3,net |-> {},faults |-> {},everRel |-> {},ds |-> <<{p1}, {p1}>>]),
    ([Q |-> (p1 :> [floor |-> 0, held |-> {1}, rel |-> {2}, relPend |-> {}, done |-> {1, 2}] @@ p2 :> [floor |-> 0, held |-> {}, rel |-> {}, relPend |-> {}, done |-> {}]),stopped |-> FALSE,hist |-> (p1 :> <<>> @@ p2 :> <<>> @@ t :> <<>>),deleted |-> {},T |-> [rec |-> {1, 2}, hiRec |-> 2, pset |-> <<{p1}, {p1}>>, ent |-> <<(p1 :> "held"), (p1 :> "pending")>>, rep |-> [ft |-> 1, exc |-> {}], floor |-> 0, jobs |-> {}, queue |-> {}],ans |-> {},part |-> <<(p1 :> "ok" @@ t :> "ok"), (p1 :> "ok" @@ t :> "ok")>>,nextSeq |-> 3,net |-> {},faults |-> {},everRel |-> {},ds |-> <<{p1}, {p1}>>]),
    ([Q |-> (p1 :> [floor |-> 0, held |-> {1}, rel |-> {2}, relPend |-> {}, done |-> {1, 2}] @@ p2 :> [floor |-> 0, held |-> {}, rel |-> {}, relPend |-> {}, done |-> {}]),stopped |-> FALSE,hist |-> (p1 :> <<>> @@ p2 :> <<>> @@ t :> <<>>),deleted |-> {},T |-> [rec |-> {1, 2}, hiRec |-> 2, pset |-> <<{p1}, {p1}>>, ent |-> <<(p1 :> "held"), (p1 :> "held")>>, rep |-> [ft |-> 2, exc |-> {}], floor |-> 0, jobs |-> {}, queue |-> {}],ans |-> {},part |-> <<(p1 :> "ok" @@ t :> "ok"), (p1 :> "ok" @@ t :> "ok")>>,nextSeq |-> 3,net |-> {},faults |-> {},everRel |-> {},ds |-> <<{p1}, {p1}>>]),
    ([Q |-> (p1 :> [floor |-> 0, held |-> {1}, rel |-> {2}, relPend |-> {}, done |-> {1, 2}] @@ p2 :> [floor |-> 0, held |-> {}, rel |-> {}, relPend |-> {}, done |-> {}]),stopped |-> TRUE,hist |-> (p1 :> <<>> @@ p2 :> <<>> @@ t :> <<>>),deleted |-> {},T |-> [rec |-> {1, 2}, hiRec |-> 2, pset |-> <<{p1}, {p1}>>, ent |-> <<(p1 :> "held"), (p1 :> "held")>>, rep |-> [ft |-> 2, exc |-> {}], floor |-> 0, jobs |-> {}, queue |-> {}],ans |-> {},part |-> <<(p1 :> "ok" @@ t :> "ok"), (p1 :> "ok" @@ t :> "ok")>>,nextSeq |-> 3,net |-> {},faults |-> {},everRel |-> {},ds |-> <<{p1}, {p1}>>]),
    ([Q |-> (p1 :> [floor |-> 0, held |-> {1}, rel |-> {2}, relPend |-> {}, done |-> {1, 2}] @@ p2 :> [floor |-> 0, held |-> {}, rel |-> {}, relPend |-> {}, done |-> {}]),stopped |-> TRUE,hist |-> (p1 :> <<>> @@ p2 :> <<>> @@ t :> <<>>),deleted |-> {},T |-> [rec |-> {1, 2}, hiRec |-> 2, pset |-> <<{p1}, {p1}>>, ent |-> <<(p1 :> "held"), (p1 :> "held")>>, rep |-> [ft |-> 2, exc |-> {}], floor |-> 2, jobs |-> {}, queue |-> {}],ans |-> {},part |-> <<(p1 :> "ok" @@ t :> "ok"), (p1 :> "ok" @@ t :> "ok")>>,nextSeq |-> 3,net |-> {},faults |-> {},everRel |-> {},ds |-> <<{p1}, {p1}>>])
    >>
----


=============================================================================

---- MODULE TrackingEntries_TEConstants ----
EXTENDS TrackingEntries

CONSTANTS p1, p2

=============================================================================

---- CONFIG TrackingEntries_TTrace_1794905107 ----
CONSTANTS
    P = { p1 , p2 }
    t = t
    MaxSeq = 2
    FixTerminal = TRUE
    FixStoreEarly = FALSE
    FixResolvedFloor = TRUE
    MaxFaults = 0
    MaxLate = 1
    p1 = p1
    p2 = p2
    t = t

INVARIANT
    _inv

CHECK_DEADLOCK
    \* CHECK_DEADLOCK off because of PROPERTY or INVARIANT above.
    FALSE

INIT
    _init

NEXT
    _next

CONSTANT
    _TETrace <- _trace

ALIAS
    _expression
=============================================================================
\* Generated on Tue Nov 17 09:45:10 CET 2026