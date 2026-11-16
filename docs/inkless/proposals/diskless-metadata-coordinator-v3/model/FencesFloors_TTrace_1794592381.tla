---- MODULE FencesFloors_TTrace_1794592381 ----
EXTENDS Sequences, TLCExt, FencesFloors, Toolbox, Naturals, TLC, FencesFloors_TEConstants

_expression ==
    LET FencesFloors_TEExpression == INSTANCE FencesFloors_TEExpression
    IN FencesFloors_TEExpression!expression
----

_trace ==
    LET FencesFloors_TETrace == INSTANCE FencesFloors_TETrace
    IN FencesFloors_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        paused = (FALSE)
        /\
        ranges = ({[tp |-> p1, a |-> 1, b |-> 2]})
        /\
        renewal = ([tp |-> "none"])
        /\
        answers = ({[tp |-> p1, j |-> <<p2, 1, 1, 1>>, refs |-> {}]})
        /\
        reqHi = (2)
        /\
        epoch = (1)
        /\
        faults = (0)
        /\
        pausedEver = (FALSE)
        /\
        toRelease = ({})
        /\
        hist = ((p1 :> <<>> @@ p2 :> <<>> @@ p3 :> <<>>))
        /\
        S = ((p1 :> [row |-> <<1, 1>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {[a |-> 1, b |-> 2, e |-> 1, live |-> TRUE]}, rec |-> (<<1, 1>> :> {p2}), known |-> {<<1, 1>>}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> (<<1, 1>> :> (p2 :> "fencing")), jobs |-> {<<p2, 1, 1, 1>>}, queue |-> {}] @@ p2 :> [row |-> <<0, 0>>, fenced |-> <<1, 1>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}] @@ p3 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}]))
        /\
        deleted = ({})
        /\
        files = ((<<1, 1>> :> [tp |-> p1, parts |-> (p1 :> "ok" @@ p2 :> "open"), ds |-> {p2}]))
        /\
        nextSeq = (2)
        /\
        net = ({})
    )
----

_init ==
    /\ epoch = _TETrace[1].epoch
    /\ ranges = _TETrace[1].ranges
    /\ paused = _TETrace[1].paused
    /\ toRelease = _TETrace[1].toRelease
    /\ S = _TETrace[1].S
    /\ deleted = _TETrace[1].deleted
    /\ renewal = _TETrace[1].renewal
    /\ faults = _TETrace[1].faults
    /\ pausedEver = _TETrace[1].pausedEver
    /\ answers = _TETrace[1].answers
    /\ files = _TETrace[1].files
    /\ net = _TETrace[1].net
    /\ hist = _TETrace[1].hist
    /\ nextSeq = _TETrace[1].nextSeq
    /\ reqHi = _TETrace[1].reqHi
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ epoch  = _TETrace[i].epoch
        /\ epoch' = _TETrace[j].epoch
        /\ ranges  = _TETrace[i].ranges
        /\ ranges' = _TETrace[j].ranges
        /\ paused  = _TETrace[i].paused
        /\ paused' = _TETrace[j].paused
        /\ toRelease  = _TETrace[i].toRelease
        /\ toRelease' = _TETrace[j].toRelease
        /\ S  = _TETrace[i].S
        /\ S' = _TETrace[j].S
        /\ deleted  = _TETrace[i].deleted
        /\ deleted' = _TETrace[j].deleted
        /\ renewal  = _TETrace[i].renewal
        /\ renewal' = _TETrace[j].renewal
        /\ faults  = _TETrace[i].faults
        /\ faults' = _TETrace[j].faults
        /\ pausedEver  = _TETrace[i].pausedEver
        /\ pausedEver' = _TETrace[j].pausedEver
        /\ answers  = _TETrace[i].answers
        /\ answers' = _TETrace[j].answers
        /\ files  = _TETrace[i].files
        /\ files' = _TETrace[j].files
        /\ net  = _TETrace[i].net
        /\ net' = _TETrace[j].net
        /\ hist  = _TETrace[i].hist
        /\ hist' = _TETrace[j].hist
        /\ nextSeq  = _TETrace[i].nextSeq
        /\ nextSeq' = _TETrace[j].nextSeq
        /\ reqHi  = _TETrace[i].reqHi
        /\ reqHi' = _TETrace[j].reqHi

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("FencesFloors_TTrace_1794592381.json", _TETrace)

=============================================================================

 Note that you can extract this module `FencesFloors_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `FencesFloors_TEExpression.tla` file takes precedence 
  over the module `FencesFloors_TEExpression` below).

---- MODULE FencesFloors_TEExpression ----
EXTENDS Sequences, TLCExt, FencesFloors, Toolbox, Naturals, TLC, FencesFloors_TEConstants

expression == 
    [
        \* To hide variables of the `FencesFloors` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        epoch |-> epoch
        ,ranges |-> ranges
        ,paused |-> paused
        ,toRelease |-> toRelease
        ,S |-> S
        ,deleted |-> deleted
        ,renewal |-> renewal
        ,faults |-> faults
        ,pausedEver |-> pausedEver
        ,answers |-> answers
        ,files |-> files
        ,net |-> net
        ,hist |-> hist
        ,nextSeq |-> nextSeq
        ,reqHi |-> reqHi
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_epochUnchanged |-> epoch = epoch'
        
        \* Format the `epoch` variable as Json value.
        \* ,_epochJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(epoch)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_epochModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].epoch # _TETrace[s-1].epoch
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE FencesFloors_TETrace ----
\*EXTENDS IOUtils, FencesFloors, TLC, FencesFloors_TEConstants
\*
\*trace == IODeserialize("FencesFloors_TTrace_1794592381.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE FencesFloors_TETrace ----
EXTENDS FencesFloors, TLC, FencesFloors_TEConstants

trace == 
    <<
    ([paused |-> FALSE,ranges |-> {},renewal |-> [tp |-> "none"],answers |-> {},reqHi |-> 0,epoch |-> 1,faults |-> 0,pausedEver |-> FALSE,toRelease |-> {},hist |-> (p1 :> <<>> @@ p2 :> <<>> @@ p3 :> <<>>),S |-> (p1 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}] @@ p2 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}] @@ p3 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}]),deleted |-> {},files |-> <<>>,nextSeq |-> 1,net |-> {}]),
    ([paused |-> FALSE,ranges |-> {},renewal |-> [tp |-> p1, a |-> 1, b |-> 2],answers |-> {},reqHi |-> 2,epoch |-> 1,faults |-> 0,pausedEver |-> FALSE,toRelease |-> {},hist |-> (p1 :> <<>> @@ p2 :> <<>> @@ p3 :> <<>>),S |-> (p1 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}] @@ p2 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}] @@ p3 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}]),deleted |-> {},files |-> <<>>,nextSeq |-> 1,net |-> {}]),
    ([paused |-> FALSE,ranges |-> {[tp |-> p1, a |-> 1, b |-> 2]},renewal |-> [tp |-> "none"],answers |-> {},reqHi |-> 2,epoch |-> 1,faults |-> 0,pausedEver |-> FALSE,toRelease |-> {},hist |-> (p1 :> <<>> @@ p2 :> <<>> @@ p3 :> <<>>),S |-> (p1 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {[a |-> 1, b |-> 2, e |-> 1, live |-> TRUE]}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}] @@ p2 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}] @@ p3 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}]),deleted |-> {},files |-> <<>>,nextSeq |-> 1,net |-> {}]),
    ([paused |-> FALSE,ranges |-> {[tp |-> p1, a |-> 1, b |-> 2]},renewal |-> [tp |-> "none"],answers |-> {},reqHi |-> 2,epoch |-> 1,faults |-> 0,pausedEver |-> FALSE,toRelease |-> {},hist |-> (p1 :> <<>> @@ p2 :> <<>> @@ p3 :> <<>>),S |-> (p1 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {[a |-> 1, b |-> 2, e |-> 1, live |-> TRUE]}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}] @@ p2 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}] @@ p3 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}]),deleted |-> {},files |-> (<<1, 1>> :> [tp |-> p1, parts |-> (p1 :> "open" @@ p2 :> "open"), ds |-> {p2}]),nextSeq |-> 2,net |-> {}]),
    ([paused |-> FALSE,ranges |-> {[tp |-> p1, a |-> 1, b |-> 2]},renewal |-> [tp |-> "none"],answers |-> {},reqHi |-> 2,epoch |-> 1,faults |-> 0,pausedEver |-> FALSE,toRelease |-> {},hist |-> (p1 :> <<>> @@ p2 :> <<>> @@ p3 :> <<>>),S |-> (p1 :> [row |-> <<1, 1>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {[a |-> 1, b |-> 2, e |-> 1, live |-> TRUE]}, rec |-> (<<1, 1>> :> {p2}), known |-> {<<1, 1>>}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> (<<1, 1>> :> (p2 :> "fencing")), jobs |-> {<<p2, 1, 1, 1>>}, queue |-> {}] @@ p2 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}] @@ p3 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}]),deleted |-> {},files |-> (<<1, 1>> :> [tp |-> p1, parts |-> (p1 :> "ok" @@ p2 :> "open"), ds |-> {p2}]),nextSeq |-> 2,net |-> {}]),
    ([paused |-> FALSE,ranges |-> {[tp |-> p1, a |-> 1, b |-> 2]},renewal |-> [tp |-> "none"],answers |-> {[tp |-> p1, j |-> <<p2, 1, 1, 1>>, refs |-> {}]},reqHi |-> 2,epoch |-> 1,faults |-> 0,pausedEver |-> FALSE,toRelease |-> {},hist |-> (p1 :> <<>> @@ p2 :> <<>> @@ p3 :> <<>>),S |-> (p1 :> [row |-> <<1, 1>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {[a |-> 1, b |-> 2, e |-> 1, live |-> TRUE]}, rec |-> (<<1, 1>> :> {p2}), known |-> {<<1, 1>>}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> (<<1, 1>> :> (p2 :> "fencing")), jobs |-> {<<p2, 1, 1, 1>>}, queue |-> {}] @@ p2 :> [row |-> <<0, 0>>, fenced |-> <<1, 1>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}] @@ p3 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}]),deleted |-> {},files |-> (<<1, 1>> :> [tp |-> p1, parts |-> (p1 :> "ok" @@ p2 :> "open"), ds |-> {p2}]),nextSeq |-> 2,net |-> {}])
    >>
----


=============================================================================

---- MODULE FencesFloors_TEConstants ----
EXTENDS FencesFloors

CONSTANTS p1, p2, p3

=============================================================================

---- CONFIG FencesFloors_TTrace_1794592381 ----
CONSTANTS
    P = { p1 , p2 , p3 }
    R = 2
    MaxSeq = 6
    MaxEpoch = 1
    MaxFiles = 4
    FixFinish = FALSE
    FixLease = TRUE
    FixBrokerWide = TRUE
    FixPlannedWait = TRUE
    MaxFaults = 0
    p3 = p3
    p2 = p2
    p1 = p1

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
\* Generated on Fri Nov 13 18:53:02 CET 2026