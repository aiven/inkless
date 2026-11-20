---- MODULE FFScenarios_TTrace_1795095668 ----
EXTENDS Sequences, TLCExt, Toolbox, Naturals, TLC, FFScenarios

_expression ==
    LET FFScenarios_TEExpression == INSTANCE FFScenarios_TEExpression
    IN FFScenarios_TEExpression!expression
----

_trace ==
    LET FFScenarios_TETrace == INSTANCE FFScenarios_TETrace
    IN FFScenarios_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        paused = (FALSE)
        /\
        ranges = ({[tp |-> p2, a |-> 3, b |-> 4]})
        /\
        renewal = ([tp |-> "none"])
        /\
        reqHi = (4)
        /\
        epoch = (1)
        /\
        faults = ({})
        /\
        pausedEver = (FALSE)
        /\
        toRelease = ({})
        /\
        hist = ((p1 :> <<>> @@ p2 :> <<>> @@ p3 :> <<>>))
        /\
        S = ((p1 :> [row |-> <<0, 0>>, fenced |-> <<1, 2>>, held |-> {}, relPend |-> {}, leases |-> {[a |-> 1, b |-> 2, e |-> 1, live |-> FALSE]}, rec |-> <<>>, known |-> {<<1, 1>>, <<1, 2>>}, rep |-> <<[ft |-> 2, exc |-> {}]>>, ent |-> (<<1, 1>> :> (p1 :> "fencing" @@ p2 :> "fencing" @@ p3 :> "fencing") @@ <<1, 2>> :> (p1 :> "fencing" @@ p2 :> "fencing" @@ p3 :> "fencing")), jobs |-> {<<p1, 1, 1, 1>>, <<p1, 1, 2, 2>>, <<p2, 1, 1, 1>>, <<p2, 1, 2, 2>>, <<p3, 1, 1, 1>>, <<p3, 1, 2, 2>>}, queue |-> {}] @@ p2 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {[a |-> 3, b |-> 4, e |-> 1, live |-> TRUE]}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}] @@ p3 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}]))
        /\
        deleted = ({})
        /\
        files = ((<<1, 3>> :> [tp |-> p2, ds |-> {p1}, parts |-> (p1 :> "open" @@ p2 :> "open")]))
        /\
        nextSeq = (4)
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
    \*         IN J!JsonSerialize("FFScenarios_TTrace_1795095668.json", _TETrace)

=============================================================================

 Note that you can extract this module `FFScenarios_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `FFScenarios_TEExpression.tla` file takes precedence 
  over the module `FFScenarios_TEExpression` below).

---- MODULE FFScenarios_TEExpression ----
EXTENDS Sequences, TLCExt, Toolbox, Naturals, TLC, FFScenarios

expression == 
    [
        \* To hide variables of the `FFScenarios` spec from the error trace,
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
\*---- MODULE FFScenarios_TETrace ----
\*EXTENDS IOUtils, TLC, FFScenarios
\*
\*trace == IODeserialize("FFScenarios_TTrace_1795095668.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE FFScenarios_TETrace ----
EXTENDS TLC, FFScenarios

trace == 
    <<
    ([paused |-> FALSE,ranges |-> {},renewal |-> [tp |-> "none"],reqHi |-> 0,epoch |-> 1,faults |-> {},pausedEver |-> FALSE,toRelease |-> {},hist |-> (p1 :> <<>> @@ p2 :> <<>> @@ p3 :> <<>>),S |-> (p1 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}] @@ p2 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}] @@ p3 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}]),deleted |-> {},files |-> <<>>,nextSeq |-> 1,net |-> {}]),
    ([paused |-> FALSE,ranges |-> {},renewal |-> [tp |-> p1, a |-> 1, b |-> 2],reqHi |-> 2,epoch |-> 1,faults |-> {},pausedEver |-> FALSE,toRelease |-> {},hist |-> (p1 :> <<>> @@ p2 :> <<>> @@ p3 :> <<>>),S |-> (p1 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}] @@ p2 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}] @@ p3 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}]),deleted |-> {},files |-> <<>>,nextSeq |-> 1,net |-> {}]),
    ([paused |-> FALSE,ranges |-> {[tp |-> p1, a |-> 1, b |-> 2]},renewal |-> [tp |-> "none"],reqHi |-> 2,epoch |-> 1,faults |-> {},pausedEver |-> FALSE,toRelease |-> {},hist |-> (p1 :> <<>> @@ p2 :> <<>> @@ p3 :> <<>>),S |-> (p1 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {[a |-> 1, b |-> 2, e |-> 1, live |-> TRUE]}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}] @@ p2 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}] @@ p3 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}]),deleted |-> {},files |-> <<>>,nextSeq |-> 1,net |-> {}]),
    ([paused |-> FALSE,ranges |-> {[tp |-> p1, a |-> 1, b |-> 2]},renewal |-> [tp |-> p2, a |-> 3, b |-> 4],reqHi |-> 4,epoch |-> 1,faults |-> {},pausedEver |-> FALSE,toRelease |-> {},hist |-> (p1 :> <<>> @@ p2 :> <<>> @@ p3 :> <<>>),S |-> (p1 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {[a |-> 1, b |-> 2, e |-> 1, live |-> TRUE]}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}] @@ p2 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}] @@ p3 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}]),deleted |-> {},files |-> <<>>,nextSeq |-> 1,net |-> {}]),
    ([paused |-> FALSE,ranges |-> {[tp |-> p2, a |-> 3, b |-> 4]},renewal |-> [tp |-> "none"],reqHi |-> 4,epoch |-> 1,faults |-> {},pausedEver |-> FALSE,toRelease |-> {[tp |-> p1, b |-> 2, e |-> 1]},hist |-> (p1 :> <<>> @@ p2 :> <<>> @@ p3 :> <<>>),S |-> (p1 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {[a |-> 1, b |-> 2, e |-> 1, live |-> TRUE]}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}] @@ p2 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {[a |-> 3, b |-> 4, e |-> 1, live |-> TRUE]}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}] @@ p3 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}]),deleted |-> {},files |-> <<>>,nextSeq |-> 1,net |-> {}]),
    ([paused |-> FALSE,ranges |-> {[tp |-> p2, a |-> 3, b |-> 4]},renewal |-> [tp |-> "none"],reqHi |-> 4,epoch |-> 1,faults |-> {},pausedEver |-> FALSE,toRelease |-> {[tp |-> p1, b |-> 2, e |-> 1]},hist |-> (p1 :> <<>> @@ p2 :> <<>> @@ p3 :> <<>>),S |-> (p1 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {[a |-> 1, b |-> 2, e |-> 1, live |-> TRUE]}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}] @@ p2 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {[a |-> 3, b |-> 4, e |-> 1, live |-> TRUE]}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}] @@ p3 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}]),deleted |-> {},files |-> (<<1, 3>> :> [tp |-> p2, ds |-> {p1}, parts |-> (p1 :> "open" @@ p2 :> "open")]),nextSeq |-> 4,net |-> {}]),
    ([paused |-> FALSE,ranges |-> {[tp |-> p2, a |-> 3, b |-> 4]},renewal |-> [tp |-> "none"],reqHi |-> 4,epoch |-> 1,faults |-> {},pausedEver |-> FALSE,toRelease |-> {},hist |-> (p1 :> <<>> @@ p2 :> <<>> @@ p3 :> <<>>),S |-> (p1 :> [row |-> <<0, 0>>, fenced |-> <<1, 2>>, held |-> {}, relPend |-> {}, leases |-> {[a |-> 1, b |-> 2, e |-> 1, live |-> FALSE]}, rec |-> <<>>, known |-> {<<1, 1>>, <<1, 2>>}, rep |-> <<[ft |-> 2, exc |-> {}]>>, ent |-> (<<1, 1>> :> (p1 :> "fencing" @@ p2 :> "fencing" @@ p3 :> "fencing") @@ <<1, 2>> :> (p1 :> "fencing" @@ p2 :> "fencing" @@ p3 :> "fencing")), jobs |-> {<<p1, 1, 1, 1>>, <<p1, 1, 2, 2>>, <<p2, 1, 1, 1>>, <<p2, 1, 2, 2>>, <<p3, 1, 1, 1>>, <<p3, 1, 2, 2>>}, queue |-> {}] @@ p2 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {[a |-> 3, b |-> 4, e |-> 1, live |-> TRUE]}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}] @@ p3 :> [row |-> <<0, 0>>, fenced |-> <<0, 0>>, held |-> {}, relPend |-> {}, leases |-> {}, rec |-> <<>>, known |-> {}, rep |-> <<[ft |-> 0, exc |-> {}]>>, ent |-> <<>>, jobs |-> {}, queue |-> {}]),deleted |-> {},files |-> (<<1, 3>> :> [tp |-> p2, ds |-> {p1}, parts |-> (p1 :> "open" @@ p2 :> "open")]),nextSeq |-> 4,net |-> {}])
    >>
----


=============================================================================

---- CONFIG FFScenarios_TTrace_1795095668 ----
CONSTANTS
    P = { p1 , p2 , p3 }
    R = 2
    MaxSeq = 4
    MaxEpoch = 1
    MaxFiles = 1
    FixFinish = TRUE
    FixLease = TRUE
    FixBrokerWide = TRUE
    FixPlannedWait = TRUE
    FixMissedFirst = TRUE
    HullMerge = TRUE
    MaxFaults = 0
    MaxLate = 1
    Scenario = "H"
    p1 = p1
    p2 = p2
    p3 = p3
    p1 = p1
    p2 = p2
    p3 = p3

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
\* Generated on Thu Nov 19 14:41:09 CET 2026