---- MODULE EpochRetirement_TTrace_1794996075 ----
EXTENDS Sequences, TLCExt, Toolbox, Naturals, TLC, EpochRetirement

_expression ==
    LET EpochRetirement_TEExpression == INSTANCE EpochRetirement_TEExpression
    IN EpochRetirement_TEExpression!expression
----

_trace ==
    LET EpochRetirement_TETrace == INSTANCE EpochRetirement_TETrace
    IN EpochRetirement_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        leases = ({})
        /\
        fenced = ((t :> 1 @@ p :> 0))
        /\
        img = ((t :> 0 @@ p :> 0))
        /\
        committed = ((t :> {} @@ p :> {}))
        /\
        deleted = ({})
        /\
        mlog = (<<[e |-> 1, k |-> "reg"]>>)
        /\
        ended = ({1})
        /\
        row = ((t :> [e |-> 0, rt |-> 1, retired |-> FALSE] @@ p :> [e |-> 0, rt |-> 0, retired |-> FALSE]))
        /\
        net = ({[e |-> 1, k |-> "fence"]})
        /\
        live = (1)
        /\
        granted = ({1})
    )
----

_init ==
    /\ fenced = _TETrace[1].fenced
    /\ deleted = _TETrace[1].deleted
    /\ row = _TETrace[1].row
    /\ net = _TETrace[1].net
    /\ img = _TETrace[1].img
    /\ mlog = _TETrace[1].mlog
    /\ leases = _TETrace[1].leases
    /\ committed = _TETrace[1].committed
    /\ live = _TETrace[1].live
    /\ granted = _TETrace[1].granted
    /\ ended = _TETrace[1].ended
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ fenced  = _TETrace[i].fenced
        /\ fenced' = _TETrace[j].fenced
        /\ deleted  = _TETrace[i].deleted
        /\ deleted' = _TETrace[j].deleted
        /\ row  = _TETrace[i].row
        /\ row' = _TETrace[j].row
        /\ net  = _TETrace[i].net
        /\ net' = _TETrace[j].net
        /\ img  = _TETrace[i].img
        /\ img' = _TETrace[j].img
        /\ mlog  = _TETrace[i].mlog
        /\ mlog' = _TETrace[j].mlog
        /\ leases  = _TETrace[i].leases
        /\ leases' = _TETrace[j].leases
        /\ committed  = _TETrace[i].committed
        /\ committed' = _TETrace[j].committed
        /\ live  = _TETrace[i].live
        /\ live' = _TETrace[j].live
        /\ granted  = _TETrace[i].granted
        /\ granted' = _TETrace[j].granted
        /\ ended  = _TETrace[i].ended
        /\ ended' = _TETrace[j].ended

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("EpochRetirement_TTrace_1794996075.json", _TETrace)

=============================================================================

 Note that you can extract this module `EpochRetirement_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `EpochRetirement_TEExpression.tla` file takes precedence 
  over the module `EpochRetirement_TEExpression` below).

---- MODULE EpochRetirement_TEExpression ----
EXTENDS Sequences, TLCExt, Toolbox, Naturals, TLC, EpochRetirement

expression == 
    [
        \* To hide variables of the `EpochRetirement` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        fenced |-> fenced
        ,deleted |-> deleted
        ,row |-> row
        ,net |-> net
        ,img |-> img
        ,mlog |-> mlog
        ,leases |-> leases
        ,committed |-> committed
        ,live |-> live
        ,granted |-> granted
        ,ended |-> ended
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_fencedUnchanged |-> fenced = fenced'
        
        \* Format the `fenced` variable as Json value.
        \* ,_fencedJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(fenced)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_fencedModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].fenced # _TETrace[s-1].fenced
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE EpochRetirement_TETrace ----
\*EXTENDS IOUtils, TLC, EpochRetirement
\*
\*trace == IODeserialize("EpochRetirement_TTrace_1794996075.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE EpochRetirement_TETrace ----
EXTENDS TLC, EpochRetirement

trace == 
    <<
    ([leases |-> {},fenced |-> (t :> 0 @@ p :> 0),img |-> (t :> 0 @@ p :> 0),committed |-> (t :> {} @@ p :> {}),deleted |-> {},mlog |-> <<>>,ended |-> {},row |-> (t :> [e |-> 0, rt |-> 0, retired |-> FALSE] @@ p :> [e |-> 0, rt |-> 0, retired |-> FALSE]),net |-> {},live |-> 0,granted |-> {}]),
    ([leases |-> {},fenced |-> (t :> 0 @@ p :> 0),img |-> (t :> 0 @@ p :> 0),committed |-> (t :> {} @@ p :> {}),deleted |-> {},mlog |-> <<[e |-> 1, k |-> "reg"]>>,ended |-> {},row |-> (t :> [e |-> 0, rt |-> 0, retired |-> FALSE] @@ p :> [e |-> 0, rt |-> 0, retired |-> FALSE]),net |-> {},live |-> 1,granted |-> {}]),
    ([leases |-> {},fenced |-> (t :> 0 @@ p :> 0),img |-> (t :> 0 @@ p :> 0),committed |-> (t :> {} @@ p :> {}),deleted |-> {},mlog |-> <<[e |-> 1, k |-> "reg"]>>,ended |-> {},row |-> (t :> [e |-> 0, rt |-> 0, retired |-> FALSE] @@ p :> [e |-> 0, rt |-> 0, retired |-> FALSE]),net |-> {[e |-> 1, k |-> "lease"]},live |-> 1,granted |-> {}]),
    ([leases |-> {1},fenced |-> (t :> 0 @@ p :> 0),img |-> (t :> 0 @@ p :> 0),committed |-> (t :> {} @@ p :> {}),deleted |-> {},mlog |-> <<[e |-> 1, k |-> "reg"]>>,ended |-> {},row |-> (t :> [e |-> 0, rt |-> 0, retired |-> FALSE] @@ p :> [e |-> 0, rt |-> 0, retired |-> FALSE]),net |-> {},live |-> 1,granted |-> {1}]),
    ([leases |-> {},fenced |-> (t :> 1 @@ p :> 0),img |-> (t :> 0 @@ p :> 0),committed |-> (t :> {} @@ p :> {}),deleted |-> {},mlog |-> <<[e |-> 1, k |-> "reg"]>>,ended |-> {1},row |-> (t :> [e |-> 0, rt |-> 1, retired |-> FALSE] @@ p :> [e |-> 0, rt |-> 0, retired |-> FALSE]),net |-> {[e |-> 1, k |-> "fence"]},live |-> 1,granted |-> {1}])
    >>
----


=============================================================================

---- CONFIG EpochRetirement_TTrace_1794996075 ----
CONSTANTS
    t = t
    p = p
    Rule = "fable"
    Guard = TRUE
    PreWhenUnreg = FALSE
    MaxLog = 5
    t = t
    p = p

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
\* Generated on Wed Nov 18 11:01:15 CET 2026