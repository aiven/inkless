------------------------------ MODULE FFDepth ------------------------------
(* PROTOTYPE. FencesFloors cut off at MaxDepth steps, to measure growth. *)
EXTENDS FencesFloors
CONSTANT MaxDepth
DepthOK == TLCGet("level") <= MaxDepth
=============================================================================
