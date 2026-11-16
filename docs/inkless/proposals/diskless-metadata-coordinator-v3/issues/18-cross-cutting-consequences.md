# Consequences for the cross-cutting sections

Type: grilling
Status: open
Assignee:
Blocked by: 01, 02, 03, 04, 05, 06, 07, 08, 09, 10, 11, 12, 13, 14, 15, 16, 17, 19, 20, 21, 22, 23

## Question

What do v2's cross-cutting sections become in v3: failure modes, accepted risks, metrics, tests, user stories, and the section list for the rewrite?

v2's [Consequences for the cross-cutting sections](../../diskless-metadata-coordinator-v2/issues/11-cross-cutting-consequences.md) settled one set for v2. Each v3 ticket leaves rows, metrics, tests, and story changes for this pass, and the rewrite needs one decided set.

Settle these points:

- The failure-mode table and the accepted-risk table under the map's data-loss bar. Every accepted risk passes clauses 2 and 3, or becomes a fix row. Rewrite each row's reason from "A classic topic loses data the same way" where the blast radius differs. [Deletion bounds: soft-delete requirement and a deletion-rate cap](10-deletion-bounds.md#accepted-risks-in-v3) supplies six rows under clause 3's exception, written with that reason, and adds no row, metric, or test from its deferred design.
- The metric set, merging each ticket's signals into v2's two metrics groups.
- The tests, placing each ticket's named test, including Fable's five, in v2's four layers and seam tests, and the model from [Model of the fence, floor, and release protocol](17-protocol-model.md) in the testing decisions.
- The user stories: story 83 from [File completion and story 83](04-file-completion.md), and the stories that the replication guard and admission tickets imply. [Object footer and recovery after losing a replica set](09-object-footer.md) and [Deletion bounds: soft-delete requirement and a deletion-rate cap](10-deletion-bounds.md) each add a Future work entry, not a story.
- The Terms entries for "Kafka parity" and "blast radius", with the map's meanings.
- The section changes for the rewrite.
