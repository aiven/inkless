# Cherry-pick Session: inkless-4.3

## Session Info
- **Date**: 2026-09-09
- **Source**: origin/main (72e432f6fb, #789)
- **Target**: inkless-4.3
- **Base**: b5f361ab7f (fix(inkless:ci): tolerate non-identifier parameterized test names in junit.py, on top of #779 upstream 4.3.1 merge)
- **Status**: Complete

---

## Commits to Cherry-pick

| # | Hash | PR | Subject | Status |
|---|------|-----|---------|--------|
| 1 | 58aed69133 | #778 | fix(inkless:storage): report an oversized fetch as a storage failure [KC-481] | Applied clean |
| 2 | ea1dc9b0ed | #702 | docs(inkless:consolidation): Diskless Consolidation documentation | Applied clean |
| 3 | 3a00358ce4 | #781 | fix(inkless:gcs): narrow fetch length before opening the reader | Applied clean |
| 4 | a6716f7037 | #782 | feat(inkless:delete): soft-delete diskless topics and purge in the background [KC-349] | Applied clean |
| 5 | 6c57f5aadf | #785 | test(inkless:consolidation): tolerate re-delivery in the consume check | Applied clean |
| 6 | f7e7412d14 | #783 | feat(inkless:storage): bound and shorten the GCS upload path | Applied clean |
| 7 | 94fe3a0a02 | #788 | fix(inkless:ci): update Inkless CI workflow and JUnit catalog parsing | Applied clean |
| 8 | 5fa6669803 | #784 | feat(inkless:consume): measure the fetch data phase and its queueing [KC-446] | Applied clean |
| 9 | 470d46973e | #794 | fix(inkless:release): exclude sync-tooling commits from the branch-consistency gate | Applied |
| 10 | 095cf08133 | #787 | refactor(inkless:consume): drop four never-recorded fetch metrics | Applied |
| 11 | 5693b9d4a2 | #792 | fix(inkless:delete): do not sleep inside the topic purger tick [KC-349] | Applied |
| 12 | 8d97bb4235 | #793 | fix(inkless:ci): copy integrationTest JUnit XML into build/junit-xml | Applied |
| 13 | 62e4c51047 | #791 | feat(inkless:storage): default S3 API call timeouts to 2s and 1s | Applied |
| 14 | 72e432f6fb | #789 | fix(inkless:storage): count retried GCS error responses | Applied |

Excluded by the gate (sync tooling, not applicable): #780 docs(inkless:sync).

---

## Conflict Resolution Log

None. All 8 commits applied clean; junit.py converged to main's content (4.3 already carried b5f361ab7f).

---

## Compilation Errors

| # | File:Line | Error | Cherry-pick | Fix | Status |
|---|-----------|-------|-------------|-----|--------|

---

## Verification

- [x] make build (BUILD SUCCESSFUL)
- [x] make test (all three gradle invocations BUILD SUCCESSFUL, no failed tests)
- [x] branch-consistency.sh --check against the local head: 0 actionable missing
- [x] #794, #787, #792, #793, #791, #789 applied clean after the first push; compile of storage:inkless and core main+test, then the tests these commits touch (TopicPurgerMockedTest, MetricCollectorTest, S3ClientBuilderTest, S3StorageConfigTest, GcsErrorHandlingTest, S3ErrorMetricsTest, InklessDisklessTopicDeleteTest): all passed.
