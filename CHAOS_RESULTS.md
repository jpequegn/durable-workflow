# Chaos Test Results

10 random crash/resume cycles — acceptance test for the durable workflow engine.

**Property tested**: Steps that were COMPLETED before a crash are never
re-executed on resume (0 unwanted re-executions).

| # | Crash at | Unwanted re-execs | Time (ms) | Pass |
|---|----------|-------------------|-----------|------|
| 1 | step_1 | 0 | 3.7 | ✅ |
| 2 | step_3 | 0 | 3.9 | ✅ |
| 3 | step_5 | 0 | 3.8 | ✅ |
| 4 | step_5 | 0 | 3.7 | ✅ |
| 5 | step_1 | 0 | 3.7 | ✅ |
| 6 | step_2 | 0 | 3.7 | ✅ |
| 7 | step_5 | 0 | 3.7 | ✅ |
| 8 | step_5 | 0 | 3.9 | ✅ |
| 9 | step_5 | 0 | 4.2 | ✅ |
| 10 | step_4 | 0 | 3.9 | ✅ |

**Result: 10/10 passed, 0 unwanted re-executions.**
