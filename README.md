# ShopFlow eval cases — Layer 1 (reference branch)

Every Layer 1 eval case, all present at once, so each can be linked to by a stable
GitHub URL.

**This branch is a reference archive. It is never scanned.** The context-graph builder
scans `main`, which holds one case at a time at the repository root. Scanning this
branch would ingest 23 cases together — several share service names like
`order-service`, so the resulting graph would merge unrelated services. That mistake
has already voided one run (`mono-test`).

```
<case-id>/
  codebase/     what AI Engineer is given — an ordinary-looking app repo
```

**Answer keys are not here.** They live in a separate repository so that no case can be
solved by reading the key next to the code:
<https://github.com/Gupshup-mock-env/msg_eval_truth>

Layer 1 isolates one variable at a time: the broker (`broker-*`), how the topic name is
referenced (`topic-*`), the messaging framework (`framework-*`), or the payload shape
(`payload-*`).

Results: <https://github.com/Gupshup-mock-env/shopflow-platform-layer2/blob/all-cases/EVAL-RESULTS.md>
