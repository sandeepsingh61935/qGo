# Mission: Interview-ready on qGo

## Why
You built **qGo** (Go + Redis job queue) for portfolio and backend interviews. You need to walk a hiring manager through the design the same way a **system design interview** works — requirements first, then high-level design, deep dives, trade-offs — so the project reads as engineering judgment, not a tutorial dump.

## Success looks like
- Run the **full SD interview arc** on qGo: clarify → FR/NFR → API → high-level design → deep dives → wrap-up
- Write crisp **functional** vs **non-functional** requirements without mixing them
- Give a **60–90 second architecture pitch** after requirements are locked
- Answer failure probes (worker crash, redelivery, DLQ) with correct delivery language
- Defend trade-offs (Redis vs Postgres, lease timeout, API vs worker split)

## Constraints
- Start from **basics** (interview structure, FR/NFR) even if the code already exists
- Short lessons; retrieval practice over long lectures
- Always map abstract SD steps **back to qGo** (concrete system, not generic Twitter)

## Out of scope
- Building new product features for qGo
- Full distributed-systems curriculum (Raft, 2PC, Kafka internals as primary goals)
- Resume writing / soft skills
