# qGo Interview Resources

## Knowledge

### System design interview framework
- [ByteByteGo: A Framework for System Design Interviews](https://bytebytego.com/courses/system-design-interview/a-framework-for-system-design-interviews)
  Alex Xu’s 4-step process: scope → high-level design → deep dive → wrap-up. Use for: overall interview shape.
- [Hello Interview: Delivery framework (FR / NFR)](https://www.hellointerview.com/learn/system-design/in-a-hurry/delivery)
  Functional = “users should be able to…”; NFR = system qualities. Use for: writing requirements cleanly.
- [interviewing.io: 3-step SD framework (requirements first)](https://interviewing.io/guides/system-design-interview/part-three)
  Inputs/outputs of the requirements step. Use for: not jumping to boxes before FR/NFR.
- [Pragmatic Engineer: review of Alex Xu’s approach](https://blog.pragmaticengineer.com/system-design-interview-an-insiders-guide-review/)
  Concise restatement of the 4 steps and why rushing answers fails. Use for: mindset.

### FR vs NFR (general SE)
- [GeeksforGeeks: Functional vs Non-functional requirements](https://www.geeksforgeeks.org/software-engineering/functional-vs-non-functional-requirements/)
  Classic definitions. Use for: vocabulary baseline only; prefer interview sources above for phrasing.

### Job queues (qGo depth later)
- [AWS: Amazon SQS visibility timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
  Lease model. Use after requirements/API/HLD lessons.
- [AWS: Amazon SQS at-least-once delivery](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/standard-queues-at-least-once-delivery.html)
  Why duplicates happen. Use for delivery deep-dive.
- [Redis: RPOPLPUSH / reliable queue](https://redis.io/docs/latest/commands/rpoplpush/)
  Claim without dropping jobs. Use for claim/lease deep-dive.
- [AWS: Dead-letter queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
  Poison message handling. Use for retries/DLQ lesson.

## Wisdom (Communities)

- [r/ExperiencedDevs](https://www.reddit.com/r/ExperiencedDevs/)
  Architecture and interview signal. Use for: how seniors probe projects.
- [r/cscareerquestions](https://www.reddit.com/r/cscareerquestions/)
  Interview patterns (filter noise).

## Gaps

- Prefer ByteByteGo/Hello Interview phrasing over random blogs for FR/NFR.
- qGo has no official product PRD; requirements are reconstructed for interview practice from the shipped MVP.
