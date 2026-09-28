# Security policy

hivebox exists to run code nobody has reviewed, much of it written by a model that is being rewarded for getting a result by any means. The isolation boundary is the product, and a report against it is the most valuable thing anyone can send.

## Reporting

Please do not open a public issue. Use [a private security advisory](https://github.com/tamnd/hivebox/security/advisories/new) on this repository. You will get an answer within three working days, and a fix or a plan within fourteen.

Include what you ran inside the cell, the tier and backend, the network profile, the host kernel version and what you were able to reach. A proof of concept is welcome but not required.

## What counts

Any of these is a vulnerability:

- Code in a cell reading or writing anything on the host, or in another cell, that its spec did not grant.
- Code in a cell reaching a network destination its egress policy denies, including through DNS.
- Code in a cell making the guest agent, the node agent or the page server crash, hang or allocate without bound.
- Forging, replaying or guessing a cell id, an API key or a capability token.
- Tampering with a verifier's result or the audit log from inside the cell being verified.
- A way to make a snapshot, a fork or a restore leak memory or files from one tenant to another.

Resource exhaustion that stays inside a cell's own limits is not a vulnerability. A cell that uses all of its own CPU is working as designed.

## Supported versions

Before 1.0 only the latest release gets fixes. The threat model is in [`spec/10_security.md`](spec/10_security.md).
