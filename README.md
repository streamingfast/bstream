# block streaming low-level tooling

The `bstream` package manages flows of blocks, fork-awareness using a
Handler-based interface similar to `net/http`.


## Usage

Flows are composed by assembling `Handler`s:

```go
type HandlerFunc func(blk *Block, obj interface{}) error
```

and are kicked off by passing them to a _Source_

## Overview

Here are some of the important components of this repository:

* _FileSource_ feeds from 100-blocks files in some [dstore-based](https://github.com/dfuse-io/dstore) location (some object storage, or local filesystem files)
* _LiveSource_ streams from a gRPC-based block streamer (fed from instrumented blockchain nodes directly).
* _JoiningSource_ which bridges a _FileSource_ and a _LiveSource_ transparently, so you can stream from files and then handoff to a real-time stream.


Handlers include:

* _Forkable_ (in [`forkable/`](forkable/)) which manages chain reorganizations, undos, according to the chain's consensus (longest chain, etc..)
* _SubscriptionHub_ (in [`hub/`](hub/)): In-process hub to dispatch blocks from a remote source to all consumers inside a Go process
* A few _gates_, that allow the flowing of blocks only upon certain conditions (_BlockNumGate_, _BlockIDGate_, _RealtimeGate_, _RealtimeTripper_, which can be inclusive or exclusive). See [gates.go](gates.go).

All streaming features of dfuse use this package.


## Contributing

**Issues and PR in this repo related strictly to the low-level functionalities of bstream**

Report any protocol-specific issues in their
[respective repositories](https://github.com/dfuse-io/dfuse#protocols)

**Please first refer to the general
[dfuse contribution guide](https://github.com/dfuse-io/dfuse#contributing)**,
if you wish to contribute to this code base.

Please write and run tests.


## License

[Apache 2.0](LICENSE)
