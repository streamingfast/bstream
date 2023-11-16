# StreamingFast Blocks Streaming Library
[![reference](https://img.shields.io/badge/godoc-reference-5272B4.svg?style=flat-square)](https://pkg.go.dev/github.com/streamingfast/bstream)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The `bstream` package manages flows of blocks and forks in a blockchain
through a Handler-based interface similar to `net/http`.


## Usage

Flows are composed by assembling `Handler`s:

```go
type HandlerFunc func(blk *pbbstream.Block, obj interface{}) error
```

and are kicked off by passing them to a _Source_

## Overview

All streaming features of streamingfast use this package.

Sources include:

* _FileSource_ feeds from 100-blocks files in some [dstore-based](https://github.com/streamingfast/dstore) location (some object storage, or local filesystem files)
* _LiveSource_ streams from a gRPC-based block streamer (fed from instrumented blockchain nodes directly).
* _JoiningSource_ which bridges a _FileSource_ and a _LiveSource_ transparently, so you can stream from files and then handoff to a real-time stream.


Handlers include:

* _Forkable_ (in [`forkable/`](forkable/)) which manages chain reorganizations, undos, according to the chain's consensus (longest chain, etc..)
* _SubscriptionHub_ (in [`hub/`](hub/)): In-process hub to dispatch blocks from a remote source to all consumers inside a Go process
* A few _gates_, that allow the flowing of blocks only upon certain conditions (_BlockNumGate_, _BlockIDGate_, _RealtimeGate_, _RealtimeTripper_, which can be inclusive or exclusive). See [gates.go](gates.go).


## Contributing

**Issues and PR in this repo related strictly to the low-level functionalities of bstream**

Report any protocol-specific issues in their
[respective repositories](https://github.com/streamingfast/streamingfast#protocols)

**Please first refer to the general
[StreamingFast contribution guide](https://github.com/streamingfast/streamingfast/blob/master/CONTRIBUTING.md)**,
if you wish to contribute to this code base.

## License

[Apache 2.0](LICENSE)
