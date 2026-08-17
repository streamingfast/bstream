# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## Unreleased

### Added

- `forkable.WithFinalizedBlockNumMetric`: new option reporting, on the live path, the LIB number of the block that just became head. Takes a `forkable.Uint64Metric` interface so consumers can provide their own metric implementation.
- `DefaultMergedBlocksBundleSize`: new package variable (default `100`) controlling the number of blocks per merged-blocks file assumed by readers when no explicit size is given. Like `GetProtocolFirstStreamableBlock`, it is meant to be set once at process startup.
- `stream.WithMergedBlocksBundleSize`: new `stream` option to set the merged-blocks bundle size for a single stream (overrides the process-wide default; used by substreams tier2 which serves multiple chains at once).
- `FileSource` now fails fast with a clear error when a merged-blocks file contains a block beyond the configured bundle size (store files bigger than the configured size).
- `SanitizeBundleSize`: guards the merged-blocks math against a bundle size of `0` (misconfigured `DefaultMergedBlocksBundleSize` or `FileSourceWithBundleSize(0)`), which would otherwise divide-by-zero panic in the hub or loop forever in `FileSource`; falls back to `100`.

### Fixed

- `hub.WithMaxConsecutiveUnlinkableBlocks` now counts blocks rather than messages: an intermediate flash block — `PartialIndex != 0` without `LastPartial` — no longer advances the counter. Every partial of a block fails the same link check, so on a chain delivering four per block the hub gave up after a quarter of the blocks the limit names. A plain block and a block's final partial still count, and any linkable block still resets the count.

- Hub subscriptions with `with_partials=false` no longer stall on flash/partial-block chains. Previously every block with `PartialIndex != 0` (including the closing `LastPartial`) was dropped, so a no-partial subscriber only advanced on separate `PartialIndex==0` full blocks, which can lag the sealed head by tens of seconds. The subscription now drops only intermediate partials and delivers each `LastPartial` as a full block (partial markers cleared on a thin copy that shares the payload; the shared original block is never mutated).
- `FileSource` with `FileSourceErrorOnMissingMergedBlocksFile` no longer truncates its output: on a missing file it now drains every already-read block through the ordered stream before surfacing the error, instead of calling `Shutdown()` immediately (which aborted in-flight reader goroutines and discarded blocks).

### Changed

- `ForkableHub` bootstrap now rounds its lowest kept block down to the configured merged-blocks bundle size instead of a hardcoded `100`.

- `BlockTimestampGate`: new gate that lets blocks through once a block's timestamp meets or exceeds a given `time.Time`, supporting both inclusive and exclusive gate types.
- `hub.WithLogger`: new `ForkableHub` option to set the logger used by the hub (and, by default, propagated to its inner forkable). Previously the hub always logged under the package-level `bstream` logger, making lines such as `processing block` indistinguishable across components (relayer, firehose, tier1, ...). Callers should pass their component logger.

## 2026-01-02

- Added field `with_partials` to `sf.bstream.v1.BlockRequest` protobuf message: blockstream no longer sends partial blocks by default.

## 2025-12-12

### Added

- Support for partial blocks (e.g., Flash Blocks), with special "StepPartial". These blocks are only sent when they are above the HEAD, and other blocks are never linked to them. They are always eventually replaced by a full block with StepNew.

## 2024-08-19

### Added

- New middleware handlers in joining source to allow for user to hook into the stream and perform custom actions.

## 2023-12-08

### Major Refactoring

- **BREAKING** Removed the `bstream.Block` object completely: the `pbbstream.Block` takes its place.
- **BREAKING** Moved the `pbbstream` package from github.com/streamingfast/pbgo to here, under `sf.bstream.v1.Block`

## 2023-11-08

### Removed

- **BREAKING** Removed `bstream.Block.PreviousRef` method as it was invalid since it was assuming that the previous number is always minus one the current block num which is not true on a lot of supported chains (Solana and NEAR for example).

### Fixed

- **BREAKING** we now enforce block continuity in filesource while reading merged-blocks: before, a corrupted merged-block-file would have been read as-is and serve wrong blocks.

### Added

- Added FileSourceWithSecondaryBlocksStores Option to allow a fallback location
- `.SetNearBlocksCount(count)` and `.Clone()` on `Tracker` object.
- `Tracker` object to streamline queries about different targets (like network head, database lib, relayer blockstream head, whatever other BlockRef tags), ask the question about them being near one another (to select between live mode or catch-up mode).  Also streamlines the requests of a start block, with a bunch of different backend implementations that can answer to the questions regarding where to start.
- `JoiningSourceWithTracker` to avoid joining to live when live and file sources are very far apart.
- `HeadBlockRefGetter` and `LIBBlockRefGetter` that targets a `HeadInfo` service, and satisfies the `Tracker` _BlockRefGetter_ func signature.

### Changed

- StreamGetter now requires a boolean param to know if it must decode the block
- **BREAKING** blockstream/v2 server now takes an array of blocksStores, to give the filesource as secondaryBlocksStores option
- Renamed `HeadBlockRefGetter` to `StreamHeadBlockRefGetter` and `NetworkHeadBlockRefGetter`. Choose what you need.
- Renamed `LIBBlockRefGetter` to `StreamLIBBlockRefGetter` and `NetworkLIBBlockRefGetter`. Choose what you need.
- Renamed `Tracker.ResolveRelativeBlock` to `Tracker.GetRelativeBlock`, to avoid confusion with the `AddResolver` function, which has nothing to do with `GetRelativeBlock` (which uses `Get()` and the Getters only).
- Greatly improve logging behavior of the various source implementations, this should greatly improved debuggability of the library.
- **BREAKING** All `Source` must now implement a `SetLogger(logger *zap.Logger)` method.
- **BREAKING** Removed all `Name`, `SetName`, and `*Name` options on all source and across `bstream`. Replaced by a proper `*zap.Logger`
               instance instead. Re-configure using the logger, you can use `SetLogger(zlog.With("name", "my-source-name"))` to emulate
               the old behavior.

## [v0.0.1] - 2020-06-22

### Added
- StartBlockResolver: interface for quickly finding out from which block to start (to cover all possible forks before your required start block)
- ParallelStartResolver: implementation of StartBlockResolver to interrogate multiple StartBlockResolvers at once, useful when all dfuse components are not "up" yet.
- SetHeadInfo on BlockStream to allow using GetHeadInfo() before stream actually starts
- WithName option on NewForkable for better logging
- License changed to Apache 2.0

### Changed
- BREAKING CHANGE: `JoiningSourceRateLimit` now takes a `time.Duration` as a second argument, instead of an `int` amount of milliseconds.


## (before dfuse-for-EOSIO)

### Changed
- `forkable.NewWithLIB(...)` replaced by `forkable.New(forkable.WithExclusiveLIB(...))`
- `Forkable.SetFilters()` replaced by the `forkable.WithFilters()` _Option_, to bassed to `New()`.
- `Forkable.EnsureBlockFlows()` replaced by the `forkable.EnsureBlockFlows()` _Option_.
- `Forkable.EnsureAllBlocksTriggerLongestChain()` replaced by the `forkable.EnsureAllBlocksTriggerLongestChain()` _Option_.

### Added
- `forkable.WithInclusiveLIB()` as an _Option_ to `forkable.New()`
