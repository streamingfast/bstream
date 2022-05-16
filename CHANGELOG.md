# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Breaking changes

* ChainConfig is now required as an input, and we've killed all global variables.
* All dbin-serialized `bstream.Block` (all 100-merged blocks files) have changed format at two levels:
  * `dbin` now does not have a special string (using latest dbin), but rather uses the `proto.Message` type to identify the contents.
  * `bstream.Block` now doesn't rely on `PayloadKind`, `PayloadVersion` and `PayloadBuffer` anymore, but rather `Payload` that is now a `google.protobuf.any.Any` object, and where the `type_url` determines what's inside.
  * This means that there aren't chain-specific things in the `bstream.Block` anymore (like there was with Protocol as an ENUM).
  * This also means that for substreams, we can reuse the `bstream.Block` as an envelope of any protobuf types, reusing the caching layers, merger, etc..
  * This is mostly abstracted from using of the library, provided they hook into the new `ChainConfig` features (they'll have no choice :).
* bstream.Block has been reworked
  *
* MemoryBlockSetters have changed, not used anymore.. boy boy we'll need to list a few things here.


## [v0.0.2] - 2022-05-05

### Added
- Added FileSourceWithSecondaryBlocksStores Option to allow a fallback location
- `.SetNearBlocksCount(count)` and `.Clone()` on `Tracker` object.
- `Tracker` object to streamline queries about different targets (like network head, database lib, relayer blockstream head, whatever other BlockRef tags), ask the question about them being near one another (to select between live mode or catch-up mode).  Also streamlines the requests of a start block, with a bunch of different backend implementations that can answer to the questions regarding where to start.
- `JoiningSourceWithTracker` to avoid joining to live when live and file sources are very far apart.
- `HeadBlockRefGetter` and `LIBBlockRefGetter` that targets a `HeadInfo` service, and satisfies the `Tracker` _BlockRefGetter_ func signature.

### Changed

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
