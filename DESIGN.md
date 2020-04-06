
Eternal sources:
* bigtable-loader?
* search
* flux
* eosws
* relayer



# package bstream.store

ArchiveStore
* manages files
* Interface
  * OpenFile()
  * Exists()
  * etc...


# package bstream

Subscriber
* Pulls blocks from somewhere feeding a live stream of blocks
* Interface:
  * Read()
  * Shutdown()
* Implementations:
  * *hub.Subscriber
  * *websocket.Subscriber
  * *ztest.Subscriber ?

Pipeliner  (Afterburner et Joiner)
* Inputs:
  *
* can consume:
  * Block Logs
  * Subscriber (real-time feed)
* acts in two fashions: fetching batch files, and streaming from real-time feed
* roll in Afterburner et Joiner
* manage le reading à partir des Archive files
* subscribe à un publisher de blocks live
* Interface:
  * RegisterPipeline()
  * Run()

interface Pipeline
interface PipelinePreprocessor
interface PipelineStateFlusher

# package bstream.websocket

WebsocketSubscriber -> NewSubscriber()
WebsocketServer -> NewServer()


# package bstream.hub ?

SubscriptionHub
* Inputs:
  * Subscriber
* Interface:
  * NewSubscriber() -> *HubSubscriber (was inprocSubscriber)
* reads input SubscriberActs as a transit between a remote live blocks endpoint and local subscribers
* Uses a Streamer internally, and feeds back to child subscribers

HubSubscriber

# package bstream.middlewares

type IrreversiblePipeline



------------


Index
- lockMap map[srcPtr]struct{}

Hub:
- []subscribers

subscriber:
- OnTerminating() {
  IterateObjects
    Make sure `srcPtr` isn't referenced in each Index
  // write somehwere that `srcPtr` is DONE
}



// On Iterate all objects:
  // Loop les indexes, loop les markeurs de terminations.
