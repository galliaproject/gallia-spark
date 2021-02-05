package gallia.data.multiple

// ===========================================================================
package object streamer {
  type JoinType = gallia.heads.merging.MergingData.JoinType
  val  JoinType = gallia.heads.merging.MergingData.JoinType

  // ---------------------------------------------------------------------------
  private[streamer] val _utils = gallia.data.multiple.streamer.RddStreamerUtils
}

// ===========================================================================
