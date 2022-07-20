package gallia

// ===========================================================================
package object streamer {
  type JoinType = gallia.heads.merging.MergingData.JoinType
  val  JoinType = gallia.heads.merging.MergingData.JoinType

  // ---------------------------------------------------------------------------
  private[streamer] val _utils = gallia.streamer.RddStreamerUtils
}

// ===========================================================================
