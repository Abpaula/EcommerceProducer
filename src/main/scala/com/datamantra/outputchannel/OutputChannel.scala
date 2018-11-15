package com.datamantra.outputchannel

import com.datamantra.model.ApacheAccessLogCombined

/**
 * Created by kafka on 12/11/18.
 */
abstract class OutputChannel {

  def process(apacheLogEvent: Object)
}
