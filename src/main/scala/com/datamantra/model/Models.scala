package com.datamantra.model

/**
 * Created by kafka on 11/11/18.
 */
case class ApacheAccessLogCombined(ipAddress: String, userId: String,
                                   clientId: String, dateTime: String,
                                   requestURI: String, responseCode: Int, contentSize: Long,
                                   referrer:String, useragent:String)

case class ApacheAccessLog(ipAddress: String, clientId: String,
                           userId: String, dateTime: String, method: String,
                           requestURI: String, protocol: String,
                           responseCode: Int, contentSize: Long)