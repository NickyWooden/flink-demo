package com.ly.flink.consts

object CaseClass {
  case class Phone(num: String,time: String,longitude: Float,latitude: Float)
  case class CrimeDoc(id: Int,idCard: String,name: String,num: String,detail: String)
  case class Warn(num: String,name: String,warnTime: String,detail: String)
}
