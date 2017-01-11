package com.datio.streaming.Structure

import spray.json
import spray.json.{DefaultJsonProtocol, JsValue, RootJsonFormat}


case class Payload(id: Double, name: String, surname: String, age: Int)


object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val payloadFormat = jsonFormat4(Payload)

  implicit object eventFormat extends RootJsonFormat[Event] {
    override def read(json: JsValue): Event = {
      val payload = json.asJsObject.getFields("payload").head.convertTo[Payload]
      Event(payload)
    }

    override def write(obj: Event): JsValue = ???
  }
}