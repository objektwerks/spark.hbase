package spark

import play.api.libs.json.Json

case class KeyValue(key: String, value: String)

object KeyValue {
  implicit val nameFormat = Json.format[KeyValue]
}