package spark

import play.api.libs.json.Json

case class KeyValue(key: String, value: Int)

object KeyValue {
  implicit val nameFormat = Json.format[KeyValue]
}