package geotrellis

/**
  * Created by marc on 24.11.17.
  */
object Weight extends Enumeration {
  type Weight = Value
  val One, Square, Big, High, Defined, Sigmoid = Value
}
