package com.qubole.spark.util

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.ObjectWritable
import org.apache.hadoop.io.Writable

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.Utils

@DeveloperApi
class SerializableWritable[T <: Writable](@transient var t: T) extends Serializable {

  def value: T = t

  override def toString: String = t.toString

  private def writeObject(out: ObjectOutputStream): Unit = Util.tryOrIOException {
    out.defaultWriteObject()
    new ObjectWritable(t).write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = Util.tryOrIOException {
    in.defaultReadObject()
    val ow = new ObjectWritable()
    ow.setConf(new Configuration(false))
    ow.readFields(in)
    t = ow.get().asInstanceOf[T]
  }
}
