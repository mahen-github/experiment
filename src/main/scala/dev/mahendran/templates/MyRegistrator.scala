package dev.mahendran.templates

import com.esotericsoftware.kryo.Kryo
import org.apache.hadoop.io.Text
import org.apache.spark.serializer.KryoRegistrator

class MyRegistrator extends KryoRegistrator {
    override def registerClasses(kryo: Kryo) {
        kryo.register(classOf[Text])
    }
}
