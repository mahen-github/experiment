package dev.mahendran.templates

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapred.{JobConf, RecordWriter}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.util.Progressable

class MultipleOutputFormatWriter extends MultipleTextOutputFormat[Any, Any] {

  override def generateActualKey(key: Any, value: Any): Any = {
    var result: Any = null
    key match {
      case e: String => result = key
      case _ =>
    }
    result
  }


  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    key match {
      case s:String => return s + "/" + name
      case t:Text => return t + "/" + name
      case _ => return "default" + "/" + name
    }
  }


  override def getBaseRecordWriter(fs: FileSystem, job: JobConf, name: String, arg3: Progressable): RecordWriter[Any, Any] = {
    var folderName = name
    val subDirName = job.getStrings("mapreduce.output.basename")
    if (subDirName.length == 1) {
      val path = new Path(name)
      folderName = path.getParent() + "/" + subDirName.head + "/" + path.getName
    }
    super.getBaseRecordWriter(fs, job, folderName, arg3)
  }
}
