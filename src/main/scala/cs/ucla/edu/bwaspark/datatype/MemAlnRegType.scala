package cs.ucla.edu.bwaspark.datatype

import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import scala.Serializable

class MemAlnRegType extends Serializable {
  var rBeg: Long = 0       // [rBeg,rEnd): reference sequence in the alignment
  var rEnd: Long = 0       
  var qBeg: Int = 0        // [qBeg,qEnd): query sequence in the alignment
  var qEnd: Int = 0
  var score: Int = 0       // best local SW score
  var trueScore: Int = 0   // actual score corresponding to the aligned region; possibly smaller than $score
  var sub: Int = 0         // 2nd best SW score
  var csub: Int = 0        // SW score of a tandem hit
  var subNum: Int = 0      // approximate number of suboptimal hits
  var width: Int = 0       // actual band width used in extension
  var seedCov: Int = 0     // length of regions coverged by seeds
  var secondary: Int = 0   // index of the parent hit shadowing the current hit; <0 if primary
  var hash: Long = 0

  private def writeObject(out: ObjectOutputStream) {
    out.writeLong(rBeg)
    out.writeLong(rEnd)
    out.writeInt(qBeg)
    out.writeInt(qEnd)
    out.writeInt(score)
    out.writeInt(trueScore)
    out.writeInt(sub)
    out.writeInt(csub)
    out.writeInt(subNum)
    out.writeInt(width)
    out.writeInt(seedCov)
    out.writeInt(secondary)
    out.writeLong(hash)
  }

  private def readObject(in: ObjectInputStream) {
    rBeg = in.readLong
    rEnd = in.readLong
    qBeg = in.readInt
    qEnd = in.readInt
    score = in.readInt
    trueScore = in.readInt
    sub = in.readInt
    csub = in.readInt
    subNum = in.readInt
    width = in.readInt
    seedCov = in.readInt
    secondary = in.readInt
    hash = in.readLong
  }

  private def readObjectNoData() {

  }

}

