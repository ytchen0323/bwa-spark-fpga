package cs.ucla.edu.bwaspark.util

import scala.util.control.Breaks._

import cs.ucla.edu.bwaspark.datatype.BNTSeqType

object BNTSeqUtil {
  /**
    *  Retrieve the reference sequence
    *  This private function is used by memChainToAln()
    *  scala: l_pac, pac, rmax[0], rmax[1], return &rlen, return rseq
    *  c: l_pac, pac, beg, end, len, return rseq
    *
    *  @param pacLen the length of the PAC array
    *  @param pac the PAC array
    *  @param beg the reference begin
    *  @param end the reference end
    */
  def bnsGetSeq(pacLen: Long, pac: Array[Byte], beg: Long, end: Long) : (Array[Byte], Long) = {
    var endVar: Long = 0//for swapping
    var begVar: Long = 0//for swapping
    if(end < beg) {//if end is smaller, swap
      endVar = beg
      begVar = end
    }
    else {//else keep the value
      endVar = end
      begVar = beg
    }
    if(endVar > (pacLen<<1)) endVar = pacLen<<1
    if(begVar < 0) begVar = 0
    var rLen: Long = endVar - begVar// for return rlen
    var seq: Array[Byte] = new Array[Byte](rLen.toInt)//for return seq

    if(begVar >= pacLen || endVar <= pacLen) {
      var k: Long = 0
      var l: Int = 0
      if( begVar >= pacLen ) {//reverse strand
        var begF: Long = (pacLen<<1) - 1 - endVar
        var endF: Long = (pacLen<<1) - 1 - begVar
        k = endF
        while(k >= (begF + 1)) {
          seq(l) = (3 - getPac(pac, k)).toByte
          l += 1
          k -= 1
        }
      }
      else {
        k = begVar
        while(k < endVar) {
          seq(l) = getPac(pac, k).toByte
          k += 1
          l += 1
        }
      }
    }
    else
      rLen = 0

    (seq, rLen)//return a Tuple
  }

  /**
    * Realize: #define _get_pac(pac, l) ((pac)[(l)>>2]>>((~(l)&3)<<1)&3)
    * Used by bnsGetSeq()
    *
    * @param pac PAC array
    * @param l
    */
  private def getPac(pac: Array[Byte], l: Long) : Long = {
    var pacValue: Long = ( pac((l>>>2).toInt) >>> (((~l)&3) <<1) ) & 3
    pacValue
  }

  
  /**
    *  bnsDepos:
    *  
    *  @param bns the bns object
    *  @param pos the position in the reference
    */
  def bnsDepos(bns: BNTSeqType, pos: Long): (Long, Int) = {
    var isRev = 0
    if(pos >= bns.l_pac) isRev = 1
    else isRev = 0

    if(isRev == 1) ((bns.l_pac << 1) - 1 - pos, 1)
    else (pos, 0)
  }

  
  /**
    *  bnsPosToRid
    *  
    *  @param bns the bns object
    *  @param posF
    */
  def bnsPosToRid(bns: BNTSeqType, posF: Long): Int = {
    if(posF >= bns.l_pac) -1
    else {
      var left = 0
      var mid = 0
      var right = bns.n_seqs
      
      // binary search
      var isBreak = false
      while(left < right && !isBreak) {
        mid = (left + right) >> 1
       
        if(posF >= bns.anns(mid).offset) {
          if(mid == bns.n_seqs - 1) isBreak = true
          if(!isBreak && posF < bns.anns(mid + 1).offset) isBreak = true
          left = mid + 1
        }
        else
          right = mid
      }

      mid
    }
  }

}

