package org.apache.hadoop.io.erasurecode.coder;

import org.junit.Test;
import org.junit.Before;

public class TestClayCodeErasureCoder extends TestVectorErasureCoderBase{

  @Before
  public void setup(){
    this.encoderClass = ClayCodeErasureEncoder.class;
    this.decoderClass = ClayCodeErasureDecoder.class;
    this.numChunksInBlock = 16;
    this.subPacketSize = 8;
  }


  @Test
  public void testCodingNoDirectBuffer_4x2_erasing_d0() {
    prepare(null, 4, 2, new int[]{0}, new int[0]);
    /**
     * Doing twice to test if the coders can be repeatedly reused. This matters
     * as the underlying coding buffers are shared, which may have bugs.
     */
    testCoding(false);
    testCoding(false);
  }

}
