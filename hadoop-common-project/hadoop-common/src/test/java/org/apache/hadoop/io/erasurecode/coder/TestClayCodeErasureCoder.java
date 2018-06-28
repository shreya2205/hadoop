package org.apache.hadoop.io.erasurecode.coder;

import org.junit.Test;
import org.junit.Before;

public class TestClayCodeErasureCoder extends TestVectorErasureCoderBase{

  @Before
  public void setup(){
    this.encoderClass = ClayCodeErasureEncoder.class;
    this.decoderClass = ClayCodeErasureDecoder.class;

  }

  @Test
  public void testCodingNoDirectBuffer_4x2_erasing_d1() {
    prepare(null, 4, 2, new int[]{1}, new int[0]);
    /**
     * Doing twice to test if the coders can be repeatedly reused. This matters
     * as the underlying coding buffers are shared, which may have bugs.
     */
    this.numChunksInBlock = 16;
    this.subPacketSize = 8;
    testCoding(false);
    testCoding(false);
  }


  @Test
  public void testCodingNoDirectBuffer_4x2_erasing_d0_d1() {
    prepare(null, 4, 2, new int[]{0,1}, new int[0]);
    /**
     * Doing twice to test if the coders can be repeatedly reused. This matters
     * as the underlying coding buffers are shared, which may have bugs.
     */
    this.numChunksInBlock = 16;
    this.subPacketSize = 8;
    testCoding(false);
    testCoding(false);
  }

  @Test
  public void testCodingNoDirectBuffer_4x2_erasing_d0_p1() {
    prepare(null, 4, 2, new int[]{0}, new int[]{0});
    /**
     * Doing twice to test if the coders can be repeatedly reused. This matters
     * as the underlying coding buffers are shared, which may have bugs.
     */
    this.numChunksInBlock = 16;
    this.subPacketSize = 8;
    testCoding(false);
    testCoding(false);
  }

  @Test
  public void testCodingNoDirectBuffer_6x3_erasing_d1_d3() {
    prepare(null, 6, 3, new int[]{1,3}, new int[0]);
    /**
     * Doing twice to test if the coders can be repeatedly reused. This matters
     * as the underlying coding buffers are shared, which may have bugs.
     */
    this.numChunksInBlock = 27;
    this.subPacketSize = 27;
    testCoding(false);
    testCoding(false);
  }

  @Test
  public void testCodingNoDirectBuffer_8x4_erasing_d1_d5_p2() {
    prepare(null, 8, 4, new int[]{1,5}, new int[]{2});
    /**
     * Doing twice to test if the coders can be repeatedly reused. This matters
     * as the underlying coding buffers are shared, which may have bugs.
     */
    this.numChunksInBlock = 64;
    this.subPacketSize = 64;
    testCoding(false);
    testCoding(false);
  }

  //@Test
  public void testCodingNoDirectBuffer_20x20_erasing_d1_d5_p2() {
    prepare(null, 20, 20, new int[]{1,5}, new int[]{2});
    /**
     * Doing twice to test if the coders can be repeatedly reused. This matters
     * as the underlying coding buffers are shared, which may have bugs.
     */
    this.numChunksInBlock = 400;
    this.subPacketSize = 400;
    testCoding(false);
    testCoding(false);
  }

//  @Test
  public void testCodingNoDirectBuffer_16x8_erasing_d1_d3_d5_d6_d12_p1_p2_p7() {
    prepare(null, 16, 8, new int[]{1,3,6,5,12}, new int[]{1,2,7});
    /**
     * Doing twice to test if the coders can be repeatedly reused. This matters
     * as the underlying coding buffers are shared, which may have bugs.
     */
    this.numChunksInBlock = 512;
    this.subPacketSize = 512;
    testCoding(false);
    //testCoding(false);
  }

  //@Test
  public void testCodingNoDirectBuffer_12x4_erasing_d1_p1_p2() {
    prepare(null, 12, 4, new int[]{1}, new int[]{1,2});
    /**
     * Doing twice to test if the coders can be repeatedly reused. This matters
     * as the underlying coding buffers are shared, which may have bugs.
     */
    this.numChunksInBlock = 512;
    this.subPacketSize = 256;
    testCoding(false);
    //testCoding(false);
  }

  @Test
  public void testCodingBothBuffers_6x3_erasing_d0_p0() {
    prepare(null, 6, 3, new int[] {0}, new int[] {0});

    /**
     * Doing in mixed buffer usage model to test if the coders can be repeatedly
     * reused with different buffer usage model. This matters as the underlying
     * coding buffers are shared, which may have bugs.
     */
    this.numChunksInBlock = 54;
    this.subPacketSize = 27;
    testCoding(true);
    testCoding(false);
    testCoding(true);
    testCoding(false);
  }

}
