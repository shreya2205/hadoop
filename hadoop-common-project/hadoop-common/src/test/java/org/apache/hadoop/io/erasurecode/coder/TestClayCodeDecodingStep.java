package org.apache.hadoop.io.erasurecode.coder;

import org.apache.hadoop.io.erasurecode.coder.ClayCodeErasureDecodingStep;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TestClayCodeDecodingStep {

  private int[] erasedIndexes;
  private int numDataUnits, numParityUnits;
  private ClayCodeErasureDecodingStep.ClayCodeUtil util;

  @Before
  public void setup(){

    erasedIndexes = new int[]{1,3,4};
    numDataUnits = 6;
    numParityUnits = 3;

    util = new ClayCodeErasureDecodingStep.ClayCodeUtil(erasedIndexes,numDataUnits,numParityUnits);

  }

  @org.junit.Test
  public void testGetZ() {

    assertEquals(util.getZ(new int[]{0,2,1}) ,  7);
    assertEquals(util.getZ(new int[]{1,2,2}) , 17);

  }

}
