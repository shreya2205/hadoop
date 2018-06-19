package org.apache.hadoop.io.erasurecode.coder;

import org.apache.hadoop.io.erasurecode.coder.ClayCodeErasureDecodingStep;

import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotEquals;

public class TestClayCodeDecodingStep {

  private int[] erasedIndexes;
  private int numDataUnits, numParityUnits;
  private ClayCodeErasureDecodingStep.ClayCodeUtil util;


  @org.junit.Test
  public void testGetZ() {

    erasedIndexes = new int[]{1,3,4};
    numDataUnits = 6;
    numParityUnits = 3;

    util = new ClayCodeErasureDecodingStep.ClayCodeUtil(erasedIndexes,numDataUnits,numParityUnits);

    assertEquals(util.getZ(new int[]{0,2,1}) ,  7);
    assertEquals(util.getZ(new int[]{1,2,2}) , 17);
  }

  @Test
  public void testGetZVector(){

    erasedIndexes = new int[]{1,3,4};
    numDataUnits = 6;
    numParityUnits = 3;

    util = new ClayCodeErasureDecodingStep.ClayCodeUtil(erasedIndexes,numDataUnits,numParityUnits);

    assertArrayEquals(util.getZVector(25), new int[]{2,2,1});
    assertArrayEquals(util.getZVector(8), new int[]{0,2,2});

    erasedIndexes = new int[]{5,9,10};
    numDataUnits = 16;
    numParityUnits = 4;

    util = new ClayCodeErasureDecodingStep.ClayCodeUtil(erasedIndexes,numDataUnits,numParityUnits);

    assertArrayEquals(util.getZVector(1012), new int[]{3,3,3,1,0});
    assertArrayEquals(util.getZVector(786), new int[]{3,0,1,0,2});

  }


  @Test
  public void testGetErasureType(){

    erasedIndexes = new int[]{1,3,4};
    numDataUnits = 6;
    numParityUnits = 3;

    util = new ClayCodeErasureDecodingStep.ClayCodeUtil(erasedIndexes,numDataUnits,numParityUnits);

    assertEquals(util.getErasureType(1, new int[]{0,1,1}), 1);
    assertEquals(util.getErasureType(4, new int[]{0,1,1}), 0);
    assertEquals(util.getErasureType(3, new int[]{0,1,1}), 2);

    erasedIndexes = new int[]{5,9,10};
    numDataUnits = 16;
    numParityUnits = 4;

    util = new ClayCodeErasureDecodingStep.ClayCodeUtil(erasedIndexes,numDataUnits,numParityUnits);

    assertEquals(util.getErasureType(1, new int[]{2,1,1,1,0}), 1);

  }

  @After
  public void cleanUp(){
    erasedIndexes = null;
    util = null;
  }

  @org.junit.Test
  public void testGetIntersectionScore() {

    erasedIndexes = new int[]{1,3,4};
    numDataUnits = 6;
    numParityUnits = 3;

    util = new ClayCodeErasureDecodingStep.ClayCodeUtil(erasedIndexes,numDataUnits,numParityUnits);

    assertEquals(util.getIntersectionScore(new int[]{0,2,1}),0);
    assertEquals(util.getIntersectionScore(new int[]{1,2,2}),1);
    assertEquals(util.getIntersectionScore(new int[]{1,1,1}),2);
  }

  @Test
  public void testGetNodeCoordinates() {

    erasedIndexes = new int[]{1,3,4};
    numDataUnits = 6;
    numParityUnits = 3;

    util = new ClayCodeErasureDecodingStep.ClayCodeUtil(erasedIndexes,numDataUnits,numParityUnits);

    assertArrayEquals(util.getNodeCoordinates(1), new int[]{1,0});
    assertArrayEquals(util.getNodeCoordinates(5), new int[]{2,1});
  }

  @Test
  public void testGetAllIntersectionScore() {
    erasedIndexes = new int[]{2,5};
    numDataUnits = 4;
    numParityUnits = 2;

    util = new ClayCodeErasureDecodingStep.ClayCodeUtil(erasedIndexes,numDataUnits,numParityUnits);

    assertEquals(util.getAllIntersectionScore().get(0),new ArrayList<>(Arrays.asList(2,6)));
    assertEquals(util.getAllIntersectionScore().get(1),new ArrayList<>(Arrays.asList(0,3,4,7)));

    erasedIndexes = new int[]{1};
    numDataUnits = 16;
    numParityUnits = 4;

    util = new ClayCodeErasureDecodingStep.ClayCodeUtil(erasedIndexes,numDataUnits,numParityUnits);
    assertEquals(util.getAllIntersectionScore().get(1).size(),256);

  }

}
