package org.apache.hadoop.io.erasurecode.coder;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.io.erasurecode.ECBlock;
import org.apache.hadoop.io.erasurecode.ECChunk;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.util.*;

public class ClayCodeErasureDecodingStep implements ErasureCodingStep {

  private ECBlock[] inputBlocks;
  private ECBlock[] outputBlocks;
  private int[] erasedIndexes;
  private RawErasureDecoder pairWiseDecoder;
  private RawErasureDecoder rsRawDecoder;
  private final int SUB_PACKETIZATION;


  private ClayCodeUtil util;


  /**
   * Basic constructor with necessary info
   * @param inputs Blocks to encode
   * @param outputs Blocks to decode
   * @param pairWiseDecoder Decoder for the pair wise transforms
   * @param rsRawDecoder Decoder for each layer
   */
  public ClayCodeErasureDecodingStep(ECBlock[] inputs,
                                     int[] erasedIndexes,
                                     ECBlock[] outputs,
                                     RawErasureDecoder pairWiseDecoder,
                                     RawErasureDecoder rsRawDecoder,
                                     int SUB_PACKETIZATION) {
    this.inputBlocks = inputs;
    this.erasedIndexes = erasedIndexes;
    this.outputBlocks = outputs;
    this.rsRawDecoder = rsRawDecoder;
    this.pairWiseDecoder = pairWiseDecoder;
    this.SUB_PACKETIZATION = SUB_PACKETIZATION;

    this.util = new ClayCodeUtil(erasedIndexes, rsRawDecoder.getNumDataUnits(), rsRawDecoder.getNumParityUnits());
  }


  /**
   * Basic utilities for ClayCode encode/decode and repair operations.
   */
  public static class ClayCodeUtil{
    private int q,t;
    private int[] erasedIndexes;


    /**
     * Clay codes are parametrized via two parameters q and t such that numOfAllUnits = q*t and numOfParityUnits = q.
     * The constructor initialises these
     * @param erasedIndexes indexes of the erased nodes
     * @param numDataUnits
     * @param numParityUnits
     */
    ClayCodeUtil(int[] erasedIndexes, int numDataUnits, int numParityUnits){
      this.q = numParityUnits;
      this.t = (numParityUnits+numDataUnits)/numParityUnits;
      this.erasedIndexes = erasedIndexes;
    }

    /**
     * Get the index of the plane as an integer from a base q notation.
     * eg. for q=4, t=5 ; 25 = [0,0,1,2,1]
     * @param z_vector plane index in vector form
     * @return z plane index in integer form
     */
    public int getZ(int[] z_vector) {
      int z =0;
      int power = 1;

      for(int i=this.t-1; i>=0; --i){
        z +=z_vector[i]*power;
        power *= this.q;
      }
      return z;
    }

    /**
     * Get the base q notation of the plane
     * @param z plane index in integer form
     * @return plane index in vector form
     */
    public int[] getZVector(int z) {
      int[] z_vec = new int[this.t];

      for(int i=this.t-1; i>=0; --i){
        z_vec[i] = z % this.q;
        z/=this.q;
      }

      return z_vec;
    }

    /**
     * Intersection score of a plane is the number of hole-dot pairs in that plane.
     *
     *    ------- * --
     *    |   |   |   |
     *    --- *-- O---
     *    |   |   |   |
     *    + ----------
     *    |   |   |   |
     *    O --------- *
     *
     * + indicates both a dot and hole pair, * denotes a dot and a erasure(hole) O.
     * The above has q = 4 and t = 4.
     * So intersection of the above plane = 1.
     *
     * @param z_vector plane in vector form
     * @return intersection score of the plane
     *
     */

    public int getIntersectionScore(int[] z_vector) {
      int intersectionScore=0;
      for (int i =0; i < this.erasedIndexes.length ; ++i) {
        int index = this.erasedIndexes[i];
        int[] a = getNodeCoordinates(index);
        int x = a[0];
        int y = a[1];

        if (z_vector[y] == x) {
          intersectionScore = intersectionScore + 1;
        }
      }
      return intersectionScore;
    }

    /**
     * For each intersection score finds out indices of all the planes whose intersection score = i.
     * @return map of intersection scores and the corresponding z indexes
     */
    public Map<Integer, ArrayList<Integer>> getAllIntersectionScore() {
      Map<Integer,ArrayList<Integer>> hm = new HashMap<>();
      for (int i=0;i < (int) Math.pow(q,t) ; i=i+1) {
        int[] z_vector = getZVector(i);
        int intersectionScore = getIntersectionScore(z_vector);

        if (!hm.containsKey(intersectionScore)) {
          ArrayList<Integer> arraylist = new ArrayList<>();
          arraylist.add(i);
          hm.put(intersectionScore,arraylist);
        }
        else {
          hm.get(intersectionScore).add(i);
        }
      }
      return hm;
    }

    /**
     * @param x x coordinate of the node in plane
     * @param y y coordinate of the node in plane
     * @return x+y*q
     */

    public int getNodeIndex(int x, int y) {
      return x+q*y;
    }

    /**
     * @param index index of the node in integer
     * @return (x,y) coordinates of the node
     */
    public int[] getNodeCoordinates(int index) {
      int[] a = new int[2];
      a[0] = index%q;
      a[1] = index/q;
      return a;
    }


    /**
     * The following are the erasure types
     *    ------- * --
     *    |   |   |   |
     *    --- *-- O --
     *    |   |   |   |
     *    + ----------
     *    |   |   |   |
     *    O --------- *
     *
     *  + indicates both a dot * and a erasure(hole) O
     *  The above has q = 4 and t = 4.
     *  (2,0) is an erasure of type 0
     *  (3,0) is an erasure of type 2
     *  (1,2) is an erasure of type 1
     *
     * @param indexInPlane integer index of the erased node in the plane (x+y*q)
     * @param z index of the plane
     * @return return the error type for a node in a plane.
     * Erasure types possible are : {0,1,2}
     */
    public int getErasureType(int indexInPlane, int z) {

      int[] z_vector = getZVector(z);
      int[] nodeCoordinates = getNodeCoordinates(indexInPlane);

      // there is a hole-dot pair at the given index => type 0
      if(z_vector[nodeCoordinates[1]] == nodeCoordinates[0])
        return 0;

      int dotInColumn = getNodeIndex(z_vector[nodeCoordinates[1]], nodeCoordinates[1]);

      // there is a hole dot pair in the same column => type 2
      for(int i=0; i<this.erasedIndexes.length; ++i){
        if(this.erasedIndexes[i] == dotInColumn)
          return 2;
      }
      // there is not hole dot pair in the same column => type 1
      return 1;

    }

    /**
     * Get the index of the couple plane of the given coordinates
     * @param coordinates
     * @param z
     * @return
     */
    public int getCouplePlaneIndex(int[] coordinates, int z){
      int[] coupleZvec = getZVector(z);
      coupleZvec[coordinates[1]] = coordinates[0];
      return getZ(coupleZvec);
    }


    public static ByteBuffer allocateByteBuffer(boolean useDirectBuffer,
                                                int bufSize) {
      if (useDirectBuffer) {
        return ByteBuffer.allocateDirect(bufSize);
      } else {
        return ByteBuffer.allocate(bufSize);
      }
    }

    /**
     * Find the valid input from all the inputs.
     * @param inputs input buffers to look for valid input
     * @return the first valid input
     */
    public static <T> T findFirstValidInput(T[] inputs) {
      for (T input : inputs) {
        if (input != null) {
          return input;
        }
      }

      throw new HadoopIllegalArgumentException(
        "Invalid inputs are found, all being null");
    }

  }


  /**
   * Convert all the input symbols of the given plane into its decoupled form. We use the rsRawDecoder to achieve this.
   * @param z plane index
   * @param temp temporary array which stores decoupled values
   * @return decoupled values for all non-null nodes
   */
  public void getDecoupledPlane(ByteBuffer[][] inputs, ByteBuffer[] temp, int z)
    throws IOException {
    int[] z_vec = util.getZVector(z);

    ByteBuffer firstValidInput = ClayCodeUtil.findFirstValidInput(inputs[z]);
    final int bufSize = firstValidInput.remaining();
    final boolean isDirect = firstValidInput.isDirect();


    ByteBuffer[] tmpOutputs = new ByteBuffer[2];

    for(int idx=0; idx<2; ++idx)
      tmpOutputs[idx] = ClayCodeUtil.allocateByteBuffer(isDirect,bufSize);

    for(int i=0; i<util.q*util.t; i++){
      int[] coordinates = util.getNodeCoordinates(i);

      if(inputs[z][i] != null) {
        // If the coordinates correspond to a dot in the plane
        if (z_vec[coordinates[1]] == coordinates[0])
          temp[i] = inputs[z][i];
        else {

          int coupleZIndex = util.getCouplePlaneIndex(coordinates,z);
          int coupleCoordinates = util.getNodeIndex(z_vec[coordinates[1]], coordinates[1]);

          getPairWiseCouple(new ByteBuffer[]{inputs[z][i], inputs[coupleZIndex][coupleCoordinates], null, null}, tmpOutputs);

          temp[i] = tmpOutputs[0];

          // clear the buffers for reuse
          for(int idx=0; idx<2; ++idx)
            tmpOutputs[idx].clear();
        }
      }
      else{
        temp[i] = null;
      }

    }

  }

  public void decodeDecoupledPlane(ByteBuffer[] decoupledPlane)
    throws IOException {

    ByteBuffer firstValidInput = ClayCodeUtil.findFirstValidInput(decoupledPlane);
    final int bufSize = firstValidInput.remaining();
    final boolean isDirect = firstValidInput.isDirect();


    ByteBuffer[] tmpOutputs = new ByteBuffer[erasedIndexes.length];

    for(int idx=0; idx<erasedIndexes.length; ++idx)
      tmpOutputs[idx] = ClayCodeUtil.allocateByteBuffer(isDirect,bufSize);

    rsRawDecoder.decode(decoupledPlane, erasedIndexes,tmpOutputs);

    int k=0;
    for(int i=0; i<decoupledPlane.length; ++i){
      if(decoupledPlane[i] == null)
        decoupledPlane[i] = tmpOutputs[k++];
    }

  }


  public void doDecodeMulti(ByteBuffer[][] inputs, int[] erasedIndexes, ByteBuffer[][] outputs)
    throws IOException {

    Map<Integer, ArrayList<Integer>> ISMap = util.getAllIntersectionScore();
    int maxIS = Collections.max(ISMap.keySet());


    // i is the current IS
    for (int i = 0; i <= maxIS; ++i) {

      ArrayList<Integer> realZIndexes = ISMap.get(i);

      // Given the IS, for each zIndex in ISMap.get(i), temp stores decoupled plane in the same order.
      ByteBuffer[][] temp = new ByteBuffer[realZIndexes.size()][inputs[0].length];
      int idx = 0;

      for (int z : realZIndexes) {

        getDecoupledPlane(inputs, temp[idx], z);
        decodeDecoupledPlane(temp[idx]);
        idx++;

      }


      // allocate temp buffers for the pairWiseDecoding to find lost A
      ByteBuffer firstValidInput = ClayCodeUtil.findFirstValidInput(temp[0]);
      final int bufSize = firstValidInput.remaining();
      final boolean isDirect = firstValidInput.isDirect();

      // temporary buffers for pairwise decoding
      ByteBuffer[] tmpOutputs = new ByteBuffer[2];

      for (int p = 0; p < 2; ++p)
        tmpOutputs[p] = ClayCodeUtil.allocateByteBuffer(isDirect, bufSize);


      for (int j = 0; j < temp.length; ++j) {

        for (int k = 0; k < erasedIndexes.length; ++k) {

          int erasureType = util.getErasureType(k, realZIndexes.get(j));

          if (erasureType == 0) {
            inputs[realZIndexes.get(j)][k] = temp[j][k];
          }
          else if (erasureType == 1) {
            int[] z_vector = util.getZVector(realZIndexes.get(j));
            int[] coordinates = util.getNodeCoordinates(k);
            int couplePlaneIndex = util.getCouplePlaneIndex(coordinates,realZIndexes.get(j));
            int coupleIndex = util.getNodeIndex(z_vector[coordinates[1]],coordinates[1]);

            getPairWiseCouple(new ByteBuffer[]{null,inputs[couplePlaneIndex][coupleIndex], temp[j][k],null},tmpOutputs);

            inputs[realZIndexes.get(j)][k] = tmpOutputs[0];

          } else {

            int[] z_vector = util.getZVector(realZIndexes.get(j));
            int[] coordinates = util.getNodeCoordinates(k);
            int couplePlaneIndex = util.getCouplePlaneIndex(coordinates,realZIndexes.get(j));
            int coupleIndex = util.getNodeIndex(z_vector[coordinates[1]],coordinates[1]);

            int tempCoupleIndex = realZIndexes.indexOf(couplePlaneIndex);

            getPairWiseCouple(new ByteBuffer[]{null, null, temp[j][k], temp[tempCoupleIndex][coupleIndex]}, tmpOutputs);

            inputs[realZIndexes.get(j)][k] = tmpOutputs[0];

          }
        }

        for(int p=0; p<2; ++p)
          tmpOutputs[p].clear();

      }
    }

    fillOutputs(inputs,outputs);

  }


  public void fillOutputs(ByteBuffer[][] inputs, ByteBuffer[][] outputs){
    for (int i=0;i< (int) Math.pow(util.q,util.t);i++){
      for (int j=0; j < erasedIndexes.length ; j++){
        outputs[i][j] = inputs[i][erasedIndexes[j]];
      }
    }
  }


  /**
   * Get the pairwise couples of the given inputs. Use the pairWiseDecoder to do this.
   * Notationally inputs are (A,A',B,B') and the outputs array contain the unknown values of the inputs
   * @param inputs pairwise known couples
   * @param outputs pairwise couples of the known values
   */
  public void getPairWiseCouple(ByteBuffer[] inputs, ByteBuffer[] outputs)
    throws IOException {
    int[] lostCouples = new int[2];

    int k=0;
    for(int i=0; i<inputs.length; ++i){
      if(inputs[i]==null)
        lostCouples[k++] = i;
    }

    pairWiseDecoder.decode(inputs, lostCouples, outputs);

  }

  @Override
  public ECBlock[] getInputBlocks() {
    return inputBlocks;
  }

  @Override
  public ECBlock[] getOutputBlocks() {
    return outputBlocks;
  }

  @Override
  public void performCoding(ECChunk[] inputChunks, ECChunk[] outputChunks) throws IOException {

  }

  @Override
  public void finish() {

  }
}
