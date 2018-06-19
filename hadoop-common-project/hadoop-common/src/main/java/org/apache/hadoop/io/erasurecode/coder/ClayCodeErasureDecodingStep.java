package org.apache.hadoop.io.erasurecode.coder;

import org.apache.hadoop.io.erasurecode.ECBlock;
import org.apache.hadoop.io.erasurecode.ECChunk;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class ClayCodeErasureDecodingStep implements ErasureCodingStep {

  private ECBlock[] inputBlocks;
  private ECBlock[] outputBlocks;
  private int[] erasedIndexes;
  private RawErasureDecoder pairWiseDecoder;
  private RawErasureDecoder rsRawDecoder;
  private final int SUB_PACKETIZATION;


  private final int q, t;


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

    this.q = rsRawDecoder.getNumParityUnits();
    this.t = (rsRawDecoder.getNumDataUnits() + rsRawDecoder.getNumParityUnits())/rsRawDecoder.getNumParityUnits();
  }

  /**
   * Get the index of the plane in integer from a base q notation.
   * @param z_vector plane index in vector form
   * @return z plane index
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
   * @param z plane index
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
   * @param z_vector plane in vector form
   * @return intersection score of the plane
   */
  public int getIntersectionScore(int[] z_vector) {}

  /**
   * @return map of intersection score and z
   * For each intersection score finds out all the planes whose intersection score = z.
   */
  public Map<Integer, int[]> getAllIntersectionScore() {}

  /**
   * @param x x coordinate of the node in plane
   * @param y y coordinate of the node in plane
   * @return x+y*q
   */
  public int getIndexInPlane(int x, int y) {
    return (x + q*y);
  }



  /**
   * The following are the error types
   *       -------*----
   *      |   |   |   |
   *      --- *-- O---
   *     |   |   |   |
   *     + ----------
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
   * @param z_vector index of the plane in vector form, i.e. base q notation
   * @return return the error type for a node in a plane.
   * Erasure types possible are : {0,1,2}
   */
  public int getErasureType(int indexInPlane, int[] z_vector) {
    int[] nodeCoordinates = getNodeCoordinates();

    // there is a hole-dot pair at the given index => type 0
    if(z_vector[nodeCoordinates[1]] == nodeCoordinates[0])
        return 0;

    int dotInColumn = getNodeIndex(z_vector[nodeCoordinates[1]], nodeCoordinates[1]);

    // there is a hole dot pair in the same column => type 2
    for(int i=0; i<this.erasedIndexes.length; ++i){
      if(this.erasedIndexes[i] == dotInColumn)
        return 2;
    }

    return 1;

  }

  /**
   * Convert all the input symbols of the given plane into its decoupled form. We use the rsRawDecoder to achieve this.
   * @param z_vector plane index in vector form
   * @param temp temporary array which stores decoupled values
   * @return decoupled values for all non-null nodes
   */
  public ByteBuffer[] getDecoupledPlane(int z_vector, ByteBuffer[] temp) {}


  /**
   * Get the pairwise couples of the given inputs. Use the pairWiseDecoder to do this.
   * Notationally inputs are (A,A',B,B') and the outputs array contain the unknown values of the inputs
   * @param inputs pairwise known couples
   * @param outputs pairwise couples of the known values
   */
  public void getPairWiseCouple(ByteBuffer[] inputs, ByteBuffer[] outputs) {}

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
