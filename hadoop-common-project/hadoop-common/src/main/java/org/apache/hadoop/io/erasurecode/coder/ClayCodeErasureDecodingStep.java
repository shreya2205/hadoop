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

  }

  /**
   * @param z_vector plane index in vector form
   * @return z plane index in integer form
   */
  public int getZ(int[] z_vector) {}

  /**
   * @param z plane index in integer form
   * @return plane index in vector form
   */
  public int[] getZVector(int z) {}

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
  public int getIndexInPlane(int x, int y) {}


  /**
   * @param indexInPlane index of the node in the plane (x+y*q)
   * @param z_vector plane index in vector form
   * @return return the error type for a node in a plane.
   * Error types possible are : {0,1,2}
   */
  public int getErrorType(int indexInPlane, int[] z_vector) {}

  /**
   * @param z_vector plane index in vector form
   * @param temp temporary array which stores decoupled values
   * @return decoupled values for all non-null nodes
   */
  public ByteBuffer[] getDecoupledPlane(int z_vector, ByteBuffer[] temp) {}


  /**
   * @param inputs
   * @param outputs
   * @return
   */
  public int[] getPairWiseCouple(ByteBuffer[] inputs, ByteBuffer[] outputs) {}

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
