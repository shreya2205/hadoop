package org.apache.hadoop.io.erasurecode.coder;

import org.apache.hadoop.io.erasurecode.ECBlock;
import org.apache.hadoop.io.erasurecode.ECChunk;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;

import java.io.IOException;

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
