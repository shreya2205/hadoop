package org.apache.hadoop.io.erasurecode.coder;

import org.apache.hadoop.io.erasurecode.*;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;

public class ClayCodeErasureEncoder extends ErasureEncoder{

    private RawErasureDecoder pairWiseDecoder;
    private RawErasureDecoder rsRawDecoder;
    private final int SUB_PACKETIZATION;

    public ClayCodeErasureEncoder(ErasureCoderOptions options) {
        super(options);
        int exp = (getNumDataUnits() + getNumParityUnits()) / getNumParityUnits();
        this.SUB_PACKETIZATION = (int) Math.pow(getNumParityUnits(), exp);
    }


    @Override
    protected ErasureCodingStep prepareEncodingStep(ECBlockGroup blockGroup) {

        RawErasureDecoder rsRawDecoder = checkCreateRSRawDecoder();
        RawErasureDecoder pairWiseDecoder = checkCreatePairWiseDecoder();

        ECBlock[] inputs = getInputBlocks(blockGroup);

        int[] erasedIndexes = new int[getNumParityUnits()];

        // Assume the m parity nodes are erased
        for(int i=0; i<getNumParityUnits(); ++i){
            erasedIndexes[i] = getNumDataUnits() + i ;
        }

        return new ClayCodeErasureDecodingStep(inputs, erasedIndexes, getOutputBlocks(blockGroup),
                pairWiseDecoder, rsRawDecoder, SUB_PACKETIZATION);
    }


    private RawErasureDecoder checkCreatePairWiseDecoder() {
        ErasureCoderOptions pairWiseTransformOptions = new ErasureCoderOptions(2,2);

        if (pairWiseDecoder == null) {
            pairWiseDecoder = CodecUtil.createRawDecoder(getConf(),
                    ErasureCodeConstants.RS_CODEC_NAME, pairWiseTransformOptions);
        }
        return pairWiseDecoder;
    }

    private RawErasureDecoder checkCreateRSRawDecoder() {

        if (rsRawDecoder == null) {
            rsRawDecoder = CodecUtil.createRawDecoder(getConf(),
                    ErasureCodeConstants.RS_CODEC_NAME, getOptions());
        }
        return rsRawDecoder;
    }

    @Override
    public void release() {
        super.release();
        if(rsRawDecoder!=null)
            rsRawDecoder.release();
        if(pairWiseDecoder!=null)
            pairWiseDecoder.release();
    }
}
