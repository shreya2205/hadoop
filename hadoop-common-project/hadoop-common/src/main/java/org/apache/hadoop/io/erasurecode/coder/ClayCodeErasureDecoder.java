package org.apache.hadoop.io.erasurecode.coder;

import org.apache.hadoop.io.erasurecode.*;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;


import java.time.chrono.Era;

public class ClayCodeErasureDecoder extends ErasureDecoder {

    private RawErasureDecoder pairWiseDecoder;
    private RawErasureDecoder rsRawDecoder;
    private final int SUB_PACKETIZATION;

    public ClayCodeErasureDecoder(ErasureCoderOptions options) {
        super(options);
        int exp = (int) Math.ceil((getNumDataUnits() + getNumParityUnits()) / getNumParityUnits());
        this.SUB_PACKETIZATION = (int) Math.pow(getNumParityUnits(), exp);
    }

    @Override
    protected ErasureCodingStep prepareDecodingStep(final ECBlockGroup blockGroup) {
        RawErasureDecoder rsRawDecoder;
        RawErasureDecoder pairWiseDecoder;

        ECBlock[] inputBlocks = getInputBlocks(blockGroup);
        ECBlock[] outputBlocks = getOutputBlocks(blockGroup);

        rsRawDecoder = checkCreateRSRawDecoder();
        pairWiseDecoder = checkCreatePairWiseDecoder();

        return new ClayCodeErasureDecodingStep(inputBlocks,getErasedIndexes(inputBlocks),outputBlocks,pairWiseDecoder,rsRawDecoder,SUB_PACKETIZATION);
    }

    private RawErasureDecoder checkCreateRSRawDecoder() {
        if (rsRawDecoder==null) {
            rsRawDecoder = CodecUtil.createRawDecoder(getConf(),
                    ErasureCodeConstants.RS_CODEC_NAME,getOptions());
        }
        return rsRawDecoder;
    }

    private RawErasureDecoder checkCreatePairWiseDecoder(){
        ErasureCoderOptions pairWiseTransformOptions = new ErasureCoderOptions(2,2);
        if (pairWiseDecoder==null) {
            pairWiseDecoder = CodecUtil.createRawDecoder(getConf(),
                    ErasureCodeConstants.RS_CODEC_NAME,pairWiseTransformOptions);
        }
        return pairWiseDecoder;
    }

    @Override
    public boolean preferDirectBuffer() {
        return false;
    }

    @Override
    public void release() {
        if (rsRawDecoder != null) {
            rsRawDecoder.release();
        }
        if (pairWiseDecoder != null) {
            pairWiseDecoder.release();
        }
    }

}
