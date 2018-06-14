package org.apache.hadoop.io.erasurecode.coder;

import org.apache.hadoop.io.erasurecode.ECBlockGroup;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;

public class ClayCodeErasureDecoder extends ErasureDecoder {

    public ClayCodeErasureDecoder(ErasureCoderOptions options) {
        super(options);
    }

    @Override
    protected ErasureCodingStep prepareDecodingStep(ECBlockGroup blockGroup) {
        return null;
    }
}
