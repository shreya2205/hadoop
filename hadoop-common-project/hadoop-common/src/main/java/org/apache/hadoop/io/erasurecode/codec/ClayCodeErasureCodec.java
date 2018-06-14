package org.apache.hadoop.io.erasurecode.codec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.erasurecode.ErasureCodecOptions;
import org.apache.hadoop.io.erasurecode.coder.ClayCodeErasureDecoder;
import org.apache.hadoop.io.erasurecode.coder.ClayCodeErasureEncoder;
import org.apache.hadoop.io.erasurecode.coder.ErasureDecoder;
import org.apache.hadoop.io.erasurecode.coder.ErasureEncoder;

public class ClayCodeErasureCodec extends ErasureCodec {
    public ClayCodeErasureCodec(Configuration conf, ErasureCodecOptions options) {
        super(conf, options);
    }

    @Override
    public ErasureEncoder createEncoder() {
        return new ClayCodeErasureEncoder(getCoderOptions());
    }

    @Override
    public ErasureDecoder createDecoder() {
        return new ClayCodeErasureDecoder(getCoderOptions());
    }
}
