package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.erasurecode.*;
import org.apache.hadoop.io.erasurecode.codec.ClayCodeErasureCodec;
import org.apache.hadoop.io.erasurecode.codec.ErasureCodec;
import org.apache.hadoop.io.erasurecode.coder.ErasureEncoder;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ClayRawEncoder extends RawErasureEncoder {

  ErasureCodec claycode;
  ErasureEncoder clayencoder;
  ECSchema schema;

  public ClayRawEncoder(ErasureCoderOptions coderOptions) {
    super(coderOptions);
    Configuration conf = new Configuration();
    conf.set(CodecUtil.IO_ERASURECODE_CODEC_RS_RAWCODERS_KEY,
        RSRawErasureCoderFactory.CODER_NAME);
    schema = new ECSchema("claycode", coderOptions.getNumDataUnits(), coderOptions.getNumParityUnits());
    claycode = new ClayCodeErasureCodec(conf, new ErasureCodecOptions(schema));
    clayencoder = claycode.createEncoder();
    clayencoder.setConf(conf);

  }

  @Override
  public void encode(ECChunk[] inputs, ECChunk[] outputs)
    throws  IOException{
    clayencoder.calculateCoding(new ECBlockGroup(new ECBlock[]{}, new ECBlock[]{})).performCoding(inputs,outputs);
  }

  @Override
  public void encode(ByteBuffer[] inputs, ByteBuffer[] outputs)
      throws IOException{

    ECBlock[] dataBlks = new ECBlock[getNumDataUnits()];
    ECBlock[] parBlks = new ECBlock[getNumParityUnits()];

    for (int i = 0; i < dataBlks.length; i++) {
      dataBlks[i] = new ECBlock();
    }

    for (int i = 0; i < parBlks.length; i++) {
      parBlks[i] = new ECBlock(true,false);
    }


    ECChunk[] inputChunks = new ECChunk[inputs.length];

    for (int i = 0; i < inputs.length; i++) {
      inputChunks[i] = new ECChunk(inputs[i]);
    }

    ECChunk[] outputChunks = new ECChunk[outputs.length];

    for (int i = 0; i < outputs.length; i++) {
      outputChunks[i] = new ECChunk(outputs[i]);
    }


    ECChunk[] inputchnks = new ECChunk[getNumDataUnits() +
        getNumParityUnits()];

    System.arraycopy(inputChunks, 0, inputchnks,
        0, getNumDataUnits());

    System.arraycopy(outputChunks, 0, inputchnks,
        getNumDataUnits(), getNumParityUnits());

    clayencoder.calculateCoding(new ECBlockGroup(dataBlks, parBlks))
                .performCoding(inputchnks,outputChunks);
  }


  @Override
  protected void doEncode(ByteBufferEncodingState encodingState) throws IOException {

  }

  @Override
  protected void doEncode(ByteArrayEncodingState encodingState) throws IOException {

  }
}
