package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.erasurecode.*;
import org.apache.hadoop.io.erasurecode.codec.ClayCodeErasureCodec;
import org.apache.hadoop.io.erasurecode.codec.ErasureCodec;
import org.apache.hadoop.io.erasurecode.coder.ErasureDecoder;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ClayRawDecoder extends RawErasureDecoder {

  ErasureCodec claycode;
  ErasureDecoder claydecoder;
  ECSchema schema;

  public ClayRawDecoder(ErasureCoderOptions coderOptions) {
    super(coderOptions);

    schema = new ECSchema("claycode", coderOptions.getNumDataUnits(), coderOptions.getNumParityUnits());
    claycode = new ClayCodeErasureCodec(new Configuration(), new ErasureCodecOptions(schema));
    claydecoder = claycode.createDecoder();

  }

  @Override
  public void decode(ECChunk[] inputs, int[] erasedIndexes, ECChunk[] outputs)
    throws  IOException{

    ECBlock[] dataBlks = new ECBlock[getNumDataUnits()];
    ECBlock[] parBlks = new ECBlock[getNumParityUnits()];

    for (int i = 0; i < dataBlks.length; i++) {
      dataBlks[i] = new ECBlock();
    }

    for (int i = 0; i < parBlks.length; i++) {
      parBlks[i] = new ECBlock(true,false);
    }

    for (int i = 0; i < erasedIndexes.length; i++) {
      if(erasedIndexes[i]<getNumDataUnits())
          dataBlks[erasedIndexes[i]].setErased(true);
      else
        parBlks[erasedIndexes[i]-getNumDataUnits()].setErased(true);
    }

    claydecoder.calculateCoding(new ECBlockGroup(dataBlks, parBlks)).performCoding(inputs,outputs);
  }

  @Override
  public void decode(ByteBuffer[] inputs, int[] erasedIndexes, ByteBuffer[] outputs)
    throws IOException{

    ECChunk[] inputChunks = new ECChunk[inputs.length];

    for (int i = 0; i < inputs.length; i++) {
      inputChunks[i] = new ECChunk(inputs[i]);
    }

    ECChunk[] outputChunks = new ECChunk[outputs.length];

    for (int i = 0; i < outputs.length; i++) {
      outputChunks[i] = new ECChunk(outputs[i]);
    }

    claydecoder.calculateCoding(new ECBlockGroup(new ECBlock[]{}, new ECBlock[]{})).performCoding(inputChunks,outputChunks);
  }

  @Override
  protected void doDecode(ByteBufferDecodingState decodingState) throws IOException {

  }

  @Override
  protected void doDecode(ByteArrayDecodingState decodingState) throws IOException {

  }
}
