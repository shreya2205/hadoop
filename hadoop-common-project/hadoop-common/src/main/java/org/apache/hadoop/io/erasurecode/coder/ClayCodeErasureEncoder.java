/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io.erasurecode.coder;

import org.apache.hadoop.io.erasurecode.*;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;

public class ClayCodeErasureEncoder extends ErasureEncoder{

  private RawErasureDecoder pairWiseDecoder;
  private RawErasureDecoder rsRawDecoder;

  public ClayCodeErasureEncoder(ErasureCoderOptions options) {
    super(options);
  }

  @Override
  protected ECBlock[] getInputBlocks(ECBlockGroup blockGroup) {
    ECBlock[] inputBlocks = new ECBlock[getNumDataUnits() +
        getNumParityUnits()];

    System.arraycopy(blockGroup.getDataBlocks(), 0, inputBlocks,
        0, getNumDataUnits());

    System.arraycopy(blockGroup.getParityBlocks(), 0, inputBlocks,
        getNumDataUnits(), getNumParityUnits());

    return inputBlocks;
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

    // in this implementation encoding and decoding are achieved via decode
    return new ClayCodeErasureDecodingStep(inputs, erasedIndexes, getOutputBlocks(blockGroup),
        pairWiseDecoder, rsRawDecoder);
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
