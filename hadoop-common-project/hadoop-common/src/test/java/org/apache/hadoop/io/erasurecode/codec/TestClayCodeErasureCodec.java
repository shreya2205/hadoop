package org.apache.hadoop.io.erasurecode.codec;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.io.erasurecode.ErasureCodecOptions;
import org.apache.hadoop.io.erasurecode.coder.ErasureCoder;

import org.junit.Test;
import org.junit.After;
import static org.junit.Assert.assertEquals;

public class TestClayCodeErasureCodec {
  private ECSchema schema;
  private ErasureCodecOptions options;


  @Test
  public void testGoodCodec(){
    schema = new ECSchema("claycode",6,3);
    options = new ErasureCodecOptions(schema);

    ClayCodeErasureCodec codec = new ClayCodeErasureCodec(new Configuration(), options);

    ErasureCoder encoder = codec.createEncoder();
    assertEquals(6, encoder.getNumDataUnits());
    assertEquals(3, encoder.getNumParityUnits());

    ErasureCoder decoder = codec.createDecoder();
    assertEquals(6, decoder.getNumDataUnits());
    assertEquals(3, decoder.getNumParityUnits());

  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadCodec(){

    // clay codes impose a constraint that numOfParityUnits divide totalNumberOfUnits

    schema = new ECSchema("claycode",10,4);
    options = new ErasureCodecOptions(schema);
  }

  @After
  public void cleanUp(){
    schema = null;
    options = null;
  }
}
