/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.client.rest;


import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

/**
 * Class the represents various errors returned by the
 * Ozone Layer.
 */
@InterfaceAudience.Private
public class OzoneException extends Exception {

  private static final ObjectReader READER =
      new ObjectMapper().readerFor(OzoneException.class);
  private static final ObjectMapper MAPPER;

  static {
    MAPPER = new ObjectMapper();
    MAPPER.setVisibility(
        MAPPER.getSerializationConfig().getDefaultVisibilityChecker()
            .withCreatorVisibility(JsonAutoDetect.Visibility.NONE)
            .withFieldVisibility(JsonAutoDetect.Visibility.NONE)
            .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
            .withIsGetterVisibility(JsonAutoDetect.Visibility.NONE)
            .withSetterVisibility(JsonAutoDetect.Visibility.NONE));
  }

  @JsonProperty("httpCode")
  private long httpCode;
  @JsonProperty("shortMessage")
  private String shortMessage;
  @JsonProperty("resource")
  private String resource;
  @JsonProperty("message")
  private String message;
  @JsonProperty("requestID")
  private String requestId;
  @JsonProperty("hostName")
  private String hostID;

  /**
   * Constructs a new exception with {@code null} as its detail message. The
   * cause is not initialized, and may subsequently be initialized by a call
   * to {@link #initCause}.
   *
   * This constructor is needed by Json Serializer.
   */
  public OzoneException() {
  }


  /**
   * Constructor that allows a shortMessage and exception.
   *
   * @param httpCode Error Code
   * @param shortMessage Short Message
   * @param ex Exception
   */
  public OzoneException(long httpCode, String shortMessage, Exception ex) {
    super(ex);
    this.message = ex.getMessage();
    this.shortMessage = shortMessage;
    this.httpCode = httpCode;
  }


  /**
   * Constructor that allows a shortMessage.
   *
   * @param httpCode Error Code
   * @param shortMessage Short Message
   */
  public OzoneException(long httpCode, String shortMessage) {
    this.shortMessage = shortMessage;
    this.httpCode = httpCode;
  }

  /**
   * Constructor that allows a shortMessage and long message.
   *
   * @param httpCode Error Code
   * @param shortMessage Short Message
   * @param message long error message
   */
  public OzoneException(long httpCode, String shortMessage, String message) {
    this.shortMessage = shortMessage;
    this.message = message;
    this.httpCode = httpCode;
  }

  /**
   * Constructor that allows a shortMessage, a long message and an exception.
   *
   * @param httpCode Error code
   * @param shortMessage Short message
   * @param message Long error message
   * @param ex Exception
   */
  public OzoneException(long httpCode, String shortMessage,
      String message, Exception ex) {
    super(ex);
    this.shortMessage = shortMessage;
    this.message = message;
    this.httpCode = httpCode;
  }

  /**
   * Returns the Resource that was involved in the stackTraceString.
   *
   * @return String
   */
  public String getResource() {
    return resource;
  }

  /**
   * Sets Resource.
   *
   * @param resourceName - Name of the Resource
   */
  public void setResource(String resourceName) {
    this.resource = resourceName;
  }

  /**
   * Gets a detailed message for the error.
   *
   * @return String
   */
  public String getMessage() {
    return message;
  }

  /**
   * Sets the error message.
   *
   * @param longMessage - Long message
   */
  public void setMessage(String longMessage) {
    this.message = longMessage;
  }

  /**
   * Returns request Id.
   *
   * @return String
   */
  public String getRequestId() {
    return requestId;
  }

  /**
   * Sets request ID.
   *
   * @param ozoneRequestId Request ID generated by the Server
   */
  public void setRequestId(String ozoneRequestId) {
    this.requestId = ozoneRequestId;
  }

  /**
   * Returns short error string.
   *
   * @return String
   */
  public String getShortMessage() {
    return shortMessage;
  }

  /**
   * Sets short error string.
   *
   * @param shortError Short Error Code
   */
  public void setShortMessage(String shortError) {
    this.shortMessage = shortError;
  }

  /**
   * Returns hostID.
   *
   * @return String
   */
  public String getHostID() {
    return hostID;
  }

  /**
   * Sets host ID.
   *
   * @param hostName host Name
   */
  public void setHostID(String hostName) {
    this.hostID = hostName;
  }

  /**
   * Returns http error code.
   *
   * @return long
   */
  public long getHttpCode() {
    return httpCode;
  }

  /**
   * Sets http status.
   *
   * @param httpStatus http error code.
   */
  public void setHttpCode(long httpStatus) {
    this.httpCode = httpStatus;
  }

  /**
   * Returns a Json String.
   *
   * @return JSON representation of the Error
   */
  public String toJsonString() {
    try {
      return MAPPER.writeValueAsString(this);
    } catch (IOException ex) {
      // TODO : Log this error on server side.
    }
    // TODO : Replace this with a JSON Object -- That represents this error.
    return "500 Internal Server Error";
  }

  /**
   * Parses an Exception record.
   *
   * @param jsonString - Exception in Json format.
   *
   * @return OzoneException Object
   *
   * @throws IOException
   */
  public static OzoneException parse(String jsonString) throws IOException {
    return READER.readValue(jsonString);
  }
}
