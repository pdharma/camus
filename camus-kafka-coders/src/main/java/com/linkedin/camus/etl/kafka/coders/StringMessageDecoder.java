package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.Message;
import com.linkedin.camus.coders.MessageDecoder;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.util.Properties;


/**
 * MessageDecoder class that will convert the payload into a String object,
 * System.currentTimeMillis() will be used to set CamusWrapper's
 * timestamp property

 * This MessageDecoder returns a CamusWrapper that works with Strings payloads,
 */
public class StringMessageDecoder extends MessageDecoder<Message, String> {
  private static final Logger log = Logger.getLogger(StringMessageDecoder.class);

  @Override
  public void init(Properties props, String topicName) {
    this.props = props;
    this.topicName = topicName;
  }

  @Override
  public CamusWrapper<String> decode(Message message) {
    long timestamp = 0;
    String payloadString;

    try {
      payloadString = new String(message.getPayload(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      log.error("Unable to load UTF-8 encoding, falling back to system default", e);
      payloadString = new String(message.getPayload());
    }
    timestamp = System.currentTimeMillis();

    return new CamusWrapper<String>(payloadString, timestamp);
  }
}
