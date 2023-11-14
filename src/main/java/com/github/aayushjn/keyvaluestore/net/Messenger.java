package com.github.aayushjn.keyvaluestore.net;

import com.github.aayushjn.keyvaluestore.model.MessageType;

import java.io.IOException;

/**
 * Interface that allows remote messaging over any arbitrary transport protocol
 */
public interface Messenger {
    MessageType getValueForKey(MessageType mt, String peer) throws IOException;
    MessageType requestAcknowledgement(MessageType mt, String peer) throws IOException;
    MessageType getAllData(String peer) throws IOException;
    void deleteKey(MessageType mt, String peer) throws IOException;
    void commitKey(MessageType mt, String peer) throws IOException;
    void exit(String peer) throws IOException;
}
