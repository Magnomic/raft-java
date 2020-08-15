package org.example;

import com.github.wenweihu86.raft.example.client.ClientMain;
import com.github.wenweihu86.raft.example.client.ConcurrentClientMain;
import org.junit.Test;

public class ClientTest {

    @Test
    public void testClient() throws InterruptedException {
        String[] args = {"list://127.0.0.1:8051,127.0.0.1:8052,127.0.0.1:8053"};
        ConcurrentClientMain.main(args);
    }

}
