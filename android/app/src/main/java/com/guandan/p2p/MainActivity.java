package com.guandan.p2p;

import android.os.Bundle;
import com.getcapacitor.BridgeActivity;

public class MainActivity extends BridgeActivity {
    @Override
    public void onCreate(Bundle savedInstanceState) {
        // Register custom plugins BEFORE super.onCreate() so the bridge picks them up.
        registerPlugin(WsServerPlugin.class);
        super.onCreate(savedInstanceState);
    }
}
