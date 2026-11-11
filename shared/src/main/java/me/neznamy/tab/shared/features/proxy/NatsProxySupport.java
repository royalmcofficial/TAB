package me.neznamy.tab.shared.features.proxy;

import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;
import io.nats.client.Options;
import me.neznamy.tab.shared.TAB;
import me.neznamy.tab.shared.chat.TabTextColor;
import me.neznamy.tab.shared.chat.component.TabTextComponent;
import me.neznamy.tab.shared.features.proxy.message.LoadRequest;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * NATS transport for cross-proxy TAB messages.
 *
 * Reconstructed from the working RoyalMC TAB 6.1.1-SNAPSHOT binary.
 * Uses the existing ProxySupport wire protocol and subject.
 */
public class NatsProxySupport extends ProxySupport {

    private final List<String> servers;
    private final String credentialsFile;
    private Connection connection;
    private Dispatcher dispatcher;

    public NatsProxySupport(@NotNull String channelName, @NotNull List<String> servers,
                            @NotNull String credentialsFile) {
        super(channelName);
        this.servers = servers;
        this.credentialsFile = credentialsFile;
    }

    @Override
    public void register() {
        try {
            Options.Builder builder = new Options.Builder()
                    .servers(servers.toArray(new String[0]))
                    .connectionName("TAB")
                    .maxReconnects(-1)
                    .connectionListener((conn, event) -> {
                        if (event == ConnectionListener.Events.RECONNECTED) {
                            sendMessage(new LoadRequest());
                        }
                    });
            if (!credentialsFile.isEmpty()) {
                builder.authHandler(Nats.credentials(credentialsFile));
            }
            connection = Nats.connect(builder.build());
            dispatcher = connection.createDispatcher(message ->
                    processMessage(new String(message.getData(), StandardCharsets.UTF_8)));
            dispatcher.subscribe(getChannelName());
            TAB.getInstance().getPlatform().logInfo(
                    new TabTextComponent("Successfully connected to NATS", TabTextColor.GREEN));
        } catch (Exception e) {
            TAB.getInstance().getErrorManager().criticalError(
                    "Failed to connect to NATS: " + e.getClass().getName() + ": " + e.getMessage(), null);
        }
    }

    @Override
    public void unregister() {
        if (connection != null) {
            try {
                connection.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        connection = null;
        dispatcher = null;
    }

    @Override
    public void sendMessage(@NotNull String message) {
        Connection current = connection;
        if (current == null) return;
        try {
            current.publish(getChannelName(), message.getBytes(StandardCharsets.UTF_8));
        } catch (IllegalStateException ignored) {
            // Connection may have closed during an orderly proxy shutdown.
        }
    }
}
