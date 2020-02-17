package org.johnsoft.reactive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

public class Application {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    private final static WebSocketCommunicationHandler handler = new WebSocketCommunicationHandler();

    public static void main(String[] args) {

        DisposableServer server =
        HttpServer.create()
                .host("localhost")
                .port(8888)
                .route(routes -> routes.ws("/ws", handler::process))
                .bindNow();
        LOGGER.info("start websocket server");
        server.onDispose().block();
    }

}
