package com.serverless;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class VertxHandler implements RequestHandler<Map, Map> {

    // Initialize Vertx instance and deploy UserService Verticle
    private Vertx vertxInstance = null;

    @Override
    public Map handleRequest(Map input, Context context) {
        final CompletableFuture future = new CompletableFuture<Map>();

        // Send message to event bus using httpmethod:resource as dynamic channel
        final String eventBusAddress = ""+input.get("httpMethod")+":"+input.get("resource");
        if (vertxInstance == null) {
            System.setProperty("vertx.disableFileCPResolving", "true");
            vertxInstance = Vertx.vertx();
            vertxInstance.deployVerticle(new UserService());
        }

        vertxInstance.eventBus().send(eventBusAddress, input, asyncResult -> {
            if (asyncResult.succeeded()) {
                future.complete(asyncResult.result().body());
            } else {
                future.completeExceptionally(asyncResult.cause());
            }
        });

        try {
            return (Map)(future.get(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            return createInternalError(e.getMessage());
        } catch (ExecutionException e) {
            return createInternalError(e.getMessage());
        } catch (TimeoutException e) {
            return createInternalError(e.getMessage());
        }
    }

    private Map createInternalError(String message) {
        HashMap<String, Object> replyContent = new HashMap<String, Object>();
        replyContent.put("statusCode", 500);
        replyContent.put("body", "Internal Error: "+message);
        return replyContent;
    }

    class UserService extends AbstractVerticle {

        @Override
        public void start() throws Exception {

            final EventBus eventBus = vertx.eventBus();

            eventBus.consumer("GET:/users", message -> {
                // Do something with Vert.x async, reactive APIs
                HashMap<String, Object> replyContent = new HashMap<String, Object>();
                replyContent.put("statusCode", 200);
                replyContent.put("body", "Received GET:/users");
                message.reply(replyContent);
            });

            eventBus.consumer("POST:/users", message -> {
                // Do something with Vert.x async, reactive APIs
                HashMap<String, Object> replyContent = new HashMap<String, Object>();
                replyContent.put("statusCode", 201);
                replyContent.put("body", "Received POST:/users");
                message.reply(replyContent);
            });

            eventBus.consumer("GET:/users/{id}", message -> {
                // Do something with Vert.x async, reactive APIs
                HashMap<String, Object> replyContent = new HashMap<String, Object>();
                replyContent.put("statusCode", 200);
                replyContent.put("body", "Received GET:/users/{id}");
                message.reply(replyContent);
            });

            eventBus.consumer("PUT:/users/{id}", message -> {
                // Do something with Vert.x async, reactive APIs
                HashMap<String, Object> replyContent = new HashMap<String, Object>();
                replyContent.put("statusCode", 200);
                replyContent.put("body", "Received PUT:/users/{id}");
                message.reply(replyContent);
            });

            eventBus.consumer("DELETE:/users/{id}", message -> {
                // Do something with Vert.x async, reactive APIs
                HashMap<String, Object> replyContent = new HashMap<String, Object>();
                replyContent.put("statusCode", 200);
                replyContent.put("body", "Received DELETE:/users/{id}");
                message.reply(replyContent);
            });
        }
    }
}
