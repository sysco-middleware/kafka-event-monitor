package no.sysco.middleware.kafka.eventmonitor.http;

import com.google.gson.Gson;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.AbstractHttpService;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.ServiceRequestContext;
import no.sysco.middleware.kafka.eventmonitor.storage.EventHistoryStore;

public class MonitorServerBuilder {

    final EventHistoryStore store;
    final int port;

    final Gson gson;

    public MonitorServerBuilder(EventHistoryStore store, int port) {
        this.store = store;
        this.port = port;
        this.gson = new Gson();
    }

    public Server build() {
        final var builder = new ServerBuilder();
        builder.http(port);
        builder.service("/monitor/events/{event_id}", new AbstractHttpService() {
            @Override
            protected HttpResponse doGet(ServiceRequestContext ctx, HttpRequest req) throws Exception {
                var eventId = ctx.pathParam("event_id");
                var history = store.get(eventId);
                return HttpResponse.of(gson.toJson(history));
            }
        });
        return builder.build();
    }
}
