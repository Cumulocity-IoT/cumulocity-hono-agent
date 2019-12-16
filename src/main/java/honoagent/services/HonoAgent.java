package honoagent.services;

import com.cumulocity.microservice.subscription.model.MicroserviceSubscriptionAddedEvent;
import com.cumulocity.microservice.subscription.service.MicroserviceSubscriptionsService;
import com.cumulocity.rest.representation.inventory.ManagedObjectReferenceRepresentation;
import com.cumulocity.rest.representation.inventory.ManagedObjectRepresentation;
import com.cumulocity.rest.representation.tenant.OptionRepresentation;
import honoagent.config.HonoConfiguration;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.qpid.proton.message.Message;
import org.eclipse.hono.client.ApplicationClientFactory;
import org.eclipse.hono.client.DisconnectListener;
import org.eclipse.hono.client.HonoConnection;
import org.eclipse.hono.client.MessageConsumer;
import org.eclipse.hono.config.ClientConfigProperties;
import org.eclipse.hono.util.MessageHelper;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Service
public class HonoAgent {

    final Logger logger = LoggerFactory.getLogger(HonoAgent.class);
    private final Vertx vertx = Vertx.vertx();
    private final long RECONNECT_INTERVAL_MILLIS = 5000;


    @Autowired
    MicroserviceSubscriptionsService subscriptionsService;
    @Autowired
    HonoConfiguration honoConfiguration;
    @Autowired
    CumulocityClient cumulocityClient;
    private ApplicationClientFactory clientFactory;
    private long reconnectTimerId = -1;
    private String honoHost = null;
    private Integer honoPort = null;
    private String honoUser = null;
    private String honoPW = null;
    private String honoTenantId = null;

    @EventListener
    private void onAdded(MicroserviceSubscriptionAddedEvent event) {
        try {
            logger.info("Subscription added for tenant: " + event.getCredentials().getTenant());
            while (honoTenantId == null || honoPort == null || honoHost == null || honoUser == null || honoPW == null) {
                retrieveRequiredConfiguration();
                boolean hasErrors = false;
                if (honoTenantId == null) {
                    logger.error("Hono 'tenantid' is missing. The Hono TenantId must be maintained in the configuration file or in tenant options");
                    hasErrors = true;
                }
                if (honoPort == null) {
                    logger.error("Hono 'port' is missing. The Hono Port must be maintained in the configuration file or in tenant options");
                }

                if (honoHost == null) {
                    logger.error("Hono 'host' is missing. The Hono Host must be maintained in the configuration file or in tenant options");
                    hasErrors = true;
                }
                if (honoUser == null) {
                    logger.error("Hono 'username' is missing. The Hono Username must be maintained in the configuration file or in tenant options");
                    hasErrors = true;
                }
                if (honoPW == null) {
                    logger.error("Hono 'password' is missing. The Hono Password must be maintained in the configuration file or in tenant options");
                    hasErrors = true;
                }
                if(hasErrors) {
                    logger.info("Will retry to retrieve required Configuration in 60 sec.!");
                    try {
                        Thread.sleep(60000);
                    } catch (InterruptedException e) {
                        logger.error("Thread sleep interrupted {}", e.getStackTrace());
                    }
                }
            }

            final ClientConfigProperties props = new ClientConfigProperties();
            props.setHost(honoConfiguration.getHost());
            props.setPort(honoConfiguration.getPort());
            props.setUsername(honoConfiguration.getUsername());
            props.setPassword(honoConfiguration.getPassword());
            //props.setTrustStorePath("target/config/hono-demo-certs-jar/trusted-certs.pem");
            props.setHostnameVerificationRequired(false);
            clientFactory = ApplicationClientFactory.create(HonoConnection.newConnection(vertx, props));
            ManagedObjectRepresentation agentMor = cumulocityClient.findAgentMor();
            cumulocityClient.registerForOperations(agentMor.getId());
            connectWithRetry();
            cumulocityClient.processFirstPendingOperation(agentMor);
        } catch (Exception e) {
            logger.error("Error on Initializatzion {}", e.getMessage() );
            e.printStackTrace();
        }
    }

    private void retrieveRequiredConfiguration() throws ExecutionException, InterruptedException {
        logger.info("Retrieving Hono Configuration from Tenant Options or Configuration File..");
        try {
            logger.info("Getting Tenant Options from Tenant {}", subscriptionsService.getTenant());
            List<OptionRepresentation> optionList = cumulocityClient.getTenantOptions("hono");
            for (OptionRepresentation op : optionList) {
                if("tenantid".equals(op.getKey())) {
                    honoTenantId = op.getValue();
                }
                if("username".equals(op.getKey())) {
                    honoUser = op.getValue();
                }
                if("credentials.password".equals(op.getKey())) {
                    honoPW = op.getValue();
                }
                if("host".equals(op.getKey())) {
                    honoHost = op.getValue();
                }
                if("port".equals(op.getKey())) {
                    if(op.getValue() != null)
                    honoPort = Integer.valueOf(op.getValue());
                }
            }
            if(honoPort == null)
                honoPort = honoConfiguration.getPort();
            if(honoHost == null)
                honoHost = honoConfiguration.getHost();
            if(honoPW == null)
                honoPW = honoConfiguration.getPassword();
            if(honoTenantId == null)
                honoTenantId = honoConfiguration.getTenantid();
            if(honoUser == null)
                honoUser = honoConfiguration.getUsername();
        } catch (Exception e) {
            logger.error("Error retrieving Tenant Options {}", e.getStackTrace());
        }
    }

    private void connectWithRetry() {
        try {
            //Refresh TenantId if TenantId or other credentials have been changed.
            retrieveRequiredConfiguration();
        } catch (Exception e) {
            e.printStackTrace();
        }
        clientFactoryConnect(this::onDisconnect).compose(connection -> {
            logger.info("Connected to IoT Hub messaging endpoint.");
            return createTelemetryConsumer().compose(createdConsumer -> {
                logger.info("Consumer ready [tenant: {}, type: Telemetry]", honoTenantId);
                return createEventConsumer().compose(createdEventConsumer -> {
                    logger.info("Consumer ready [tenant: {}, type: Event]", honoTenantId);
                    return Future.succeededFuture();
                });
            });
        }).otherwise(connectException -> {
            logger.info("Connecting or creating a consumer failed with an exception: ", connectException);
            logger.info("Reconnecting in {} ms...", RECONNECT_INTERVAL_MILLIS);

            // As timer could be triggered by detach or disconnect we need to ensure here that timer runs only once
            vertx.cancelTimer(reconnectTimerId);
            reconnectTimerId = vertx.setTimer(RECONNECT_INTERVAL_MILLIS, timerId -> connectWithRetry());
            return null;
        });
    }

    Future<HonoConnection> clientFactoryConnect(DisconnectListener<HonoConnection> disconnectHandler) {
        logger.info("Connecting to IoT Hub messaging endpoint...");
        clientFactory.addDisconnectListener(disconnectHandler);
        return clientFactory.connect();
    }

    Future<MessageConsumer> createTelemetryConsumer() {
        logger.info("Creating telemetry consumer...");
        return clientFactory.createTelemetryConsumer(honoTenantId, this::handleTelemetryMessage, this::onDetach);
    }

    Future<MessageConsumer> createEventConsumer() {
        logger.info("Creating Event consumer...");
        return clientFactory.createEventConsumer(honoTenantId, this::handleEventMessage, this::onDetach);
    }

    private void onDisconnect(final HonoConnection connection) {
        logger.info("Client got disconnected. Reconnecting...");
        connectWithRetry();
    }

    private void onDetach(Void event) {
        logger.info("Client got detached. Reconnecting...");
        connectWithRetry();
    }



    /**
     * /**
     * Handler method for a Message from Hono that was received as telemetry data.
     * <p>
     * The tenant, the device, the payload, the content-type, the creation-time and the application properties will be printed to stdout.
     *
     * @param msg The message that was received.
     */
    private void handleTelemetryMessage(final Message msg) {
        subscriptionsService.runForEachTenant(() -> {
            final String content = MessageHelper.getPayloadAsString(msg);
            final String deviceId = MessageHelper.getDeviceId(msg);
            JsonObject contentJson = MessageHelper.getJsonPayload(msg);
            logger.info("Telemetry received for Device {} with Payload {}", deviceId, content);
            ManagedObjectRepresentation mor = cumulocityClient.upsertHonoDevice(deviceId, deviceId, content, DateTime.now());
            cumulocityClient.checkAgentAssignment(mor);
            cumulocityClient.createEvent(mor, "hono_Telemetry", "Hono Telemetry Message", content, contentJson, DateTime.now());
        });
    }

    /**
     * Handler method for a Message from Hono that was received as event data.
     * <p>
     * The tenant, the device, the payload, the content-type, the creation-time and the application properties will be printed to stdout.
     *
     * @param msg The message that was received.
     */
    private void handleEventMessage(final Message msg) {
        subscriptionsService.runForEachTenant(() -> {
            final String content = MessageHelper.getPayloadAsString(msg);
            final String deviceId = MessageHelper.getDeviceId(msg);
            final JsonObject jsonContent = MessageHelper.getJsonPayload(msg);

            logger.info("Event received for Device {} with Payload {}", deviceId, content);
            ManagedObjectRepresentation mor = cumulocityClient.upsertHonoDevice(deviceId, deviceId, content, DateTime.now());
            cumulocityClient.checkAgentAssignment(mor);
            cumulocityClient.createEvent(mor, "hono_Event", "Hono Event Message", content, jsonContent, DateTime.now());
        });
    }
}