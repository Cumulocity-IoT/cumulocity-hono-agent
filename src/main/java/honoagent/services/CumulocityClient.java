package honoagent.services;

import c8y.Battery;
import c8y.IsDevice;
import c8y.SignalStrength;
import c8y.TemperatureMeasurement;
import com.cumulocity.microservice.subscription.service.MicroserviceSubscriptionsService;
import com.cumulocity.model.DateTimeConverter;
import com.cumulocity.model.ID;
import com.cumulocity.model.idtype.GId;
import com.cumulocity.model.operation.OperationStatus;
import com.cumulocity.rest.representation.event.EventRepresentation;
import com.cumulocity.rest.representation.identity.ExternalIDRepresentation;
import com.cumulocity.rest.representation.inventory.ManagedObjectReferenceRepresentation;
import com.cumulocity.rest.representation.inventory.ManagedObjectRepresentation;
import com.cumulocity.rest.representation.measurement.MeasurementRepresentation;
import com.cumulocity.rest.representation.operation.OperationRepresentation;
import com.cumulocity.rest.representation.tenant.OptionRepresentation;
import com.cumulocity.sdk.client.SDKException;
import com.cumulocity.sdk.client.devicecontrol.DeviceControlApi;
import com.cumulocity.sdk.client.devicecontrol.OperationFilter;
import com.cumulocity.sdk.client.event.EventApi;
import com.cumulocity.sdk.client.identity.IdentityApi;
import com.cumulocity.sdk.client.inventory.InventoryApi;
import com.cumulocity.sdk.client.measurement.MeasurementApi;
import com.cumulocity.sdk.client.notification.Subscriber;
import com.cumulocity.sdk.client.notification.Subscription;
import com.cumulocity.sdk.client.notification.SubscriptionListener;
import com.cumulocity.sdk.client.option.TenantOptionApi;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import org.eclipse.hono.util.BufferResult;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Service
public class CumulocityClient {
    @Autowired
    EventApi eventApi;

    @Autowired
    InventoryApi inventoryApi;

    @Autowired
    TenantOptionApi tenantOptionApi;

    @Autowired
    IdentityApi identityApi;

    @Autowired
    MeasurementApi measurementApi;

    @Autowired
    DeviceControlApi deviceControlApi;

    @Autowired
    HonoAgent honoAgent;

    @Autowired
    MicroserviceSubscriptionsService subscriptionsService;

    private ManagedObjectRepresentation loggingDevice;

    private final String LOGGING_ID = "HONO_LOGGING";
    private final String SERIAL_TYPE = "c8y_Serial";

    @Value("${C8Y.agentName}")
    public String agentName;

    @Value("${C8Y.agentId}")
    public String agentId;

    private ManagedObjectRepresentation agentMor;

    final Logger logger = LoggerFactory.getLogger(CumulocityClient.class);

    public ExternalIDRepresentation findExternalId(String externalId, String type) {
        ID id = new ID();
        id.setType(type);
        id.setValue(externalId);
        ExternalIDRepresentation extId = null;
        try {
            extId = identityApi.getExternalId(id);
        } catch (SDKException e) {
            logger.info("External ID {} not found", externalId);
        }
        return extId;
    }

    public ManagedObjectRepresentation upsertHonoDevice(String name, String id, String data, DateTime updateTime) {

        try {
            logger.info("Upsert device with name {} and id {} with Data {}", name, id, data);
            ExternalIDRepresentation extId = findExternalId(id, SERIAL_TYPE);
            ManagedObjectRepresentation mor;
            boolean deviceExists = true;
            if (extId == null) {
                mor = new ManagedObjectRepresentation();
                mor.setType("c8y_HonoDevice");
                mor.setName(name);
                deviceExists = false;
            } else {
                mor = extId.getManagedObject();
            }
            mor.set(new IsDevice());

            mor.set(DateTimeConverter.date2String(updateTime), "lastHonoUpdate");
            if (!deviceExists) {
                mor = inventoryApi.create(mor);
                extId = new ExternalIDRepresentation();
                extId.setExternalId(id);
                extId.setType(SERIAL_TYPE);
                extId.setManagedObject(mor);
                identityApi.create(extId);
            } else
                mor = inventoryApi.update(mor);
            return mor;
        } catch (SDKException e) {
            logger.info("Error on creating DT Device", e);
            return null;
        }

    }

    public void createTemperatureMeasurement(ManagedObjectRepresentation mor, Double temperature, DateTime dateTime) {
        try {
            MeasurementRepresentation measurementRepresentation = new MeasurementRepresentation();
            TemperatureMeasurement temperatureMeasurement = new TemperatureMeasurement();
            temperatureMeasurement.setTemperature(BigDecimal.valueOf(temperature));
            measurementRepresentation.set(temperatureMeasurement);
            measurementRepresentation.setSource(mor);
            measurementRepresentation.setDateTime(dateTime);
            measurementRepresentation.setType("c8y_TemperatureMeasurement");
            measurementApi.create(measurementRepresentation);
        } catch (SDKException e) {
            logger.error("Error on creating Temperature Measurement", e);
        }
    }

    public void createBatteryMeasurement(ManagedObjectRepresentation mor, double batteryValue, DateTime dateTime) {
        try {
            MeasurementRepresentation measurementRepresentation = new MeasurementRepresentation();
            Battery battery = new Battery();
            battery.setLevelValue(BigDecimal.valueOf(batteryValue));
            measurementRepresentation.set(battery);
            measurementRepresentation.setSource(mor);
            measurementRepresentation.setDateTime(dateTime);
            measurementRepresentation.setType("c8y_BatteryMeasurement");
            measurementApi.create(measurementRepresentation);
        } catch (SDKException e) {
            logger.error("Error on creating Temperature Measurement", e);
        }
    }

    public void createSignalStrengthMeasurement(ManagedObjectRepresentation mor, long signalStrength, DateTime dataTime) {
        try {
            MeasurementRepresentation measurementRepresentation = new MeasurementRepresentation();
            SignalStrength sigStrength = new SignalStrength();
            sigStrength.setRssiValue(BigDecimal.valueOf(signalStrength));
            measurementRepresentation.set(sigStrength);
            measurementRepresentation.setSource(mor);
            measurementRepresentation.setDateTime(dataTime);
            measurementRepresentation.setType("c8y_SignalStrengthMeasurement");
            measurementApi.create(measurementRepresentation);
        } catch (SDKException e) {
            logger.error("Error on creating Signal Strength Measurement", e);
        }
    }

    public List<ManagedObjectReferenceRepresentation> getParentDevicesByDeviceId(ManagedObjectRepresentation mor) {
        try {
            List<ManagedObjectReferenceRepresentation> referenceRepresentations = inventoryApi.get(mor.getId()).getDeviceParents().getReferences();
            return referenceRepresentations;
        } catch (SDKException e) {
            logger.error("Error receiving References for Device {}", mor.getId());
            return null;
        }
    }

    public List<OptionRepresentation> getTenantOptions(String category) {
        List<OptionRepresentation> optionList = new ArrayList<>();
        try {
            if (category != null)
                optionList = tenantOptionApi.getAllOptionsForCategory(category);
            else {
                Iterator<OptionRepresentation> optionIt = tenantOptionApi.getOptions().get().allPages().iterator();
                while (optionIt.hasNext()) {
                    OptionRepresentation option = optionIt.next();
                    optionList.add(option);
                }
            }
            return optionList;
        } catch (SDKException e) {
            logger.error("Error retrieving Tenant Options");
            return optionList;
        }
    }

    public EventRepresentation createEvent(ManagedObjectRepresentation mor, String eventType, String eventText, String content, JsonObject jsonContent, DateTime dateTime) {
        EventRepresentation event = null;
        try {
            EventRepresentation eventRepresentation = new EventRepresentation();
            eventRepresentation.setSource(mor);
            eventRepresentation.setDateTime(dateTime);
            eventRepresentation.setText(eventText);
            if (jsonContent != null) {
                eventRepresentation.set(jsonContent.getMap(), "hono_Content");
            } else {
                eventRepresentation.set(content, "hono_Content");
            }
            eventRepresentation.setType(eventType);
            eventRepresentation = eventApi.create(eventRepresentation);
            return eventRepresentation;
        } catch (SDKException e) {
            logger.error("Error on creating Event", e);
            return event;
        }
    }

    public ManagedObjectRepresentation findAgentMor() {
        ExternalIDRepresentation extId = null;
        if (extId == null)
            extId = findExternalId(agentId, "c8y_Serial");

        if (extId == null) {
            logger.info("Creating Agent Object...");
            extId = createAgent(agentName, agentId);
            logger.info("Agent Object has been created with id {}", extId.getManagedObject().getId().getLong());
        }
        agentMor = extId.getManagedObject();
        return extId.getManagedObject();
    }

    private ExternalIDRepresentation createAgent(String name, String id) {
        logger.info("Creating new Agent with name {} and id {}", name, id);
        ManagedObjectRepresentation mor = new ManagedObjectRepresentation();
        mor.setType(id);
        mor.setName(name);
        mor.set(new com.cumulocity.model.Agent());
        mor = inventoryApi.create(mor);
        ExternalIDRepresentation externalIDRepresentation = new ExternalIDRepresentation();
        externalIDRepresentation.setType("c8y_Serial");
        externalIDRepresentation.setExternalId(id);
        externalIDRepresentation.setManagedObject(mor);
        externalIDRepresentation = identityApi.create(externalIDRepresentation);
        return externalIDRepresentation;
    }

    public void processFirstPendingOperation(ManagedObjectRepresentation agentMor) {
        OperationFilter filter = new OperationFilter();
        filter = filter.byAgent(agentMor.getId().getValue());
        filter = filter.byStatus(OperationStatus.PENDING);
        Iterator<OperationRepresentation> opIt = deviceControlApi.getOperationsByFilter(filter).get().allPages().iterator();
        while (opIt.hasNext()) {
            OperationRepresentation op = opIt.next();
            op.setStatus(OperationStatus.EXECUTING.toString());
            deviceControlApi.update(op);
            processOperations(op);
        }
    }

    public void checkAgentAssignment(ManagedObjectRepresentation mor) {
        boolean agentAssigned = false;
        try {
            List<ManagedObjectReferenceRepresentation> referencesList = getParentDevicesByDeviceId(mor);
            for (ManagedObjectReferenceRepresentation reference : referencesList) {
                ManagedObjectRepresentation parentMor = reference.getManagedObject();
                if (agentName.equals(parentMor.getName())) {
                    agentAssigned = true;
                    continue;
                }
            }
        } catch (Exception e) {
            logger.error("Error on finding MORs", e);
        }

        // Assign Agent
        if (!agentAssigned) {
            ManagedObjectRepresentation agentMor = findAgentMor();
            assignDeviceToAgent(mor, agentMor);
        }
    }

    public void assignDeviceToAgent(ManagedObjectRepresentation deviceMor, ManagedObjectRepresentation agentMor) {
        ManagedObjectReferenceRepresentation child2Ref = new ManagedObjectReferenceRepresentation();
        child2Ref.setManagedObject(deviceMor);
        inventoryApi.getManagedObjectApi(agentMor.getId()).addChildDevice(child2Ref);
    }

    public void registerForOperations(GId agentId) {
        Subscriber<GId, OperationRepresentation> subscriber = deviceControlApi.getNotificationsSubscriber();
        OperationListener<GId, OperationRepresentation> operationListener = new OperationListener<>();
        subscriber.subscribe(agentId, operationListener);
    }

    public void processOperations(OperationRepresentation op) {
        logger.info("Operation received {}", op.toString());
        op.setStatus(OperationStatus.EXECUTING.toString());
        deviceControlApi.update(op);
        boolean oneWay = true;
        String honoCommand = null;
        String honoContentType = null;
        Buffer honoData = null;
        Map<String, Object> honoHeaders = null;
        if (!op.hasProperty("hono_Command")) {
            op.setFailureReason("hono_Command was missing in the Operation!");
            op.setStatus(OperationStatus.FAILED.toString());
            deviceControlApi.update(op);
        } else {
            honoCommand = op.get("hono_Command").toString();
            if(op.hasProperty("hono_OneWay"))
                oneWay = (boolean) op.get("hono_OneWay");
            if (op.hasProperty("hono_Data")) {
                honoData = Buffer.buffer(op.get("hono_Data").toString());
            }
            if (op.hasProperty("hono_Headers")) {
                ObjectMapper m = new ObjectMapper();
                honoHeaders = m.convertValue(op.get("hono_Headers"), Map.class);
            }
            if (op.hasProperty("hono_ContentType"))
                honoContentType = op.get("hono_ContentType").toString();


            if(oneWay) {
                Future<Void> commandResult = honoAgent.sendOneWayCommand(op.getDeviceName(), honoContentType, honoCommand, honoData, honoHeaders);
                commandResult.setHandler(result -> {
                    subscriptionsService.runForEachTenant(() -> {
                        if (result.succeeded()) {
                            logger.info("Command successfully send");
                            op.setStatus(OperationStatus.SUCCESSFUL.toString());
                            deviceControlApi.update(op);
                        } else {
                            logger.error("Command not successfully send: {}", result.cause().getMessage());
                            op.setStatus(OperationStatus.FAILED.toString());
                            op.setFailureReason(result.cause().getMessage());
                            deviceControlApi.update(op);
                        }
                    });
                });
            } else {
                Future<BufferResult> commandResult = honoAgent.sendCommand(op.getDeviceName(), honoContentType, honoCommand, honoData, honoHeaders);
                commandResult.setHandler(result -> {
                    subscriptionsService.runForEachTenant(() -> {
                        if (result.succeeded()) {
                            logger.info("Command was successful {}", result.result().toString());
                            op.setStatus(OperationStatus.SUCCESSFUL.toString());
                            deviceControlApi.update(op);
                        } else {
                            logger.error("Command was not successful: {}", result.cause().getMessage());
                            op.setStatus(OperationStatus.FAILED.toString());
                            op.setFailureReason(result.cause().getMessage());
                            deviceControlApi.update(op);
                        }
                    });
                });
            }
        }
    }

    public class OperationListener<GId, OperationRepresentation>
            implements SubscriptionListener<GId, OperationRepresentation> {

        @Override
        public void onNotification(Subscription<GId> sub, OperationRepresentation operation) {
            subscriptionsService.runForEachTenant(() -> {
                com.cumulocity.rest.representation.operation.OperationRepresentation op = (com.cumulocity.rest.representation.operation.OperationRepresentation) operation;
                processOperations(op);
            });
        }

        @Override
        public void onError(Subscription<GId> sub, Throwable throwable) {
            logger.info("Error on Operation Listener: {}", throwable.getLocalizedMessage());

        }
    }

}
