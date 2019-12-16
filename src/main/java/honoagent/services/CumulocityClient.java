package honoagent.services;

import c8y.Battery;
import c8y.IsDevice;
import c8y.SignalStrength;
import c8y.TemperatureMeasurement;
import com.cumulocity.microservice.subscription.service.MicroserviceSubscriptionsService;
import com.cumulocity.model.DateTimeConverter;
import com.cumulocity.model.ID;
import com.cumulocity.rest.representation.event.EventRepresentation;
import com.cumulocity.rest.representation.identity.ExternalIDRepresentation;
import com.cumulocity.rest.representation.inventory.ManagedObjectRepresentation;
import com.cumulocity.rest.representation.measurement.MeasurementRepresentation;
import com.cumulocity.rest.representation.tenant.OptionRepresentation;
import com.cumulocity.sdk.client.SDKException;
import com.cumulocity.sdk.client.event.EventApi;
import com.cumulocity.sdk.client.identity.IdentityApi;
import com.cumulocity.sdk.client.inventory.InventoryApi;
import com.cumulocity.sdk.client.measurement.MeasurementApi;
import com.cumulocity.sdk.client.option.TenantOptionApi;
import com.cumulocity.sdk.client.option.TenantOptionCollection;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.JsonObject;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.swing.text.html.Option;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

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
    MicroserviceSubscriptionsService subscriptionsService;

    private ManagedObjectRepresentation loggingDevice;

    private final String LOGGING_ID = "HONO_LOGGING";
    private final String SERIAL_TYPE = "c8y_Serial";

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

    public List<OptionRepresentation> getTenantOptions(String category) {
        List<OptionRepresentation> optionList = new ArrayList<>();
        try {
            if(category != null)
                optionList = tenantOptionApi.getAllOptionsForCategory(category);
            else {
                Iterator<OptionRepresentation> optionIt = tenantOptionApi.getOptions().get().allPages().iterator();
                while(optionIt.hasNext()) {
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
            if(jsonContent != null) {
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

}
