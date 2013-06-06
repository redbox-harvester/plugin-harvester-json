package au.com.redboxresearchdata.fascinator.plugins;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.fascinator.api.PluginException;
import com.googlecode.fascinator.api.PluginManager;
import com.googlecode.fascinator.api.harvester.HarvesterException;
import com.googlecode.fascinator.api.indexer.Indexer;
import com.googlecode.fascinator.api.storage.DigitalObject;
import com.googlecode.fascinator.api.storage.Payload;
import com.googlecode.fascinator.api.storage.Storage;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.api.transformer.Transformer;
import com.googlecode.fascinator.api.transformer.TransformerException;
import com.googlecode.fascinator.common.JsonObject;
import com.googlecode.fascinator.common.JsonSimple;
import com.googlecode.fascinator.common.JsonSimpleConfig;
import com.googlecode.fascinator.common.messaging.GenericListener;
import com.googlecode.fascinator.common.messaging.MessagingException;
import com.googlecode.fascinator.common.messaging.MessagingServices;
import com.googlecode.fascinator.common.storage.StorageUtils;

/**
 * 
 * 
 * @author Shilo Banihit
 * 
 */
public class JsonHarvestQueueConsumer implements GenericListener {
	
	/** Service Loader will look for this */
    public static final String LISTENER_ID = "jsonHarvester";
    
    /** Default payload ID */
    private static final String DEFAULT_PAYLOAD_ID = "metadata.json";
    
    /** Render queue string */
    private String QUEUE_ID;

    /** Logging */
    private Logger log = LoggerFactory.getLogger(MessagingServices.class);
    
    /** JSON configuration */
    private JsonSimpleConfig globalConfig;
 
    /** JMS connection */
    private Connection connection;

    /** JMS Session */
    private Session session;

    /** JMS Topic */
    // private Topic broadcast;

    /** Indexer object */
    private Indexer indexer;

    /** Storage */
    private Storage storage;

    /** Message Consumer instance */
    private MessageConsumer consumer;

    /** Message Producer instance */
    private MessageProducer producer;

    /** Name identifier to be put in the queue */
    private String name;

    /** Thread reference */
    private Thread thread;

    /** Messaging services */
    private MessagingServices messaging;
    
    /** Tool Chain entry queue */
    private String toolChainEntry;
    
    /** Default tool chain queue */
    private static final String DEFAULT_TOOL_CHAIN_QUEUE = "transactionManager";

    public JsonHarvestQueueConsumer() {
    	 thread = new Thread(this, LISTENER_ID);
    }

    /**
     * Return the ID string for this listener
     * 
     */
    public String getId() {
        return LISTENER_ID;
    }
    
    
    public void run() {
        try {
            log.info("Starting {}...", name);

            // Get a connection to the broker
            String brokerUrl = globalConfig.getString(
                    ActiveMQConnectionFactory.DEFAULT_BROKER_BIND_URL,
                    "messaging", "url");
            ActiveMQConnectionFactory connectionFactory =
                    new ActiveMQConnectionFactory(brokerUrl);
            connection = connectionFactory.createConnection();

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            consumer = session.createConsumer(session.createQueue(QUEUE_ID));
            consumer.setMessageListener(this);

            // broadcast = session.createTopic(MessagingServices.MESSAGE_TOPIC);
            producer = session.createProducer(null);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            connection.start();
            
            toolChainEntry = globalConfig.getString(DEFAULT_TOOL_CHAIN_QUEUE,
                    "messaging", "toolChainQueue");
        } catch (JMSException ex) {
            log.error("Error starting message thread!", ex);
        }
    }
    
    /**
     * Start the queue based on the name identifier
     * 
     * @throws JMSException if an error occurred starting the JMS connections
     */
    public void start() throws Exception {
        thread.start();
    }

    /**
     * Stop the JSON Harvest Queue Consumer. Including stopping the storage and
     * indexer
     */
    public void stop() throws Exception {
        log.info("Stopping {}...", name);
        if (indexer != null) {
            try {
                indexer.shutdown();
            } catch (PluginException pe) {
                log.error("Failed to shutdown indexer: {}", pe.getMessage());
                throw pe;
            }
        }
        if (storage != null) {
            try {
                storage.shutdown();
            } catch (PluginException pe) {
                log.error("Failed to shutdown storage: {}", pe.getMessage());
                throw pe;
            }
        }
        if (producer != null) {
            try {
                producer.close();
            } catch (JMSException jmse) {
                log.warn("Failed to close producer: {}", jmse);
            }
        }
        if (consumer != null) {
            try {
                consumer.close();
            } catch (JMSException jmse) {
                log.warn("Failed to close consumer: {}", jmse.getMessage());
                throw jmse;
            }
        }
        if (session != null) {
            try {
                session.close();
            } catch (JMSException jmse) {
                log.warn("Failed to close consumer session: {}", jmse);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (JMSException jmse) {
                log.warn("Failed to close connection: {}", jmse);
            }
        }
        if (messaging != null) {
            messaging.release();
        }
    }
    /**
     * Harvests JSON objects as messages.
     * 
     * Expects the ff. JSON structure:
     * "data" -  data object
     * "type" -  indicates which harvest config file to use, maps to "{portal.harvestFiles}/{type}.json"
     */
	public void onMessage(Message message) {

        try {
        	String text = ((TextMessage) message).getText();
        	log.info(name + ", got message: " + text);
			JsonSimple json = new JsonSimple(text);
			// prepare for the harvest
			String type = json.getString(null, "type");
			if (type == null) {
				log.error("No type specified, ignoring object:" + text);
				return;
			}
			String configFilePath = globalConfig.getString(null, "portal", "harvestFiles") + "/" + type + ".json";
			File harvestConfigFile = new File(configFilePath);
			if (!harvestConfigFile.exists()) {
				log.error("Harvest config file not found, please check the set up. Ignoring object:" + text);
				return;
			}
			log.info("Using config file path:" + configFilePath);
			JsonSimple harvestConfig = new JsonSimple(harvestConfigFile);
			String rulesFilePath = harvestConfig.getString(null, "indexer", "script", "rules");
			File rulesFile = new File(rulesFilePath);
			if (!rulesFile.exists()) {
				log.error("Rules file not found, please check the set up:" + rulesFilePath);
				return;
			}
			log.info("Using rules file path:" + rulesFilePath);
			JsonSimple data= new JsonSimple(json.getObject("data"));
			String idField = harvestConfig.getString("", "harvester", "idField");
			String idPrefix = harvestConfig.getString("", "harvester", "recordIDPrefix");
			String payloadId = harvestConfig.getString(DEFAULT_PAYLOAD_ID, "harvester", "payloadId");
			// save the object...
			String oid = DigestUtils.md5Hex(type+  ":" + idPrefix + data.getString("", idField));
			storeJsonInObject(data.getObject("data"), data.getObject("metadata"), oid, payloadId, idPrefix);
			// prepare for placing the object on the toolchain
			DigitalObject configObject = updateHarvestFile(harvestConfigFile);
			DigitalObject rulesObject = updateHarvestFile(rulesFile);
			processObject(oid, false, configObject, rulesObject, harvestConfig, harvestConfigFile);
			log.info("JSON Object on the toolchain.");
        } catch (JMSException jmse) {
            log.error("Failed to send/receive message: {}", jmse.getMessage());
        } catch (IOException ioe) {
            log.error("Failed to parse message: {}", ioe.getMessage());
        } catch (Exception ex) {
        	log.error("Failed to harvest object: {}", ex.getMessage());
        	log.error("Stack trace:", ex);
        }
	}
	
	private void storeJsonInObject(JsonObject dataJson, JsonObject metaJson,
            String oid, String payloadId, String idPrefix) throws Exception {
		// Does the object already exist?
        DigitalObject object = null;
        try {
            object = storage.getObject(oid);
            storeJsonInPayload(dataJson, metaJson, object, payloadId, idPrefix);

        } catch (StorageException ex) {
            // This is going to be brand new
            try {
                object = StorageUtils.getDigitalObject(storage, oid);
                storeJsonInPayload(dataJson, metaJson, object, payloadId, idPrefix);
            } catch (StorageException ex2) {
            	throw new Exception("Error creating new digital object: ", ex2);
            }
        }
        // Set the pending flag
        if (object != null) {
            try {
                object.getMetadata().setProperty("render-pending", "true");
                object.close();
            } catch (Exception ex) {
                log.error("Error setting 'render-pending' flag: ", ex);
            }
        }
	}	
	/**
     * Store the processed data and metadata in a payload
     *
     * @param dataJson an instantiated JSON object containing data to store
     * @param metaJson an instantiated JSON object containing metadata to store
     * @param object the object to put our payload in
     * @throws HarvesterException if an error occurs
     */
    private void storeJsonInPayload(JsonObject dataJson, JsonObject metaJson,
            DigitalObject object, String payloadId, String idPrefix) throws Exception {
    	Payload payload = null;
        JsonSimple json = new JsonSimple();
        try {
            // New payloads
            payload = object.getPayload(payloadId);
            //log.debug("Updating existing payload: '{}' => '{}'",
            //        object.getId(), payloadId);

            // Get the old JSON to merge
            try {
                json = new JsonSimple(payload.open());
            } catch (IOException ex) {
                log.error("Error parsing existing JSON: '{}' => '{}'",
                    object.getId(), payloadId);
                throw new HarvesterException(
                        "Error parsing existing JSON: ", ex);
            } finally {
                payload.close();
            }

            // Update storage
            try {
                InputStream in = streamMergedJson(dataJson, metaJson, json, idPrefix);
                object.updatePayload(payloadId, in);

            } catch (IOException ex2) {
            	throw new Exception("Error processing JSON data: ", ex2);
            } catch (StorageException ex2) {
                throw new Exception("Error updating payload: ", ex2);
            }
        } catch (StorageException ex) {
            // Create a new Payload
            try {
                //log.debug("Creating new payload: '{}' => '{}'",
                //        object.getId(), payloadId);
                InputStream in = streamMergedJson(dataJson, metaJson, json, idPrefix);
                payload = object.createStoredPayload(payloadId, in);

            } catch (IOException ex2) {
                throw new Exception("Error parsing JSON encoding: ", ex2);
            } catch (StorageException ex2) {
                throw new Exception("Error creating new payload: ", ex2);
            }
        }
    }
    /**
     * Merge the newly processed data with an (possible) existing data already
     * present, also convert the completed JSON merge into a Stream for storage.
     *
     * @param dataJson an instantiated JSON object containing data to store
     * @param metaJson an instantiated JSON object containing metadata to store
     * @param existing an instantiated JsonSimple object with any existing data
     * @throws IOException if any character encoding issues effect the Stream
     */
    private InputStream streamMergedJson(JsonObject dataJson,
            JsonObject metaJson, JsonSimple existing, String idPrefix) throws IOException {
        // Overwrite and/or create only nodes we consider new data
        existing.getJsonObject().put("recordIDPrefix", idPrefix);
        JsonObject existingData = existing.writeObject("data");
        existingData.putAll(dataJson);
        JsonObject existingMeta = existing.writeObject("metadata");
        existingMeta.putAll(metaJson);

        // Turn into a stream to return
        String jsonString = existing.toString(true);
        return IOUtils.toInputStream(jsonString, "UTF-8");
    }
	
    /**
     * Process each objects
     * 
     * @param oid Object Id
     * @param commit Flag to commit after indexing
     * @throws StorageException If storage is not found
     * @throws TransformerException If transformer fail to transform the object
     * @throws MessagingException If the object could not be queue'd
     */
    private void processObject(String oid, boolean commit, DigitalObject configObject, DigitalObject rulesObject, JsonSimple config, File harvestConfigFile)
            throws TransformerException, StorageException, MessagingException, Exception {
        // get the object
        DigitalObject object = storage.getObject(oid);
        
        if (object == null) {
        	log.error("Object was not saved:" + oid);
        	throw new Exception("Digital object is not in storage.");
        }

        // update object metadata
        Properties props = object.getMetadata();
        if (props == null) {
        	log.error("Object has no metadata:" + oid);
        	throw new Exception("Digital object has no metadata.");
        }
        // TODO - objectId is redundant now?
        props.setProperty("objectId", object.getId());
        props.setProperty("scriptType", config.getString(null,
                "indexer", "script", "type"));
        // Set our config and rules data as properties on the object
        props.setProperty("rulesOid", rulesObject.getId());
        props.setProperty("rulesPid", rulesObject.getSourceId());
        props.setProperty("jsonConfigOid", configObject.getId());
        props.setProperty("jsonConfigPid", configObject.getSourceId());
        
        JsonObject params = config.getObject("indexer", "params");
        for (Object key : params.keySet()) {
            props.setProperty(key.toString(), params.get(key).toString());
        }

        // done with the object
        object.close();

        // put in event log
        sentMessage(oid, "modify");

        // queue the object for indexing
        queueHarvest(oid, harvestConfigFile, commit, toolChainEntry);
    }
    
    /**
     * To queue object to be processed
     * 
     * @param oid Object id
     * @param jsonFile Configuration file
     * @param commit To commit each request to Queue (true) or not (false)
     * @param queueName Name of the queue to route to
     * @throws MessagingException if the message could not be sent
     */
    private void queueHarvest(String oid, File jsonFile, boolean commit,
            String queueName) throws MessagingException {
        try {
            JsonObject json = new JsonSimple(jsonFile).getJsonObject();
            json.put("oid", oid);
            if (commit) {
                json.put("commit", "true");
            }
            log.info("Sending message after harvest:");
            log.info(json.toString());
            messaging.queueMessage(queueName, json.toString());
        } catch (IOException ioe) {
            log.error("Failed to parse message: {}", ioe.getMessage());
            throw new MessagingException(ioe);
        }
    }
    
    /**
     * To put events to subscriber queue
     * 
     * @param oid Object id
     * @param eventType type of events happened
     * @param context where the event happened
     * @param jsonFile Configuration file
     */
    private void sentMessage(String oid, String eventType) {
        Map<String, String> param = new LinkedHashMap<String, String>();
        param.put("oid", oid);
        param.put("eventType", eventType);
        param.put("username", "system");
        param.put("context", "HarvestClient");
        try {
            messaging.onEvent(param);
        } catch (MessagingException ex) {
            log.error("Unable to send message: ", ex);
        }
    }
    
    /**
     * Update the harvest file in storage if required
     * 
     * @param file The harvest file to store
     * @return DigitalObject The storage object with the file
     * @throws StorageException If storage failed
     */
    private DigitalObject updateHarvestFile(File file) throws StorageException {
        // Check the file in storage
        DigitalObject object = StorageUtils.checkHarvestFile(storage, file);
        //log.info("=== Check harvest file: '{}'=> '{}'", file.getName(), object);
        if (object != null) {
            // If we got an object back its new or updated
            JsonObject message = new JsonObject();
            message.put("type", "harvest-update");
            message.put("oid", object.getId());
            try {
                messaging.queueMessage("houseKeeping", message.toString());
            } catch (MessagingException ex) {
                log.error("Error sending message: ", ex);
            }
        } else {
            // Otherwise grab the existing object
            String oid = StorageUtils.generateOid(file);
            object = StorageUtils.getDigitalObject(storage, oid);
            //log.info("=== Try again: '{}'=> '{}'", file.getName(), object);
        }
        return object;
    }

	public void init(JsonSimpleConfig config) throws Exception {
		name = config.getString(null, "config", "name");
        QUEUE_ID = name;
        thread.setName(name);
        File sysFile = null;

        try {
            globalConfig = new JsonSimpleConfig();
            sysFile = JsonSimpleConfig.getSystemFile();

            // Load the indexer plugin
            String indexerId = globalConfig.getString(
                    "solr", "indexer", "type");
            if (indexerId == null) {
                throw new Exception("No Indexer ID provided");
            }
            indexer = PluginManager.getIndexer(indexerId);
            if (indexer == null) {
                throw new Exception("Unable to load Indexer '"+indexerId+"'");
            }
            indexer.init(sysFile);

            // Load the storage plugin
            String storageId = globalConfig.getString(
                    "file-system", "storage", "type");
            if (storageId == null) {
                throw new Exception("No Storage ID provided");
            }
            storage = PluginManager.getStorage(storageId);
            if (storage == null) {
                throw new Exception("Unable to load Storage '"+storageId+"'");
            }
            storage.init(sysFile);

        } catch (IOException ioe) {
            log.error("Failed to read configuration: {}", ioe.getMessage());
            throw ioe;
        } catch (PluginException pe) {
            log.error("Failed to initialise plugin: {}", pe.getMessage());
            throw pe;
        }

        try {
            messaging = MessagingServices.getInstance();
        } catch (MessagingException ex) {
            log.error("Failed to start connection: {}", ex.getMessage());
            throw ex;
        }

	}

	public void setPriority(int newPriority) {
		if (newPriority >= Thread.MIN_PRIORITY
                && newPriority <= Thread.MAX_PRIORITY) {
            thread.setPriority(newPriority);
        }
	}

}
