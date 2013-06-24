package au.com.redboxresearchdata.fascinator.plugins;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.redboxresearchdata.fascinator.harvester.GenericJsonHarvester;
import au.com.redboxresearchdata.fascinator.harvester.HarvestItem;
import au.com.redboxresearchdata.fascinator.mxbeans.JsonHarvestQueueMXBean;

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
public class JsonHarvestQueueConsumer implements GenericListener, JsonHarvestQueueMXBean {
	
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
    
    private JsonSimpleConfig config;
    
    private Map<String, HarvestItem> failedJsonMap;
    
    private List<String> failedJsonReq;
    
    private Map<String, GenericJsonHarvester> harvesters;

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

            producer = session.createProducer(null);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            connection.start();
            
            toolChainEntry = globalConfig.getString(DEFAULT_TOOL_CHAIN_QUEUE,
                    "messaging", "toolChainQueue");
            
            failedJsonMap = new HashMap<String, HarvestItem>();
            failedJsonReq = new ArrayList<String>();
            harvesters = new HashMap<String, GenericJsonHarvester>();
            // registering managed bean...
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName mxbeanName = new ObjectName("au.com.redboxresearchdata.fascinator.plugins:type=JsonHarvestQueue");
            mbs.registerMBean(this, mxbeanName);
            
        } catch (JMSException ex) {
            log.error("Error starting message thread!", ex);
        } catch (MalformedObjectNameException e) {
        	log.error("Error configuring MBean, invalid name", e);
		} catch (InstanceAlreadyExistsException e) {
			log.error("Error configuring MBean, instance exists.", e);
		} catch (MBeanRegistrationException e) {
			log.error("Error registering MBean. ", e);
		} catch (NotCompliantMBeanException e) {
			log.error("Error configuring Mbean, non-compliant!", e);
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
			processJsonText(text);
        } catch (JMSException jmse) {
            log.error("Failed to send/receive message: {}", jmse.getMessage());
        } catch (IOException ioe) {
            log.error("Failed to parse message: {}", ioe.getMessage());
        } catch (Exception ex) {
        	log.error("Failed to harvest object: {}", ex.getMessage());
        	log.error("Stack trace:", ex);
        }
	}

	protected void processJsonText(String text) throws IOException,
			HarvesterException, TransformerException, StorageException,
			MessagingException, Exception {
		JsonSimple json = new JsonSimple(text);
		processJson(json);
	}
	
	protected void logFailedRequest(String errmsg, JsonSimple json) {
		String text = json.toString();
		json.getJsonObject().put("error",  errmsg);
		log.error( errmsg + text);
		failedJsonReq.add(json.toString());
	}

	protected synchronized void processJson(JsonSimple json)
			throws IOException, PluginException, HarvesterException,
			TransformerException, StorageException, MessagingException,
			Exception {
		// prepare for the harvest
		String type = json.getString(null, "type");
		log.debug("Got request to process JSON of type:" + type);
		if (type == null) {
			logFailedRequest("No type specified, ignoring object....", json);
			return;
		}
		String configFilePath = globalConfig.getString(null, "portal", "harvestFiles") + "/" + type + ".json";
		File harvestConfigFile = new File(configFilePath);
		if (!harvestConfigFile.exists()) {
			logFailedRequest("Harvest config file not found, please check the set up. Ignoring object...", json);
			return;
		}
		log.info("Using config file path:" + configFilePath);
		JsonSimple harvestConfig = new JsonSimple(harvestConfigFile);
		String rulesFilePath = harvestConfig.getString(null, "indexer", "script", "rules");
		File rulesFile = new File(rulesFilePath);
		if (!rulesFile.exists()) {
			logFailedRequest("Rules file not found, please check the set up...", json);
			return;
		}
		log.info("Using rules file path:" + rulesFilePath);
		GenericJsonHarvester harvester = harvesters.get(type);
		if (harvester == null) {
			harvester = (GenericJsonHarvester) PluginManager.getHarvester(type, storage);
			if (harvester == null) {
				logFailedRequest("Harvester not found for type:" + type + ". Please check your configuration...", json);
				return;
			}
			harvester.init(harvestConfigFile);
			harvester.setShouldPark(config.getBoolean(false, "shouldPark"));
			harvesters.put(type, harvester);
		}
		JsonSimple data = new JsonSimple(json.getObject("data"));
		log.debug("Data json is:" + data.toString(true));
		List<HarvestItem> harvestList = harvester.harvest(data, type, harvestConfigFile, rulesFile);
		log.debug("Number of Objects in list:" + harvester.getItemList().size());
		log.debug("Number of Objects in harvest list:" + harvestList.size());
		log.debug("Number of Objects successfully harvested:" + harvester.getSuccessOidList().size());
		for (HarvestItem item : harvestList) {
			if (item.isShouldBeTransformed()) {
				String oid = item.getOid();
				transformObject(oid, false, harvestConfig);
				log.info("JSON Object on the toolchain, oid:" + oid);
			}
		}
		// check if there are any failed items...
		if ( harvester.getItemList().size() > 0 && 
			(harvester.getItemList().size() != harvestList.size())  
				|| (harvestList.size() != harvester.getSuccessOidList().size()) ) {
			log.error("There are items that failed to harvest..");
			for (HarvestItem item : harvester.getItemList()) {
				if (!item.isHarvested()) {
					JsonSimple jsonData = (JsonSimple) item.getData();
					JsonObject jsonObj = new JsonObject();
					jsonObj.put("type", type);
					JsonObject dataObj = new JsonObject();
					dataObj.put("data", jsonData.getJsonObject());
					jsonObj.put("data", dataObj);
					JsonSimple failedJson = new JsonSimple(jsonObj);
					item.setData(failedJson);
					log.error("Failed hid:" + item.getHid());
					failedJsonMap.put(item.getHid(), item);
					if (!item.isValid()) {
						// failed validation...
						log.error("Failed validation:" + failedJson.toString(true));
					}
					if (item.isValid() && !item.isHarvested()) {
						// exception thrown while harvesting...
						log.error("Failed harvest:" + failedJson.toString(true));
					}
				}
			}
			log.error(getFailedJsonList());
		}
	}
	
    /**
     * Place objects in the toolchain for transformation. Object eventually gets indexed.
     * 
     * @param oid Object Id
     * @param commit Flag to commit after indexing
     * @throws StorageException If storage is not found
     * @throws TransformerException If transformer fail to transform the object
     * @throws MessagingException If the object could not be queue'd
     */
    private void transformObject(String oid, boolean commit, JsonSimple harvestConfig)
            throws TransformerException, StorageException, MessagingException, Exception {
        // put in event log
        sentMessage(oid, "modify");

        // queue the object for indexing
        queueHarvest(oid, harvestConfig, commit, toolChainEntry);
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
    private void queueHarvest(String oid, JsonSimple harvestConfig, boolean commit,
            String queueName) throws MessagingException {
        JsonObject json = harvestConfig.getJsonObject();
		json.put("oid", oid);
		if (commit) {
		    json.put("commit", "true");
		}
		log.info("Sending message after harvest:");
		log.info(json.toString());
		messaging.queueMessage(queueName, json.toString());
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

	public void init(JsonSimpleConfig config) throws Exception {
		this.config = config;
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

	
	public void harvestJsonText(String text) {
		try {
			processJsonText(text);
		} catch (Exception e) {
			log.error("Error while harvesting JSON: ", e);
		}
	}

	public synchronized String getFailedJsonList() {
		return "[\"" + StringUtils.join(failedJsonMap.keySet(), "\",\"") + "\"]";
	}

	public synchronized String getFailedJsonText(String harvestId) {
		return failedJsonMap.get(harvestId).getData().toString();
	}

	public synchronized void setFailedJsonText(String harvestId, String json) {
		HarvestItem item = failedJsonMap.get(harvestId);
		try {
			item.setData(new JsonSimple(json));
		} catch (IOException e) {
			log.error("Please ensure this text is a valid JSON: " + json);
		}
	}

	/**
	 * Attempts a reharvest of the specified harvest id, removing it from the failed map.
	 *  
	 */
	public synchronized void reharvestFailedJson(String harvestId) {
		HarvestItem item = failedJsonMap.get(harvestId);
		failedJsonMap.remove(harvestId);
		JsonSimple jsonData = (JsonSimple)item.getData();
		try {
			processJson(jsonData);
		} catch (Exception ex) {
			log.error("Error while reharvesting JSON: " + jsonData.toString(true), ex);
		}
	}

	public synchronized void removeFailedJson(String harvestId) {
		failedJsonMap.remove(harvestId);
	}

	public synchronized String getFailedRequests() {
		return "[" + StringUtils.join(failedJsonReq, ",") + "]";
	}

	public synchronized void clearFailedRequests() {
		failedJsonReq.clear();
	}
		
}
