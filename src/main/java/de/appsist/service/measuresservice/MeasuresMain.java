package de.appsist.service.measuresservice;

import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;
import org.vertx.java.platform.Verticle;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;

import de.appsist.commons.event.*;
import de.appsist.commons.misc.StatusSignalConfiguration;
import de.appsist.commons.misc.StatusSignalSender;
import de.appsist.commons.util.EventUtil;
import de.appsist.service.iid.server.connector.IIDConnector;
import de.appsist.service.iid.server.model.*;
import de.appsist.service.iid.server.model.Notification.Builder;
import de.appsist.service.measuresservice.model.*;
import de.appsist.service.measuresservice.addresses.Addresses;
import de.appsist.service.measuresservice.queries.AdditionalSparQLQueries;
import de.appsist.service.measuresservice.queries.BasicSparQLQueries;

/*
 * This verticle is executed with the module itself, i.e. initializes all components required by the service.
 * The super class provides two main objects used to interact with the container:
 * - <code>vertx</code> provides access to the Vert.x runtime like event bus, servers, etc.
 * - <code>container</code> provides access to the container, e.g, for accessing the module configuration an the logging mechanism.  
 */
public class MeasuresMain
    extends Verticle
{
    // configuration object
    private JsonObject config;
    // routematcher with pre set base path
    private BasePathRouteMatcher routeMatcher;

    // basePath String
    private String basePath = "";

    // eventbus
    private EventBus eb = null;

    // logger object
    private static final Logger log = LoggerFactory.getLogger(MeasuresMain.class);
    // this Map stores session IDs of Logged in Users
    // userId, sessionId
    private Map<String, String> userSessionMap;

    // this Map stores session IDs of Logged in Users
    // sessionId, userId
    private Map<String, String> sessionUserID;

    // bridge to "InhalteInteraktionsdienst"
    private IIDConnector conn;

    // Demo section
    // ============
    // are we running in demo mode (==no connection to a machine)
    boolean isDemo = false;

    // this map stores demousers and corresponding machinestates
    // username, machinestate
    private Map<String, String> demoUserMachinestateMap;

    //
    private final Map<String, Set<String>> sessionAllowedMeasures = new HashMap<String, Set<String>>();
    private final Map<String, Set<String>> sessionMeasuresAllowedWithAssistance = new HashMap<String, Set<String>>();

    private final Map<String, Set<String>> workplaceGroupsSessions = new HashMap<String, Set<String>> ();
    private final Map<String, Set<String>> sessionWorkplaceGroups = new HashMap<String, Set<String>>();
    private final Map<String, Set<String>> sessionDevelopmentGoals = new HashMap<String, Set<String>>();
    private final Map<String, Set<String>> sessionStations = new HashMap<String, Set<String>>();
    private final Map<String, Set<String>> sessionMachines = new HashMap<String, Set<String>>();
    private final Map<String, Set<String>> sessionStates = new HashMap<String, Set<String>>();
    
    private final Map<String, Set<LocalState>> sessionLocalStates = new HashMap<String, Set<LocalState>>();
    
    private final Map<String, Set<StateMeasure>> sessionStateMeasures = new HashMap<String, Set<StateMeasure>>();
    
    private final Map<String, Set<String>> sessionMeasures = new HashMap<String, Set<String>>();
    
    // Set of users in Nebentätigkeit
    private final Set<String> usersInNebenzeit = new HashSet<String>();

    // map storing labels retrieved form ontology
    private final Map<String, String> labelMap = new HashMap<String, String>();

    // map storing a list of machine states for each user (session)
    private final Map<String, List<String>> sessionMachineStates = new HashMap<String, List<String>>();

    // map storing a LinkedHashSet of mandatory service items 
    private Map<String, LinkedHashSet<ServiceItem>> sessionMandatoryServiceItems = new HashMap<String, LinkedHashSet<ServiceItem>>();
    private Map<String, LinkedHashSet<String>> sessionMandatoryMeasureIds = new HashMap<String, LinkedHashSet<String>>();
    
    // for each session store the last presented service item catalogue
    Map<String,LinkedHashSet<ServiceItem>> sessionLastServiceItemList = new HashMap<String,LinkedHashSet<ServiceItem>>();
    
    Set<String> systemProcesses = new HashSet<String>();
    
	@Override
	public void start() {
        // load configuration
        if (container.config() != null && container.config().size() > 0) {
			config = container.config();
		} else {
                log.warn("Using default configuration");
			config = getDefaultConfiguration();
		}
        // init eventbus
        this.eb = vertx.eventBus();
        // init IID bridge
        this.conn = new IIDConnector(this.eb, IIDConnector.DEFAULT_ADDRESS);
        // set basePath
        this.basePath = config.getObject("webserver").getString("basePath");

        // init all handlers listening for eventbus events
		initializeEventBusHandler();
		
        // init all handlers listening for http(s) requests
		initializeHTTPRouting();
        // start the verticle webserver
		vertx.createHttpServer()
			.requestHandler(routeMatcher)
			.listen(config.getObject("webserver")
			.getInteger("port"));

        // initialize maps
        // this.clearedMeasures = new HashMap<String, Set<JsonNode>>();
        this.userSessionMap = new HashMap<String, String>();
        this.sessionUserID = new HashMap<String, String>();

        this.isDemo = config.getObject("demosettings").getBoolean("active", false);
        if (this.isDemo) {
            this.demoUserMachinestateMap = new HashMap<String, String>();
            JsonArray demoUsers = config.getObject("demosettings").getArray("demousers");
            Iterator<Object> demoUserIterator = demoUsers.iterator();
            while (demoUserIterator.hasNext()) {
                Object demoUserObject = demoUserIterator.next();
                if (demoUserObject instanceof JsonObject) {
                    JsonObject demoUserJsonObject = (JsonObject) demoUserObject;

                    this.demoUserMachinestateMap.put(demoUserJsonObject.getString("username"),
                            demoUserJsonObject.getString("machinestate"));
                }
            }
        }

        log.info("*******************");
        log.info(
                "  Maßnahmendienst auf Port " + config.getObject("webserver").getNumber("port")
                        + " gestartet ");
        log.info("                              *******************");
        
        JsonObject statusSignalObject = config.getObject("statusSignal");
        StatusSignalConfiguration statusSignalConfig;
        if (statusSignalObject != null) {
          statusSignalConfig = new StatusSignalConfiguration(statusSignalObject);
        } else {
          statusSignalConfig = new StatusSignalConfiguration();
        }

        StatusSignalSender statusSignalSender =
          new StatusSignalSender("measures-service", vertx, statusSignalConfig);
        statusSignalSender.start();
        
        
	}
	
	@Override
	public void stop() {
        container.logger().info("APPsist service \"Massnahmendienst\" has been stopped.");
	}
	
	private void retrieveExistingProcesses(){
		// call URL pki/processes
		HttpClient client = vertx.createHttpClient()
	               .setPort(8080)
	               .setKeepAlive(false);
		client.getNow("/services/pki/processes", new Handler<HttpClientResponse>() {
		public void handle(final HttpClientResponse resp) {
			final Buffer result = new Buffer();
		        resp.dataHandler(new Handler<Buffer>() {
		            public void handle(Buffer data) {
		                result.appendBuffer(data);
		            }
					
		        });
		        resp.endHandler(new Handler<Void>(){

					@Override
					public void handle(Void event) {
						// TODO Auto-generated method stub
						JsonObject resultObject = new JsonObject(result.toString());	
						  JsonArray processDefinitionArray = resultObject.getArray("processDefinitions");
						  for (int i=0; i!=processDefinitionArray.size(); i++){
							  systemProcesses.add(processDefinitionArray.get(i).toString());
						  }
					}
		        	
		        });
		        
		    }
		});
	}
	
	    /**
     * Create a configuration which is used if no configuration is passed to the module.
     * 
     * @return Configuration object.
     */
	private static JsonObject getDefaultConfiguration() {
		JsonObject defaultConfig =  new JsonObject();
		JsonObject webserverConfig = new JsonObject();
        JsonObject servicesConfig = new JsonObject();
        // default webserver configuration
        webserverConfig.putNumber("port", 7082);
        webserverConfig.putString("basePath", "/services/measures");
        webserverConfig.putString("statics", "/www");

        // default sparql configuration
        JsonObject sparqlConfig = new JsonObject();

        // default services configuration
        servicesConfig.putBoolean("proxy", false);
        servicesConfig.putBoolean("secure", false);
        servicesConfig.putString("host", "localhost");
        servicesConfig.putNumber("host", 8080);
        JsonObject servicePaths = new JsonObject();
        servicePaths.putString("psd", "/services/psd");
        servicesConfig.putObject("paths", servicePaths);

        defaultConfig.putObject("webserver", webserverConfig);
        defaultConfig.putObject("sparql", sparqlConfig);
        defaultConfig.putObject("services", servicesConfig);
		return defaultConfig;
	}
	
	
	
	/**
	 * In this method the handlers for the event bus are initialized.
	 */
	private void initializeEventBusHandler() {
        // handler for machinestate changes
        Handler<Message<JsonObject>> machineStateChangedHandler = new Handler<Message<JsonObject>>()
        {
            
        	public void handle(Message<JsonObject> jsonMessage)
            {
            	MachineStateChangedEvent msce = EventUtil.parseEvent(jsonMessage.body().toMap(),
                        MachineStateChangedEvent.class);

                log.debug("Received MachineStateChangedEvent " + msce.toString());
                if (!sessionUserID.isEmpty()) {
                    for (String uId : sessionUserID.values()) {

                        if (!usersInNebenzeit.contains(userSessionMap.get(uId))) {
                            processMachineStateChangedEvent(msce, false, uId);
                        }
                    }
                }

            }
        };

        this.eb.registerHandler(Addresses.MACHINE_STATE_CHANGED,
                machineStateChangedHandler);
		
        // handler for users coming online
        Handler<Message<JsonObject>> userOnlineEventHandler = new Handler<Message<JsonObject>>()
        {
            public void handle(Message<JsonObject> jsonMessage)
            {
                UserOnlineEvent uoe = EventUtil.parseEvent(jsonMessage.body().toMap(),
                        UserOnlineEvent.class);
                processUserOnlineEvent(uoe);
                // TODO: remove this line as soon as connection to real machines is available
                // triggers a new machinestate for demo purpose
                log.debug(" - User " + uoe.getUserId() + " with session "
                        + uoe.getSessionId() + " came online");
                sessionUserID.put(uoe.getSessionId(), uoe.getUserId());
                
                if (uoe.getUserId().indexOf("@fezto") != -1){
                	// add LoctiteWechseln
                	log.info("adding loctite wechseln");
                	getLabelForNewMandatoryServiceItem("http://www.appsist.de/ontology/festo/0b4e2ad2-09dc-11e5-a6c0-1697f925ec7b", uoe.getSessionId(), null);
                	// add FettfassWechseln
                	log.info("adding Fettfass wechseln");
                	getLabelForNewMandatoryServiceItem("http://www.appsist.de/ontology/festo/0b4e629a-09dc-11e5-a6c0-1697f925ec7b", uoe.getSessionId(), null);
                	log.info("adding Gehäuse von aussen reinigen");
                	getLabelForNewMandatoryServiceItem("http://www.appsist.de/ontology/StationsgehaeuseVonAussenReinigen", uoe.getSessionId(), null);
                	LinkedHashSet<String> lhs = new LinkedHashSet<String>();
                	lhs.add("http://www.appsist.de/ontology/festo/0b4e2ad2-09dc-11e5-a6c0-1697f925ec7b");
                	lhs.add("http://www.appsist.de/ontology/festo/0b4e629a-09dc-11e5-a6c0-1697f925ec7b");
                	lhs.add("http://www.appsist.de/ontology/StationsgehaeuseVonAussenReinigen");
                	sessionMandatoryMeasureIds.put(uoe.getSessionId(), lhs);
                }
                log.info("isDemo:"+isDemo);
                if (isDemo) {
                    // processMachineStateChangedEvent(null, true, uoe.getUserId());
                    requestUserInformation(uoe.getSessionId(), uoe.getUserId(), "token", false);
                }
            }
        };

        this.eb.registerHandler(Addresses.USER_ONLINE_EVENT,
                userOnlineEventHandler);

        // handler for users going offline
        Handler<Message<JsonObject>> userOfflineEventHandler = new Handler<Message<JsonObject>>()
        {
            public void handle(Message<JsonObject> jsonMessage)
            {
                log.debug(" userOfflineEventHandler jsonMessage"
                            + jsonMessage.toString());
                UserOfflineEvent uoe = EventUtil.parseEvent(jsonMessage.body().toMap(),
                        UserOfflineEvent.class);
                processUserOfflineEvent(uoe);
            }
        };

        this.eb.registerHandler(Addresses.USER_OFFLINE_EVENT,
                userOfflineEventHandler);

        // handler for users switching workstate
        Handler<Message<JsonObject>> userActivitySwitchHandler = new Handler<Message<JsonObject>>()
        {
            @Override
            public void handle(Message<JsonObject> jsonMessage)
            {
                JsonObject body = jsonMessage.body();
                String sessionId = body.getString("sessionId");
                String userId = sessionUserID.get(sessionId);
                boolean isNebenzeit = body.getString("activity").equals("side");
                if (isNebenzeit) {
                    usersInNebenzeit.add(sessionId);
                }
                else {
                    usersInNebenzeit.remove(sessionId);
                }
               
                requestUserInformation(sessionId, userId, "token",
                        isNebenzeit);
            }
        };
        this.eb.registerHandler(Addresses.USER_ACTIVITY_SWITCH,
                userActivitySwitchHandler);
        
        
        // handler for requested measures
        Handler<Message<JsonObject>> measureRequestedEventHandler = new Handler<Message<JsonObject>>()
        {
            public void handle(Message<JsonObject> jsonMessage)
            {
                MeasureRequestedEvent mre = EventUtil.parseEvent(jsonMessage.body().toMap(), MeasureRequestedEvent.class);
                processMeasureRequestedEvent(mre);
            }
        };

        this.eb.registerHandler(Addresses.MEASURE_REQUESTED,
                measureRequestedEventHandler);
        
     // handler for requested measures
        Handler<Message<JsonObject>> measureCompletedEventHandler = new Handler<Message<JsonObject>>()
        {
            public void handle(Message<JsonObject> jsonMessage)
            {
                MeasureCompletedEvent mce = EventUtil.parseEvent(jsonMessage.body().toMap(), MeasureCompletedEvent.class);
                processMeasureCompletedEvent(mce);
            }
        };

        this.eb.registerHandler(Addresses.MEASURE_COMPLETED,
                measureCompletedEventHandler);
	}
	
	/**
	 * In this method the HTTP API build using a route matcher.
	 */
	private void initializeHTTPRouting() {
        routeMatcher = new BasePathRouteMatcher(this.basePath);
        // trigger to list all available measures in UI.
        routeMatcher.get("/user/:uid/measures/", new Handler<HttpServerRequest>()
        {
            @Override
            public void handle(final HttpServerRequest request)
            {
                String userId = request.params().get("uid");

                processShowAllMeasures(userId);
                request.response().end("Show all measures triggered");
            }
        });
        
        routeMatcher.post("/action/surveyanswer", new Handler<HttpServerRequest>()
        {
            @Override
            public void handle(final HttpServerRequest request)
            {
                
            	final Buffer result = new Buffer(); 
            	request.dataHandler(new Handler<Buffer>() {
 		            public void handle(Buffer data) {
 		                result.appendBuffer(data);
 		            }
 					
 		        });
 		        request.endHandler(new Handler<Void>(){

 					@Override
 					public void handle(Void event) {
 						// TODO Auto-generated method stub
 						log.info(result);
 					}
 		        	
 		        });
            	
            	
                log.info(request.params().toString());
                request.response().end(request.params().toString());
            }
        });

        routeMatcher.getWithRegEx("/nebenzeit/*", new Handler<HttpServerRequest>()
        {
            @Override
            public void handle(final HttpServerRequest request)
            {
                findMeasuresSide();
                request.response().end("Find measures applicable in Nebenzeit");
            }
        });
        
        routeMatcher.getWithRegEx("/measureRequested/*", new Handler<HttpServerRequest>()
        {
            @Override
            public void handle(final HttpServerRequest request)
            {
            	Map<String, Object> contextMap  = new HashMap<String, Object>();
            	contextMap.put("context", new JsonObject().putString("jobId", "JobId"));
            	MeasureRequestedEvent mre = new MeasureRequestedEvent("testEvent", "http://www.appsist.de/ontology/festo/0b4e2ad2-09dc-11e5-a6c0-1697f925ec7b", "http://www.appsist.de/ontology/bul/ArbeitsplatzgruppeWerkzeugbau", null);
                processMeasureRequestedEvent(mre);
                request.response().end("Triggered MeasureRequestedEvent");
            }
        
        });
        
        routeMatcher.getWithRegEx("/measureCompleted/*", new Handler<HttpServerRequest>()
        {
            @Override
            public void handle(final HttpServerRequest request)
            {
            	MeasureCompletedEvent mce = new MeasureCompletedEvent("measureCompleteEvent", "http://www.appsist.de/ontology/festo/0b4e2ad2-09dc-11e5-a6c0-1697f925ec7b","http://www.appsist.de/ontology/bul/ArbeitsplatzgruppeWerkzeugbau", null); 
            	processMeasureCompletedEvent(mce);
                request.response().end("Triggered MeasureCompletedEvent");
            }
        
        });

        
        final String staticFileDirectory = config.getObject("webserver").getString("statics");

        routeMatcher.getWithRegEx("/.*", new Handler<HttpServerRequest>()
        {

            @Override
            public void handle(HttpServerRequest request)
            {
                String path = request.path();
                path = path.replace("/services/measureservice", "");
                request.response().sendFile(staticFileDirectory + path);
            }
        });
	}

    private void findMeasuresSide(){
	    Iterator<String> userSessionKeyIterator = this.userSessionMap.keySet().iterator();
        String uId = userSessionKeyIterator.next();

        final String sessionId = this.userSessionMap.get(uId);
        requestUserInformation(sessionId, uId, "token", true);
	}

    private void processMeasureRequestedEvent(MeasureRequestedEvent mre){
    	String newMeasureId = mre.getMeasureId();
    	boolean measureAdded = false;
//    	log.info("Target Group "+ mre.getTargetGroup()+ " exists in workplaceGroupSessions? "+ this.workplaceGroupsSessions.containsKey(mre.getTargetGroup()));
    	if(this.workplaceGroupsSessions.containsKey(mre.getTargetGroup())){
    		for (String sessionId: this.workplaceGroupsSessions.get(mre.getTargetGroup())){
        		if (this.sessionMandatoryMeasureIds.containsKey(sessionId)){
            		LinkedHashSet<String> measureIdSet = this.sessionMandatoryMeasureIds.get(sessionId);
            		if (!measureIdSet.contains(newMeasureId)){
            			measureIdSet.add(newMeasureId);
            			this.sessionMandatoryMeasureIds.remove(sessionId);
            			this.sessionMandatoryMeasureIds.put(sessionId, measureIdSet);
            			measureAdded = true;
            		}
            	} else {
            		LinkedHashSet<String> measureIdSet  = new LinkedHashSet<String>();
            		measureIdSet.add(newMeasureId);
            		this.sessionMandatoryMeasureIds.put(sessionId, measureIdSet);
            		measureAdded = true;
            	}
            	if (measureAdded){
            		getLabelForNewMandatoryServiceItem(mre.getMeasureId(), sessionId, mre.getContext());
            	}
            }
    	} 
    }
    	
    
    
    
    private void getLabelForNewMandatoryServiceItem(final String measureId, final String sessionId, final Map<String,Object> context){
    	
    	Handler<Message<String>> labelHandler = new Handler<Message<String>>(){

            @Override
            public void handle(Message<String> arg0)
            {
                // build HashMap with measure/label
                JsonObject jsonObject = new JsonObject(arg0.body());
                JsonArray jsonArray = jsonObject.getObject("results").getArray("bindings");
                Iterator<Object> jsonArrayIterator = jsonArray.iterator();
                while (jsonArrayIterator.hasNext()) {
                    Object currentObject = jsonArrayIterator.next();
                    if (currentObject instanceof JsonObject) {
                        JsonObject currentJsonObject = (JsonObject) currentObject;
                        String measure = currentJsonObject.getObject("oc")
                                .getString("value");
                        String label = currentJsonObject.getObject("label")
                                .getString("value");
                        log.debug("label "+ label + " for measure "+measure+" found");
                        labelMap.put(measure, label);
                    }
                }
                // continue adding mandatory service item
                addMandatoryServiceItem(measureId, getLabelFor(measureId), sessionId, context);
             }
    	};
    	
    	if (labelMap.containsKey(measureId)){
    		log.debug("label for "+measureId+" found in labelMap");
    		addMandatoryServiceItem(measureId, getLabelFor(measureId), sessionId, context);
    	} else {
    		BasicSparQLQueries.getLabelFor("{<"+measureId+">}", "de", eb, labelHandler);
    	}
    }
    
    private void processMeasureCompletedEvent(MeasureCompletedEvent mce){
    	String measureId = mce.getMeasureId();
    	if (this.workplaceGroupsSessions.containsKey(mce.getTargetGroup())){
    		Set<String> sessionIds = this.workplaceGroupsSessions.get(mce.getTargetGroup());
        	for (String sessionId: sessionIds){
        		removeMandatoryServiceItem(measureId, sessionId);
            	conn.purgeServiceItems(sessionId, "measures-service", null);
                conn.addServiceItems(sessionId, concatenateServiceItemSets(sessionId), null);
        	}
    	} else{
    		log.info("No user in WPG");
    	}
    }
    
    private void processShowAllMeasures(final String uId)
    {
        Handler<Message<String>> handleMeasuresResult = new Handler<Message<String>>()
        {

            @Override
            public void handle(Message<String> arg0)
            {
                processAllMeasuresList(arg0.body(), uId);
            }
        };
        BasicSparQLQueries.getAllMeasures(eb, handleMeasuresResult);
    }

    private void processAllMeasuresList(String messageString, String uId)
    {
        JsonObject jo = new JsonObject(messageString);
        JsonArray ja = jo.getObject("results").getArray("bindings");
        ArrayList<String> measureList = new ArrayList<String>();
        Iterator<Object> jaIterator = ja.iterator();
        while (jaIterator.hasNext()) {
            Object jaObject = jaIterator.next();
            if (jaObject instanceof JsonObject) {
                JsonObject jaJo = (JsonObject) jaObject;
                measureList.add(jaJo.getObject("massnahme").getString("value"));
            }
        }


        final String sessionId = this.userSessionMap.get(uId);
        this.sessionStateMeasures.remove(sessionId);
        retrieveLabelsFor(measureList, sessionId);
    }

    private void processMachineStateChangedEvent(MachineStateChangedEvent msce, boolean demo,
            final String uId)
    {
        Handler<Message<String>> handleMeasuresForStatesResult = new Handler<Message<String>>()
        {

            @Override
            public void handle(Message<String> arg0)
            {
                processReceivedMeasureList(arg0.body(), uId);
            }
        };

        String newMachineState = "";
        if (!demo) {
            newMachineState = "{";
            String machineStates = msce.getMachineState();
            
            machineStates = machineStates.substring(1, machineStates.length() - 1);
            for (String ms : machineStates.split(",")) {
            	log.debug("Processing machinestate: "+ms);
                if (ms.indexOf("Federmagazin") != -1) {
                    newMachineState += "(<http://www.appsist.de/ontology/demonstrator/FedermagazinLeer> <http://www.appsist.de/ontology/demonstrator/StationMontage> 9)";
                }
                else if (ms.indexOf("Deckelmagazin") != -1) {
                    newMachineState += "(<http://www.appsist.de/ontology/demonstrator/DeckelmagazinLeer> <http://www.appsist.de/ontology/demonstrator/StationMontage> 9)";
                }
                else if (ms.indexOf("Tuer") != -1) {
                    newMachineState += "(<http://www.appsist.de/ontology/demonstrator/TuerOffen> <http://www.appsist.de/ontology/demonstrator/StationMontage> 9)";
                }
                else if (ms.indexOf("Bauteil fehlt") != -1) {
                	newMachineState += "(<http://www.appsist.de/ontology/mbb/FehlendesBauteil> <http://www.appsist.de/ontology/mbb/MBBProduktionsanlage> 9)";
                }
                else if (ms.indexOf("Bauteil verloren") != -1) {
                    newMachineState += "(<http://www.appsist.de/ontology/demonstrator/Bauteilverlust> <http://www.appsist.de/ontology/demonstrator/StationMontage> 1)";
                }
                else if (ms.indexOf("Fett") != -1) {
                    newMachineState += "(<http://www.appsist.de/ontology/festo/FettLeer> <http://www.appsist.de/ontology/festo/S30> 10)";
                }
                else if (ms.indexOf("Loctite") != -1) {
                    newMachineState += "(<http://www.appsist.de/ontology/festo/LoctiteLeer> <http://www.appsist.de/ontology/festo/S20> 10)";
                }
                else if (ms.indexOf("Funktionsfähiger") != -1 || ms.indexOf("Funktionsfaehiger") != -1) {
                    newMachineState += "(<http://www.appsist.de/ontology/FunktionsfaehigerZustand> <http://www.appsist.de/ontology/demonstrator/StationMontage> 1)";
                }
                else {
                	log.warn("Received unknown machinestate: "+ms);
                }
            }
            newMachineState += "}";

        }
        else {

            if (this.demoUserMachinestateMap.containsKey(uId)) {
                newMachineState = this.demoUserMachinestateMap.get(uId);
            }
            else {
                // provide default machinestate
            		newMachineState = "{Warnungsfrei}";
                
            }

        }
            log.debug("set new machinestate:" + newMachineState);
            if (!newMachineState.equals("{}")){
            	BasicSparQLQueries.getMeasuresForStates(newMachineState, eb, 
                        handleMeasuresForStatesResult);
            } 
        

        // we currently assume that new machine state is related to the user currently logged
        // into the system

    }

    // rule 2.1.1.3
    private void processReceivedMeasureList(String messageString, String uId)
    {
    	
    	log.debug("MessageString: "+messageString);
        JsonObject jo = new JsonObject(messageString);
        // JsonObject message = new JsonObject();
        String sId = this.userSessionMap.get(uId);

        // parse messageString to a set of StateMeasure
        // resulting set only contains allowed measures
        // TODO: add additional information if measure is only allowed with assistance
        Set<StateMeasure> stateMeasureSet = measureListToStateMeasuresSet(jo, sId);
        this.sessionStateMeasures.put(sId, stateMeasureSet);
        
        // log.debug("stateMeasureSet:" + stateMeasureSet);
        retrieveLabelsFor(this.sessionStateMeasures.get(sId), sId);
    }
    
    private Set<StateMeasure> measureListToStateMeasuresSet(JsonObject measureList, String sId)
    {
        SortedSet<StateMeasure> sortedResult = new TreeSet<StateMeasure>(
                new StateMeasureDistanceComparator());
        JsonArray resultArray = measureList.getObject("results").getArray("bindings");
        Set<String> allowedMeasuresForUser = this.sessionAllowedMeasures.get(sId);
        log.debug("allowed measures: "+ allowedMeasuresForUser);
        Map<String, Map<String, Set<String>>> aggregatedStateMeasures = new HashMap<String, Map<String, Set<String>>>();
        Map<String, Integer> measurePriority = new HashMap<String, Integer>();
        Map<String, Integer> measureDistance = new HashMap<String, Integer>();
        Map<String, String> measureDeviceState = new HashMap<String, String>();
        Set<String> measureSet = new HashSet<String>();
        for (Object resultArrayObject : resultArray) {
            if (resultArrayObject instanceof JsonObject) {
                JsonObject resultArrayJsonObject = (JsonObject) resultArrayObject;
                // log.debug(resultArrayJsonObject.encodePrettily());
                String measureId = resultArrayJsonObject.getObject("massnahme").getString("value","");
                try {
                	URI uri = new URI(measureId);
                } catch (Exception e){
                	continue;
                }
                measureSet.add(measureId);
                
                String stationId = "";
                if (null != resultArrayJsonObject.getObject("station")) {
                    stationId = resultArrayJsonObject.getObject("station").getString("value");
                }
                String anlageId = resultArrayJsonObject.getObject("anlage").getString("value");
                String deviceState = resultArrayJsonObject.getObject("massnahmenzustand")
                        .getString("value");
                int priority = Integer
                        .parseInt(resultArrayJsonObject.getObject("prioritaet").getString("value"));
                int distance = Integer
                        .parseInt(resultArrayJsonObject.getObject("distance").getString("value"));
                if (!measurePriority.keySet().contains(measureId)) {
                    measurePriority.put(measureId, priority);
                }
                if (!measureDistance.keySet().contains(measureId)) {
                    measureDistance.put(measureId, distance);
                }
                if (!measureDeviceState.keySet().contains(measureId)) {
                    measureDeviceState.put(measureId, deviceState);
                }
                
                StateMeasure sm = new StateMeasure(measureId, anlageId, stationId, deviceState,
                        priority,
                        distance);
                String smMeasureId = sm.getMeasureId();
                if (allowedMeasuresForUser.contains(sm.getMeasureId()) || this.sessionUserID.get(sId).indexOf("@festo") != -1) {
                    String smAnlageId = sm.getAnlageId();
                    String smStationId = sm.getStationId();
                    if (aggregatedStateMeasures.keySet().contains(smMeasureId)) {
                        Map<String, Set<String>> anlageStationenMap = aggregatedStateMeasures.get(smMeasureId);
                        if (anlageStationenMap.keySet().contains(smAnlageId)) {
                            Set<String> anlageStationenSet = anlageStationenMap.get(smAnlageId);
                            anlageStationenSet.add(smStationId);
                        }
                        else {
                            Set<String> anlageStationenSet = new HashSet<String>();
                            anlageStationenSet.add(smStationId);
                            anlageStationenMap.put(smAnlageId, anlageStationenSet);
                        }

                    }
                    else {
                        Map<String, Set<String>> anlageStationenMap = new HashMap<String, Set<String>>();
                        Set<String> anlageStationenSet = new HashSet<String>();
                        anlageStationenSet.add(smStationId);
                        anlageStationenMap.put(smAnlageId, anlageStationenSet);
                        aggregatedStateMeasures.put(smMeasureId, anlageStationenMap);
                    }
                    sortedResult.add(sm);
                } else{
                    // log.debug("measure not allowed: "+measureId);
                }
            }
        }// end of for loop
        this.sessionMeasures.put(sId, measureSet);
        sortedResult.clear();
        for (String smMeasure : aggregatedStateMeasures.keySet()) {
            StateMeasure test = new StateMeasure(smMeasure, measureDeviceState.get(smMeasure), measurePriority.get(smMeasure), measureDistance.get(smMeasure), aggregatedStateMeasures.get(smMeasure));
            sortedResult.add(test);
         //   log.debug("StateMeasureTest2: " + test.printAnlageStationenMapAsJson());
        }
        return sortedResult;
    }
    
    
    // method to build ServiceItemsList
    private void buildServiceItems(Set<StateMeasure> stateMeasuresSet, String sessionId)
    {
        List<ServiceItem> serviceItemList = new ArrayList<ServiceItem>();
        int priority = 1;
        if (this.sessionMandatoryServiceItems.containsKey(sessionId)){
        	priority = this.sessionMandatoryServiceItems.get(sessionId).size();
        	priority++;
        }
        
        conn.purgeNotifications(sessionId, null);
        for (StateMeasure stateMeasure : stateMeasuresSet) {
            
            String measureId = stateMeasure.getMeasureId();
            if (this.systemProcesses.contains(measureId.substring(measureId.lastIndexOf("/")+1))){
            	InstructionItemBuilder iib = new InstructionItemBuilder();
            	iib.setTitle(getLabelFor(measureId));
                iib.setId("ms-" + priority);
                iib.setPriority(priority++);
                iib.setService("measures-service");

                String defaultNotificationMessage = "Maßnahme notwendig: " + getLabelFor(measureId);
                // set preview image
                iib.setImageUrl(getPreviewImage(measureId));
                if (measureId.indexOf("/demonstrator/")!=-1){
                    // select preview image for Festo Demonstrator
                    if (measureId.indexOf("Druckluft")!=-1){
                        defaultNotificationMessage = "Fehler am Deckelmagazin";
                    } else if (measureId.indexOf("Federmagazin")!=-1){
                        defaultNotificationMessage = "Federmagazin leer";
                    } else if (measureId.indexOf("TuerImLaufenden")!=-1){
                        defaultNotificationMessage = "Tür im laufenden Betrieb geöffnet";
                    } else if (measureId.indexOf("ba66718d-607c-4c81-82d7-f1a55fbbef3f") != -1){
                        defaultNotificationMessage = "Bauteil verloren";
                    }
                }
                
                // send notification to IID
                Builder notificationBuilder = new Builder("nms-" + priority, defaultNotificationMessage,
                        Level.ERROR);
                notificationBuilder.setRelatedItem(measureId.substring(measureId.lastIndexOf("/") + 1));
                conn.notify(sessionId, notificationBuilder.build(), null);

                iib.setSite(getLabelFor(stateMeasure.getAnlageId()));
                iib.setStation(getLabelFor(stateMeasure.getStationId()));

                StringBuilder postActionStringBuilder = new StringBuilder(300);
                JsonObject serviceConfig = config.getObject("services");
                if (serviceConfig.getBoolean("secure")) {
                    postActionStringBuilder.append("https://");
                }
                else {
                    postActionStringBuilder.append("http://");
                }
                postActionStringBuilder.append(serviceConfig.getString("host"));

                if (!serviceConfig.getBoolean("proxy")) {
                    postActionStringBuilder.append(":")
    .append(serviceConfig.getNumber("port").toString());
                }

                postActionStringBuilder.append(serviceConfig.getObject("paths").getString("psd"))
                        .append("/startSupport/")
                        .append(measureId.substring(measureId.lastIndexOf("/") + 1));
                HttpPostAction hpa = new HttpPostAction(postActionStringBuilder.toString(), null);
                iib.setAction(hpa);
                InstructionItem ii = iib.build();

                serviceItemList.add(ii);
        	}

        }
        
        // store current service item catalogue
        LinkedHashSet<ServiceItem> serviceItemsToStore = new LinkedHashSet<ServiceItem>();
        // TODO: add mandatory service items
        // add service item list
        serviceItemsToStore.addAll(serviceItemList);
    	this.sessionLastServiceItemList.remove(sessionId);
    	this.sessionLastServiceItemList.put(sessionId, serviceItemsToStore);

        
    	if (this.sessionUserID.get(sessionId).indexOf("@bul.de")!= -1 ){
    		
    		if (null == this.sessionMandatoryServiceItems.get(sessionId) || this.sessionMandatoryServiceItems.get(sessionId).isEmpty()){
    			Map<String, Object> jobId = new HashMap<String, Object>();
            	jobId.put("jobId",	"12345");
            	MeasureRequestedEvent mre = new MeasureRequestedEvent(UUID.randomUUID().toString(), "http://www.appsist.de/ontology/bul/cffbc9dc-1f37-11e5-b5f7-727283247c7f", "http://www.appsist.de/ontology/bul/ArbeitsplatzgruppeWerkzeugbau", jobId);
            	processMeasureRequestedEvent(mre);
    		}
        }
    	// clear and update Service-Items catalogue
        
        conn.purgeServiceItems(sessionId, "measures-service", null);
        conn.addServiceItems(sessionId, concatenateServiceItemSets(sessionId), null);
    }

    // return label for entry (if available)
    // or entry ID (otherwise)
    private String getLabelFor(String entry)
    {
    	if (labelMap.containsKey(entry)) {
            return labelMap.get(entry);
        }
        return entry;
    }

    // method to build ServiceItemsList
    private void buildServiceItems(Map<String, String> measureLabels, String sessionId)
    {
    	List<ServiceItem> serviceItemList = new ArrayList<ServiceItem>();
        int priority = 1;
        Set<String> measureKeySet = new LinkedHashSet<String>();
        if (this.sessionMandatoryServiceItems.containsKey(sessionId)){
        	serviceItemList.addAll(this.sessionMandatoryServiceItems.get(sessionId));
        	priority = this.sessionMandatoryServiceItems.get(sessionId).size();
        	priority++;
        }
   
        measureKeySet.addAll(measureLabels.keySet());
        
        for (String measure : measureKeySet) {
        	if (this.systemProcesses.contains(measure.substring(measure.lastIndexOf("/")+1))){
                InstructionItemBuilder iib = new InstructionItemBuilder();
                iib.setTitle(measureLabels.get(measure));
                iib.setId("ms-" + priority);
                iib.setPriority(priority++);
                iib.setService("measures-service");
                iib.setImageUrl(getPreviewImage(measure));

                StringBuilder postActionStringBuilder = new StringBuilder(300);
                JsonObject serviceConfig = config.getObject("services");
                if (serviceConfig.getBoolean("secure")) {
                    postActionStringBuilder.append("https://");
                }
                else {
                    postActionStringBuilder.append("http://");
                }
                postActionStringBuilder.append(serviceConfig.getString("host"));
                if (!serviceConfig.getBoolean("proxy")) {
                    postActionStringBuilder.append(":")
                            .append(serviceConfig.getNumber("port").toString());
                }

                postActionStringBuilder.append(serviceConfig.getObject("paths").getString("psd"))
                        .append("/startSupport/")
                        .append(measure.substring(measure.lastIndexOf("/") + 1));
                HttpPostAction hpa = new HttpPostAction(postActionStringBuilder.toString(), null);
                iib.setAction(hpa);
                InstructionItem ii = iib.build();
                serviceItemList.add(ii);
        	}
        }
        
     // store current service item catalogue
        LinkedHashSet<ServiceItem> serviceItemsToStore = new LinkedHashSet<ServiceItem>();
        // TODO: add mandatory service items
        // add service item list
        serviceItemsToStore.addAll(serviceItemList);
    	this.sessionLastServiceItemList.remove(sessionId);
    	this.sessionLastServiceItemList.put(sessionId, serviceItemsToStore);
        
        conn.purgeServiceItems(sessionId, "measures-service", null);
        conn.addServiceItems(sessionId, concatenateServiceItemSets(sessionId), null);
        
        
    }
    
    private String getPreviewImage(String measure){
    	String previewString= "/services/cds/static/appsist-comingsoon.png";
    	
    	if (measure.indexOf("/bul/") != -1) {
    		if (measure.indexOf("cffb772a-1f37-11e5-b5f7-727283247c7f")!= -1){
    			previewString="/services/cds/static/bul-massnahme.png";
    		} else{
    			previewString = "/services/cds/static/vorschau_greiferschienenmontage.jpg";
    			
    		}
             
         }
         else if (measure.indexOf("/festo/") != -1) {
         	 if (measure.indexOf("0b4e629a-09dc-11e5-a6c0-1697f925ec7b") != -1) {
                  previewString= "/services/cds/static/startbildschirm_fettfassbehaelter_quadratisch.jpg";
              }  else if (measure.indexOf("0b4e2ad2-09dc-11e5-a6c0-1697f925ec7b") != -1) {
                  previewString="/services/cds/static/startbildschirm_loctite_hand_quadratisch.jpg";
              } else {
                  previewString = "/services/cds/static/festo-massnahme.jpg";
              }
         }
         else if (measure.indexOf("/lps/") != -1) {
        	 previewString="/services/cds/static/lps-massnahme.jpg";
        	 if (measure.indexOf("a8824a7a-1feb-11e5-b5f7-727283247c7f") != -1 ){
        		 previewString="/services/cds/static/vorschaubild_schiebehaube_offen.jpg";
        	 } 
         }
         else if (measure.indexOf("/mbb/") != -1) {
        	 if(measure.indexOf("567fa292-1f2b-11e5-b5f7-727283247c7f") != -1){
         		// Schweissbrand
         		previewString = "/services/cds/static/vorschaubild_schweissbrand.png";
         	} else if (measure.indexOf("567f5c4c-1f2b-11e5-b5f7-727283247c7f") != -1){
         		previewString = "/services/cds/static/FehlendesBauteil.jpg";
         	} 
         } else if (measure.indexOf("/demonstrator/")!=-1){
             // select preview image for Festo Didaktik Demonstrator
             if (measure.indexOf("efefd3bb-5d78-4106-ad82-d50d1f22a1c8")!=-1){
                 previewString="/services/cds/static/vorschaubild_druckluft.jpg";
             } else if (measure.indexOf("67bbe15a-387c-4342-b8fc-2c4a6a779a09")!=-1){
                 previewString="/services/cds/static/vorschaubild_federmagazin.jpg";
             } else if (measure.indexOf("b55be936-8192-42bc-b7f3-cef947b6f4ce")!=-1){
                 previewString="/services/cds/static/vorschaubild_tuer_geoeffnet.jpg";
             } else if (measure.indexOf("ba66718d-607c-4c81-82d7-f1a55fbbef3f") != -1){
                previewString = "/services/cds/static/vorschaubild_bauteilverlust.jpg";
             } 
         }
         
    	 return previewString;
    }
    
    
    
    // after user logs in -> store user and session
    private void processUserOnlineEvent(UserOnlineEvent uoe)
    {
        String user = uoe.getUserId();
        this.userSessionMap.put(user, uoe.getSessionId());
        retrieveExistingProcesses();
    }
    
    // after user logs out -> remove entries from maps
    private void processUserOfflineEvent(UserOfflineEvent uoe)
    {

        String user = uoe.getUserId();
        log.debug("User " + user + " went offline");
        String userSession = uoe.getSessionId();
        this.userSessionMap.remove(user);
        this.sessionDevelopmentGoals.remove(userSession);
        this.sessionLocalStates.remove(userSession);
        this.sessionMachines.remove(userSession);
        this.sessionStates.remove(userSession);
        this.sessionStations.remove(userSession);
        this.sessionUserID.remove(userSession);
        if (this.sessionWorkplaceGroups.containsKey(userSession)){
        	this.workplaceGroupsSessions.remove(this.sessionWorkplaceGroups.get(userSession));
        }
        this.sessionMandatoryMeasureIds.remove(userSession);
        this.sessionMandatoryServiceItems.remove(userSession);
        this.sessionWorkplaceGroups.remove(userSession);
        
        // lower traffic on eventbus
        // only send message if current user is a demo user
        JsonObject userOfflineData = new JsonObject();
        userOfflineData.putString("sessionId", userSession);
        userOfflineData.putString("userId", user);
        // TODO: add token later (currently not available from Event)
        // userOfflineData.putString("token", userSessionToken);
        // informs LearnEventListener and UserModelService about user going Offline
        eb.publish(Addresses.USER_OFFLINE, userOfflineData);
    }

    private void requestUserInformation(String sessionId, String userId, String token,
            final Boolean isNebenzeit)
    {
        this.sessionUserID.put(sessionId, userId);
        JsonObject request = new JsonObject();
        request.putString("sid", sessionId);
        request.putString("userId", userId);
        request.putString("token", token);
        Handler<Message<JsonObject>> userInformationHandler = new Handler<Message<JsonObject>>()
        {

            @Override
            public void handle(Message<JsonObject> message)
            {
                JsonObject messageBody = message.body();
                processUserInformation(messageBody, isNebenzeit);

            }

        };
        log.debug(" Sending request for userInformation" + request);
        eb.send(Addresses.USER_GET_INFORMAION, request, userInformationHandler);
    }
    
    private void processUserInformation(JsonObject messageBody, final Boolean isNebenzeit)
    {
        JsonObject userInformation = messageBody.getObject("userInformation");
        // store information about user in corresponding maps
        String sessionId = messageBody.getString("sid");
        if (!isNebenzeit) {
            try {
                Set<String> apgs = new ObjectMapper()
                        .readValue(userInformation.getString("workplaceGroups"), Set.class);
                Set<String> devgoals = new ObjectMapper()
                        .readValue(userInformation.getString("developmentGoals"), Set.class);
                Set<String> allowedMeasures = new ObjectMapper()
                        .readValue(userInformation.getString("allowedMeasures"), Set.class);
                Set<String> allowedMeasuresWithAssistance = new ObjectMapper().readValue(
                        userInformation.getString("allowedMeasuresWithAssistance"), Set.class);
                // log.debug(" - developmentgoals: " + devgoals);
                // log.debug(" - workplaceGroups: " + apgs);
                // log.debug(" - allowedMeasures: " + allowedMeasures);
                // log.debug(" - allowedMeasuresWithAssistance: "
                // + allowedMeasuresWithAssistance);
                if (null != devgoals) {
                    sessionDevelopmentGoals.put(sessionId, devgoals);
                }
                else {
                    log.warn("Keine Entwicklungsziele angegeben");
                }

                if (null != apgs) {
                    this.sessionWorkplaceGroups.put(sessionId, apgs);
                    for (String apg:apgs){
                    	Set<String> sessions;
                    	if (!this.workplaceGroupsSessions.containsKey(apg)){
                    		sessions = new HashSet<String>();
                    		sessions.add(sessionId);
                    		this.workplaceGroupsSessions.put(apg, sessions);
                    	} else{
                    		sessions = this.workplaceGroupsSessions.get(apg);
                    		sessions.add(sessionId);
                    	}
                    	this.workplaceGroupsSessions.put(apg, sessions);
                    }
                }
                else {
                    log.error("Keine Arbeitsplatzgruppe vorhanden");
                }

                if (null != allowedMeasures) {
                    sessionAllowedMeasures.put(sessionId, allowedMeasures);
                }
                else {
                    log.warn("Keine Freischaltungen vorhanden");
                }
                if (null != allowedMeasuresWithAssistance) {
                    sessionMeasuresAllowedWithAssistance.put(sessionId,
                            allowedMeasuresWithAssistance);
                }
                else {
                    log.warn("Keine Freischaltungen mit Assistenz vorhanden");
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            // log.debug(" userInformation currentWorkstate: "
            // + userInformation.getString("currentWorkstate"));
            requestStationsInWorkplaceGroups(sessionId);
        }
        else {
            JsonObject developmentGoalsObject = new JsonObject(
                    userInformation.getString("developmentGoalsObject"));
            String positionGoal = developmentGoalsObject.getString("position");
            if (this.sessionDevelopmentGoals.containsKey(sessionId)) {
                this.sessionDevelopmentGoals.remove(sessionId);
            }
            HashSet<String> set = new HashSet<String>();
            set.add(positionGoal);
            this.sessionDevelopmentGoals.put(sessionId, set);
            requestMeasuresForPositionDG(sessionId);
        }
    }

    private void requestMeasuresForPositionDG(final String sessionId)
    {
        Set<String> workingSet = this.sessionDevelopmentGoals.get(sessionId);
        // log.debug("workingSet: " + workingSet);
        if (null != workingSet && !workingSet.isEmpty()) {
            String sg = "{";
            for (String positionGoal : workingSet) {
                // log.debug("positiongoal " + positionGoal);
                sg = sg + " " + sparqlPrefix(positionGoal);
            }
            sg = sg + " }";
            Handler<Message<String>> measuresForPositionDGHandler = new Handler<Message<String>>()
            {

                @Override
                public void handle(Message<String> stringMessage)
                {
                    JsonObject result = new JsonObject(stringMessage.body());
                    Set<String> resultSet = new HashSet<String>();
                    JsonArray messageArray = result.getObject("results").getArray("bindings");
                    Iterator<Object> messageArrayIterator = messageArray.iterator();
                    while (messageArrayIterator.hasNext()) {
                        Object currentArrayEntry = messageArrayIterator.next();
                        if (currentArrayEntry instanceof JsonObject) {
                            resultSet.add(((JsonObject) currentArrayEntry).getObject("m")
                                    .getString("value"));
                        }
                        else {
                            log.error("Expected JsonObject. Found "
                                        + currentArrayEntry.getClass());
                        }
                    }
                    // ArrayList<String> finalMeasureList = new ArrayList<String>();
                    // finalMeasureList.addAll(resultSet);
                    removeMasteredMeasures(resultSet, sessionId);

                }

            };
            AdditionalSparQLQueries.getMeasuresForPosition(sg, eb, measuresForPositionDGHandler);
        }
        else {
            removeMasteredMeasures(new HashSet<String>(), sessionId);
        }
    }


    // rule 2.1.2.3 in adaptionsregeln
    private void removeMasteredMeasures(Set<String> measureSet, String sessionId)
    {
        JsonObject request = new JsonObject();
        request.putString("sid", sessionId);
        request.putString("userId", this.sessionUserID.get(sessionId));
        request.putString("token", "token");
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        try {
            request.putString("processList", ow.writeValueAsString(measureSet));
        }
        catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Handler<Message<JsonObject>> removeMasteredProcessesHandler = new Handler<Message<JsonObject>>()
        {

            @Override
            public void handle(Message<JsonObject> message)
            {
                JsonObject messageBody = message.body();
                Set<String> rps = new HashSet<String>();
                try {
                    rps = new ObjectMapper().readValue(messageBody.getString("remainingProcesses"),
                            Set.class);
                }
                catch (JsonParseException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                catch (JsonMappingException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                ArrayList<String> finalMeasureList = new ArrayList<String>();
                finalMeasureList.addAll(rps);
                // log.debug(" finalMeasureList in removeMasteredMeasures - "
                // + finalMeasureList.toString());
                retrieveLabelsFor(finalMeasureList, messageBody.getString("sid"));

            }

        };

        eb.send(Addresses.REMOVE_MASTERED_PROCESSES, request,
                removeMasteredProcessesHandler);
    }

    // rule 2.1.1.1 in document adaptionsregeln
    private void requestStationsInWorkplaceGroups(final String sessionId)
    {

        String workplaceGroups = "{";
        for (String workplaceGroup : sessionWorkplaceGroups.get(sessionId)) {
            workplaceGroups += " " + sparqlPrefix(workplaceGroup);
        }
        workplaceGroups += "}";

        Handler<Message<String>> stationsInWorkplaceGroupsHandler = new Handler<Message<String>>()
        {

            @Override
            public void handle(Message<String> stringMessage)
            {
                JsonObject result = new JsonObject(stringMessage.body());
                Set<String> resultSet = new HashSet<String>();
                JsonArray messageArray = result.getObject("results").getArray("bindings");
                Iterator<Object> messageArrayIterator = messageArray.iterator();
                while (messageArrayIterator.hasNext()) {
                    Object currentArrayEntry = messageArrayIterator.next();
                    if (currentArrayEntry instanceof JsonObject) {
                    	JsonObject arrayEntryObject = (JsonObject) currentArrayEntry;
                    	log.debug("Adding station to Workplace Group: "+arrayEntryObject.encodePrettily());
                        resultSet.add((arrayEntryObject.getObject("device").getString("value")));
                    }
                    else {
                            log.error("Expected JsonObject. Found " + currentArrayEntry.getClass());
                    }
                }
                // log.debug(" stations in workplacegroup:" + resultSet);
                sessionStations.put(sessionId, resultSet);
                requestCurrentStatesForStations(sessionId, resultSet);
            }

        };
        log.debug(" stations in workplacegroup:" + workplaceGroups);

        BasicSparQLQueries.getStationsInWorkplaceGroups(workplaceGroups, eb,
                stationsInWorkplaceGroupsHandler);

    }

    private void requestCurrentStatesForStations(String sessionId, Set<String> stationSet)
    {
        // TODO: implement connection to Maschineninformations-Dienst
    	String demoMachineState = "<http://www.appsist.de/ontology/FunktionsfaehigerZustand>";
    	String uId =  this.sessionUserID.get(sessionId);
        if (isDemo) {
            demoMachineState = demoUserMachinestateMap.get(this.sessionUserID.get(sessionId));
        }

        String stateStation = "{";
        if (this.sessionUserID.get(sessionId).indexOf("@appsist.de") != -1) {
        	stateStation += "(<http://www.appsist.de/ontology/FunktionsfaehigerZustand> <http://www.appsist.de/ontology/demonstrator/StationMontage>) ";
        	// to show all measures on startup uncomment this line
            //stateStation += "(<http://www.appsist.de/ontology/demonstrator/Bauteilverlust> <http://www.appsist.de/ontology/demonstrator/StationMontage>) (<http://www.appsist.de/ontology/demonstrator/TuerOffen> <http://www.appsist.de/ontology/demonstrator/StationMontage>) (<http://www.appsist.de/ontology/demonstrator/DeckelmagazinLeer> <http://www.appsist.de/ontology/demonstrator/StationMontage>) (<http://www.appsist.de/ontology/demonstrator/FedermagazinLeer> <http://www.appsist.de/ontology/demonstrator/StationMontage>)";
        } else 
        	if (this.sessionUserID.get(sessionId).indexOf("@festo") != -1) {
        	stateStation += "(<http://www.appsist.de/ontology/festo/FettLeer> <http://www.appsist.de/ontology/festo/S20>) (<http://www.appsist.de/ontology/festo/LoctiteLeer> <http://www.appsist.de/ontology/festo/S20>)";
        } 
        else {
        	if (null == demoMachineState) {
        		demoMachineState = "<http://www.appsist.de/ontology/FunktionsfaehigerZustand>";
        	}
            for (String station : stationSet) {
                stateStation += "(" + demoMachineState + sparqlPrefix(station) + ")";
            }
        }

        stateStation += "}";
        requestLocalStates(sessionId, stateStation);
    }

    private void requestLocalStates(final String sessionId, String stateStation)
    {
        Handler<Message<String>> localStatesHandler = new Handler<Message<String>>()
        {
            @Override
            public void handle(Message<String> messageString)
            {
            	JsonObject result = new JsonObject(messageString.body());
                //log.debug(" received local states: " + result.encodePrettily());
                JsonArray messageArray = result.getObject("results").getArray("bindings");
                Iterator<Object> jsonArrayIterator = messageArray.iterator();
                Set<LocalState> lsSet = new HashSet<LocalState>();

                Set<String> localStates = new HashSet<String>();
                while (jsonArrayIterator.hasNext()) {
                    Object iteratorObject = jsonArrayIterator.next();
                    if (iteratorObject instanceof JsonObject) {
                        JsonObject jsonObject = (JsonObject) iteratorObject;
                        LocalState ls = new LocalState(
                                sparqlPrefix(jsonObject.getObject("z").getString("value")),
                                sparqlPrefix(jsonObject.getObject("station").getString("value")),
                                jsonObject.getObject("p").getString("value"));
                        lsSet.add(ls);
                        localStates.add(ls.getState());
                    }
                }
                sessionLocalStates.put(sessionId, lsSet);
                sessionStates.put(sessionId, localStates);
                requestMeasuresForMachineStates(sessionId);
            }
        };
        BasicSparQLQueries.getLocalStates(stateStation, eb, 
                localStatesHandler);
    }

    // rule 2.1.1.2 in document adaptionsregeln
    private void requestMeasuresForMachineStates(String sessionId)
    {
        final String uId = this.sessionUserID.get(sessionId);
        Handler<Message<String>> handleMeasuresForStatesResult = new Handler<Message<String>>()
        {

            @Override
            public void handle(Message<String> arg0)
            {
                log.debug("handleMeasuresForStatesResult-" + arg0.body());
                processReceivedMeasureList(arg0.body(), uId);
            }
        };

        String machineStates = "{";
        for (LocalState ls : this.sessionLocalStates.get(sessionId)) {
            machineStates += ls.toString();
        }
        machineStates += "}";
        // log.debug(" machineStates:" + machineStates);
        BasicSparQLQueries.getMeasuresForStates(machineStates, eb, 
                handleMeasuresForStatesResult);

    }

    // method surround Ontology URIs with "<" and ">"
    private String sparqlPrefix(String original)
    {

        if (null == original) {
            return "";
        }
        original = original.trim();
        String result = "";
        if (!original.startsWith("<")) {
            result = "<" + original;
        }
        if (!original.endsWith(">")) {
            result += ">";
        }
        return result;

    }

    private void retrieveLabelsFor(final List<String> finalMeasureList, final String sessionId)
    {
        Handler<Message<String>> handleMeasureLabels = new Handler<Message<String>>(){

            @Override
            public void handle(Message<String> arg0)
            {
                // build HashMap with measure/label
                HashMap<String, String> measureLabels = new HashMap<String, String>();
                JsonObject jsonObject = new JsonObject(arg0.body());
                JsonArray jsonArray = jsonObject.getObject("results").getArray("bindings");
                Iterator<Object> jsonArrayIterator = jsonArray.iterator();
                while (jsonArrayIterator.hasNext()) {
                    Object currentObject = jsonArrayIterator.next();
                    if (currentObject instanceof JsonObject) {
                        JsonObject currentJsonObject = (JsonObject) currentObject;
                        String measure = currentJsonObject.getObject("oc")
                                .getString("value");
                        String label = currentJsonObject.getObject("label")
                                .getString("value");
                        measureLabels.put(measure, label);
                    }
                }
                // update catalog

                if (!measureLabels.isEmpty()) {
                    // add measureIds for measures with missing labels
                    for (String measure : finalMeasureList) {
                        if (!measureLabels.containsKey(measure)) {
                            measureLabels.put(measure, measure);
                        }
                    }

                }
                else {
                    // if no label is found in ontology add measureIds as labels
                    for (String measure : finalMeasureList) {
                        measureLabels.put(measure, measure);
                    }
                }

                buildServiceItems(measureLabels, sessionId);
            }
        };
        String measuresForQuery = "{";
        for (String measure : finalMeasureList) {
            measuresForQuery = measuresForQuery + " <" + measure + ">";
        }
        measuresForQuery += "}";
        BasicSparQLQueries.getLabelFor(measuresForQuery, "de", eb, 
                handleMeasureLabels);
    }

    private void retrieveLabelsFor(final Set<StateMeasure> stateMeasureSet, final String sessionId)
    {
        Handler<Message<String>> handleMeasureLabels = new Handler<Message<String>>()
        {

            @Override
            public void handle(Message<String> stringMessage)
            {

                // build HashMap with measure/label
                JsonObject jsonObject = new JsonObject(stringMessage.body());
                JsonArray jsonArray = jsonObject.getObject("results").getArray("bindings");
                Iterator<Object> jsonArrayIterator = jsonArray.iterator();
                while (jsonArrayIterator.hasNext()) {
                    Object currentObject = jsonArrayIterator.next();
                    if (currentObject instanceof JsonObject) {
                        JsonObject currentJsonObject = (JsonObject) currentObject;
                        if (currentJsonObject.getObject("oc") != null
                                && currentJsonObject.getObject("label") != null) {
                            String entry = currentJsonObject.getObject("oc").getString("value");
                            if (!labelMap.containsKey(entry)) {
                                String label = currentJsonObject.getObject("label")
                                        .getString("value");
                                labelMap.put(entry, label);
                            }
                        }
                    }
                }


                buildServiceItems(stateMeasureSet, sessionId);
            }
        };
        String measuresForQuery = "{";
        for (StateMeasure stateMeasure : stateMeasureSet) {
            measuresForQuery = measuresForQuery + stateMeasure.toString();
        }
        measuresForQuery += "}";
        // log.debug("-measuresForQuery: " + measuresForQuery);
        BasicSparQLQueries.getLabelFor(measuresForQuery, "de", eb, 
                handleMeasureLabels);
    }
    
    
    // add mandatory measure (ServiceItem) 
    private void addMandatoryServiceItem(String measureId, String measureLabel, String sessionId, Map<String, Object> context){
    	// build service item
    	//log.info("Adding measureId:"+measureId +" for sessionId: "+sessionId+" as mandatory service item");
    	LinkedHashSet<ServiceItem> serviceItems;
    	if(this.sessionMandatoryServiceItems.containsKey(sessionId)){
    		serviceItems = this.sessionMandatoryServiceItems.get(sessionId);
    		this.sessionMandatoryServiceItems.remove(sessionId);
    	} else{
    		serviceItems = new LinkedHashSet<ServiceItem>();
    	}
    	InstructionItemBuilder iib = new InstructionItemBuilder();
        iib.setTitle(measureLabel);
        int serviceItemsSize =serviceItems.size();
        serviceItemsSize++;
        iib.setId("msm-"+serviceItemsSize);
        iib.setPriority(serviceItemsSize);
        iib.setService("measures-service");
        iib.setSite("DNC_DNCB_DSBC");
        iib.setStation("Station 20");
        // set preview image
        iib.setImageUrl(getPreviewImage(measureId));
        
        // add Action
        StringBuilder postActionStringBuilder = new StringBuilder(300);
        JsonObject serviceConfig = config.getObject("services");
        if (serviceConfig.getBoolean("secure")) {
            postActionStringBuilder.append("https://");
        }
        else {
            postActionStringBuilder.append("http://");
        }
        postActionStringBuilder.append(serviceConfig.getString("host"));

        if (!serviceConfig.getBoolean("proxy")) {
            postActionStringBuilder.append(":")
.append(serviceConfig.getNumber("port").toString());
        }

        postActionStringBuilder.append(serviceConfig.getObject("paths").getString("psd"))
                .append("/startSupport/")
                .append(measureId.substring(measureId.lastIndexOf("/") + 1));
        JsonObject contextObject = null;
        if (context != null){
        	contextObject = new JsonObject().putObject("context", new JsonObject(context));
        }
        //log.info("Received content object: "+ contextObject.encodePrettily());
        HttpPostAction hpa = new HttpPostAction(postActionStringBuilder.toString(), contextObject);
        
        
        iib.setAction(hpa);
        //TODO: add context
        
    	serviceItems.add(iib.build());
    	this.sessionMandatoryServiceItems.put(sessionId, serviceItems);
    	//log.info(sessionId+" || "+this.sessionMandatoryServiceItems.get(sessionId));
    	
    	this.conn.purgeServiceItems(sessionId, "measures-service", null);
        this.conn.addServiceItems(sessionId, concatenateServiceItemSets(sessionId), null);
    }
    
    private void removeMandatoryServiceItem(String measureId, String sessionId){
//    	log.info("sessionId: "+sessionId +" is contained in map? "+this.sessionMandatoryMeasureIds.containsKey(sessionId));
//    	log.info(this.sessionMandatoryMeasureIds.keySet());
    	if (this.sessionMandatoryMeasureIds.containsKey(sessionId)){
    		boolean blip = this.sessionMandatoryMeasureIds.get(sessionId).remove(measureId);
    		log.debug("Successfully removed measureId "+ measureId +"from mandatoryMeasureIDs:" + blip);
    	}
    	measureId = measureId.substring(measureId.lastIndexOf("/")+1);
    	if (this.sessionMandatoryServiceItems.containsKey(sessionId)){
    		LinkedHashSet<ServiceItem> serviceItems = this.sessionMandatoryServiceItems.get(sessionId);
    		Iterator<ServiceItem> siIterator = serviceItems.iterator();
    		boolean serviceItemFound = false;
    		while(siIterator.hasNext() && !serviceItemFound){
    			ServiceItem si = siIterator.next();
    			if (measureId.equals(si.getAction().asJson().getString("address", "").split("/startSupport/")[1])){
    				log.debug("service item "+measureId + " has been successfully removed? "+ serviceItems.remove(si));
    				serviceItemFound = true;
    				//log.info("Successfully removed service item "+this.sessionMandatoryServiceItems.remove(sessionId));
    				this.sessionMandatoryServiceItems.put(sessionId, serviceItems);
    			}
    		}
    	}
    }
    
    // concatenate requested and deduced service items to one List<ServiceItems>
    private List<ServiceItem> concatenateServiceItemSets(String sessionId){
    	List<ServiceItem> concatenatedSIList = new ArrayList<ServiceItem>();
    	if (this.sessionMandatoryServiceItems.containsKey(sessionId)){
    		for (ServiceItem si : this.sessionMandatoryServiceItems.get(sessionId)){
    			concatenatedSIList.add(si);
    		}
    	}
    	if (this.sessionLastServiceItemList.containsKey(sessionId)){
    		for (ServiceItem si : this.sessionLastServiceItemList.get(sessionId)){
    			concatenatedSIList.add(si);
    		}
    	}
    	// add notifications for service items
    	if (this.sessionMandatoryMeasureIds.containsKey(sessionId)){
    		int priority=1;
    		for (String measureId: this.sessionMandatoryMeasureIds.get(sessionId)){
    			String defaultNotificationMessage = "Maßnahme notwendig: "+ getLabelFor(measureId) ;
        		Builder notificationBuilder = new Builder("nmsman-"+priority++, defaultNotificationMessage,
                        Level.ERROR);
                notificationBuilder.setRelatedItem(measureId.substring(measureId.lastIndexOf("/") + 1));
                conn.notify(sessionId, notificationBuilder.build(), null);
        	}
    	}
    	return concatenatedSIList;
    }
    
   
}
