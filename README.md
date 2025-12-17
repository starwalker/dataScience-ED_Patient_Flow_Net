#####################
##### ED_Flow_Net Project
#####################
    
    Objective: Predict a path and wait times along the path for patients in the MUSC Emergency Department, and create a deployable web application.
    
    Contents:  -SQL Code building the dataset for model training
               -Code for near real time data link to epic
               -Python Code building wait time predictive models
               -Python Code defining path and time prediction functions using pre trained models
               -Python Code building visualization for network state, and predictive path/wait times
               -Web application code GUI 
    

####################
##### PAT STATUS NETWORK
####################
    
    Ed Flow Dataset Design:
    Grain:  Every row is a patients movement from point A to point B
    PrimaryKey: composite { patID,dateTime departing Origin Node}
    
    One Sample Row: {
    	Origin Node: Waiting for Triage, 
    	Target Node: Waiting for Room, 
    	Time Between Nodes: 17 minutes, 
    	DateTime.Departing.Origin : yyymmdd hhssmm,
    	Pat Chief Complaint: headache
    	Patients.in.Triage : 5,
    	Patients.in.Rooms : 10} 
    
    Nodes: Patient Status and Radiology Status 
    
    Edges:One Patient
    	-Moves from one Status (node) to Another Status (node)
    
    Edges Attributes:
    	Patient Based:
    	-dateTime departing Origin Node
    	-Pat Time between Nodes (Dependent Variable)
    	-Pat Chief Complaint
    	-Means of Arrival
    	-Unique Set of Ed Events that have occurred
    	
    	ED Status Based:
    	-Number of people currently at each node



###################
##### PAT EVENT NETWORK
###################

    Grain:  Every row is a patients movement from point A to point B
    Primary Key:  composite { patID,dateTime departing Origin Node}
    
    One Sample Row: {
    	Origin Node: CT Ordered, 
    	Target Node: CT Waiting, 
    	Time Between Nodes: 17 minutes, 
    	DateTime.Departing.Origin : yyymmdd hhssmm,
    	Pat Chief Complaint: headache
    	Patients.in.Triage : 5,
    	Patients.in.Rooms : 10} 
    	
    	
    Nodes: Patient ED Events
    	
    Edges:One Patient
    	-Moves from one Status (node) to Another Status (node)
    
    Edges Attributes:
    	Patient Based:
    	-dateTime departing Origin Node
    	-Pat Time between Nodes (Dependent Variable)
    	-Pat Chief Complaint
    	-Means of Arrival
    	-Pat ED Status at the time departing origin node
    	-Pat ED Status at the time arriving at target node
    	
############
##### Modeling 
############

    Modeling Strategies:
    Option 1: use clustering of events as dim reduction, predict clusters on arrival, use in time prediction between statuses
    Option 2: use ED Event list to predict unique times between statuses
    Option 3: use arrival status to predict network paths and wait times 
