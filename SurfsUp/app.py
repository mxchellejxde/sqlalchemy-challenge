# Import the dependencies.
from flask import Flask, jsonify
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, inspect, select
import datetime as dt
import numpy as np
import pandas as pd

#################################################
# Database Setup
#################################################
# create engine to hawaii.sqlite
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(autoload_with=engine, reflect=True)

# Save references to each table
measurement = Base.classes.measurement
station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)



#################################################
# Flask Setup
#################################################
# @TODO: Initialize your Flask app here
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
@app.route("/")
def homepage():
    return( 
           f"Welcome to the Climate App<br/>"
           f"Available routes: <br/>"
           f"The last 12 months of precipitation data - <a href=\"/api/v1.0/precipitation\">/api/v1.0/precipitation<a><br/>"
           f"A list of stations - <a href=\"/api/v1.0/stations\">/api/v1.0/stations<a><br/>"
           f"A list of the most active station for the previous year's temperature data - <a href=\"/api/v1.0/tobs\">/api/v1.0/tobs<a><br/>"
           f"Enter a start date (yyyy-mm-dd) for min, max, and avg temp for all dates after specified date - <a href=\"/api/v1.0/<start>\">/api/v1.0/<start><a><br/>"
           f"Enter a start date (yyyy-mm-dd) and end date (yyyy-mm-dd) for min, max, and avg temp for all dates between specified dates - <a href=\"/api/v1.0/<start>/<end>\">/api/v1.0/<start>/<end><a><br/>")

#precipitation route
@app.route("/api/v1.0/precipitation")
def precipitation():
    # Design a query to retrieve the last 12 months of precipitation data
    
    # Starting from the most recent data point in the database. 
    earliest = session.query(measurement.date).order_by(measurement.date.desc()).first()
    first_date = dt.datetime(2017, 8, 23)
    # Calculate the date one year from the last date in data set.
    year_ago = first_date - dt.timedelta(days=366)
    # Perform a query to retrieve the data and precipitation scores
    sel = [measurement.date,measurement.prcp]
    precipitation_dataset = session.query(*sel).\
        filter(measurement.date >= year_ago).\
        filter(measurement.date < first_date).\
        order_by(measurement.date).all()
    
    #Close session
    session.close()
    
    #Returns json with the date as the key and the value as the precipitation 
    #Create dictionaries
    prcp_date = []
    prcp_prcp = []
    
    for date, prcp in precipitation_dataset:
        prcp_date.append(date)
        prcp_prcp.append(prcp)
        
    prcp_dict = dict(zip(prcp_date, prcp_prcp))
    
    return jsonify(prcp_dict)

#station route
@app.route("/api/v1.0/stations")
def stations():
    # Design a query to find the most active stations (i.e. which stations have the most rows?)
    # List the stations and their counts in descending order.
    sel = [station.station,station.name]
    
    all_stations = session.query(*sel).\
        group_by(station.station).all()
        
    #Close session
    session.close()
    
    #Create list of stations
    stations = []
    names = []
    
    for stat,name in all_stations:
        stations.append(stat)
        names.append(name)
    
    return jsonify(stations)

#tobs route
@app.route("/api/v1.0/tobs")
def tobs():  
    #determine the most active stations
    sel = [measurement.station,func.count(measurement.id)]
    active_stations = session.query(*sel).\
        group_by(measurement.station).\
        order_by(func.count(measurement.id).desc()).all()
    
    # Starting from the most recent data point in the database. 
    earliest = session.query(measurement.date).order_by(measurement.date.desc()).first()
    first_date = dt.datetime(2017, 8, 23)
    # Calculate the date one year from the last date in data set.
    year_ago = first_date - dt.timedelta(days=366)
    
    #using the most active station and print the results of the last year
    sel = [measurement.tobs,measurement.date]
    most_active = session.query(*sel).\
        filter(measurement.station == active_stations[0][0]).\
        filter(measurement.date >= year_ago).\
        filter(measurement.date <= first_date).\
        order_by(measurement.tobs).all()
    
    #close session
    session.close()
    
    #create list and print
    temps = []
    dates = []
    for temp, date in most_active:
        temps.append(temp)
        dates.append(date)
        
    tobs_dict = dict(zip(dates, temps))
    return jsonify(tobs_dict)

#specified start date only route
@app.route("/api/v1.0/<start>")
def start(start_date):   
    #query and find results starting at start date
    data = session.query(func.min(measurement.tobs), 
                        func.max(measurement.tobs), 
                        func.avg(measurement.tobs)).\
        filter(measurement.date >= start_date).all()
    
    #close session
    session.close()
    
    #create list to return
    tobs_list = []
    
    #run through loop to append the min, max, avg
    for tmin, tmax, tavg in data:
        #set up dictionary of value
        tobs = {}
        tobs["minimum"] = tmin
        tobs["maximum"] = tmax
        tobs["average"] = tavg
        tobs_list.append(tobs)
        
    return jsonify(tobs_list)

#specified start date only route
@app.route("/api/v1.0/<start>/<end>")
def startend(start_date,end_date): 
    #run query for start and end date filter
    results = session.query(func.min(measurement.tobs), 
                            func.max(measurement.tobs), 
                            func.avg(measurement.tobs)).\
                            filter(measurement.date >= start_date).\
                            filter(measurement.date <= end_date).all()
    
        #close session
    session.close()
    
    #create list to return
    tobs_list = []
    
    #run through loop to append the min, max, avg
    for tmin, tmax, tavg in results:
        #set up dictionary of value
        tobs = {}
        tobs["minimum"] = tmin
        tobs["maximum"] = tmax
        tobs["average"] = tavg
        tobs_list.append(tobs)
        
    return jsonify(tobs_list) 


if __name__ == "__main__":
    app.run(debug=True)