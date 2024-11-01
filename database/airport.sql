CREATE DATABASE airport;

USE airport;

CREATE TABLE Vuelos (
    FlightID INT PRIMARY KEY AUTO_INCREMENT,
	Origin_airport VARCHAR(100),
    Destination_airport VARCHAR(100),
    Origin_city VARCHAR(100),
    Destination_city VARCHAR(100),
    Passengers INT,
    Seats INT,
    Flights INT,
    Distance INT,
    Fly_date DATE,
    Origin_population INT,
    Destination_population INT,
	Org_airport_lat DECIMAL(10, 8),
	Org_airport_long DECIMAL(11, 8),
	Dest_airport_lat DECIMAL(10, 8),
	Dest_airport_long DECIMAL(11, 8)
);

