# Vehicle Telematics
Course project from Cloud Computing and Big Data Ecosystems Design @ Universidad Politecnica de Madrid.
A simple, simulated, real-time traffic analysis using Apache Flink.

## Authors
- **Daniel Reyes**
- **Manuel Scurti**

## Description
Drivers, fleet owners, transport operations, insurance companies are stakeholders of
vehicle monitoring applications which need to have analytical reporting on the mobility patterns of their
vehicles, as well as real-time views in order to support quick and efficient decisions towards eco-friendly
moves, cost-effective maintenance of vehicles, improved navigation, safety and adaptive risk
management.
Vehicle sensors do continuously provide data, while on-the-move, they are processed in order to
provide valuable information to stakeholders. Applications identify speed violations, abnormal driver
behaviors, and/or other extraordinary vehicle conditions, produce statistics per driver/vehicle/fleet/trip,
correlate events with map positions and route, assist navigation, monitor fuel consumptions, and
perform many other reporting and alerting functions.

## Specifications
each vehicle reports a position event every 30 seconds with the following format: 
	*Time, VID, Spd, XWay, Lane, Dir, Seg, Pos*

- *Time* a timestamp (integer) in seconds identifying the time at which the position event was emitted
- *VID* is an integer that identifies the vehicle
- *Spd* (0 - 100) is an integer that represents the speed mph (miles per hour) of the vehicle
- *XWay* (0 . . .L−1) identifies the highway from which the position report is emitted
- *Lane* (0 . . . 4) identifies the lane of the highway from which the position report is emitted (0 if it is an entrance ramp (ENTRY), 1 − 3 if it is a travel lane (TRAVEL) and 4 if it is an exit ramp (EXIT))
- *Dir* (0 . . . 1) indicates the direction (0 for Eastbound and 1 for Westbound) the vehicle is traveling
- *Seg* (0 . . . 99) identifies the segment from which the position report is emitted
- *Pos* (0 . . . 527999) identifies the horizontal position of the vehicle as the number of meters from the
westernmost point on the highway (i.e., Pos = x)

## Goals
- **Speed Radar**: detects cars that overcome the speed limit of 90 mph
- **Average Speed Control**: detects cars with an average speed higher than 60 mph between
segments 52 and 56 (both included) in both directions. If a car sends several reports on
segments 52 or 56, the ones taken for the average speed are the ones that cover a longer
distance.
- **Accident Reporter**: detects stopped vehicles on any segment. A vehicle is stopped when it
reports at least 4 consecutive events from the same position.

## Input
Example dataset is available at lsd11.ls.fi.upm.es/traffic-3xways-new.7z

## Output
Output to be generated:
The program must generate 3 output CSV files.
- **speedfines.csv**: stores the output of the speed radar
	format: Time, VID, XWay, Seg, Dir, Spd
- **avgspeedfines.csv**: stores the output of the average speed control
	format: Time1, Time2, VID, XWay, Dir, AvgSpd, where Time1 is the time of the first event
of the segment and Time2 is the time of the last event of the segment.
- **accidents.csv**: stores the output of the accident detector.
	format: Time1, Time2, VID, XWay, Seg, Dir, Pos, where Time1 is the time of the first
event the car stops and Time2 is the time of the fourth event the car reports to be
stopped.

## Requirements
- Java 8
- Flink 1.3.2
- Maven

## Run instructions
From the root folder:
- mvn clean package -Pbuild-jar
- flink run -p 10 -c master2018.flink.VehicleTelematics target/**$JAR_FILE** **$PATH_TO_INPUT_FILE** **$PATH_TO_OUTPUT_FOLDER**

## Additional Notes 
The program is optimized to run on a Flink cluster with 10 task manager slots available
