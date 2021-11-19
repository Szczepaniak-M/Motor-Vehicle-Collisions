# Motor-Vehicle-Collision
Project transforms data avaiable in CSV files and presents the top 3 most dangerous streets in Manhattan for each type of road user.
Project using MapReduce and Hive to process data about collisions in NYC.
The final results are stored as JSON file.

## Data
The source of the data is `https://opendata.cityofnewyork.us/data/`.

The project use two files:
1. `NYPD_Motor_Vehicle_Collisions.csv` – data about motor vehicle collision in NYC
2. `zips-boroughs.csv` – data about zip codes in NYC

## Processing
### MapReduce
The data from `NYPD_Motor_Vehicle_Collisions.csv` is firstly processed by MapReduce.
Mappers extract from each row of CSV file data about:
- the streets where the collision happened (up to three streets)
- zip code
- the type of the injured people (pedestrian, cyclist or motorist)
- the type of damage (injury or death)
- the number of people

Mapper also filter out rows:
- about collisions before 2012
- without zipcode

Then Combiners and Reducers collect data produced by Mappers and sum obtained data.
The Input and Output keys for Combiners and Reducers are (street, zipcode, type of the injured people, type of damage).
The output of the Reducers are stored in SequenceFile and are input for Hive

### Hive
Hive script processes data using only external table and then save result to external table, which is stored as JSON.
It joins data about boroughs and zip code from `zips-boroughs.csv` with the results of MapReduce.
Then the data are ranked and are chosen top 3 most dangerous streets for each type of road user.

### Example final results
```json
{"street":"2 AVENUE","person_type":"CYCLIST","killed":0,"injured":481}
{"street":"BROADWAY","person_type":"CYCLIST","killed":1,"injured":470}
{"street":"1 AVENUE","person_type":"CYCLIST","killed":1,"injured":416}
{"street":"BROADWAY","person_type":"MOTORIST","killed":1,"injured":1466}
{"street":"2 AVENUE","person_type":"MOTORIST","killed":0,"injured":1206}
{"street":"3 AVENUE","person_type":"MOTORIST","killed":2,"injured":957}
{"street":"BROADWAY","person_type":"PEDESTRIAN","killed":9,"injured":1050}
{"street":"3 AVENUE","person_type":"PEDESTRIAN","killed":11,"injured":858}
{"street":"2 AVENUE","person_type":"PEDESTRIAN","killed":7,"injured":848}
```
