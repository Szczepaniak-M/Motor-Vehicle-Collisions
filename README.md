# Motor-Vehicle-Collision
Project using MapReduce and Hive to process data about collisions in NYC.

## Description



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
The output of thReducers are stored in SequenceFile and are input for Hive

### Hive