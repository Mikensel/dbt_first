select * 
from {{ref ("transform")}}
where trip_distance <= 0