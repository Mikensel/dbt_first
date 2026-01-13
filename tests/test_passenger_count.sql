select * 
from {{ref("transform")}}
where 
    passenger_count <= 0
    and passenger_count != cast(passenger_count as bigint)