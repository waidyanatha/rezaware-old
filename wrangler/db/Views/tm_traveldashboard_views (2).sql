DROP VIEW IF EXISTS booking_dashboard;
DROP VIEW IF EXISTS booking_by_destination_dashboard;
DROP VIEW IF EXISTS booking_by_hotel_dashboard;
DROP VIEW IF EXISTS comparison_dashboard;
DROP VIEW IF EXISTS profit_dashboard;

/* 
     View contains the data which is used for booking dashboard and bookings by touroperator dashboard
*/ 

CREATE VIEW booking_dashboard AS 
 select  
checkindate as check_in_date ,						/* customer checkin date */
sum(a.totalcost) as cost,   	 					/* total booking value*/ 
Round(avg(a.noofnights) ) as nights,   					/* number of nights booked*/  
Round(avg(a.totnoofadults+a.totnoofchildren)) as guests,    		/* number of guests*/ 
Round(avg(a.totnoofrooms)) as rooms,  					/* number of rooms booked*/   
count(a.reservationno ) as bookings,  					/* number of bookings*/  
Round(avg((checkindate-reservationdate)) ) as window_length,  		/* number of days difference between checkin date and booking date*/ 
a.touroperatorid as touroperator_id,   					/* touroperator id*/  
tourmappers.fn_get_touroperator_name(a.touroperatorid) as touroperator_name,  	/* touroperator name*/ 
tourmappers.fn_get_touroperator_country(a.touroperatorid) as country  ,  /* touroperator's country*/ 
case when round(avg(a.noofnights))in (1,2) then 'Low' when round(avg(a.noofnights)) in (3,4,5) then 'Medium' else 'High' end as length_of_stay,  /* length of stay*/ 
(sum(a.totalcost)/avg(a.totnoofrooms)/avg(a.noofnights)/count(a.reservationno )) as avg_room_rate  /* average room rate*/ 

from 
tourmappers.resconfirmsv as a    
where  
resstatus != 'C'     
group by touroperator_id, check_in_date
order by touroperator_id;


/* 
     View contains the data which is used for bookings by destination dashboard
*/ 

CREATE VIEW booking_by_destination_dashboard AS 
select 
checkindate as check_in_date , 														/* checkin date */
tourmappers.fn_get_res_hotel_details(id,'hotelcountry') as country,  							/* hotel located country */
tourmappers.fn_get_res_hotel_details(id,'hotelcity') as city,  								/* hotel located city*/
count(reservationno) as bookings,  											/* total number of bookings*/
sum(totalcost) as booking_value,      											/* total booking value */
round(avg(noofnights)) as nights,    											/* number of nights booked*/  
case when round(avg(noofnights))in (1,2) then 'Low' when round(avg(noofnights)) in (3,4,5) then 'Medium' else 'High' end as length_of_stay,  /* length of stay*/  
round(avg((checkindate-reservationdate))) as window_length,        							/* number of days difference between checkin date and booking date*/ 
round(avg(totnoofadults+totnoofchildren)) as guests,   									/* number of guests*/  
round(avg(totnoofrooms)) as rooms    											/* number of booked rooms */

from 
tourmappers.resconfirmsv   as a
where resstatus != 'C'     
group by check_in_date,country,city   
order by check_in_date,country,city;

 
/* 
     View contains the data which is used for bookings by hotels dashboard
*/ 

CREATE VIEW booking_by_hotel_dashboard AS 
select 
a.reservationno as reservation_no,													/* reservation number */
checkindate as check_in_date,  														/* checkin date */
sum(a.totalcost) as booking_value,     											/* total booking value  */
Round(avg(a.noofnights) ) as nights,     										/* total booked nights */
Round(avg(a.totnoofadults+a.totnoofchildren)) as guests,     								/* nuber of guests*/
Round(avg(a.totnoofrooms)) as rooms,   											/* number of booked rooms */  
count(a.reservationno ) as bookings,     										/* total number of bookings*/
--Round(avg((checkindate-reservationdate)) ) as window_length,   							/* number of days difference between checkin date and booking date*/ 
a.hotelcode as hotel_code,  														/* hotel code */ 
b.hotelname as hotel_name,  														/* name of the hotel */ 
b.hotelcity as city,  														/* hotel located city */
b.hotelcountry as country, 													/* hotel located country*/
c.roomtypedesc room_type      												/* room type */
--bonotel.fn_get_touroperator_name(a.touroperatorid) as touroperator_name,    							/* touroperator name */
case when round(avg(a.noofnights))in (1,2) then 'Low' when round(avg(a.noofnights)) in (3,4,5) then 'Medium' else 'High' end as length_of_stay,   /* total booked nights*/
(sum(a.totalcost)/avg(a.totnoofrooms)/avg(a.noofnights)/count(a.reservationno )) as avg_room_rate    			/* average room rate */ 
from   
tourmappers.resconfirmsv as a  ,   
tourmappers.reshoteldetailssv as b,  
tourmappers.resroomssv  as c   
where    
resstatus != 'C'     
and a.id=b.resconfirmid   
and a.id=c.resconfirmid  
and b.resconfirmid =c.resconfirmid  
group by  reservation_no , a.touroperatorid , check_in_date,hotel_code,hotel_name,city,country,room_type  
order by reservation_no ;

/* 
     View contains the data which is used for comparison dashboard
*/ 

CREATE VIEW comparison_dashboard AS 
select 
distinct checkindate as check_in_date,   												/* checkin date */
tourmappers.fn_get_res_hotel_details(a.id,'hotelcountry') as country,  								/* hotel located country */				  
reservationno as bookings,    													/* total number of bookings*/
totalcost as booking_value,     													/* total booking value */
noofnights as nights,      													/* number of nights booked*/
case when round(noofnights)in (1,2) then 'Low' when round(noofnights) in (3,4,5) then 'Medium' else 'High' end as length_of_stay,  /* length of stay*/
a.touroperatorid as touroperator_id,  													/* touroperator id */
tourmappers.fn_get_touroperator_name(a.touroperatorid) as touroperator_name, 								/* touroperator name */
tourmappers.fn_get_roomtype_name  (cast(b.roomtypecode as int))  as room_type  							/* room type */
from 
tourmappers.resconfirmsv  as a,
tourmappers.resroomssv  as b   
where  a.id=b.resconfirmid and  
resstatus != 'C'       
order by reservationno;


/* 
     View contains the data which is used for profit analysis dashboard
*/

CREATE VIEW profit_dashboard AS 
select 
checkindate as check_in_date,   														/* checkin date */
TO_CHAR(checkindate, 'Month') as month, 										/* checkin date (month) */ 
extract(year from checkindate) as year, 										/* checkin date (year) */ 
DATE_PART('month',checkindate) as mon,  										/* checkin date (month) */ 
tourmappers.fn_get_res_hotel_details(id,'hotelcountry') as country,   							/* hotel located country */
tourmappers.fn_get_res_hotel_details(id,'hotelcity') as city,   							/* hotel located city */
count(reservationno) as bookings,   											/* total number of bookings*/
sum(totalcost) as booking_value,    											/* total booking value */
a.touroperatorid as touroperator_id,  												/* touroperator id */
tourmappers.fn_get_touroperator_name(a.touroperatorid) as touroperator_name,  							/* touroperator name */
sum((select sum(sellrate) from  tourmappers.resdailyratessv as b where b.resconfirmid=a.id))  as sell_rate,  		/* sell rate */
sum((select sum(netrate) from  tourmappers.resdailyratessv as b where b.resconfirmid=a.id))  as net_rate,  		/* net rate */
sum((select sum(sellrate-netrate) from  tourmappers.resdailyratessv as b where b.resconfirmid=a.id))  as profit   	/* prfit-diffrence of the sell rate & net rate */
from 
tourmappers.resconfirmsv  as a  
where resstatus != 'C'           
group by check_in_date,country,city ,touroperator_id   
order by check_in_date,country,city;   

commit;