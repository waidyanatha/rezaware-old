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
checkindate as check_in_date, 									/* customer checkin date */
sum(a.totalcost) as cost,  							               /* total booking value*/  
Round(avg(a.noofnights) ) as 	nights,						          /* number of nights booked*/
Round(avg(a.totnoofadults+a.totnoofchildren)) as guests, 			     /* number of guests*/
Round(avg(a.totnoofrooms)) as rooms,   						          /* number of rooms booked*/
count(a.reservationno ) as bookings,   							     /* number of bookings*/  
Round(avg((checkindate-reservationdate)) ) as window_length,   			/* number of days difference between checkin date and booking date*/ 
a.touroperatorid as touroperator_id,   								/* touroperator id*/  
--bonotel.fn_get_touroperator_name(a.touroperatorid) as touroperator_name	/* touroperator name*/ 
--bonotel.fn_get_touroperator_country(a.touroperatorid) as country  , 	/* touroperator's country*/ 
case when round(avg(a.noofnights))in (1,2) then 'Low' when round(avg(a.noofnights)) in (3,4,5) then 'Medium' else 'High' end as length_of_stay,   /* number of nights booked*/ 
(sum(a.totalcost)/avg(a.totnoofrooms)/avg(a.noofnights)/count(a.reservationno )) as avg_room_rate         /* average room rate*/ 
from ds_bk_resconfirmsv as a
where  
resstatus != 'C'    
group by touroperator_id , check_in_date
order by touroperator_id ;


/* 
     View contains the data which is used for bookings by destination dashboard
*/ 

CREATE VIEW booking_by_destination_dashboard AS 
select 
checkindate as check_in_date, 									/* checkin date */
--bonotel.fn_get_res_hotel_details(id,'hotelcountry') as country, 		/* hotel located country */
--bonotel.fn_get_res_hotel_details(id,'hotelcity') as city, 			/* hotel located city*/
count(reservationno) as bookings, 									/* total number of bookings*/
sum(totalcost) as booking_value,  									/* total booking value */
round(avg(noofnights)) as nights,   								/* number of nights booked*/
case when round(avg(noofnights))in (1,2) then 'Low' when round(avg(noofnights)) in (3,4,5) then 'Medium' else 'High' end as length_of_stay, 	/* length of stay*/ 
round(avg((checkindate-reservationdate))) as window_length,  			/* number of days difference between checkin date and booking date*/
round(avg(totnoofadults+totnoofchildren)) as guests,   				/* number of guests*/
round(avg(totnoofrooms)) as rooms   								/* number of booked rooms */
from 
ds_bk_resconfirmsv  
where resstatus != 'C'    
group by check_in_date 
order by check_in_date;

 
/* 
     View contains the data which is used for bookings by hotels dashboard
*/ 

CREATE VIEW booking_by_hotel_dashboard AS 
select 
a.reservationno, 													/* reservation number */
checkindate,  														/* checkin date */
a.totalcost as bookingvalue,     											/* total booking value  */
a.noofnights as nights,     												/* total booked nights */
(a.totnoofadults+a.totnoofchildren) as guests,    									/* number of guests */ 
a.totnoofrooms as rooms,     												/* number of booked rooms */ 												/* number of booked rooms */  
count(distinct a.reservationno ) as bookings,     									/* total number of bookings*/
a.hotelcode,  														/* hotel code */ 
b.hotelname,  														/* name of the hotel */
b.hotelcity as city, 													/* hotel located city */
b.hotelcountry as country,  												/* hotel located country*/
c.roomtypedesc as roomtype,  												/* room type */
--bonotel.fn_get_touroperator_name(a.touroperatorid) as toname,								/* touroperator name */  
case when a.noofnights in (1,2) then 'Low' when a.noofnights in (3,4,5) then 'Medium' else 'High' end as lengthofstay,   /* total booked nights*/
(a.totalcost/a.totnoofrooms/a.noofnights/count(distinct a.reservationno ) ) as avgroomrate     				/* average room rate */
from  
ds_bk_resconfirmsv a  , 
ds_bk_reshoteldetailssv as b, 
ds_bk_resroomssv  as c  
where resstatus != 'C'     
and a.id=b.resconfirmid   
and a.id=c.resconfirmid  
and b.resconfirmid =c.resconfirmid  
group by checkindate,bookingvalue, a.reservationno,nights,guests,rooms,a.hotelcode,b.hotelname,country,city, lengthofstay,roomtype ;

/* 
     View contains the data which is used for comparison dashboard
*/ 

CREATE VIEW comparison_dashboard AS 
select distinct 
checkindate as checkindate,  													/* checkin date */
--bonotel.fn_get_res_hotel_details(a.id,'hotelcountry') as country,   								/* hotel located country */
reservationno as bookings,   													/* total number of bookings*/
totalcost as bookingvalue,    													/* total booking value */
noofnights as nights,     													/* number of nights booked*/
case when round(noofnights)in (1,2) then 'Low' when round(noofnights) in (3,4,5) then 'Medium' else 'High' end as lengthofstay, /* length of stay*/
a.touroperatorid as toid--, 													/* touroperator id */
--bonotel.fn_get_touroperator_name(a.touroperatorid) as toname, 									/* touroperator name */																																								
--bonotel.fn_get_roomtype_name  (cast(b.roomtypecode as int))  as roomtype 							/* room type */
from 
ds_bk_resconfirmsv  as a,
ds_bk_resroomssv    as b  
where  a.id=b.resconfirmid and 
resstatus != 'C'      
order by reservationno ;


/* 
     View contains the data which is used for profit analysis dashboard
*/

CREATE VIEW profit_dashboard AS 
select 
checkindate,  														/* checkin date */
TO_CHAR(checkindate, 'Month') AS month,											/* checkin date (month) */  
extract(year from checkindate) AS year, 										/* checkin date (year) */
DATE_PART('month',checkindate) as mon, 											/* checkin date (month) */ 
--bonotel.fn_get_res_hotel_details(id,'hotelcountry') as country,  							/* hotel located country */
--bonotel.fn_get_res_hotel_details(id,'hotelcity') as city,  								/* hotel located city */
count(reservationno) as bookings,  											/* total number of bookings*/
sum(totalcost) as bookingvalue,   											/* total booking value */
a.touroperatorid as toid--, 												/* touroperator id */			
--bonotel.fn_get_touroperator_name(a.touroperatorid) as toname--, 								/* touroperator name */
--sum((select sum(sellrate) from  bonotel.resdailyratessv as b where b.resconfirmid=a.id))  as sellrate, 			/* sell rate */
--sum((select sum(netrate) from  bonotel.resdailyratessv as b where b.resconfirmid=a.id))  as netrate, 			/* net rate */
--sum((select sum(sellrate-netrate) from  bonotel.resdailyratessv as b where b.resconfirmid=a.id))  as profit  		/* prfit-diffrence of the sell rate & net rate */
from 
ds_bk_resconfirmsv  as a 
where resstatus != 'C'     
group by checkindate,a.touroperatorid  
order by checkindate;   

commit;