DROP VIEW IF EXISTS bono_demo_dashboard_otherwidgets;

/* 
     View contains the data which is used for all the widgets other than pace report in demo Dashboard - Bonotel
*/ 

CREATE VIEW bono_demo_dashboard_otherwidgets AS 
select   
'source_bn' as source, 											/* data source */
checkindate as check_in_date, 									/* checkin date */
a.reservationno as reservation_no,  								/* reservation number */ 
to_char(checkindate, 'Month') as month,  							/* checkin date (month) */
extract(year from checkindate) as year,  							/* checkin date (year) */
date_part('month',checkindate) as mon, 								/* checkin date (month as a number) */ 
a.totalcost as booking_value, 									/* total booking value */
(a.totalcost/a.totnoofrooms) as average_room_rate,					/* average room rate */
a.noofnights as nights,     										/* number of nights booked */
a.totnoofadults+a.totnoofchildren as guests,   						/* number of guests */
a.totnoofrooms as rooms, 										/* number of rooms booked */
count(a.reservationno)   as bookings, 								/* total number of bookings*/
(checkindate-reservationdate) as window_length, 	 					/* number of days difference between checkin date and booking date*/
a.hotelcode as hotel_code,  										/* hotel code */
b.hotelname as hotel_name,  										/* hotel name  */
b.hotelcity as city,  											/* hotel located city */
b.hotelcountry as country, 										/* hotel located country */
case when a.noofnights in (1,2) then 'Low' when a.noofnights in (3,4,5) then 'Medium' else 'High' end as length_of_stay  /* number of nights booked */
--(select sum(sellrate-netrate) from  bonotel.resdailyratessv as c where c.resconfirmid=a.id)  as profit   		/* profit (sell rate-net rate) */
from 
ds_bk_resconfirmsv as a   , 
ds_bk_reshoteldetailssv as b 
where  resstatus != 'C'  
and a.id=b.resconfirmid  
group by check_in_date,booking_value, reservation_no,nights,guests,rooms,window_length,hotel_code,hotel_name,city,country,length_of_stay;
 
commit;