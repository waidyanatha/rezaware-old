DROP VIEW IF EXISTS tm_demo_dashboard_otherwidgets;
DROP VIEW IF EXISTS tm_demo_dashboard_pacereport;

/* 
     View contains the data which is used for all the widgets other than pace report in demo Dashboard - Tourmappers
*/ 

CREATE VIEW tm_demo_dashboard_otherwidgets AS 
select    
'source_tm' as source, 													/* data source */
checkindate as check_in_date,  												/* checkin date */
a.reservationno as reservation_no,											/* reservation number */  
to_char(checkindate, 'Month') as month,   										/* checkin date (month) */
extract(year from checkindate) as year,   										/* checkin date (year) */
date_part('month',checkindate) as mon,   										/* checkin date (month as a number) */
a.totalcost as booking_value,  												/* total booking value */
(a.totalcost/a.totnoofrooms) as average_daily_rate,									/* average daily rate */
a.noofnights as nights,      												/* number of nights booked */
a.totnoofadults+a.totnoofchildren as guests,    									/* number of guests */
a.totnoofrooms as rooms,  												/* number of rooms booked */
count(a.reservationno)   as bookings,  											/* total number of bookings*/
(checkindate-reservationdate) as window_length,  									/* number of days difference between checkin date and booking date*/
a.hotelcode as hotel_code,   														/* hotel code */
b.hotelname as hote_lname,   												/* hotel name  */
b.hotelcity as city,   													/* hotel located city */ 
b.hotelcountry as country,  												/* hotel located country */
case when a.noofnights in (1,2) then 'Low' when a.noofnights in (3,4,5) then 'Medium' else 'High' end as length_of_stay,  /* number of nights booked */
(select sum(sellrate-netrate)  from  tourmappers.resdailyratessv as c where c.resconfirmid=a.id)  as profit    		/* profit (sell rate-net rate) */
from 
resconfirmsv asca   ,  
reshoteldetailssv as b  
where  resstatus != 'C'  
and a.id=b.resconfirmid   
group by check_in_date,booking_value, reservation_no,nights,guests,rooms,window_length,hotel_code,hotel_name,city,country,length_of_stay ,a.id ;
 

/* 
     View contains the data which is used for pace report in demo Dashboard - Tourmappers
*/ 

CREATE VIEW tm_demo_dashboard_pacereport AS 

select distinct  
'source_tm' as source,  													/* data source */
a.checkindate as check_in_date,  														/* checkin date */
to_char(a.checkindate, 'Month') as month,   											/* checkin date (month) */
extract(year from a.checkindate) as year,   											/* checkin date (year) */
date_part('month',a.checkindate) as mon,  											/* checkin date (month) */
a.reservationno as reservation_no,   														/* reservation number */
a.totalcost as bookingvalue,  													/* total booking value */
(a.totalcost/a.totnoofrooms) as average_room_rate,  										/* average room rate */										        
a.noofnights as nights,  													/* number of nights booked */
a.totnoofadults+a.totnoofchildren as guests,    										/* number guests */
a.totnoofrooms as sold_rooms,  													/* number of sold rooms */
count(a.reservationno)   as bookings,  												/* total number of bookings*/
(checkindate-reservationdate) as window_length,  										/* number of days difference between checkin date and booking date*/ 
a.hotel_code,   															/* hotel code */ 
b.hotelname as hotel_name,   													/* hotel name  */ 
b.hotelcity as city,   														/* hotel located city */ 
b.hotelcountry as country,  													/* hotel located country */ 
case when a.noofnights in (1,2) then 'Low' when a.noofnights in (3,4,5) then 'Medium' else 'High' end as length_of_stay,  	/* number of nights booked */ 
d.totalrooms as total_rooms,  													/* number of rooms booked */
d.vacantqty as available_rooms,  												/* number of rooms available  */
round(((cast(a.totnoofrooms as float)/cast(d.totalrooms as float) )*100):: numeric,2) as occupancy,  				/* occupancy */
round((a.totalcost/NULLIF(d.vacantqty, 0)) :: numeric,2)  as revpar,  								/* revenue per available room */
(select sum(sellrate) from  tourmappers.resdailyratessv as c where c.resconfirmid=a.id) as sell_rate,                    	/* sell rate */
(select sum(netrate) from  tourmappers.resdailyratessv as c where c.resconfirmid=a.id ) as net_rate, 				/* net rate */
(select (sum(sellrate)-sum(netrate)) from  tourmappers.resdailyratessv as c where c.resconfirmid=a.id) as profit   		/* profit */

from   
resconfirmsv a, 
reshoteldetailssv as b,  
inventory as d,  
room as e,  
resroomssv as f  
where d.roomid=e.id and   
a.id=b.resconfirmid and  
e.id=cast(f.roomcode as int)  
and f.resconfirmid=a.id  
and a.id=f.resconfirmid  
and a.checkindate=d.staydate   
and b.resconfirmid =f.resconfirmid  
and resstatus != 'C'   
and to_char(checkindate,'YYYY-MM-DD HH24:MI:SS')>='2020-05-01 00:00:00'      
and to_char(checkindate,'YYYY-MM-DD HH24:MI:SS')<='2022-06-27 23:59:59'   
group by check_in_date,booking_value, reservation_no,nights,guests,   
sold_rooms,window_length,hotel_code,hotel_name,city,country,  
length_of_stay,total_rooms,available_rooms,occupancy,revpar,a.id  
Order by check_in_date;

commit;