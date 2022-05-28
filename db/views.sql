DROP VIEW IF EXISTS reservations;
DROP VIEW IF EXISTS customerbookings;

/* 
     View contains purely the booking reservation time-series data
*/ 
CREATE VIEW reservations AS 
        SELECT vendorid AS vendor_uuid,    /* vendor id associated with the tour operator making the booking */
                agentname AS agent_name,   /* travel agent's name affiliated with the tour operator making the booking */
                hotelcode AS hotel_uuid,   /* hotel unique identifier */
                hotelname AS hotel_name,   /* hote's name */
                resstatus AS reserved_status,     /* reservation status */ 
                resdatetime AS reserved_date,     /* date and time reservation is made */ 
                --requestreservationdate AS booking_for_datetime,    /* date the reservation is made for */ 
                reservationdate AS reserved_for_datetime,          /* date reservation is made for */
                checkindate AS check_in_date,     /* check in date */
                checkoutdate AS check_out_date,   /* checkout date */
                noofnights AS number_of_nights,   /* number of nights of the booking */
                totnoofrooms AS number_of_rooms,  /* total number of rooms for this booking */
                booktype AS booking_type,         /* booking type ??? */
                currency AS agent_booking_currency, /* currency the agent makes the booking */
                totalrate AS agent_daily_rate,      /* agent's booking daily rate */
                totalcost AS agent_total_cost,      /* agent qualted total cost of the booking # nights * daily rate + amnities + taxes */
                suppliecurr AS hotel_currency,      /* hotel's working currency */
                suptotalrate AS hotel_daily_rate,   /* hotel quaoted daily rate */
                groupbookingtype AS group_booking_type     /* WHAT IS THIS ??? */
        FROM ds_bk_resconfirmsv 
        ORDER BY reservationdate;

/*
     View comprises data where the customer is from, where the booking was made, and the reservation information 
*/
CREATE VIEW customerbookings AS
        SELECT custcont.hcity AS customer_city,            /* customer located city */
                custcont.hstate AS customer_state,         /* customer located state */
                custcont.hcountry AS customer_country,     /* customer located country */
                hoteldetails.hotelname AS hotel_name,       /* hotel name */
                hoteldetails.hotelcity AS hotel_city,       /* hotel located city */
                hoteldetails.hotelstate AS hotel_state,     /* hotel located state */
                hoteldetails.hotelcountry AS hotel_country, /* hotel located country */
                resvconfirm.checkindate AS checkin_date,     /* visitor checkin date */
                resvconfirm.checkoutdate AS checkout_date,   /* visitor checkout date */
                resvconfirm.noofnights AS num_of_nights,     /* number of nights booked */
                resvconfirm.reservationdate AS booked_date,  /* date reservation was placed */
                resvconfirm.totnoofrooms AS num_of_rooms,    /* number of rooms booked */
                resvconfirm.totalrate AS daily_rate,         /* booking daily rate */
                resvconfirm.currency AS booked_currency      /* currency booking was made */  
FROM ds_bk_resconfirmsv AS resvconfirm
        RIGHT OUTER JOIN ds_bk_rescustomersv AS resvcust ON resvconfirm.id = resvcust.resconfirmid
        INNER JOIN ds_bk_reshoteldetailssv AS hoteldetails ON hoteldetails.resconfirmid = resvcust.resconfirmid
        LEFT JOIN ds_bk_rescustomercontactsv AS custcont ON custcont.id = resvcust.rescustomercontactid;
        
COMMIT;