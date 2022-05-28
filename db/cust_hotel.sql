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