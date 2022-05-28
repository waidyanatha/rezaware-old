SELECT custcont.hcountry AS customer_country,
        custcont.hstate AS customer_state,
        custcont.hcity AS customer_city
FROM ds_bk_resconfirmsv as resvconfim 
        LEFT OUTER JOIN ds_bk_rescustomersv AS rescust ON resvconfim.id = rescust.resconfirmid
        RIGHT OUTER JOIN ds_bk_rescustomercontactsv AS custcont ON rescust.rescustomercontactid = custcont.id;
        /* resvconfim.id, rescust.resconfirmid, rescust.rescustomercontactid, custcont.id */