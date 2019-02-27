SELECT
     call.idcallcent_queuecalls as demand_id,
     (
       CASE
         WHEN (call.call_result IN ('WPS', 'WPCLBS', 'WCPLBS', 'WCPL', 'WPFS', 'WPCL')) OR (call.call_result IN ('W', 'WP') AND call.to_dn != '')
           THEN FALSE
         ELSE TRUE
       END
     ) AS missed,
     EXTRACT(seconds from (call.ts_polling + call.ts_waiting)::time)::numeric::integer + EXTRACT(minutes from (call.ts_polling + call.ts_waiting)::time)::numeric::integer * 60 + EXTRACT(hours from (call.ts_polling + call.ts_waiting)::time)::numeric::integer * 3600 as waiting_time,
     call.from_userpart as customer,
     call.time_start,
     EXTRACT(seconds from call.time_end - call.time_start) + 60*EXTRACT(minutes from call.time_end - call.time_start) + 3600*EXTRACT(hours from call.time_end - call.time_start) as duration,
     call.q_num as queue,
     call.to_dn AS agent
   FROM
     callcent_queuecalls call
   WHERE
     --call.q_num != '8011'
     --AND
     call.time_end >= '{}'
     AND call.time_end <= '{}'
     AND call.call_result IN ('WPS', 'WP', 'W', 'WPN', 'WN', 'WPCLBS', 'WPCLF', 'WCPLBS', 'WCPL', 'WPFS', 'WCPLF', 'WPCL')
     AND (
       call.call_result IN ('WPS', 'WPCLBS', 'WCPLBS', 'WCPL', 'WPFS', 'WPCL')
       OR (call.call_result NOT IN ('WPS', 'WPCLBS', 'WCPLBS', 'WCPL', 'WPFS', 'WPCL') AND call.to_dn = '')
       OR (call.call_result IN ('W', 'WP') AND call.to_dn != '')
     );