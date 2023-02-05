-- какие запросы выполняются в текущих процессах:
select rq.session_id as [spid]
     , substring( st.text
               	, ( rq.statement_start_offset / 2 ) + 1
               	, ( ( case rq.statement_end_offset when -1 
                                                   then datalength( st.text ) 
                                                   else rq.statement_end_offset 
                      end - rq.statement_start_offset 
                    ) / 2 ) + 1 ) as [sql]
     , db_name( rq.database_id ) as [db]
     , rq.start_time
     , ss.login_name
     , ss.program_name
     , rq.blocking_session_id
     , rq.open_transaction_count
     , rq.wait_type
     , rq.wait_time
     , rq.last_wait_type
     , cast( pt.query_plan as xml ) as [query_plan]
  from sys.dm_exec_requests rq 
    inner join sys.dm_exec_sessions ss on ss.session_id = rq.session_id
    cross apply sys.dm_exec_sql_text( rq.sql_handle ) st
    cross apply sys.dm_exec_text_query_plan( rq.plan_handle, rq.statement_start_offset, rq.statement_end_offset ) pt
  where rq.session_id <> @@spid
    and substring( st.text
                 , ( rq.statement_start_offset / 2 ) + 1
                 , ( ( case rq.statement_end_offset when -1 
                                                    then datalength( st.text ) 
                                                    else rq.statement_end_offset 
                       end - rq.statement_start_offset 
                     ) / 2 ) + 1 )
        like '%Client%'
