------------------------------------------------------------
-- 1. use case

declare @start_time datetime = getdate()
      , @params     xml
      , @begin_rid  bigint

set  @params = ( select isnull( cast( @ContractID as nvarchar )         , 'null') as [contractid]
                      , isnull( convert( nvarchar, @Date, 126 )         , 'null') as [date]
                      , isnull( cast( @Balance as nvarchar )            , 'null') as [balance]
                      , isnull( cast( @CurrentExchangeRate as nvarchar ), 'null') as [currentexchangerate]
                      , isnull( cast( @CurrencyID as nvarchar )         , 'null') as [currencyid]
                 for xml raw('p') );

insert dbo.queries_log( proc_name, params, step, ds, dt )
  values( 'p_name_1', @params, 0, @start_time, @start_time );
set @begin_rid = scope_identity();

-- <some code>

insert into dbo.queries_log( proc_name, params, step, ds, dt, rows_affected, begin_rid )
  values ( 'p_name_1', @params, 30, @start_time, getdate(), @@rowcount, @begin_rid )

-- <some code>

begin catch  
  insert dbo.queries_log( proc_name, params, step, ds, dt, [message], begin_rid )  
    select 'p_name_1', @params, -100, @start_time, getdate(), error_message(), @log_rid


------------------------------------------------------------
-- 2. log table creation script

-- 2.1. create partition function

set ansi_nulls on
set quoted_identifier on
go

if ( not exists( select top 1 1 from sys.partition_functions where name = 'pf_queries_log' ) )
begin
  declare @function_name  nvarchar(128)
        , @end_date       datetime
        , @ds             datetime
        , @dt             datetime
        , @ScriptOnly     bit = 0
        , @cmd            nvarchar(max)

  select  @ds = dateadd( day, datediff( day, '19000101', datediff( day, datepart( day, getdate() ) - 1, getdate() ) ), '19000101' )
        , @dt = dateadd( mm, 36, @ds )
        , @function_name = 'pf_queries_log'

  select @cmd = 'create partition function ' + @function_name + ' ( datetime )
    as range right
    for values( ''' + convert( nvarchar, @ds, 126 ) + ''''

  select @ds = dateadd( month, 1, @ds )

  while ( @ds < @dt )
  begin

    select @cmd = @cmd + ', ''' + convert( nvarchar, @ds, 126 ) + ''''
    select @ds = dateadd( month, 1, @ds )

  end
    
  select @cmd = @cmd + ')'
        
  if @ScriptOnly = 0 exec sp_executesql @cmd else print @cmd
end
go

-- 2.2. create partition schema

set ansi_nulls on
set quoted_identifier on
go

if ( not exists( select top 1 1 from sys.partition_schemes where name = 'ps_queries_log' ) )
  create partition scheme ps_queries_log
      as partition pf_queries_log all to ( [data] )
go

-- 2.3. create log table

if object_id( N'dbo.queries_log', 'U' ) is not null
  drop table dbo.queries_log
go

set ansi_nulls on
set quoted_identifier on
set ansi_padding on
go

create table dbo.queries_Log (
        rid               bigint identity ( -9223372036854775808, 1 ) not null
      , proc_name         sysname           not null
      , params            xml                   null
      , [user]            sysname               null
      , step              int               not null
      , ds                datetime          not null
      , dt                datetime          not null
      , rows_affected     int                   null
      , [message]         nvarchar(4000)        null
      , userid            int                   null
      , begin_rid         bigint                null
) on ps_queries_log( ds )
go

alter table dbo.queries_Log add constraint pk_queries_log primary key nonclustered( rid, ds );
go

create clustered index clix_queries_Log_proc_name_step_ds 
  on dbo.queries_log ( proc_name, step, ds );

create index ix_queries_Log_begin_rid
  on dbo.queries_log ( begin_rid, step )
    where begin_rid is not null;
go
