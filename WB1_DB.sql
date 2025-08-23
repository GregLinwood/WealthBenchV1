use WealthBench

Begin /* Drop Objects */
print 'Drop Objects'

/* Drop Procedures */
drop procedure if exists [int].[RebalanceFund]
drop procedure if exists [int].[RebalanceAcct]
drop procedure if exists [ext].[Rebalance]
drop procedure if exists [int].[GetThreadsToComplete]
drop procedure if exists [int].[GetThreadNextAccnt]
drop procedure if exists [int].[GetThreadNextQueueNo]
drop procedure if exists [ext].[PriceUpdate]
drop procedure if exists [int].[PriceUpdate]
drop procedure if exists [ext].[Dividend]
drop procedure if exists [int].[Dividend]
drop procedure if exists [ext].[Deposit]
drop procedure if exists [int].[Deposit]
drop procedure if exists [ext].[Withdrawal]
drop procedure if exists [int].[Withdrawal]
drop procedure if exists [mon].[GetProgressBars]
drop procedure if exists [mon].[ResetProgressBars]
drop procedure if exists [mon].[ThreadIncrementTimeUs]
drop procedure if exists [mon].[AcctRebalCount]
drop procedure if exists [mon].[FundRebalCount]
drop procedure if exists [load].[WorkerThreads]
drop procedure if exists [load].[Investors]
drop procedure if exists [load].[ModelAssets]
drop procedure if exists [load].[Models]
drop procedure if exists [load].[Advisors]
drop procedure if exists [load].[Assets]
drop procedure if exists [load].[DeleteAllData]

/* Drop Table Types */
drop type if exists dbo.Holdings
drop type if exists dbo.RebalanceAsset
drop type if exists dbo.FundVsAccntsHolding
drop type if exists dbo.ChartCountIntKey
drop type if exists [load].[ModelRandomizer]

/* Drop Tables */
drop table if exists Logging.GainUnrealised
drop table if exists Logging.Rebalance
drop table if exists Logging.RebalanceAsset
drop table if exists Logging.PriceUpdate
drop table if exists Mutex.PriceSnapshot
drop table if exists Thread.ErrorLog
drop table if exists Thread.Monitor
drop table if exists Thread.Queue
drop table if exists Thread.State
drop table if exists Investor.Withdrawal
drop table if exists Investor.Deposit
drop table if exists Investor.CorporateAction
drop table if exists Investor.Holding
drop table if exists Investor.Transact
drop table if exists Investor.GainUnrealised
drop table if exists Investor.GainRealised
drop table if exists Investor.Trade
drop table if exists Investor.AccountRebalance
drop table if exists Investor.Balance
drop table if exists Investor.AccountSeq
drop table if exists Investor.Account
drop table if exists Investor.Investor
drop table if exists Fund.ModelAssetPrice
drop table if exists Fund.CorporateAction
drop table if exists Fund.RebalancePricing
drop table if exists Fund.HoldingHistory
drop table if exists Fund.Holding
drop table if exists Fund.Trade
drop table if exists Fund.Rebalance
drop table if exists Fund.BalanceHistory
drop table if exists Fund.Balance
drop table if exists Fund.Fund
drop table if exists Advisor.ModelAsset
drop table if exists Advisor.Model
drop table if exists Advisor.Advisor
drop table if exists Market.CorporateActionFund
drop table if exists Market.CorporateAction
drop table if exists Market.ListingPriceHistory
drop table if exists Market.ListingPrice
drop table if exists Market.Listing
drop table if exists Market.Asset
drop table if exists Market.AssetClass
drop table if exists Market.Exchange
drop table if exists Market.Currency
drop table if exists Load.Nums

End
go

Begin /* Drop Schemas */
print 'Drop Schemas'

/* Drop Schemas */
drop schema if exists Mutex
drop schema if exists Logging
drop schema if exists Thread
drop schema if exists Advisor
drop schema if exists Market
drop schema if exists Fund
drop schema if exists Investor
drop schema if exists [int]
drop schema if exists [ext]
drop schema if exists [mon]
drop schema if exists [load]

End
go

Begin /* Create Schemas */
print 'CreateSchemas'

/* Create Schemas */
exec('create schema Investor')
exec('create schema Fund')
exec('create schema Market')
exec('create schema Advisor')
exec('create schema Thread')
exec('create schema Logging')
exec('create schema Mutex')

exec('create schema [int]')
exec('create schema [ext]')
exec('create schema [mon]')
exec('create schema [load]')
End
go

Begin /* Create Tables */
print 'Create Tables'

/* Create Tables */

declare @s nvarchar(max)
      , @inmemtyp tinyint = 1
      , @inmemtbl tinyint = 1
      , @inmemtbldurable tinyint = 0
	  , @dur nvarchar(max)
      , @fks tinyint = 0

if @inmemtbldurable = 0 select @dur = N', durability = SCHEMA_ONLY' else select @dur = N''

Begin /* Table Types */
print 'Table Types'

/* dbo.Holdings */
select @s = N'
create type dbo.Holdings as table (
   Asset_id integer not null
 , Exch_id smallint not null
 , Weighting decimal(8, 2) null
 , Units decimal(18, 2) not null
 , Price decimal(10, 2) not null default(0)'
if @inmemtyp = 1 select @s += N' , primary key nonclustered (Asset_id)
)  with (memory_optimized = on) '
else select @s += N' , primary key clustered (Asset_id)
) '
exec(@s)

/* dbo.RebalanceAsset */
select @s = N'
create type dbo.RebalanceAsset as table (
   Asset_id integer not null
 , Exch_id smallint not null
 , Curr_id tinyint not null
 , Price decimal(10, 2) not null
 , Units_current decimal(18, 2) not null
 , Units_target decimal(18, 2) not null
 , IsBuy tinyint null
 , IsSell tinyint null
 , IsTrade tinyint null'
if @inmemtyp = 1 select @s += N' 
 , primary key nonclustered (Asset_id)
 , index ncix_RebalanceAsset_IsTrade nonclustered (Asset_id, Exch_id, Curr_id, IsTrade)
 , index ncix_RebalanceAsset_IsBuy nonclustered   (Asset_id, Exch_id, Curr_id, IsBuy)
 , index ncix_RebalanceAsset_IsSell nonclustered  (Asset_id, Exch_id, Curr_id, IsSell)
)  with (memory_optimized = on) '
else select @s += N'
 , primary key nonclustered (Asset_id)
) '
exec(@s)

/* dbo.FundVsAccntsHolding */
select @s = N'
create type dbo.FundVsAccntsHolding as table (
Id integer not null identity(1, 1)
 , Asset_id_Fund integer not null
 , Exch_id_Fund smallint not null
 , Units_Fund decimal(18, 2) not null
 , Asset_id_Accnts integer not null
 , Exch_id_Accnts smallint not null
 , Units_Accnts decimal(18, 2) not null'
if @inmemtyp = 1 select @s += N' , primary key nonclustered (Id)
)  with (memory_optimized = on) '
else select @s += N' , primary key clustered (Id)
) '
exec(@s)

/* dbo.ChartCountIntKey */
select @s = N'
create type dbo.ChartCountIntKey as table (
   Id bigint not null
 , Cnt bigint not null'
if @inmemtbl = 1 select @s += N'
 , primary key nonclustered (Id)
)  with (memory_optimized = on) '
else select @s += N'
 , primary key clustered (Id)
) '
exec(@s)

/* load.ModelRandomizer */
select @s = N'
create type load.ModelRandomizer as table (
   Id int not null
 , Model_id smallint not null
 --, Model_name nvarchar (256) not null
 --, Advisor_id smallint not null
 --, Curr_id tinyint not null
 --, AssetsCount int not null
'
if @inmemtbl = 1 select @s += N'
 , primary key nonclustered (Id)
)  with (memory_optimized = on) '
else select @s += N'
 , primary key clustered (Id)
) '
exec(@s)

End

Begin /* Market Tables */
print 'Market Tables'

/* Market.Currency */
select @s = N'
create table Market.Currency (
   Curr_id tinyint not null
 , Curr_code char(3) not null
 , Curr_name nvarchar(256) not null'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_MarketCurrency primary key nonclustered (Curr_code)
 , constraint uncix_MarketCurrency_id unique nonclustered (Curr_id)
 , constraint uncix_MarketCurrency_name unique nonclustered (Curr_name)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_MarketCurrency primary key clustered (Curr_code)
 , constraint uncix_MarketCurrency_id unique nonclustered (Curr_id)
 , constraint uncix_MarketCurrency_name unique nonclustered (Curr_name)
) '
exec(@s)

/* Market.Exchange */
select @s = N'
create table Market.Exchange (
   Exch_id smallint not null
 , Exch_code varchar(10) not null
 , Exch_name nvarchar(256) not null
 , ISIN_Prefix char(2) not null
 , Curr_id tinyint not null'
if @fks = 1 select @s += N'
 , constraint fk_MarketExchange_Curr_id foreign key (Curr_id) references Market.Currency (Curr_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_MarketExchange primary key nonclustered (Exch_code)
 , constraint sk_MarketExchange_Curr_id unique nonclustered (Exch_id)
 , constraint ak_MarketExchange_Curr_id unique nonclustered (Exch_code, Curr_id)
 , constraint uncix_MarketExchange_name unique nonclustered (Exch_name)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_MarketExchange primary key  clustered (Exch_code)
 , constraint sk_MarketExchange_Curr_id unique nonclustered (Exch_id)
 , constraint ak_MarketExchange_Curr_id unique nonclustered (Exch_code, Curr_id)
 , constraint uncix_MarketExchange_name unique nonclustered (Exch_name)
) '
exec(@s)

/* Market.AssetClass */
select @s = N'
create table Market.AssetClass (
   Class_code varchar(64) not null
 , Class_name nvarchar(256) not null'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_MarketAssetClass primary key nonclustered (Class_code)
 , constraint uncix_MarketAssetClass unique nonclustered (Class_name)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_MarketAssetClass primary key clustered (Class_code)
 , constraint uncix_MarketAssetClass unique nonclustered (Class_name)
) '
exec(@s)

/* Market.Asset */
select @s = N'
create table Market.Asset (
   Asset_id integer not null
 , ISIN_code char(12)  null
 , Asset_name nvarchar(256) not null
 , Class_code varchar(64) not null'
if @fks = 1 select @s += N'
 , constraint fk_MarketAsset_Class_code foreign key (Class_code) references Market.AssetClass (Class_code)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_MarketAsset primary key nonclustered (Asset_id)
 , constraint uncix_MarketAsset unique nonclustered (Asset_name)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_MarketAsset primary key nonclustered (Asset_id)
 , constraint uncix_MarketAsset unique nonclustered (Asset_name)
) '
exec(@s)

/* Market.Listing */
select @s = N'
create table Market.Listing (
   Asset_id integer not null
 , Exch_id smallint not null
 , ISIN_Code char(12) not null
 , Curr_id tinyint not null'
if @fks = 1 select @s += N'
 , constraint fk_MarketListing_Asset_id foreign key (Asset_id) references Market.Asset (Asset_id)
 , constraint fk_MarketListing_Exch_id foreign key (Exch_id, Curr_id) references Market.Exchange (Exch_id, Curr_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_MarketListing primary key nonclustered (Asset_id, Exch_id, Curr_id)
 , constraint uncix_MarketListing_Curr_id unique nonclustered (Asset_id, Curr_id)
 , index ncix_MarketListing_Curr_id unique nonclustered (Curr_id, Asset_id)
 , index ncix_MarketListing_Exch_id unique nonclustered (Exch_id, Asset_id, Curr_id)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_MarketListing primary key clustered (Asset_id, Exch_id, Curr_id)
 , constraint uncix_MarketListing_Curr_id unique nonclustered (Asset_id, Curr_id)
 , index ncix_MarketListing_Curr_id unique nonclustered (Curr_id, Asset_id)
 , index ncix_MarketListing_Exch_id unique nonclustered (Exch_id, Asset_id, Curr_id)
) '
exec(@s)

/* Market.ListingPrice */
select @s = N'
create table Market.ListingPrice (
   Asset_id integer not null
 , Exch_id smallint not null
 , Curr_id tinyint not null
 , Price decimal(20, 2) not null'
if @fks = 1 select @s += N'
 , constraint fk_MarketListingPrice_Key foreign key (Asset_id, Exch_id, Curr_id) references Market.Listing (Asset_id, Exch_id, Curr_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_MarketListingPrice primary key nonclustered (Asset_id, Exch_id, Curr_id)
 , index ix_MarketListingPrice nonclustered (Curr_id, Asset_id, Exch_id)
 , index ix_MarketListingPrice_Curr_id nonclustered (Curr_id)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_MarketListingPrice primary key clustered (Asset_id, Exch_id, Curr_id)
 , index ix_MarketListingPrice nonclustered (Curr_id, Asset_id, Exch_id)
) '
exec(@s)

/* Market.ListingPriceHistory */
select @s = N'
create table Market.ListingPriceHistory (
   Id bigint not null identity(1, 1)
 , Asset_id integer not null
 , Exch_id smallint not null
 , Price_dt datetime not null
 , Price decimal(20, 2) not null
 , Curr_id tinyint not null'
if @fks = 1 select @s += N'
 , constraint fk_MarketListingPriceHistory_Asset_id foreign key (Asset_id, Exch_id, Curr_id) references Market.Listing (Asset_id, Exch_id, Curr_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_MarketListingPriceHistory primary key nonclustered (Id)--(Asset_id, Exch_id, Price_dt)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_MarketListingPriceHistory primary key clustered  (Id)--(Asset_id, Exch_id, Price_dt)
) '
exec(@s)

/* Market.CorporateAction */
select @s = N'
create table Market.CorporateAction (
   CorpAct_id uniqueidentifier not null
 , CorpAct_type char(3) not null constraint chkMarketCorporateActionCorpActType check (CorpAct_type in (''Div'', ''Spl'', ''Rev'', ''Mer'', ''Spi''))
 , Asset_id integer not null
 , To_Asset_id integer null
 , Exch_id smallint not null
 , Curr_id tinyint not null
 , CorpAct_dt datetime2 null
 , Ex_dt datetime2 null
 , Inc_dt datetime2 null
 , Amount decimal(20, 2) null
 , Ratio_from decimal(20, 12) null
 , Ratio_to decimal(20, 12) null'
if @fks = 1 select @s += N'
 , constraint fk_MarketCorporateAction_Asset_id foreign key (Asset_id, Exch_id, Curr_id) references Market.Listing (Asset_id, Exch_id, Curr_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_MarketCorporateAction primary key nonclustered (CorpAct_id)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_MarketCorporateAction primary key nonclustered (CorpAct_id)
) '
exec(@s)

/* Market.CorporateActionFund */
select @s = N'
create table Market.CorporateActionFund (
   CorpAct_id uniqueidentifier not null
 , Fund_id smallint not null
 , CorpAct_type char(3) not null constraint chkMarketCorporateActionFundCorpActType check (CorpAct_type in (''Div'', ''Spl'', ''Rev'', ''Mer'', ''Spi''))
 , Asset_id integer not null
 , To_Asset_id integer null
 , Exch_id smallint not null
 , Curr_id tinyint not null
 , CorpAct_dt datetime2 null
 , Ex_dt datetime2 null
 , Inc_dt datetime2 null
 , Amount decimal(20, 2) null
 , Ratio_from decimal(20, 12) null
 , Ratio_to decimal(20, 12) null'
if @fks = 1 select @s += N'
 , constraint fk_MarketCorporateActionFund_Asset_id foreign key (Asset_id, Exch_id, Curr_id) references Market.Listing (Asset_id, Exch_id, Curr_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_MarketCorporateActionFund primary key nonclustered (Fund_id, CorpAct_id)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_MarketCorporateActionFund primary key nonclustered (Fund_id, CorpAct_id)
) '
exec(@s)

End

Begin /* Fund Tables */
print 'Fund Tables'

/* Fund.Fund */
select @s = N'
create table Fund.Fund (
   Fund_id smallint not null
 , Fund_name nvarchar(256) not null
 , Curr_id tinyint not null
 , AccntsCount integer not null
 , MaxAccnt_id integer not null'
if @fks = 1 select @s += N'
 , constraint fk_Fund_Curr_id foreign key (Curr_id) references Market.Currency (Curr_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_Fund primary key nonclustered (Fund_id, Curr_id)
 , constraint ak_Fund_id_Curr_id unique nonclustered (Fund_id)
 , constraint uncix_Fund_name unique nonclustered (Fund_name)
 , constraint index_Fund_Curr_id unique nonclustered (Curr_id)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_Fund primary key clustered (Fund_id, Curr_id)
 , constraint ak_Fund_id_Curr_id unique nonclustered (Fund_id)
 , constraint uncix_Fund_name unique nonclustered (Fund_name)
 , constraint index_Fund_Curr_id unique nonclustered (Curr_id)
) '
exec(@s)

/* Fund.Balance */
select @s = N'
create table Fund.Balance (
   Fund_id smallint not null
 , Rebal_id integer null
 , Curr_id tinyint not null
 , FundHoldings decimal(20, 2) not null default (0)
 , InvestorHoldings decimal(20, 2) not null default (0)
 , InvestorCash decimal(20, 2) not null default (0)'
if @fks = 1 select @s += N'
 , constraint fk_FundBalance_Fund_id foreign key (Fund_id, Curr_id) references Fund.Fund (Fund_id, Curr_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_FundBalance primary key nonclustered (Fund_id, Curr_id)
 , constraint ak_FundBalance_Curr_id unique nonclustered (Fund_id)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_FundBalance primary key clustered (Fund_id, Curr_id)
 , constraint ak_FundBalance_Curr_id unique nonclustered (Fund_id)
) '
exec(@s)

/* Fund.BalanceHistory */
select @s = N'
create table Fund.BalanceHistory (
   Fund_id smallint not null
 , Rebal_id integer not null
 , Curr_id tinyint not null
 , FundHoldings decimal(20, 2) not null default (0)
 , InvestorHoldings decimal(20, 2) not null default (0)
 , InvestorCash decimal(20, 2) not null default (0)'
if @fks = 1 select @s += N'
 , constraint fk_FundBalanceHistory_Fund_id foreign key (Fund_id, Curr_id) references Fund.Fund (Fund_id, Curr_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_FundBalanceHistory primary key nonclustered (Fund_id, Rebal_id, Curr_id)
 , constraint ak_FundBalanceHistory_Curr_id unique nonclustered (Fund_id, Rebal_id)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_FundBalanceHistory primary key clustered (Fund_id, Rebal_id, Curr_id)
 , constraint ak_FundBalanceHistory_Curr_id unique nonclustered (Fund_id, Rebal_id)
) '
exec(@s)

/* Fund.Rebalance */
select @s = N'
create table Fund.Rebalance (
   Fund_id smallint not null
 , Rebal_id integer not null
 , Curr_id tinyint not null
 , AccntsCount integer not null default(-1)
 , Start_dt datetime not null
 , End_dt datetime not null default(''01-Jan-1900'')
 , InvestorHoldings decimal(20, 2) null
 , InvestorCash decimal(20, 2) null
 , Deposits decimal(20, 2) null
 , Withdrawals decimal(20, 2) null
 , Income decimal(20, 2) null
 , FeesCharged decimal(20, 2) null'
if @fks = 1 select @s += N'
 , constraint fk_RebalanceFund_Fund_id foreign key (Fund_id, Curr_id) references Fund.Fund (Fund_id, Curr_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_FundRebalance_Rebal_Fund_Curr_id primary key nonclustered (Fund_id, Rebal_id, Curr_id)
 , constraint ak_FundRebalance_1 unique nonclustered (Fund_id, Rebal_id)
 , constraint ak_FundRebalance_2 unique nonclustered (Fund_id, End_dt) /* Funds should only have one Rebalance "open" at once */
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_FundRebalance_Rebal_Fund_Curr_id primary key clustered (Rebal_id, Fund_id, Curr_id)
 , constraint ak_FundRebalance_1 unique nonclustered (Fund_id, Rebal_id)
 , constraint ak_FundRebalance_2 unique nonclustered (Fund_id, End_dt) /* Funds should only have one Rebalance "open" at once */
) '
exec(@s)

/* Fund.RebalancePricing */
select @s = N'
create table Fund.RebalancePricing (
   Fund_id smallint not null
 , Rebal_id integer not null
 , Asset_id integer not null
 , Exch_id smallint not null
 , Curr_id tinyint not null
 , Price decimal(10, 2) not null'
--if @fks = 1 select @s += N'
-- , constraint fk_ListingPrice_Key foreign key (Asset_id, Exch_id, Curr_id) references Market.Listing (Asset_id, Exch_id, Curr_id)
--'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_FundRebalancePricing primary key nonclustered (Fund_id, Rebal_id, Asset_id, Exch_id, Curr_id)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_FundRebalancePricing primary key clustered (Fund_id, Rebal_id, Asset_id, Exch_id, Curr_id)
) '
exec(@s)

/* Fund.Trade */
select @s = N'
create table Fund.Trade (
   Rebal_id integer not null
 , Asset_id integer not null
 , Trade_type char(1) not null constraint chkFundTradeType check (Trade_type in (''B'', ''S''))
 , Exch_id smallint not null
 , Fund_id smallint not null
 , Curr_id tinyint not null
 , CurrentUnits decimal(18, 2) not null
 , TradeUnits decimal(18, 2) not null
 , NewUnits decimal(18, 2) not null
 , CurrentPrice decimal(10, 2) not null
 , CurrentValue decimal(20, 2) not null
 , Status char(1) null default(''P'')'
if @fks = 1 select @s += N'
 , constraint fk_FundTrade_Listing foreign key (Asset_id, Exch_id, Curr_id) references Market.Listing (Asset_id, Exch_id, Curr_id)
 , constraint fk_FundTrade_Rebal_id_Fund_id foreign key (Fund_id, Rebal_id, Curr_id) references Fund.Rebalance (Fund_id, Rebal_id, Curr_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_FundTrade primary key nonclustered (Fund_id, Rebal_id, Asset_id, Trade_type)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_FundTrade primary key clustered (Fund_id, Rebal_id, Asset_id, Trade_type)
) '
exec(@s)

/* Fund.Holding */
select @s = N'
create table Fund.Holding (
   Fund_id smallint not null
 , Asset_id integer not null
 , Exch_id smallint not null
 , Curr_id tinyint not null
 , Units decimal(18, 2) not null
 , AcctUnits decimal(18, 2) not null
 , RebalPrice decimal(10, 2) not null'
if @fks = 1 select @s += N'
 , constraint fk_FundHolding_Fund_Curr_id foreign key (Fund_id, Curr_id) references Fund.Fund (Fund_id, Curr_id)
 , constraint fk_FundHolding_Listing foreign key (Asset_id, Exch_id, Curr_id) references Market.Listing (Asset_id, Exch_id, Curr_id)
'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_FundHolding primary key nonclustered (Fund_id, Asset_id, Exch_id, Curr_id)
 --, constraint ak_FundHolding unique nonclustered (Fund_id, Asset_id)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_FundHolding primary key clustered (Fund_id, Asset_id, Exch_id, Curr_id)
 --, constraint ak_FundHolding unique nonclustered (Fund_id, Asset_id)
) '
exec(@s)

/* Fund.HoldingHistory */
select @s = N'
create table Fund.HoldingHistory (
   Fund_id smallint not null
 , Rebal_id integer not null
 , Asset_id integer not null
 , Exch_id smallint not null
 , Curr_id tinyint not null
 , Units decimal(18, 2) not null
 , AcctUnits decimal(18, 2) not null
 , RebalPrice decimal(10, 2) not null'
if @fks = 1 select @s += N'
 , constraint fk_FundHoldingHistory_Fund_Curr_id foreign key (Fund_id, Curr_id) references Fund.Fund (Fund_id, Curr_id)
 , constraint fk_FundHoldingHistory_Listing foreign key (Asset_id, Exch_id, Curr_id) references Market.Listing (Asset_id, Exch_id, Curr_id)
'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_FundHoldingHistory primary key nonclustered (Fund_id, Rebal_id, Curr_id, Asset_id)
 --, constraint ak_FundHoldingHistory unique nonclustered (Fund_id, Rebal_id, Asset_id)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_FundHoldingHistory primary key clustered (Fund_id, Rebal_id, Curr_id, Asset_id)
 --, constraint ak_FundHoldingHistory unique nonclustered (Fund_id, Rebal_id, Asset_id)
) '
exec(@s)

/* Fund.CorporateAction */
select @s = N'
create table Fund.CorporateAction (
   Fund_id smallint not null
 , Rebal_id integer null
 , CorpAct_id uniqueidentifier not null
 , CorpAct_type char(3) not null constraint chkFundCorporateActionCorpActType check (CorpAct_type in (''Div'', ''Spl'', ''Rev'', ''Mer'', ''Spi''))
 , Asset_id integer not null
 , To_Asset_id integer null
 , Exch_id smallint not null
 , Curr_id tinyint not null
 , CorpAct_dt datetime2 null
 , Ex_dt datetime2 null
 , Inc_dt datetime2 null
 , Amount decimal(20, 2) null
 , Ratio_from decimal(20, 12) null
 , Ratio_to decimal(20, 12) null'
if @fks = 1 select @s += N'
 , constraint fk_FundCorporateAction_Asset_id foreign key (CorpAct_id) references Market.CorporateAction (CorpAct_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_FundCorporateAction primary key nonclustered (Fund_id, CorpAct_id)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_FundCorporateAction primary key nonclustered (Fund_id, CorpAct_id)
) '
exec(@s)

/* Fund.ModelAssetPrice */
select @s = N'
create table Fund.ModelAssetPrice (
   Fund_id smallint not null
 , Rebal_id integer not null
 , Model_id smallint not null
 , Asset_id integer not null
 , Exch_id smallint not null
 , Curr_id tinyint not null
 , Weighting decimal(8, 2) not null
 , Curr_Price decimal(10, 2) not null'
if @fks = 1 select @s += N'
 , constraint fk_FundModelAssetPrice_Model_Curr_id foreign key (Model_id, Curr_id) references Advisor.Model (Model_id, Curr_id)
 , constraint fk_FundModelAssetPrice_Asset_Listing foreign key (Asset_id, Exch_id, Curr_id) references Market.Listing (Asset_id, Exch_id, Curr_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_FundModelAssetPrice primary key nonclustered (Fund_id, Rebal_id, Model_id, Asset_id, Exch_id, Curr_id)
 , constraint ak_FundModelAssetPrice unique nonclustered (Fund_id, Rebal_id, Model_id, Asset_id, Exch_id)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_FundModelAssetPrice primary key clustered (Fund_id, Rebal_id, Model_id, Asset_id, Exch_id, Curr_id)
 , constraint ak_FundModelAssetPrice unique nonclustered (Fund_id, Rebal_id, Model_id, Asset_id, Exch_id)
) '
exec(@s)

End

Begin /* Advisor Tables */
print 'Advisor Tables'

/* Advisor.Advisor */
select @s = N'
create table Advisor.Advisor (
   Advisor_id smallint not null
 , Advisor_name nvarchar(256) not null'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_Advisor primary key nonclustered (Advisor_id)
 , constraint uncix_Advisor_name unique nonclustered (Advisor_name)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_Advisor primary key clustered (Advisor_id)
 , constraint uncix_Advisor_name unique nonclustered (Advisor_name)
) '
exec(@s)

/* Advisor.Model */
select @s = N'
create table Advisor.Model (
   Model_id smallint not null
 , Model_name nvarchar(256) not null
 , Advisor_id smallint not null
 , Curr_id tinyint not null
 , AssetsCount int null'
if @fks = 1 select @s += N'
 , constraint fk_AdvisorModel_Advisor_id foreign key (Advisor_id) references Advisor.Advisor (Advisor_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_AdvisorModel_Curr_id primary key nonclustered (Model_id, Curr_id)
 , constraint ak_AdvisorModel unique nonclustered (Model_id)
 , constraint uncix_AdvisorModel_name unique nonclustered (Model_name)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_AdvisorModel_Curr_id primary key clustered (Model_id, Curr_id)
 , constraint ak_AdvisorModel unique nonclustered (Model_id)
 , constraint uncix_AdvisorModel_name unique nonclustered (Model_name)
) '
exec(@s)

/* Advisor.ModelAsset */
select @s = N'
create table Advisor.ModelAsset (
   Model_id smallint not null
 , Asset_id integer not null
 , Exch_id smallint not null
 , Curr_id tinyint not null
 , Weighting decimal(8, 2) not null'
if @fks = 1 select @s += N'
 , constraint fk_AdvisorModelAsset_Model_Curr_id foreign key (Model_id, Curr_id) references Advisor.Model (Model_id, Curr_id)
 , constraint fk_AdvisorModelAsset_Asset_Listing foreign key (Asset_id, Exch_id, Curr_id) references Market.Listing (Asset_id, Exch_id, Curr_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_AdvisorModelAsset primary key nonclustered (Model_id, Asset_id, Curr_id)
 , constraint ak_AdvisorModelAsset unique nonclustered (Model_id, Asset_id)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_AdvisorModelAsset primary key clustered (Model_id, Asset_id, Curr_id)
 , constraint ak_AdvisorModelAsset unique nonclustered (Model_id, Asset_id)
) '
exec(@s)

end

Begin /* Investor Tables */
print 'Investor Tables'

/* Investor.Investor */
select @s = N'
create table Investor.Investor (
   Inv_id integer not null
 , Inv_name nvarchar(256) not null'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_Investor primary key nonclustered (Inv_id)
 , constraint uncix_Investor_name unique nonclustered (Inv_name)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_Investor primary key clustered (Inv_id)
 , constraint uncix_Investor_name unique nonclustered (Inv_name)
) '
exec(@s)

/* Investor.Account */
select @s = N'
create table Investor.Account (
   Accnt_id integer not null
 , Inv_id integer not null
 , Curr_id tinyint not null
 , Fund_id smallint not null
 , Model_id smallint not null'
if @fks = 1 select @s += N'
 , constraint fk_InvestorAccount_Inv_Id foreign key (Inv_id) references Investor.Investor (Inv_id)
 , constraint fk_InvestorAccount_Curr_id foreign key (Curr_id) references Market.Currency (Curr_id)
 , constraint fk_InvestorAccount_Model_id_Curr_id foreign key (Model_id, Curr_id) references Advisor.Model (Model_id, Curr_id)
 , constraint fk_InvestorAccount_Fund_id foreign key (Fund_id, Curr_id) references Fund.Fund (Fund_id, Curr_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_InvestorAccount primary key nonclustered (Fund_id, Accnt_id, Curr_id)
 , constraint ak_InvestorAccount unique nonclustered (Accnt_id)
 , index ix_InvestorAccount_Curr_id nonclustered (Curr_id, Accnt_id)
)  with (memory_optimized = on'+@dur+N')
--alter table Investor.Account add index ix_InvestorAccount_Fund_id nonclustered (Fund_id, Accnt_id, Curr_id)'
else select @s += N'
 , constraint pk_InvestorAccount primary key clustered (Fund_id, Accnt_id, Curr_id)
 , constraint ak_InvestorAccount unique nonclustered (Accnt_id)
 , index ix_InvestorAccount_Curr_id nonclustered (Curr_id, Accnt_id)
) 
--alter table Investor.Account add index ix_InvestorAccount_Fund_id nonclustered (Fund_id, Accnt_id, Curr_id)
--create nonclustered index ix_InvestorAccount_Fund_id on Investor.Account (Fund_id, Accnt_id, Curr_id) '
exec(@s)

/* Investor.AccountSeq */
select @s = N'
create table Investor.AccountSeq (
   Fund_id smallint not null
 , Accnt_id integer not null
 , Tran_id integer not null
 , Trade_id integer not null'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_InvestorAccountSeq primary key nonclustered (Fund_id, Accnt_id)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_InvestorAccountSeq primary key clustered (Fund_id, Accnt_id)
) '
exec(@s)

/* Investor.Balance */
select @s = N'
create table Investor.Balance (
   Fund_id smallint not null
 , Accnt_id integer not null
 , Rebal_id integer not null
 , Rebalanced_dt datetime2 null
 , HoldingsValue decimal(20, 2) not null default (0)
 , CashBalance decimal(20, 2) not null default (0)'
if @fks = 1 select @s += N'
 , constraint fk_InvestorBalance_Account foreign key (Fund_id, Accnt_id, Curr_id) references Investor.Account (Fund_id, Accnt_id, Curr_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_InvestorBalance primary key nonclustered (Fund_id, Accnt_id, Rebal_id)
 , constraint ak_InvestorBalance unique nonclustered (Fund_id, Rebal_id, Accnt_id)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_InvestorBalance primary key clustered (Fund_id, Accnt_id, Rebal_id)
 , constraint ak_InvestorBalance unique nonclustered (Fund_id, Rebal_id, Accnt_id)
) '
exec(@s)

/* Investor.AccountRebalance */
select @s = N'
create table Investor.AccountRebalance (
   Fund_id smallint not null
 , Rebal_id integer not null
 , Accnt_id integer not null
 , Curr_id tinyint not null
 , Rebal_dt datetime2(6) not null
 , Model_id smallint not null
 , Sql_Process_id smallint null
 , ThreadId smallint not null
 , CurrentCashBalance decimal(20, 2) null
 , NewDeposits decimal(20, 2) null
 , NewWithdrawals decimal(20, 2) null
 , CurrentHoldingsValue decimal(20, 2) null
 , MinimumBalance decimal(20, 2) null
 , TargetHoldingsValue decimal(20, 2) null
 , RebalancedHoldingsValue decimal(20, 2) null
 , SumOfBuys decimal(20, 2) null
 , SumOfSells decimal(20, 2) null
 , NoOfBuys integer null
 , NoOfSells integer null
 , FeeCharged decimal(20, 2) null
 , NewIncome decimal(20, 2) null
 , NewCashBalance decimal(20, 2) null
 , BuyTranId integer null
 , SellTranId integer null
 , FeeTranId integer null'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_InvestorAccountRebalance primary key nonclustered (Fund_id, Rebal_id, Accnt_id)
)  with (memory_optimized = on, durability = SCHEMA_ONLY) '
else select @s += N'
 , constraint pk_InvestorAccountRebalance primary key clustered (Fund_id, Rebal_id, Accnt_id)
) '
exec(@s)

/* Investor.Trade */
select @s = N'
create table Investor.Trade (
   Fund_id smallint not null
 , Rebal_id integer not null
 , Accnt_id integer not null
 , Trade_id integer not null
 , TradeTime datetime2 not null
 , Trade_type char(1) not null
 , Asset_id integer not null
 , Exch_id smallint not null
 , Curr_id tinyint not null
 , Units decimal(18, 2) not null
 , Price decimal(10, 2) not null
 , Value decimal(20, 2) not null
 , Has_Units tinyint not null
 , Hold_Units decimal(18, 2) not null '
if @fks = 1 select @s += N'
 , constraint fk_InvestorTrade_Listing foreign key (Asset_id, Exch_id, Curr_id) references Market.Listing (Asset_id, Exch_id, Curr_id)
 , constraint fk_InvestorTrade_Rebal_id_Accnt_id foreign key (Fund_id, Rebal_id, Accnt_id, Fund_id, Curr_id) references Investor.AccountRebalance (Fund_id, Rebal_id, Accnt_id, Fund_id, Curr_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_InvestorTrade primary key                     nonclustered (Fund_id, Rebal_id, Accnt_id, Asset_id, Trade_type)
 , index ix_InvestorTrade_Accnt_id_Asset_id                    nonclustered (Fund_id, Accnt_id, Asset_id, Exch_id, Curr_id, Trade_type, TradeTime)
 , index ix_InvestorTrade_Fund_id_Accnt_id_Asset_id_Trade_type nonclustered (Fund_id, Rebal_id, Accnt_id, Trade_type, Trade_id)
 , index ix_InvestorTrade_RealisedGain                         nonclustered (Fund_id, Accnt_id, Asset_id, Exch_id, Trade_type, Has_Units, TradeTime)
 , index ix_InvestorTrade_GainUnrealised2                      nonclustered (Fund_id, Accnt_id, Trade_type, Has_Units, Trade_id)
 , index ix_InvestorTrade_GainUnrealised                       nonclustered (Fund_id, Accnt_id, Trade_type, Has_Units)
 , index ix_InvestorTrade_Trade_id                      unique nonclustered (Fund_id, Accnt_id, Trade_id)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_InvestorTrade primary key                        clustered (Fund_id, Rebal_id, Accnt_id, Asset_id, Trade_type)
 , index ix_InvestorTrade_Accnt_id_Asset_id                    nonclustered (Fund_id, Accnt_id, Asset_id, Exch_id, Curr_id, Trade_type, TradeTime)
 , index ix_InvestorTrade_Fund_id_Accnt_id_Asset_id_Trade_type nonclustered (Fund_id, Rebal_id, Accnt_id, Trade_type, Trade_id)
 , index ix_InvestorTrade_RealisedGain                         nonclustered (Fund_id, Accnt_id, Asset_id, Exch_id, Trade_type, Has_Units, TradeTime)
 , index ix_InvestorTrade_GainUnrealised                       nonclustered (Fund_id, Accnt_id, Trade_type, Has_Units)
 , index ix_InvestorTrade_GainUnrealised2                      nonclustered (Fund_id, Accnt_id, Trade_type, Has_Units, Trade_id)
 , index ix_InvestorTrade_Trade_id                      unique nonclustered (Fund_id, Accnt_id, Trade_id)
) '
exec(@s)

/* Investor.GainUnrealised */
select @s = N'
create table Investor.GainUnrealised (
   Fund_id smallint not null
 , Accnt_id integer not null
 , Trade_id integer not null
 , Asset_id integer not null
 , Exch_id smallint not null
 , Curr_id tinyint not null
 , Hold_Units decimal(18, 2) not null
 , Buy_Price decimal(10, 2) not null
 , Curr_Price decimal(10, 2) not null
 , GainUnrealised decimal(20, 2) not null'
if @fks = 1 select @s += N'
 , constraint fk_InvestorGainUnrealised_Listing foreign key (Asset_id, Exch_id, Curr_id) references Market.Listing (Asset_id, Exch_id, Curr_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_InvestorGainUnrealised primary key nonclustered (Fund_id, Accnt_id, Trade_id, Asset_id)
 , index ix_InvestorGainUnrealised_01_Holdings                      nonclustered (Fund_id, Accnt_id, Asset_id, Exch_id, Curr_id)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_InvestorGainUnrealised primary key clustered (Fund_id, Accnt_id, Trade_id, Asset_id)
 , index ix_InvestorGainUnrealised_01_Holdings                      nonclustered (Fund_id, Accnt_id, Asset_id, Exch_id, Curr_id)
) '
exec(@s)

/* Investor.GainRealised */
select @s = N'
create table Investor.GainRealised (
   Fund_id smallint not null
 , Rebal_id integer not null
 , Accnt_id integer not null
 , Asset_id integer not null
 , Exch_id smallint not null
 , Curr_id tinyint not null
 , Sell_Trade_id integer not null
 , Buy_Trade_id integer not null
 , Sell_Units decimal(18, 2) null 
 , Hold_Units decimal(18, 2) null
 , Realised_Units decimal(18, 2) not null
 , Remaining_Units decimal(18, 2) null 
 , Buy_Price decimal(10, 2) not null
 , Sell_Price decimal(10, 2) not null
 , Buy_Value decimal(20, 4) not null
 , Sell_Value decimal(20, 4) not null
 , GainRealised decimal(20, 4) not null'
if @fks = 1 select @s += N'
 , constraint fk_InvestorGainRealised_SellTradeId foreign key (Fund_id, Accnt_id, Sell_Trade_id) references Investor.Trade (Fund_id, Accnt_id, Trade_id)
 , constraint fk_InvestorGainRealised_BuyTradeId foreign key (Fund_id, Accnt_id, Buy_Trade_id) references Investor.Trade (Fund_id, Accnt_id, Trade_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_InvestorGainRealised primary key nonclustered (Fund_id, Accnt_id, Sell_Trade_id, Buy_Trade_id)
 , index ix_InvestorGainRealised_Accnt_id_Asset_id nonclustered (Fund_id, Rebal_id, Accnt_id, Asset_id, Buy_Trade_id, Exch_id, Curr_id)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_InvestorGainRealised primary key clustered (Fund_id, Accnt_id, Sell_Trade_id, Buy_Trade_id)
 , index ix_InvestorGainRealised_Accnt_id_Asset_id nonclustered (Fund_id, Rebal_id, Accnt_id, Asset_id, Buy_Trade_id, Exch_id, Curr_id)
) '
exec(@s)

/* Investor.Transact */
select @s = N'
create table Investor.Transact (
   Tran_id integer not null
 , Accnt_id integer not null
 , TxType char(1) not null constraint chkInvestorTransactType check (TxType in (''B'', ''S'', ''D'', ''W'', ''F'', ''I''))
 , Fund_id smallint not null
 , Rebal_id integer null
 , Amount decimal(20, 2) not null
 , Asset_id integer null
 , Exch_id varchar(10) null
 , Curr_id tinyint not null
 , CorpAct_id uniqueidentifier null
 , Tran_dt datetime not null default(getdate())
 , Income_dt datetime null'
if @fks = 1 select @s += N'
 , constraint fk_InvestorTransact_Accnt_Curr_id foreign key (Fund_id, Accnt_id, Curr_id) references Investor.Account (Fund_id, Accnt_id, Curr_id)
 , constraint fk_InvestorTransact_Rebal_id_Accnt_id foreign key (Fund_id, Rebal_id, Accnt_id, Fund_id, Curr_id) references Investor.AccountRebalance (Fund_id, Rebal_id, Accnt_id, Fund_id, Curr_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_InvestorTransact primary key nonclustered (Fund_id, Accnt_id, Tran_id)
 , index ix_InvestorTransact_Accnt_id_TxType  nonclustered (Fund_id, Accnt_id, Rebal_id, TxType)
 , index ix_InvestorTransact_Rebal_id_TxType  nonclustered (Fund_id, Rebal_id, TxType)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_InvestorTransact primary key    clustered (Fund_id, Accnt_id, Tran_id)
 , index ix_InvestorTransact_Accnt_id_TxType  nonclustered (Fund_id, Accnt_id, Rebal_id, TxType)
 , index ix_InvestorTransact_Rebal_id_TxType  nonclustered (Fund_id, Rebal_id, TxType)
) '
exec(@s)

/* Investor.Holding */
select @s = N'
create table Investor.Holding (
   Fund_id smallint not null
 , Rebal_id integer not null
 , Accnt_id integer not null
 , Asset_id integer not null
 , Exch_id smallint not null
 , Curr_id tinyint not null
 , Units decimal(18, 2) not null'
if @fks = 1 select @s += N'
 , constraint fk_InvestorHolding_Accnt_Curr_id foreign key (Fund_id, Accnt_id, Curr_id) references Investor.Account (Fund_id, Accnt_id, Curr_id)
 , constraint fk_InvestorHolding_Listing foreign key (Asset_id, Exch_id, Curr_id) references Market.Listing (Asset_id, Exch_id, Curr_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_InvestorHolding primary key          nonclustered (Fund_id, Rebal_id, Accnt_id, Asset_id)
 , index ix_InvestorHolding_Fund_id_Rebal_id          nonclustered (Fund_id, Rebal_id, Asset_id, Exch_id, Curr_id)
 , index ix_InvestorHolding_Fund_id_Accnt_id          nonclustered (Fund_id, Accnt_id, Rebal_id)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_InvestorHolding primary key             clustered (Fund_id, Rebal_id, Accnt_id, Asset_id)
 , index ix_InvestorHolding_Fund_id_Rebal_id          nonclustered (Fund_id, Rebal_id, Asset_id, Exch_id, Curr_id)
 , index ix_InvestorHolding_Fund_id_Accnt_id          nonclustered (Fund_id, Accnt_id, Rebal_id)
) '
exec(@s)

/* Investor.CorporateAction */
select @s = N'
create table Investor.CorporateAction (
   Fund_id smallint not null
 , Accnt_id integer not null
 , CorpAct_id uniqueidentifier not null
 , Rebal_id integer null
 , CorpAct_type char(3) not null constraint chkInvestorCorporateActionCorpActType check (CorpAct_type in (''Div'', ''Spl'', ''Rev'', ''Mer'', ''Spi''))
 , Asset_id integer not null
 , To_Asset_id integer null
 , Exch_id smallint not null
 , Curr_id tinyint not null
 , CorpAct_dt datetime2 null
 , Ex_dt datetime2 null
 , Inc_dt datetime2 null
 , Amount decimal(20, 2) null
 , Ratio_from decimal(20, 12) null
 , Ratio_to decimal(20, 12) null'
if @fks = 1 select @s += N'
 , constraint fk_InvestorCorporateAction_Asset_id foreign key (CorpAct_id) references Market.CorporateAction (CorpAct_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_InvestorCorporateAction primary key nonclustered (Fund_id, Accnt_id, CorpAct_id)
 , index ix_InvestorCorporateAction_01               nonclustered (Fund_id, Accnt_id, Rebal_id, CorpAct_type, CorpAct_id)
 , index ix_InvestorCorporateAction_02               nonclustered (Fund_id, Accnt_id, Rebal_id, CorpAct_type)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_InvestorCorporateAction primary key nonclustered (Fund_id, Accnt_id, CorpAct_id)
 , index ix_InvestorCorporateAction_CorpAct_id       nonclustered (Fund_id, Accnt_id, Rebal_id, CorpAct_type, CorpAct_id)
 , index ix_InvestorCorporateAction_02               nonclustered (Fund_id, Accnt_id, Rebal_id, CorpAct_type)
) '
exec(@s)

/* Investor.Deposit */
select @s = N'
create table Investor.Deposit (
   Fund_id smallint not null
 , Accnt_id integer not null
 , Deposit_id integer not null
 , Deposit_dt datetime2 not null
 , Amount decimal(20, 2) not null
 , Rebal_id integer null'
if @fks = 1 select @s += N'
 , constraint fk_InvestorDeposit_Accnt_id foreign key (Fund_id, Accnt_id) references Investor.Account (Fund_id, Accnt_id)
 , constraint fk_InvestorDeposit_Rebal_id foreign key (Fund_id, Accnt_id, Rebal_id) references Investor.AccountRebalance (Fund_id, Accnt_id, Rebal_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_InvestorDeposit primary key nonclustered (Fund_id, Accnt_id, Deposit_id)
 , index ix_InvestorDeposit_01               nonclustered (Fund_id, Accnt_id, Rebal_id)
 , index ix_InvestorDeposit_02               nonclustered (Fund_id, Accnt_id, Rebal_id, Deposit_id)
 , index ix_InvestorDeposit_03               nonclustered (Fund_id, Rebal_id)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_InvestorDeposit primary key    clustered (Fund_id, Accnt_id, Deposit_id)
 , index ix_InvestorDeposit_01               nonclustered (Fund_id, Accnt_id, Rebal_id)
 , index ix_InvestorDeposit_02               nonclustered (Fund_id, Accnt_id, Rebal_id, Deposit_id)
 , index ix_InvestorDeposit_03               nonclustered (Fund_id, Rebal_id)
) '
exec(@s)

/* Investor.Withdrawal */
select @s = N'
create table Investor.Withdrawal (
   Fund_id smallint not null
 , Accnt_id integer not null
 , Withdrawal_id integer not null
 , Withdrawal_dt datetime2 not null
 , Amount decimal(20, 2) not null
 , Rebal_id integer null'
if @fks = 1 select @s += N'
 , constraint fk_InvestorWithdrawal_Accnt_id foreign key (Fund_id, Accnt_id) references Investor.Account (Fund_id, Accnt_id)
 , constraint fk_InvestorWithdrawal_Rebal_id foreign key (Fund_id, Accnt_id, Rebal_id) references Investor.AccountRebalance (Fund_id, Accnt_id, Rebal_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_InvestorWithdrawal primary key nonclustered (Fund_id, Accnt_id, Withdrawal_id)
 , index ix_InvestorWithdrawal_01               nonclustered (Fund_id, Accnt_id, Rebal_id)
 , index ix_InvestorWithdrawal_02               nonclustered (Fund_id, Accnt_id, Rebal_id, Withdrawal_id)
 , index ix_InvestorWithdrawal_03               nonclustered (Fund_id, Rebal_id)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_InvestorWithdrawal primary key    clustered (Fund_id, Accnt_id, Withdrawal_id)
 , index ix_InvestorWithdrawal_01               nonclustered (Fund_id, Accnt_id, Rebal_id)
 , index ix_InvestorWithdrawal_02               nonclustered (Fund_id, Accnt_id, Rebal_id, Withdrawal_id)
 , index ix_InvestorWithdrawal_03               nonclustered (Fund_id, Rebal_id)
) '
exec(@s)

End

Begin /* Thread Tables */
print 'Thread Tables'

/* Thread.State */
select @s = N'
create table Thread.State (
   Fund_id smallint not null
 , ThreadId smallint not null 
 , Curr_id tinyint not null
 , QueueNoFrom integer not null
 , QueueNoTo integer not null
 , NextQueueNo integer not null'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_ThreadState primary key nonclustered (Fund_id, ThreadId)
 --, index ncix_ThreadState_Fund_id nonclustered (Fund_id)
)  with (memory_optimized = on, durability = SCHEMA_ONLY) '
else select @s += N'
 , constraint pk_ThreadState primary key clustered (Fund_id, ThreadId)
 --, index ncix_ThreadState_Fund_id nonclustered (Fund_id)
) '
exec(@s)

/* Thread.Queue */
select @s = N'
create table Thread.Queue (
   Queue_No integer not null
 , Fund_id smallint not null
 , Curr_id tinyint not null
 , Accnt_id integer not null
 , Model_id smallint not null'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_ThreadQueue primary key nonclustered (Fund_id, Accnt_id)
)  with (memory_optimized = on, durability = SCHEMA_ONLY) 
alter table Thread.Queue add index ix_ThreadQueueNo_01 unique nonclustered (Fund_id, Queue_No)'
else select @s += N'
 , constraint pk_ThreadQueue primary key clustered (Fund_id, Accnt_id)
) 
create unique nonclustered index ix_ThreadQueueNo_01 on Thread.Queue (Fund_id, Queue_No) '
exec(@s)

/* Thread.Monitor */
select @s = N'
create table Thread.Monitor (
   Fund_id smallint not null
 , ThreadId smallint not null 
 , AcctRebalCount bigint not null
 , AcctRebalTimeUs bigint not null /* "Us" denotes Microseconds */
 , FundRebalCount bigint not null
 , FundRebalTimeUs bigint not null
 , DividendCount bigint not null
 , DividendTimeUs bigint not null
 , DepositCount bigint not null
 , DepositTimeUs bigint not null
 , WithdrawalCount bigint not null
 , WithdrawalTimeUs bigint not null
 , PriceUpdateCount bigint not null
 , PriceUpdateTimeUs bigint not null
 '
if @inmemtbl = 1 select @s += N' 
 , constraint pk_ThreadMonitor primary key nonclustered (Fund_id, ThreadId)
)  with (memory_optimized = on, durability = SCHEMA_ONLY) '
else select @s += N'
 , constraint pk_ThreadMonitor primary key clustered (Fund_id, ThreadId)
) '
exec(@s)

/* Thread.ErrorLog */
select @s = N'
create table Thread.ErrorLog (
   Error_id uniqueidentifier not null
 , Error_number bigint null
 , Error_severity int null
 , Error_state int null
 , Error_procedure nvarchar(128) null
 , Error_line int null
 , Error_message nvarchar(max) null
 , Error_dt datetime null
 , Sql_Process_id bigint null
 , Line_no int null
 , Log_point int null
 , rebal_accts_line_no bigint null
 , rebal_acct_line_no bigint null
 , RetryNo bigint null
 , MaxRetries bigint null
 , Rebal_id integer null
 , Fund_id smallint null
 , Accnt_id integer null
 , ThreadId smallint not null
 , MaxThreadId smallint not null
 , LogId bigint null'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_ThreadErrorLog primary key nonclustered (Error_id)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_ThreadErrorLog primary key clustered (Error_id)
) '
exec(@s)

End

Begin /* Load Tables */
print 'Load Tables'

/* Load.Nums */
select @s = N'
create table Load.Nums (
   n bigint not null'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_LoadNums primary key nonclustered (n)
)  with (memory_optimized = on, durability = SCHEMA_ONLY) '
else select @s += N'
 , constraint pk_LoadNums primary key clustered (n)
) '
exec(@s)

/* Populate Load.Nums */
select @s = N' with
 a as (select 1 as n union all select 1),
 b as (select 1 as n from a as a, a as b),
 c as (select 1 as n from b as a, b as b),
 d as (select 1 as n from c as a, c as b),
 e as (select 1 as n from d as a, d as b),
 f as (select 1 as n from e as a, e as b),
nums as (select row_number() over (order by n) as n from f)
insert load.nums select n from nums where n <= ##Nums##;'
exec(@s)

End

Begin /* Mutex Tables */
print 'Mutex Tables'

/* Mutex.PriceSnapshot */
select @s = N'
create table Mutex.PriceSnapshot (
   Curr_id tinyint not null
 , ThreadId smallint not null
 , Mutex_dt datetime null'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_MutexPriceSnapshot primary key nonclustered (Curr_id)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_MutexPriceSnapshot primary key clustered (Curr_id)
) '
exec(@s)

End

Begin /* Logging Tables */
print 'Logging Tables'

/* Logging.PriceUpdate */
select @s = N'
create table Logging.PriceUpdate (
  id bigint not null identity(1, 1)
, logdt datetime2 not null
, Exch_id smallint not null
, Curr_id tinyint not null
, PriceVariance int not null
, ThreadId bigint not null
, TryNo bigint not null'
if @inmemtyp = 1 select @s += N' 
 , constraint pk_LoggingPriceUpdate primary key nonclustered (id)
)  with (memory_optimized = on, durability = SCHEMA_ONLY) '
else select @s += N'
 , constraint pk_LoggingPriceUpdate primary key clustered (id)
) '
exec(@s)

/* Logging.RebalanceAsset */
select @s = N'
create table Logging.RebalanceAsset (
   RebalHistory_Id bigint not null identity(1, 1)
 , Fund_id bigint not null
 , Rebal_id bigint not null
 , Accnt_id bigint not null
 , Asset_id bigint not null
 , Model_id bigint not null
 , Exch_id smallint not null
 , Curr_id tinyint not null
 , Price decimal(10, 2) null
 , Units_current decimal(18, 2) null
 , Units_target decimal(18, 2) null'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_LoggingRebalanceAsset primary key nonclustered (Rebal_id, Accnt_id, Fund_id, Curr_id, Asset_id)
 , constraint ak_LoggingRebalanceAsset unique      nonclustered (Rebal_id, Accnt_id, Asset_id)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_LoggingRebalanceAsset primary key    clustered (Rebal_id, Accnt_id, Fund_id, Curr_id, Asset_id)
 , constraint ak_LoggingRebalanceAsset unique      nonclustered (Rebal_id, Accnt_id, Asset_id)
) '
exec(@s)

/* Logging.Rebalance */
select @s = N'
create table Logging.Rebalance (
   LogId bigint not null identity(1, 1)
 , ThreadId bigint not null
 , MaxThreadId bigint not null
 , Fund_id bigint not null
 , Rebal_id bigint null
 , Curr_id tinyint not null
 , logstate smallint not null
 , Sql_process_id bigint not null
 , Start_dt datetime2 not null
 , Acct_dt datetime2 null
 , End_dt datetime2 null
 , ThreadsToComplete smallint null
 , QueueNo bigint null
 , QueueNoFrom bigint null
 , QueueNoTo bigint null
 , Accnt_id bigint null
 , AccntRetryNo bigint null
 , TryNo bigint null
 , acct_st datetime2 null
 , acct_et datetime2 null
 , acct_ms bigint null
 , psm_ThreadId bigint null
 , fund_st datetime2 null
 , fund_et datetime2 null
 , fund_ms bigint null'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_LoggingRebalance primary key nonclustered (LogId)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_LoggingRebalance primary key clustered (LogId)
) '
exec(@s)

/* Logging.GainUnrealised */
select @s = N'
create table Logging.GainUnrealised (
   Log_id bigint identity(1, 1)
 , Fund_id bigint not null
 , Rebal_id bigint not null
 , Accnt_id bigint not null
 , Asset_id bigint not null
 , Trade_type char(1) not null
 , Exch_id smallint not null
 , Curr_id tinyint not null
 , Trade_id bigint not null
 , TradeTime datetime2 not null
 , Buy_Units decimal(18, 2) not null
 , Buy_Price decimal(10, 2) not null
 , Buy_Value decimal(20, 2) not null
 , Hold_Units decimal(18, 2) null
 , Sum_Units decimal(18, 2) null'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_LoggingGainUnrealised primary key nonclustered (Fund_id, Rebal_id, Accnt_id, Asset_id, Exch_id, Curr_id, Trade_type, TradeTime, Trade_id)
)  with (memory_optimized = on'+@dur+N') '
else select @s += N'
 , constraint pk_LoggingGainUnrealised primary key nonclustered (Fund_id, Rebal_id, Accnt_id, Asset_id, Exch_id, Curr_id, Trade_type, TradeTime, Trade_id)
) '
exec(@s)

End

End
go

use master



