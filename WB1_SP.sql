set nocount on
go
use master
go
use WealthBench
go

Begin /* Drop Procedures */
print 'Drop Procedures'

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
End
go

Begin /* Drop / Build Schemas */
print 'Drop / Build Schemas'

/* Schemas */
drop schema if exists [int]
drop schema if exists [ext]
drop schema if exists [mon]
exec('create schema [int]')
exec('create schema [ext]')
exec('create schema [mon]')
End

go
/* [int].[RebalanceFund] */
create or alter procedure [int].[RebalanceFund] @Fund_id smallint, @Rebal_id smallint, @Curr_id tinyint, @ThreadId smallint
with native_compilation, schemabinding, execute as owner as begin atomic with (transaction isolation level = snapshot, language = N'us_english')
--as begin
if @Rebal_id is null return
declare @FundHoldings decimal(20, 2), @InvestorHoldings decimal(20, 2), @InvestorCash decimal(20, 2), @Rebal_id_New integer
, @Deposits decimal(20, 2), @Withdrawals decimal(20, 2), @FeesCharged decimal(20, 2), @Income decimal(20, 2)

    select @Rebal_id_New = @Rebal_id + 1

    update       Fund.CorporateAction set Rebal_id = @Rebal_id where Fund_id = @Fund_id and Curr_id = @Curr_id and Ex_dt < sysdatetime() and Rebal_id is null

    insert       Investor.CorporateAction (Fund_id, Accnt_id, CorpAct_id, CorpAct_type, Asset_id, To_Asset_id, Exch_id, Curr_id, CorpAct_dt, Ex_dt, Inc_dt, Amount, Ratio_from, Ratio_to)
    select       ih.Fund_id, ih.Accnt_id, fca.CorpAct_id, fca.CorpAct_type, fca.Asset_id, fca.To_Asset_id, fca.Exch_id, @Curr_id, fca.CorpAct_dt, fca.Ex_dt, fca.Inc_dt, ih.Units * fca.Amount, fca.Ratio_from, fca.Ratio_to
    from         Fund.CorporateAction fca
	cross apply (select top 1 fr.Fund_id, fr.Rebal_id from Fund.Rebalance fr where fr.Fund_id = fca.Fund_id and fr.Start_dt >= fca.Ex_dt order by fr.Start_dt) fr
	inner join   Investor.Holding ih on fca.Fund_id = ih.Fund_id and fr.Rebal_id = ih.Rebal_id and fca.Asset_id = ih.Asset_id and fca.Exch_id = ih.Exch_id and fca.Curr_id = ih.Curr_id
    where        fca.Fund_id = @Fund_id and fca.Rebal_id = @Rebal_id and fca.CorpAct_type = 'Div'

	declare @AccntHoldings dbo.Holdings
	declare @FundVsAccntsHolding dbo.FundVsAccntsHolding

	insert    @AccntHoldings (Asset_id, Exch_id, Units)
	select    Asset_id, Exch_id, sum(Units)
	from      Investor.Holding
	where     Fund_id = @Fund_id and Rebal_id = @Rebal_id
	group by  Asset_id, Exch_id

	insert    @FundVsAccntsHolding (Asset_id_Fund, Exch_id_Fund, Units_Fund, Asset_id_Accnts, Exch_id_Accnts, Units_Accnts)
	select    isnull(fh.Asset_id, 0) as Asset_id_Fund, isnull(fh.Exch_id, 0) as Exch_id_Fund, isnull(fh.Units, 0) as Units_Fund, isnull(ah.Asset_id, 0) as Asset_id_Accnts, isnull(ah.Exch_id, 0) as Exch_id_Accnts, isnull(ah.Units, 0) as Units_Accnts
	from      Fund.Holding fh
	left join @AccntHoldings ah on fh.Asset_id = ah.Asset_id and fh.Exch_id = ah.Exch_id
	where     fh.Fund_id = @Fund_id
	union
	select    isnull(fh.Asset_id, 0) as Asset_id_Fund, isnull(fh.Exch_id, 0) as Exch_id_Fund, isnull(fh.Units, 0) as Units_Fund, isnull(ah.Asset_id, 0) as Asset_id_Accnts, isnull(ah.Exch_id, 0) as Exch_id_Accnts, isnull(ah.Units, 0) as Units_Accnts
	from      @AccntHoldings ah
	left join Fund.Holding fh on fh.Asset_id = ah.Asset_id and fh.Exch_id = ah.Exch_id and fh.Fund_id = @Fund_id

    insert    Fund.Trade (Fund_id, Rebal_id, Asset_id, Trade_type, Exch_id, Curr_id, CurrentUnits, TradeUnits, NewUnits, CurrentPrice, CurrentValue) 
    select    @Fund_id, @Rebal_id, Asset_id_Accnts, 'B' as Trade_type, fah.Exch_id_Accnts, @Curr_id, fah.Units_Fund, ceiling(fah.Units_Accnts - fah.Units_Fund), fah.Units_Fund + ceiling(fah.Units_Accnts - fah.Units_Fund), rp.Price, (fah.Units_Accnts - fah.Units_Fund) * rp.Price as Value
    from      @FundVsAccntsHolding fah
    join      Fund.RebalancePricing rp on rp.Fund_id = @Fund_id and rp.Rebal_id = @Rebal_id and fah.Asset_id_Accnts = rp.Asset_id and fah.Exch_id_Accnts = rp.Exch_id and rp.Curr_id = @Curr_id
    where     Units_Accnts > Units_Fund

    insert    Fund.Trade (Fund_id, Rebal_id, Asset_id, Trade_type, Exch_id, Curr_id, CurrentUnits, TradeUnits,  NewUnits, CurrentPrice, CurrentValue) 
    select    @Fund_id, @Rebal_id, Asset_id_Fund, 'S' as Trade_type, fah.Exch_id_Fund, @Curr_id, fah.Units_Fund, ceiling(fah.Units_Accnts - fah.Units_Fund), fah.Units_Fund + ceiling(fah.Units_Accnts - fah.Units_Fund), rp.Price, (fah.Units_Accnts - fah.Units_Fund) * rp.Price as Value
    from      @FundVsAccntsHolding fah
    join      Fund.RebalancePricing rp on rp.Fund_id = @Fund_id and rp.Rebal_id = @Rebal_id and fah.Asset_id_Fund = rp.Asset_id and fah.Exch_id_Fund = rp.Exch_id and rp.Curr_id = @Curr_id
    where     Units_Accnts < Units_Fund

    delete    Fund.Holding where Fund_id = @Fund_id

    insert    Fund.Holding (Fund_id,Asset_id,Exch_id,Curr_id,Units,AcctUnits,RebalPrice)
	select    ft.Fund_id,ft.Asset_id,ft.Exch_id,ft.Curr_id,ft.CurrentUnits+ft.TradeUnits,ah.Units,ft.CurrentPrice
	from      Fund.Trade ft
	left join @AccntHoldings ah on ft.Asset_id = ah.Asset_id and ft.Exch_id = ah.Exch_id
	where     ft.Fund_id = @Fund_id and ft.Rebal_id = @Rebal_id and ft.Trade_type in ('B', 'S') and ah.Units > 0

    insert    Fund.Holding (Fund_id,Asset_id,Exch_id,Curr_id,Units,AcctUnits,RebalPrice)
	select    @Fund_id,fah.Asset_id_Fund,fah.Exch_id_Fund,@Curr_id,fah.Units_Fund,fah.Units_Accnts,rp.Price
	from      @FundVsAccntsHolding fah
    join      Fund.RebalancePricing rp on rp.Fund_id = @Fund_id and rp.Rebal_id = @Rebal_id and fah.Asset_id_Fund = rp.Asset_id and fah.Exch_id_Fund = rp.Exch_id and rp.Curr_id = @Curr_id
	where     fah.Units_Accnts = fah.Units_Fund

    insert    Fund.HoldingHistory (Rebal_id,Fund_id,Asset_id,Exch_id,Curr_id,Units,AcctUnits,RebalPrice) 
	select    @Rebal_id,Fund_id,Asset_id,Exch_id,Curr_id,Units,AcctUnits,RebalPrice
	from      Fund.Holding
	where     Fund_id = @Fund_id

    select    @InvestorHoldings = sum(isnull(HoldingsValue, 0)), @InvestorCash = sum(isnull(CashBalance, 0)) from Investor.Balance where Fund_id = @Fund_id and Rebal_id = @Rebal_id
	select    @FundHoldings = sum(isnull(Units*RebalPrice, 0)) from Fund.Holding where Fund_id = @Fund_id
	select    @InvestorHoldings = isnull(@InvestorHoldings, 0), @InvestorCash = isnull(@InvestorCash, 0), @FundHoldings = isnull(@FundHoldings, 0)

    select    @Deposits    = sum(isnull(Amount, 0)) from Investor.Transact where Fund_id = @Fund_id and Rebal_id = @Rebal_id and TxType = 'D'
    select    @Withdrawals = sum(isnull(Amount, 0)) from Investor.Transact where Fund_id = @Fund_id and Rebal_id = @Rebal_id and TxType = 'W'
    select    @FeesCharged = sum(isnull(Amount, 0)) from Investor.Transact where Fund_id = @Fund_id and Rebal_id = @Rebal_id and TxType = 'F'
    select    @Income      = sum(isnull(Amount, 0)) from Investor.Transact where Fund_id = @Fund_id and Rebal_id = @Rebal_id and TxType = 'I'
	select    @Deposits    = isnull(@Deposits, 0), @Withdrawals = isnull(@Withdrawals, 0), @FeesCharged = isnull(@FeesCharged, 0), @Income = isnull(@Income, 0)

    update    Fund.Balance set FundHoldings = @FundHoldings, InvestorHoldings = @InvestorHoldings, InvestorCash = @InvestorCash, Rebal_id = @Rebal_id where Fund_id = @Fund_id
    
    insert    Fund.BalanceHistory (Fund_id,Rebal_id,Curr_id,FundHoldings,InvestorHoldings,InvestorCash)
    select    Fund_id,Rebal_id,Curr_id,FundHoldings,InvestorHoldings,InvestorCash	
    from      Fund.Balance
	where     Fund_id = @Fund_id

    update    Fund.Rebalance
    set       End_dt = getdate(), InvestorHoldings = isnull(@InvestorHoldings,-1), InvestorCash = isnull(@InvestorCash,-1), Deposits = isnull(@Deposits,-1)
	                            , Withdrawals = isnull(@Withdrawals,-1), FeesCharged = isnull(@FeesCharged,-1), Income = isnull(@Income,-1)
    where     Fund_id = @Fund_id and Rebal_id = @Rebal_id

    insert    Fund.Rebalance (Rebal_id, Fund_id, Curr_id, Start_dt, End_dt, InvestorHoldings, InvestorCash, Deposits, Withdrawals, FeesCharged)
    values   (@Rebal_id_New, @Fund_id, @Curr_id, getdate(), '01-Jan-1900', 0, 0, 0, 0, 0)

    insert    Fund.RebalancePricing (Rebal_id, Fund_id, Asset_id, Exch_id, Curr_id, Price)
	select    @Rebal_id_New, @Fund_id, Asset_id, Exch_id, Curr_id, Price from Market.ListingPrice where Curr_id = @Curr_id

    insert    Fund.ModelAssetPrice (Fund_id, Rebal_id, Model_id, Asset_id, Exch_id, Curr_id, Weighting, Curr_Price)
    select    @Fund_id, @Rebal_id_New, a.Model_id, a.Asset_id, a.Exch_id, a.Curr_id, a.Weighting, rp.Price
    from      Fund.RebalancePricing rp
    join      Advisor.ModelAsset a on a.Asset_id = rp.Asset_id and a.Exch_id = rp.Exch_id and a.Curr_id = rp.Curr_id
    where     rp.Fund_id = @Fund_id and rp.Rebal_id = @Rebal_id_New

    update    Thread.Monitor set FundRebalCount += 1 where Fund_id = @Fund_id and ThreadId = @ThreadId
    update    Thread.State   set NextQueueNo = QueueNoFrom where Fund_id = @Fund_id
end
go
/* [int].[RebalanceAcct] */
create or alter procedure [int].[RebalanceAcct]
   @Rebal_id smallint
 , @Accnt_id integer
 , @Curr_id tinyint
 , @Fund_id smallint
 , @ThreadId smallint
 , @QueueNo integer
 , @logstate smallint
with native_compilation, schemabinding, execute as owner as begin atomic with (transaction isolation level = snapshot, language = N'us_english')
--as begin

declare @ReturnVal                  integer = 0
      , @Rebal_dt                   datetime2 = sysdatetime()
      , @Model_id                   smallint

declare @CurrentHoldings            dbo.Holdings
      , @TargetHoldings             dbo.Holdings
      , @RebalanceAsset             dbo.RebalanceAsset

declare @CurrentCashBalance         decimal(20, 2)
      , @CurrentHoldingsValue       decimal(20, 2)
      , @NewDeposits                decimal(20, 2)
      , @NewWithdrawals             decimal(20, 2)
      , @NewIncome                  decimal(20, 2)

declare @TargetHoldingsValue        decimal(20, 2)
      , @RebalancedHoldingsValue    decimal(20, 2)
      , @NewCashBalance             decimal(20, 2)

declare @MinimumBalance             decimal(20, 2) = 100.00
      , @MinimumTrade               decimal(20, 2) = 100.00
      , @AdjAsset_id                integer
      , @BuyTranId                  integer
      , @SellTranId                 integer
      , @SumOfBuys                  decimal(20, 2)
      , @SumOfSells                 decimal(20, 2)
      , @TranCount                  integer = 0
      , @FeeTranId                  integer
      , @FeeCharged                 decimal(20, 2) = 0

declare @Rebal_id_prev integer = @Rebal_id - 1

/* Get current Account model */
select @Model_id = Model_id from Investor.Account where Fund_id = @Fund_id and Accnt_id = @Accnt_id

/* Get current Account balance */
select @CurrentCashBalance = CashBalance 
from   Investor.Balance where Fund_id = @Fund_id and Accnt_id = @Accnt_id and Rebal_id = @Rebal_id_prev

insert Investor.AccountRebalance (Fund_id, Rebal_id, Curr_id, Accnt_id, Model_id, Sql_Process_id, ThreadId, Rebal_dt)
values (@Fund_id, @Rebal_id, @Curr_id, @Accnt_id, @Model_id, @@spid, @ThreadId, sysdatetime())

/* Update pending Deposit Transaction with this Rebal_id */
update Investor.Deposit set Rebal_id = @Rebal_id where  Fund_id = @Fund_id and Accnt_id = @Accnt_id and Rebal_id is null

declare @Max_Tran_id integer, @Max_Trade_id integer
select @Max_Tran_id = Tran_id, @Max_Trade_id = Trade_id from Investor.AccountSeq where Fund_id = @Fund_id and Accnt_id = @Accnt_id

declare @Deposit_id integer
select @Deposit_id = min(Deposit_id) from Investor.Deposit d where d.Fund_id = @Fund_id and d.Accnt_id = @Accnt_id and d.Rebal_id = @Rebal_id
while @Deposit_id is not null
 begin
  select @Max_Tran_id += 1
  insert Investor.Transact (Fund_id,Accnt_id,Tran_id,TxType,Rebal_id,Amount,Curr_id,Tran_dt)
  select d.Fund_id,d.Accnt_id,@Max_Tran_id,'D',d.Rebal_id,d.Amount,@Curr_id,d.Deposit_dt
  from   Investor.Deposit d
  where  d.Fund_id = @Fund_id and d.Accnt_id = @Accnt_id and d.Rebal_id = @Rebal_id and d.Deposit_id = @Deposit_id
  select @Deposit_id = null
  select @Deposit_id = min(Deposit_id) from Investor.Deposit d where d.Fund_id = @Fund_id and d.Accnt_id = @Accnt_id and d.Rebal_id = @Rebal_id and Deposit_id > @Deposit_id
 end

/* Update pending Withdrawal Transaction with this Rebal_id */
update Investor.Withdrawal set Rebal_id = @Rebal_id where  Fund_id = @Fund_id and Accnt_id = @Accnt_id and Rebal_id is null

declare @Withdrawal_id integer
select @Withdrawal_id = min(Withdrawal_id) from Investor.Withdrawal w where w.Fund_id = @Fund_id and w.Accnt_id = @Accnt_id and w.Rebal_id = @Rebal_id
while @Withdrawal_id is not null
 begin
  select @Max_Tran_id += 1
  insert Investor.Transact (Fund_id,Accnt_id,Tran_id,TxType,Rebal_id,Amount,Curr_id,Tran_dt)
  select w.Fund_id,w.Accnt_id,@Max_Tran_id,'W',w.Rebal_id,w.Amount,@Curr_id,w.Withdrawal_dt
  from   Investor.Withdrawal w
  where  w.Fund_id = @Fund_id and w.Accnt_id = @Accnt_id and w.Rebal_id = @Rebal_id and w.Withdrawal_id = @Withdrawal_id
  select @Withdrawal_id = null
  select @Withdrawal_id = min(Withdrawal_id) from Investor.Withdrawal w where w.Fund_id = @Fund_id and w.Accnt_id = @Accnt_id and w.Rebal_id = @Rebal_id and Withdrawal_id > @Withdrawal_id
 end

/* Get value of unprocessed Deposits and Withdrawals */
select @NewDeposits    = sum(isnull(Amount,0))
from   Investor.Transact
where  Fund_id = @Fund_id and Rebal_id = @Rebal_id and Accnt_id = @Accnt_id and TxType = 'D'

select @NewWithdrawals = sum(isnull(Amount,0))
from   Investor.Transact
where  Fund_id = @Fund_id and Rebal_id = @Rebal_id and Accnt_id = @Accnt_id and TxType = 'W'

/* Update pending Corporate Actions with this Rebal_id */
update Investor.CorporateAction set Rebal_id = @Rebal_id
where  Fund_id = @Fund_id and Accnt_id = @Accnt_id and Rebal_id is null and CorpAct_type = 'Div' and Inc_dt <= sysdatetime()

declare @CorpAct_id uniqueidentifier
select @CorpAct_id = min(CorpAct_id)
from   Investor.CorporateAction ca with (forceseek, index = ix_InvestorCorporateAction_02)
where  ca.Fund_id = @Fund_id and ca.Accnt_id = @Accnt_id and ca.Rebal_id = @Rebal_id and ca.CorpAct_type = 'Div'

while @CorpAct_id is not null
 begin

  select @Max_Tran_id += 1

  insert Investor.Transact (Fund_id,Accnt_id,Tran_id,TxType,Rebal_id,Amount,Asset_id,Exch_id,Curr_id,Tran_dt,Income_dt)
  select ca.Fund_id,ca.Accnt_id,@Max_Tran_id,'I',ca.Rebal_id,ca.Amount,ca.Asset_id,ca.Exch_id,ca.Curr_id,ca.CorpAct_dt,ca.Inc_dt
  from   Investor.CorporateAction ca with (forceseek, index = ix_InvestorCorporateAction_02)
  where  ca.Fund_id = @Fund_id and ca.Accnt_id = @Accnt_id and ca.Rebal_id = @Rebal_id and ca.CorpAct_type = 'Div' and ca.CorpAct_id > @CorpAct_id

  select @CorpAct_id = null
  select @CorpAct_id = min(CorpAct_id)
  from   Investor.CorporateAction ca with (forceseek, index = ix_InvestorCorporateAction_02)
  where  ca.Fund_id = @Fund_id and ca.Accnt_id = @Accnt_id and ca.Rebal_id = @Rebal_id and ca.CorpAct_type = 'Div' and ca.CorpAct_id > @CorpAct_id

 end

/* Get value of unprocessed Corporate Actions */
select @NewIncome = sum(isnull(Amount,0))
from   Investor.CorporateAction
where  Fund_id = @Fund_id and Rebal_id = @Rebal_id and Accnt_id = @Accnt_id and Rebal_id = @Rebal_id

/* Get current Holdings */
insert @CurrentHoldings (Asset_id, Exch_id, Units, Price)
select h.Asset_id, h.Exch_id, h.Units, rp.Price
from   Investor.Holding h
join   Fund.RebalancePricing rp on rp.Fund_id = h.Fund_id and rp.Rebal_id = h.Rebal_id and rp.Asset_id = h.Asset_id and rp.Exch_id = h.Exch_id and rp.Curr_id = h.Curr_id
where  h.Fund_id = @Fund_id and h.Accnt_id = @Accnt_id and h.Rebal_id = @Rebal_id_prev

select @CurrentHoldingsValue = sum(isnull(c.Units, 0) * isnull(c.Price, 0)) from @CurrentHoldings c

select @CurrentCashBalance     = isnull(@CurrentCashBalance, 0)
     , @CurrentHoldingsValue   = isnull(@CurrentHoldingsValue, 0)
     , @NewDeposits            = isnull(@NewDeposits, 0)
     , @NewWithdrawals         = isnull(@NewWithdrawals, 0)
     , @NewIncome              = isnull(@NewIncome, 0)

/* Set the Target Balance (Existing Holdings + Cash + Deposits - Withdrawals) */
select @TargetHoldingsValue = @CurrentHoldingsValue + @CurrentCashBalance + @NewDeposits + @NewWithdrawals + @NewIncome
select @TargetHoldingsValue = case when @TargetHoldingsValue > @MinimumBalance then @TargetHoldingsValue - @MinimumBalance else 0.00 end

/* Get Model Assets & weighting, based on current listing prices */
insert @TargetHoldings (Asset_id, Exch_id, Weighting, Price, Units)
select map.Asset_id, map.Exch_id, map.Weighting, map.Curr_Price, (@TargetHoldingsValue * map.Weighting / map.Curr_Price) /* <-- Allocate assets based upon model weighting */
from   Fund.ModelAssetPrice map
where  map.Fund_id = @Fund_id and map.Rebal_id = @Rebal_id and map.Model_id = @Model_id

select @RebalancedHoldingsValue = sum(isnull(t.Units, 0) * isnull(t.Price, 0)) from @TargetHoldings t

/* Merge Current & Target Holdings, identify Buy / Sell trades which exceed the minimal value threshold */
insert @RebalanceAsset (Asset_id, Exch_id, Curr_id, Price, Units_current, Units_target, IsBuy, IsSell, IsTrade)
select  Asset_id, Exch_id, Curr_id, Price, Units_current, Units_target, IsBuy, IsSell, IsTrade = case when (IsBuy = 1 or IsSell = 1) then 1 else 0 end
from   (select Asset_id, Exch_id, Fund_id, Curr_id, Price, Units_current, Units_target
             , IsBuy  = case when Units_target > Units_current and ((Units_target - Units_current) * Price) >= @MinimumTrade then 1 else 0 end
             , IsSell = case when Units_target < Units_current and ((Units_current - Units_target) * Price) >= @MinimumTrade then 1 else 0 end
        from  (select     @Rebal_id as "Rebal_id", @Accnt_id as "Accnt_id", c.Exch_id, @Fund_id as Fund_id, @Curr_id as Curr_id
                        , t.Price, c.Asset_id, isnull(c.Units, 0) as "Units_current", isnull(t.Units, 0) as "Units_target"
               from       @CurrentHoldings c
               left join  @TargetHoldings t on c.Asset_id = t.Asset_id
               union
               select     @Rebal_id as "Rebal_id", @Accnt_id as "Accnt_id", t.Exch_id, @Fund_id as Fund_id, @Curr_id as Curr_id
                        , t.Price, t.Asset_id, isnull(c.Units, 0) as "Units_current", isnull(t.Units, 0) as "Units_target"
               from       @TargetHoldings t
               left join  @CurrentHoldings c on c.Asset_id = t.Asset_id) ra
       ) ra

/* Record Buy Orders */
declare @NoOfBuys integer
select  @NoOfBuys = count(*), @SumOfBuys = -sum((Units_target - Units_current) * Price) from @RebalanceAsset ra where IsBuy = 1
select  @NoOfBuys = isnull(@NoOfBuys, 0), @SumOfBuys = isnull(@SumOfBuys, 0)

if @NoOfBuys > 0
 begin
  /* Individual Buy Asset Trades */
  insert  Investor.Trade (Trade_id, Fund_id, Rebal_id, Accnt_id, Asset_id, Trade_type, Exch_id, Curr_id, TradeTime, Units, Price, Value, Has_Units, Hold_Units)
  select (select @Max_Trade_id + sum(1) from @RebalanceAsset rb where IsBuy = 1 and rb.Asset_id <= ra.Asset_id) as Trade_id
        , @Fund_id, @Rebal_id, @Accnt_id, Asset_id, 'B' as Trade_type, Exch_id, Curr_id, sysdatetime() as TradeTime, (Units_target - Units_current) as Units, Price
        , Value = (Units_target - Units_current) * Price, 1 as Has_Units, (Units_target - Units_current) as Hold_Units
  from    @RebalanceAsset ra
  where   IsBuy = 1
  select  @Max_Trade_id += @NoOfBuys

  select   @Max_Tran_id = isnull(@Max_Tran_id, 0) + 1
  select   @BuyTranId = @Max_Tran_id

  /* Summary Buy transaction, records reduction in investor account balance from Buys */
  insert   Investor.Transact (Tran_id, Rebal_id, Fund_id, Accnt_id, TxType, Amount, Curr_id)
  select   @BuyTranId, @Rebal_id, @Fund_id, @Accnt_id, 'B', @SumOfBuys, @Curr_id
  from     @RebalanceAsset
  where    IsBuy = 1
  group by IsBuy

 end

/* Record Sell Orders */
declare @NoOfSells integer
select  @NoOfSells = count(*), @SumOfSells = sum((Units_current - Units_target) * Price) from @RebalanceAsset ra where IsSell = 1
select  @NoOfSells = isnull(@NoOfSells, 0), @SumOfSells = isnull(@SumOfSells, 0)

if @NoOfSells > 0
 begin
  /* Individual Sell Asset Trades */
  insert  Investor.Trade (Trade_id, Fund_id, Rebal_id, Accnt_id, Asset_id, Trade_type, Exch_id, Curr_id, TradeTime, Units, Price, Value, Has_Units, Hold_Units)
  select (select @Max_Trade_id + sum(1) from @RebalanceAsset rb where IsSell = 1 and rb.Asset_id <= ra.Asset_id) as Trade_id
        , @Fund_id, @Rebal_id, @Accnt_id, Asset_id, 'S' as Trade_type, Exch_id, Curr_id, sysdatetime(), (Units_current - Units_target) as Units
        , Price, (Units_target - Units_current) * Price as Value, 0 as Has_Units, 0 as Hold_Units
  from    @RebalanceAsset ra
  where   IsSell = 1
  select  @Max_Trade_id += @NoOfSells

  select   @Max_Tran_id = isnull(@Max_Tran_id, 0) + 1
  select   @SellTranId = @Max_Tran_id

  /* Summary Sell transaction, records increase in investor account balance from Sells */
  insert   Investor.Transact (Tran_id, Fund_id, Rebal_id, Accnt_id, TxType, Amount, Curr_id)
  select   @SellTranId, @Fund_id, @Rebal_id, @Accnt_id, 'S', @SumOfSells, @Curr_id
  from     @RebalanceAsset
  where    IsSell = 1
  group by IsSell

 end

/* Realised Gains - calculated on Sells only */
declare @Sell_Trade_id integer, @Sell_Asset_id integer, @Sell_Units decimal(18, 2), @Remaining_Units decimal(18, 2), @Realised_Units decimal(18, 2), @Buy_Trade_id integer, @Hold_Units decimal(18, 2)
      , @Sell_Exch_id smallint, @Sell_Curr_id tinyint, @Sell_Price decimal(10, 2), @Buy_Price decimal(10, 2)
select @Sell_Trade_id = min(Trade_id) from Investor.Trade where Fund_id = @Fund_id and Rebal_id = @Rebal_id and Accnt_id = @Accnt_id and Trade_type = 'S'
while @Sell_Trade_id is not null
 begin
  select @Sell_Asset_id = null, @Remaining_Units = null, @Sell_Price = null, @Sell_Exch_id = null
  select @Sell_Asset_id = Asset_id, @Sell_Exch_id = Exch_id, @Sell_Units = Units, @Remaining_Units = Units, @Sell_Price = Price from Investor.Trade where Fund_id = @Fund_id and Rebal_id = @Rebal_id and Accnt_id = @accnt_id and Trade_id = @Sell_Trade_id
  select @Buy_Trade_id = (select top 1 Trade_id from Investor.Trade where Fund_id = @Fund_id and Accnt_id = @Accnt_id and Asset_id = @Sell_Asset_id and Exch_id = @Sell_Exch_id and Trade_type = 'B' and Has_Units = 1 order by Trade_id)
  while @Buy_Trade_id is not null
   begin
    select @Hold_Units = null, @Realised_Units = null, @Sell_Curr_id = null, @Buy_Price = null

    select @Hold_Units = Hold_Units, @Sell_Curr_id = Curr_id, @Buy_Price = Price
	from Investor.Trade where Fund_id = @Fund_id and Accnt_id = @accnt_id and Trade_id = @Buy_Trade_id
	
	select @Realised_Units = case when @Hold_Units >= @Remaining_Units then @Remaining_Units else @Hold_Units end
    
	update Investor.Trade
	set    Hold_Units -= @Realised_Units, Has_Units = case when (@Hold_Units - @Remaining_Units) > 0.0 then 1 else 0 end
	where  Fund_id = @Fund_id and Accnt_id = @Accnt_id and Trade_id = @Buy_Trade_id

    insert Investor.GainRealised (Fund_id,Rebal_id,Accnt_id,Asset_id,Exch_id,Curr_id,Sell_Trade_id,Buy_Trade_id,Sell_Units,Remaining_Units,Hold_Units,Realised_Units,Sell_Price,Buy_Price,Sell_Value,Buy_Value,GainRealised)
    values (@Fund_id,@Rebal_id,@Accnt_id,@Sell_Asset_id,@Sell_Exch_id,@Sell_Curr_id,@Sell_Trade_id,@Buy_Trade_id,@Sell_Units,(@Remaining_Units - @Realised_Units),@Hold_Units,@Realised_Units,@Sell_Price,@Buy_Price,@Remaining_Units*@Sell_Price,@Remaining_Units*@Buy_Price
	     , (@Remaining_Units * @Sell_Price) - (@Remaining_Units * @Buy_Price))

    select @Remaining_Units -= @Realised_Units

	if @Remaining_Units = 0 select @Buy_Trade_id = null
    else select @Buy_Trade_id = (select top 1 Trade_id from Investor.Trade where Fund_id = @Fund_id and Accnt_id = @Accnt_id and Asset_id = @Sell_Asset_id and Exch_id = @Sell_Exch_id and Trade_type = 'B' and Has_Units = 1 and Trade_id > @Buy_Trade_id order by Trade_id)
   end

  select @Sell_Trade_id = min(Trade_id) from Investor.Trade where Fund_id = @Fund_id and Rebal_id = @Rebal_id and Accnt_id = @Accnt_id and Trade_type = 'S' and Trade_id > @Sell_Trade_id
 end

 /* Unrealised Gains - calculated on Trades still holding units */
delete from Investor.GainUnrealised where Fund_id = @Fund_id and Accnt_id = @Accnt_id
 
insert Investor.GainUnrealised (Fund_id, Accnt_id, Trade_id, Asset_id, Exch_id, Curr_id, Hold_Units, Buy_Price, Curr_Price, GainUnrealised)
select t.Fund_id, t.Accnt_id, t.Trade_id, t.Asset_id, t.Exch_id, t.Curr_id, t.Hold_Units, Buy_Price = t.Price, Curr_Price = rp.Price, GainUnrealised = (Hold_Units * rp.Price) - (Hold_Units * t.Price)
from   Investor.Trade t with (forceseek, index = ix_InvestorTrade_GainUnrealised2)
join   Fund.RebalancePricing rp on rp.Fund_id = t.Fund_id and rp.Rebal_id = @Rebal_id and rp.Asset_id = t.Asset_id and rp.Exch_id = t.Exch_id and rp.Curr_id = t.Curr_id
where  t.Fund_id = @Fund_id and t.Accnt_id = @Accnt_id and t.Trade_type = 'B' and t.Has_Units = 1

/* If any Buys or Sells, charge a fee transaction */
select @TranCount = count(*) from @RebalanceAsset where IsBuy = 1 or IsSell = 1
if @TranCount > 0
 begin
  select @Max_Tran_id = isnull(@Max_Tran_id, 0) + 1
  select @FeeTranId = @Max_Tran_id

  select @FeeCharged = .10
  insert Investor.Transact (Tran_id, Fund_id, Rebal_id, Accnt_id, TxType, Amount, Curr_id)
  select @FeeTranId, @Fund_id, @Rebal_id, @Accnt_id, 'F', -@FeeCharged, @Curr_id
 end
select @FeeCharged = isnull(@FeeCharged, 0)

/* Insert Holdings for this Rebalance, after deleting old Holdings */
delete Investor.Holding where Fund_id = @Fund_id and Accnt_id = @Accnt_id and Rebal_id = @Rebal_id_prev - 1

insert   Investor.Holding (Fund_id,Rebal_id,Accnt_id,Asset_id,Exch_id,Curr_id,Units)
select   @Fund_id,@Rebal_id,@Accnt_id,Asset_id,Exch_id,Curr_id,sum(Hold_Units)
from     Investor.GainUnrealised with (forceseek, index = ix_InvestorGainUnrealised_01_Holdings)
where    Fund_id = @Fund_id and Accnt_id = @Accnt_id
group by Asset_id,Exch_id,Curr_id

select @NewCashBalance = @CurrentCashBalance + @NewDeposits + @NewIncome + @NewWithdrawals + @SumOfBuys + @SumOfSells + -@FeeCharged

/* Store Account Balance */
insert Investor.Balance (Fund_id,Rebal_id,Accnt_id,HoldingsValue,CashBalance,Rebalanced_dt)
values (@Fund_id,@Rebal_id,@Accnt_id,@RebalancedHoldingsValue,@NewCashBalance,@Rebal_dt)

update Investor.AccountSeq
set    Tran_id = @Max_Tran_id, Trade_id = @Max_Trade_id
where  Fund_id = @Fund_id and Accnt_id = @Accnt_id

if @logstate = 1
 begin
  /* Log Rebalance Assets for audit */
  insert Logging.RebalanceAsset
        ( Fund_id,  Rebal_id, Accnt_id, Asset_id, Model_id, Exch_id, Curr_id, Price, Units_current, Units_target)
  select @Fund_id, @Rebal_id,@Accnt_id, Asset_id,@Model_id, Exch_id, Curr_id, Price, Units_current, Units_target
  from   @RebalanceAsset
end
  update Investor.AccountRebalance
  set    CurrentCashBalance = @CurrentCashBalance, NewCashBalance = @NewCashBalance, MinimumBalance = @MinimumBalance, NewDeposits = @NewDeposits, NewWithdrawals = @NewWithdrawals
       , CurrentHoldingsValue = @CurrentHoldingsValue, TargetHoldingsValue = @TargetHoldingsValue, RebalancedHoldingsValue = @RebalancedHoldingsValue
       , BuyTranId = @BuyTranId, SellTranId = @SellTranId, SumOfBuys = @SumOfBuys, SumOfSells = @SumOfSells, FeeCharged = @FeeCharged, FeeTranId = @FeeTranId, NewIncome = @NewIncome
       , NoOfBuys = @NoOfBuys, NoOfSells = @NoOfSells
  where  Fund_id = @Fund_id and Rebal_id = @Rebal_id and Accnt_id = @Accnt_id and Curr_id = @Curr_id

update Thread.Monitor set AcctRebalCount += 1 where Fund_id = @Fund_id and ThreadId = @ThreadId
update Thread.State set NextQueueNo += 1 where Fund_id = @Fund_id and ThreadId = @ThreadId

select @ReturnVal = 1
return @ReturnVal
end
go
/* [int].[GetThreadsToComplete] */
create or alter procedure [int].[GetThreadsToComplete] @Fund_id smallint, @ThreadsToComplete integer output
with native_compilation, schemabinding, execute as owner as begin atomic with (transaction isolation level = snapshot, language = N'us_english')
--as begin
select @ThreadsToComplete = count(*) from Thread.State where Fund_id = @Fund_id and NextQueueNo < QueueNoTo
end
go
/* [int].[GetThreadNextAccnt] */
create or alter procedure [int].[GetThreadNextAccnt] @Fund_id smallint, @ThreadId smallint, @Rebal_id integer output, @QueueNo integer output, @QueueNoFrom integer output, @QueueNoTo integer output, @Accnt_id integer output
with native_compilation, schemabinding, execute as owner as begin atomic with (transaction isolation level = snapshot, language = N'us_english')
--as begin
select          @Rebal_id = Rebal_id, @QueueNo = t.NextQueueNo, @QueueNoFrom = t.QueueNoFrom, @QueueNoTo = t.QueueNoTo, @Accnt_id = q.Accnt_id
from            Thread.State t
inner loop join Fund.Rebalance r on r.Fund_id = t.Fund_id and r.End_dt = '01-Jan-1900'
left  loop join Thread.Queue q on t.Fund_id = q.Fund_id and t.NextQueueNo = q.Queue_No
where           r.Fund_id = @Fund_id
    and         t.ThreadId = @ThreadId
end
go
/* [int].[GetThreadNextQueueNo] */
create or alter procedure [int].[GetThreadNextQueueNo] @Fund_id smallint, @ThreadId smallint, @QueueNo integer output
with native_compilation, schemabinding, execute as owner as begin atomic with (transaction isolation level = snapshot, language = N'us_english')
--as begin
select @QueueNo = NextQueueNo from Thread.State where Fund_id = @Fund_id and ThreadId = @ThreadId
end
go
/* [ext].[Rebalance] */
create or alter procedure [ext].[Rebalance] @ThreadId smallint, @MaxThreadId smallint, @Fund_id smallint, @Curr_id tinyint, @logstate smallint = 1, @rv int output as
set nocount on
select @rv = 1
declare @LogId bigint, @st datetime2 = sysdatetime(), @et datetime2, @Acct_dt datetime2
declare @Err_id uniqueidentifier, @Err_num bigint, @Err_sev int, @Err_st int, @Err_prc nvarchar(128), @Err_ln int, @Err_msg nvarchar(max)
declare @Rebal_id integer, @RetryNo bigint, @MaxRetries bigint
declare @Accnt_id integer, @ReturnVal integer
declare @QueueNo integer, @QueueNoFrom integer, @QueueNoTo integer

select @logstate = 1
if @logstate = 1
 begin
insert Logging.Rebalance (ThreadId, MaxThreadId, Fund_id, Curr_id, logstate, Sql_process_id, Start_dt)
values (@ThreadId, @MaxThreadId, @Fund_id, @Curr_id, @logstate, @@spid, @st)
select @LogId = scope_identity()
 end

/*
* Rebalance all Account on this thread's queue
*/

exec [int].[GetThreadNextAccnt] @Fund_id = @Fund_id, @ThreadId = @ThreadId, @Rebal_id = @Rebal_id output, @QueueNo = @QueueNo output, @QueueNoFrom = @QueueNoFrom output, @QueueNoTo = @QueueNoTo output, @Accnt_id = @Accnt_id output
if @logstate = 1 update Logging.Rebalance set QueueNo = @QueueNo, QueueNoFrom = @QueueNoFrom, QueueNoTo = @QueueNoTo, Accnt_id = @Accnt_id, Rebal_id = @Rebal_id where LogId = @LogId

while @QueueNo between @QueueNoFrom and @QueueNoTo
 begin
if @logstate = 1
 begin
insert Logging.Rebalance (ThreadId, MaxThreadId, Fund_id, Curr_id, logstate, Sql_process_id, Start_dt, QueueNo, QueueNoFrom, QueueNoTo, Accnt_id, Rebal_id)
values (@ThreadId, @MaxThreadId, @Fund_id, @Curr_id, @logstate, @@spid, @st, @QueueNo, @QueueNoFrom, @QueueNoTo, @Accnt_id, @Rebal_id)
select @LogId = scope_identity()
 end
begin
declare @acct_st datetime2, @acct_et datetime2, @acct_ms bigint
begin try
declare @AccntRetryNo smallint = 1, @AccntMaxRetries smallint = 5
while @AccntRetryNo < @AccntMaxRetries
 begin
  select @Err_id = null, @Err_num = null

  if @Rebal_id is not null and @Accnt_id is not null and @QueueNo between @QueueNoFrom and @QueueNoTo
     begin
      select @RetryNo = 1, @MaxRetries = 5
      while @RetryNo <= @MaxRetries
       begin
        begin try
         select @acct_st = null, @acct_et = null, @acct_ms = null
		 select @acct_st = sysdatetime()
		 if @logstate = 1 update Logging.Rebalance set acct_st = @acct_st, TryNo = @RetryNo where LogId = @LogId
         exec @ReturnVal = [int].[RebalanceAcct] @Rebal_id, @Accnt_id, @Curr_id, @Fund_id, @ThreadId, @QueueNo, @logstate
		 --select @rv = 0
		 select @acct_et = sysdatetime()
         exec [mon].[ThreadIncrementTimeUs] @Fund_id = @Fund_id, @ThreadId = @ThreadId, @MeasureId = 1, @FromDt2 = @acct_st, @ToDt2 = @acct_et, @IncrementTimeUs = @acct_ms output
		 --select @acct_ms = datediff_big(microsecond, @acct_st, @acct_et)
		 if @logstate = 1 update Logging.Rebalance set acct_et = @acct_et, acct_ms = @acct_ms where LogId = @LogId
         exec [int].[GetThreadNextQueueNo] @Fund_id = @Fund_id, @ThreadId = @ThreadId, @QueueNo = @QueueNo output
		 if @QueueNo >= @QueueNoTo select @rv = 0
         select @RetryNo = @MaxRetries
        end try
        begin catch
         select @Err_num = error_number(), @Err_sev = error_severity(), @Err_st = error_severity(), @Err_prc = error_procedure(), @Err_ln = error_line(), @Err_msg = error_message(), @Err_id = newid()
		 --select @rv = 0
         if @Err_num not in (2627, 41325)
          begin
           insert Thread.ErrorLog(LogId,Error_id,Error_number,Error_severity,Error_state,Error_procedure,Error_line,Error_message,Error_dt,Sql_Process_id,Log_point,Rebal_id,RetryNo,MaxRetries,ThreadId,MaxThreadId,Fund_id,Accnt_id)
           values (@LogId,@Err_id,@Err_num,@Err_sev,@Err_st,@Err_prc,@Err_ln,@Err_msg,getdate(),@@spid,100,@Rebal_id,@RetryNo,@MaxRetries,@ThreadId,@MaxThreadId,@Fund_id,@Accnt_id);
           select @RetryNo = @MaxRetries
          end
         else
          throw
        end catch
        if @ReturnVal = 1 break
        select @RetryNo += 1--, @RetryCount += 1
       end
   end

  if @ReturnVal = 1 begin select @AccntRetryNo = @AccntMaxRetries break end-- waitfor delay '00:00:00.50'; 
  else waitfor delay '00:00:00.100'; --select @AccntRetryNo = @AccntMaxRetries
  select @AccntRetryNo += 1
 end

end try
begin catch
 select @Err_num = error_number(), @Err_sev = error_severity(), @Err_st = error_severity(), @Err_prc = error_procedure(), @Err_ln = error_line(), @Err_msg = error_message(), @Err_id = newid()
 insert Thread.ErrorLog(Error_id,Error_number,Error_severity,Error_state,Error_procedure,Error_line,Error_message,Error_dt,Sql_Process_id,Log_point,Rebal_id,ThreadId,MaxThreadId)
 values (@Err_id,@Err_num,@Err_sev,@Err_st,@Err_prc,@Err_ln,@Err_msg,getdate(),@@spid,200,@Rebal_id,@ThreadId,@MaxThreadId);
end catch

end

exec [int].[GetThreadNextAccnt] @Fund_id = @Fund_id, @ThreadId = @ThreadId, @Rebal_id = @Rebal_id output, @QueueNo = @QueueNo output, @QueueNoFrom = @QueueNoFrom output, @QueueNoTo = @QueueNoTo output, @Accnt_id = @Accnt_id output

 end



select @Acct_dt = sysdatetime()
if @logstate = 1 update Logging.Rebalance set Acct_dt = @Acct_dt where LogId = @LogId

/*
* Close this Fund Rebalance if the last Account has been rebalanced, and open a new Fund Rebalance
*/
begin
select @ReturnVal = 0
declare @fund_st datetime2, @fund_et datetime2, @fund_ms bigint, @psm_ThreadId smallint
begin try
--select @QueueNo as "@QueueNo", @QueueNoTo as "@QueueNoTo"
 if @QueueNo > @QueueNoTo
  begin
   
   declare @ThreadsToComplete integer
   exec [int].[GetThreadsToComplete] @Fund_id = @Fund_id, @ThreadsToComplete = @ThreadsToComplete output
   select @ThreadsToComplete = isnull(@ThreadsToComplete, 0)
   if @logstate = 1 update Logging.Rebalance set ThreadsToComplete = @ThreadsToComplete where LogId = @LogId
   if @ThreadsToComplete = 0
    begin
     begin try
      delete Mutex.PriceSnapshot where Curr_id = @Curr_id and ThreadId = @ThreadId
      insert Mutex.PriceSnapshot (Curr_id, ThreadId) values (@Curr_id, @ThreadId)
      select @psm_ThreadId = ThreadId from Mutex.PriceSnapshot where Curr_id = @Curr_id
	  if @logstate = 1 update Logging.Rebalance set psm_ThreadId = @psm_ThreadId where LogId = @LogId
      if @psm_ThreadId = @ThreadId 
       begin
        waitfor delay '00:00:00.050';
        select @fund_st = null, @fund_et = null, @fund_ms = null
		select @fund_st = sysdatetime()
		if @logstate = 1 update Logging.Rebalance set fund_st = @fund_st, TryNo = @RetryNo where LogId = @LogId
        exec @ReturnVal = [int].[RebalanceFund] @Fund_id, @Rebal_id, @Curr_id, @ThreadId
		--select @rv = 0
		select @fund_et = sysdatetime()
        exec [mon].[ThreadIncrementTimeUs] @Fund_id = @Fund_id, @ThreadId = @ThreadId, @MeasureId = 2, @FromDt2 = @fund_st, @ToDt2 = @fund_et, @IncrementTimeUs = @fund_ms output
		--select @fund_ms = datediff(microsecond, @fund_st, @fund_et)
		if @logstate = 1 update Logging.Rebalance set fund_et = @fund_et, fund_ms = @fund_ms where LogId = @LogId
       end
     end try
     begin catch
      select @Err_num = error_number()
	  --select @rv = 1
	  if @Err_num not in (2627, 41325)
       throw
     end catch
     delete Mutex.PriceSnapshot where Curr_id = @Curr_id and ThreadId = @ThreadId
    end
  end
end try
begin catch
 select @Err_num = error_number(), @Err_sev = error_severity(), @Err_st = error_severity(), @Err_prc = error_procedure(), @Err_ln = error_line(), @Err_msg = error_message(), @Err_id = newid()
 insert into Thread.ErrorLog(Error_id,Error_number,Error_severity,Error_state,Error_procedure,Error_line,Error_message,Error_dt,Sql_Process_id,Log_point,Rebal_id,ThreadId,MaxThreadId,LogId,Fund_id,Accnt_id)
 values (@Err_id,@Err_num,@Err_sev,@Err_st,@Err_prc,@Err_ln,@Err_msg,getdate(),@@spid,300,@Rebal_id,@ThreadId,@MaxThreadId,@LogId,@Fund_id,@Accnt_id);
end catch

select @et = sysdatetime()
if @logstate = 1 update Logging.Rebalance set End_dt = @et where LogId = @LogId

waitfor delay '00:00:00.500'; 
--return @rv
end
go
/* [int].[Deposit] */
create or alter procedure [int].[Deposit] @Fund_id smallint, @Accnt_id integer
with native_compilation, schemabinding, execute as owner as begin atomic with (transaction isolation level = snapshot, language = N'us_english')
--as begin
declare @DepositAmount decimal(20, 2)
select @DepositAmount = convert(decimal(20, 2), (abs(convert(bigint, convert(binary(8), newid()))) % 100) * 100)

declare @Max_Deposit_Id integer
select @Max_Deposit_Id = max(Deposit_Id) from Investor.Deposit where Fund_id = @Fund_id and Accnt_id = @Accnt_id
select @Max_Deposit_Id = isnull(@Max_Deposit_Id, 0) + 1
insert into Investor.Deposit (Fund_id,Accnt_id,Deposit_id,Rebal_id,Amount,Deposit_dt)
values (@Fund_id,@Accnt_id,@Max_Deposit_Id,null,@DepositAmount,sysdatetime())
Return 1
end
go
/* [ext].[Deposit] */
create or alter procedure [ext].[Deposit] @ThreadId smallint, @Fund_id smallint, @Accnt_id integer
as begin
declare @dep_st datetime2, @dep_et datetime2, @dep_ms bigint
select @dep_st = sysdatetime()
exec [int].[Deposit] @Fund_id = @Fund_id, @Accnt_id = @Accnt_id
select @dep_et = sysdatetime()
exec [mon].[ThreadIncrementTimeUs] @Fund_id = @Fund_id, @ThreadId = @ThreadId, @MeasureId = 4, @FromDt2 = @dep_st, @ToDt2 = @dep_et, @IncrementTimeUs = @dep_ms output
end
go
/* [int].[Withdrawal] */
create or alter procedure [int].[Withdrawal] @Fund_id smallint, @Accnt_id integer
with native_compilation, schemabinding, execute as owner as begin atomic with (transaction isolation level = snapshot, language = N'us_english')
--as begin

declare @Rebal_id smallint
select @Rebal_id = max(Rebal_id) from Investor.Balance where Fund_id = @Fund_id and Accnt_id = @Accnt_id

declare @WithdrawalAmount decimal(20, 2), @CurrBalance decimal(20, 2)
select @CurrBalance = isnull(HoldingsValue, 0) + isnull(CashBalance, 0) from Investor.Balance where Fund_id = @Fund_id and Accnt_id = @Accnt_id and Rebal_id = @Rebal_id
select @WithdrawalAmount = (abs(convert(bigint, convert(binary(8), newid()))) % 100) / 100.00 * .2 * @CurrBalance
if @WithdrawalAmount > 100 select @WithdrawalAmount = round(@WithdrawalAmount, -2)

declare @Max_Withdrawal_id integer
select @Max_Withdrawal_id = max(Withdrawal_id) from Investor.Withdrawal where Fund_id = @Fund_id and Accnt_id = @Accnt_id
select @Max_Withdrawal_id = isnull(@Max_Withdrawal_id, 0) + 1
insert into Investor.Withdrawal (Fund_id,Accnt_id,Withdrawal_id,Rebal_id,Amount,Withdrawal_dt)
values (@Fund_id,@Accnt_id,@Max_Withdrawal_id,null,-@WithdrawalAmount,sysdatetime())
Return 1
end
go
/* [ext].[Withdrawal] */
create or alter procedure [ext].[Withdrawal] @ThreadId smallint, @Fund_id smallint, @Accnt_id integer
as begin
declare @with_st datetime2, @with_et datetime2, @with_ms bigint
select @with_st = sysdatetime()
exec [int].[Withdrawal] @Fund_id = @Fund_id, @Accnt_id = @Accnt_id
select @with_et = sysdatetime()
exec [mon].[ThreadIncrementTimeUs] @Fund_id = @Fund_id, @ThreadId = @ThreadId, @MeasureId = 5, @FromDt2 = @with_st, @ToDt2 = @with_et, @IncrementTimeUs = @with_ms output
end
go
/* [int].[PriceUpdate] */
create or alter procedure [int].[PriceUpdate] @Exch_id smallint, @Curr_id tinyint, @PriceVariance int, @ThreadId smallint
with native_compilation, schemabinding, execute as owner as begin atomic with (transaction isolation level = snapshot, language = N'us_english')
--as begin

   delete Mutex.PriceSnapshot where Curr_id = @Curr_id and ThreadId = @ThreadId
   insert Mutex.PriceSnapshot (Curr_id, ThreadId) values (@Curr_id, @ThreadId)

   /* Update Exchange Price */
   update Market.ListingPrice
   set Price = Price + (Price * (((abs(convert(bigint, convert(binary(8), newid()))) % 100) / 100.00) * @PriceVariance - (@PriceVariance / 2)+.02)/100.0)
   where Exch_id = @Exch_id

   /* Log new Exchange prices */
   insert into Market.ListingPriceHistory (Asset_id, Exch_id, Price_dt, Price, Curr_id)
   select Asset_id, Exch_id, sysdatetime(), Price, Curr_id
   from Market.ListingPrice
   where Exch_id = @Exch_id

   delete from Mutex.PriceSnapshot where Curr_id = @Curr_id and ThreadId = @ThreadId
end
go
/* [ext].[PriceUpdate] */
create or alter procedure [ext].[PriceUpdate] @Exch_id smallint, @Curr_id tinyint, @PriceVariance int, @ThreadId smallint
as
begin
declare @with_st datetime2, @with_et datetime2, @with_ms bigint
select @with_st = sysdatetime()
declare @rv int = 0, @tryno int = 0
while @tryno <= 5
 begin
  begin try
   select @tryno += 1
   exec [int].[PriceUpdate] @Exch_id, @Curr_id, @PriceVariance, @ThreadId
   break
  end try
  begin catch 
  select @rv = 1
  end catch
 end
select @with_et = sysdatetime()
exec [mon].[ThreadIncrementTimeUs] @Fund_id = 0, @ThreadId = @ThreadId, @MeasureId = 6, @FromDt2 = @with_st, @ToDt2 = @with_et, @IncrementTimeUs = @with_ms output
return @rv
end
go
/* [int].[Dividend] */
create or alter procedure [int].[Dividend] @Asset_id integer, @Exch_id smallint, @Curr_id tinyint
with native_compilation, schemabinding, execute as owner as begin atomic with (transaction isolation level = snapshot, language = N'us_english')
--as begin
 declare @DividendAmount decimal(20, 2), @CurrentPrice decimal(10, 2)

	select @CurrentPrice = Price from Market.ListingPrice where Asset_id = @Asset_id and Exch_id = @Exch_id and Curr_id = @Curr_id
	select @CurrentPrice = isnull(@CurrentPrice, 0)

	select @DividendAmount = round((@CurrentPrice * .01 * rand()), 2)

if @DividendAmount >= .01
 begin
  declare @CorpAct_id uniqueidentifier = newid()
  insert Market.CorporateAction (CorpAct_id,CorpAct_type,Asset_id,Exch_id,Curr_id,CorpAct_dt,Ex_dt,Inc_dt,Amount)
  values (@CorpAct_id,'Div',@Asset_id,@Exch_id,@Curr_id,sysdatetime(),dateadd(second, 10, sysdatetime()), dateadd(second, 30, sysdatetime()), @DividendAmount)

  insert Fund.CorporateAction (CorpAct_id,Fund_id,CorpAct_type,Asset_id,Exch_id,Curr_id,CorpAct_dt,Ex_dt,Inc_dt,Amount)
  select ca.CorpAct_id,f.Fund_id,ca.CorpAct_type,ca.Asset_id,ca.Exch_id,ca.Curr_id,ca.CorpAct_dt,ca.Ex_dt,ca.Inc_dt,ca.Amount
  from   Market.CorporateAction ca
  join   Fund.Fund f on ca.Curr_id = f.Curr_id
  where  ca.CorpAct_id = @CorpAct_id
 end
end
go
/* [ext].[Dividend] */
create or alter procedure [ext].[Dividend] @ThreadId smallint, @Asset_id integer, @Exch_id smallint, @Curr_id tinyint
as
begin
declare @rv int = 0, @tryno int = 0
declare @div_st datetime2, @div_et datetime2, @div_ms bigint
select @div_st = sysdatetime()
while @tryno <= 5
 begin
  begin try
   select @tryno += 1

   exec [int].[Dividend] @Asset_id, @Exch_id, @Curr_id

   select @tryno = 6

  end try
  begin catch 
   select @rv = 1
  end catch
 end
select @div_et = sysdatetime()
exec [mon].[ThreadIncrementTimeUs] @Fund_id = 0, @ThreadId = @ThreadId, @MeasureId = 3, @FromDt2 = @div_st, @ToDt2 = @div_et, @IncrementTimeUs = @div_ms output
return @rv
end
go
/* [mon].[AcctRebalCount] */
create or alter procedure [mon].[AcctRebalCount]
with native_compilation, schemabinding, execute as owner as begin atomic with (transaction isolation level = snapshot, language = N'us_english')
--as begin

declare @dt varchar(32) = convert(varchar(32), getdate(), 121)

declare @SampleDT bigint;
select @SampleDT = convert(bigint, substring(@dt, 1, 4)+substring(@dt, 6, 2)+substring(@dt, 9, 2)+substring(@dt, 12, 2)+substring(@dt, 15, 2)+substring(@dt, 18, 2))

declare @ChartCountIntKey as dbo.ChartCountIntKey
insert @ChartCountIntKey (Id, Cnt) 
select Fund_id, sum(AcctRebalCount) as AcctRebalCount from Thread.Monitor group by Fund_id

select @SampleDT as time, f.Fund_Name as Series, ik.Cnt as Value
from   @ChartCountIntKey ik
join   Fund.Fund f on ik.Id = f.Fund_id  
union  
select @SampleDT as time, 'Total' as Series, isnull(sum(Cnt), 0) as Value
from   @ChartCountIntKey
end
go
/* [mon].[FundRebalCount] */
create or alter procedure [mon].[FundRebalCount]
with native_compilation, schemabinding, execute as owner as begin atomic with (transaction isolation level = snapshot, language = N'us_english')
--as begin

declare @dt varchar(32) = convert(varchar(32), getdate(), 121)

declare @SampleDT bigint;
select @SampleDT = convert(bigint, substring(@dt, 1, 4)+substring(@dt, 6, 2)+substring(@dt, 9, 2)+substring(@dt, 12, 2)+substring(@dt, 15, 2)+substring(@dt, 18, 2))

declare @ChartCountIntKey as dbo.ChartCountIntKey
insert @ChartCountIntKey (Id, Cnt)
select Fund_id, sum(FundRebalCount) as FundRebalCount from Thread.Monitor group by Fund_id

select @SampleDT as time, f.Fund_Name as Series, ik.Cnt as Value
from   @ChartCountIntKey ik
join   Fund.Fund f on ik.Id = f.Fund_id  
union  
select @SampleDT as time, 'Total' as Series, isnull(sum(Cnt), 0) as Value
from   @ChartCountIntKey
end
go
/* [mon].[ThreadIncrementTimeUs] */
create or alter procedure [mon].[ThreadIncrementTimeUs] @Fund_id smallint, @ThreadId smallint, @MeasureId smallint, @FromDt2 datetime2, @ToDt2 datetime2, @IncrementTimeUs bigint output
with native_compilation, schemabinding, execute as owner as begin atomic with (transaction isolation level = snapshot, language = N'us_english')
--as begin
select @IncrementTimeUs = datediff_big(microsecond, @FromDt2, @ToDt2) 
--select @Fund_id, @ThreadId, @MeasureId, @FromDt2, @ToDt2, @IncrementTimeUs
if @MeasureId = 1
 update Thread.Monitor set AcctRebalTimeUs += @IncrementTimeUs where Fund_id = @Fund_id and ThreadId = @ThreadId
if @MeasureId = 2
 update Thread.Monitor set FundRebalTimeUs += @IncrementTimeUs where Fund_id = @Fund_id and ThreadId = @ThreadId
if @MeasureId = 3
 update Thread.Monitor set DividendTimeUs += @IncrementTimeUs, DividendCount += 1 where Fund_id = @Fund_id and ThreadId = @ThreadId
if @MeasureId = 4
 update Thread.Monitor set DepositTimeUs += @IncrementTimeUs, DepositCount += 1 where Fund_id = @Fund_id and ThreadId = @ThreadId
if @MeasureId = 5
 update Thread.Monitor set WithdrawalTimeUs += @IncrementTimeUs, WithdrawalCount += 1 where Fund_id = @Fund_id and ThreadId = @ThreadId
if @MeasureId = 6
 update Thread.Monitor set PriceUpdateTimeUs += @IncrementTimeUs, PriceUpdateCount += 1 where Fund_id = @Fund_id and ThreadId = @ThreadId
end
go
/* [mon].[GetProgressBars] */
create or alter procedure [mon].[GetProgressBars] @CountOrTime char(1)
with native_compilation, schemabinding, execute as owner as begin atomic with (transaction isolation level = snapshot, language = N'us_english')
if @CountOrTime = 'C'
select ThreadId, 'AcctRebal' as [Type], AcctRebalCount as [Value], '#06B025' as [Color] from Thread.Monitor union all 
select ThreadId, 'FundRebal' as [Type], FundRebalCount as [Value], '#ffe119' as [Color] from Thread.Monitor union all 
select ThreadId, 'Dividend' as [Type], DividendCount as [Value], '#0078d7' as [Color] from Thread.Monitor union all 
select ThreadId, 'Deposit' as [Type], DepositCount as [Value], '#f032e6' as [Color] from Thread.Monitor union all 
select ThreadId, 'Withdrawal' as [Type], WithdrawalCount as [Value], '#E6194B' as [Color] from Thread.Monitor union all 
select ThreadId, 'PriceUpdate' as [Type], PriceUpdateCount as [Value], '#9a6324' as [Color] from Thread.Monitor  
order by ThreadId, [Type]
if @CountOrTime = 'T'
select ThreadId, 'AcctRebal' as [Type], AcctRebalTimeUs as [Value], '#06B025' as [Color] from Thread.Monitor union all 
select ThreadId, 'FundRebal' as [Type], FundRebalTimeUs as [Value], '#ffe119' as [Color] from Thread.Monitor union all 
select ThreadId, 'Dividend' as [Type], DividendTimeUs as [Value], '#0078d7' as [Color] from Thread.Monitor union all 
select ThreadId, 'Deposit' as [Type], DepositTimeUs as [Value], '#f032e6' as [Color] from Thread.Monitor union all 
select ThreadId, 'Withdrawal' as [Type], WithdrawalTimeUs as [Value], '#E6194B' as [Color] from Thread.Monitor union all 
select ThreadId, 'PriceUpdate' as [Type], PriceUpdateTimeUs as [Value], '#9a6324' as [Color] from Thread.Monitor  
order by ThreadId, [Type]
end
go
/* [load].[WorkerThreads] */
create or alter procedure [load].[WorkerThreads] @SQLBenchTestNoOfThreads smallint, @SQLBenchTestWorkerCount smallint
as

set nocount on

delete from Thread.Queue
delete from Thread.State
delete from Thread.Monitor

insert Thread.Queue (Queue_No, Fund_id, Curr_id, Accnt_id, Model_id)
select row_number() over (partition by Fund_id order by Accnt_id) as Queue_No, Fund_id, Curr_id, Accnt_id, Model_id from Investor.Account

declare @TotNoOfAccnts decimal(14, 2), @ThreadCntRnd decimal(14, 2), @Adj int = 0, @TotNoOfThreads decimal(14, 2) = @SQLBenchTestWorkerCount, @AdjFund_id smallint
select @TotNoOfAccnts = count(*) from Thread.Queue;

if object_id('tempdb..#FundAccnts') is not null drop table #FundAccnts
select    *, (ThreadCntRnd - ThreadCnt) as RndOrd, ThreadCntRnd as ThreadCntAdj
into      #FundAccnts
from     (select *
               , ThreadCnt = FundNoOfAccnts / @TotNoOfAccnts * @TotNoOfThreads
               , ThreadCntRnd = round(FundNoOfAccnts / @TotNoOfAccnts * @TotNoOfThreads, 0)
          from   (select Fund_id, FundNoOfAccnts = count(*) from Thread.Queue group by Fund_id) fa ) fa
order by  RndOrd desc

select @ThreadCntRnd = sum(ThreadCntRnd) from #FundAccnts
select @Adj = @ThreadCntRnd - @TotNoOfThreads;

if abs(@Adj) > 0
 update #FundAccnts set ThreadCntAdj -= @Adj where Fund_id = (select top 1 Fund_id from #FundAccnts order by abs(RndOrd) desc);

with
FundAccnts as (
select Fund_id, FundNoOfAccnts, MaxThreadId = sum(ThreadCnt) over (order by Fund_id) 
from   (select Fund_Id, FundNoOfAccnts, ThreadCnt = ThreadCntAdj
        from   (select *, rownum = row_number() over (order by RndOrd desc, FundNoOfAccnts desc, Fund_id desc) from #FundAccnts) fa) fa
),
ThreadQueue as (
select   Fund_id, FromThreadId, ToThreadId
       , FundNoOfAccnts
       , FundNoOfThreads = ToThreadId - FromThreadId + 1
       , ThreadAccnts    = convert(bigint, floor((convert(decimal(14, 2), FundNoOfAccnts) / convert(decimal(14, 2), (ToThreadId - FromThreadId + 1))))) 
from    (select   Fund_id
                , FromThreadId = convert(bigint, round((lag(MaxThreadId, 1, 0) over (order by MaxThreadId) + 1), 0)) 
                , ToThreadId   = convert(bigint, MaxThreadId)
                , FundNoOfAccnts
         from    FundAccnts
         ) b
),
ThreadState as (
select  ThreadId, Fund_id, FundThreadOrdinal, FundNoOfThreads
      , QueueNoFrom = (FundThreadOrdinal * ThreadAccnts) - ThreadAccnts + 1
      , QueueNoTo   = (FundThreadOrdinal * ThreadAccnts)
      , NextQueueNo = (FundThreadOrdinal * ThreadAccnts) - ThreadAccnts + 1
from   (select t.n as ThreadId
             , c.Fund_id
             , FundThreadOrdinal = row_number() over (partition by c.Fund_id order by t.n)
             , c.ThreadAccnts
             , c.FundNoOfThreads
        from   Load.Nums t
        join   ThreadQueue c on t.n between c.FromThreadId and c.ToThreadId
        where  t.n <= @TotNoOfThreads) c
),
ThreadState2 as (
select ts.ThreadId, ts.Fund_id, ts.FundThreadOrdinal, ts.FundNoOfThreads, ts.QueueNoFrom, ts.QueueNoTo, ts.NextQueueNo, ca.FundNoOfAccnts - ts2.QueueNoTo as RoundingAccounts, ca.FundNoOfAccnts
from   FundAccnts ca 
join   ThreadState ts on ts.Fund_id = ca.Fund_id
cross apply (select top 1 * from ThreadState ts2 where ts2.Fund_id = ca.Fund_id order by ts2.ThreadId desc) ts2
),
ThreadState3 as (
select *, (QueueNoToAdj - QueueNoFromAdj + 1) as NoOfAccounts
from  (select ThreadId, Fund_id, FundThreadOrdinal, FundNoOfThreads, QueueNoFrom, QueueNoTo, NextQueueNo, RoundingAccounts, FundNoOfAccnts
            , QueueNoFromAdj = case when FundThreadOrdinal <= RoundingAccounts then QueueNoFrom + FundThreadOrdinal - 1 else QueueNoFrom + RoundingAccounts end
            , QueueNoToAdj   = case when FundThreadOrdinal <= RoundingAccounts then QueueNoTo + FundThreadOrdinal else QueueNoTo + RoundingAccounts end
            
       from ThreadState2) ts
)
insert into Thread.State (ThreadId, Fund_id, Curr_id, QueueNoFrom, QueueNoTo, NextQueueNo)
select ts.ThreadId, ts.Fund_id, f.Curr_id, QueueNoFrom = ts.QueueNoFromAdj, QueueNoTo = ts.QueueNoToAdj, NextQueueNo = ts.QueueNoFromAdj
from ThreadState3 ts join Fund.Fund f on ts.Fund_id = f.Fund_id order by ts.ThreadId

insert into Thread.Monitor (Fund_id,ThreadId,AcctRebalCount,AcctRebalTimeUs,FundRebalCount,FundRebalTimeUs,DividendCount,DividendTimeUs,DepositCount,DepositTimeUs,WithdrawalCount,WithdrawalTimeUs,PriceUpdateCount,PriceUpdateTimeUs) 
select Fund_id, ThreadId, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
from   Thread.State

insert into Thread.Monitor (Fund_id,ThreadId,AcctRebalCount,AcctRebalTimeUs,FundRebalCount,FundRebalTimeUs,DividendCount,DividendTimeUs,DepositCount,DepositTimeUs,WithdrawalCount,WithdrawalTimeUs,PriceUpdateCount,PriceUpdateTimeUs) 
select 0, n, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
from Load.Nums where n between (@SQLBenchTestWorkerCount + 1) and @SQLBenchTestNoOfThreads
go
/* [load].[Investors] */
create or alter procedure [load].[Investors] @NoOfInvestorsPerFund bigint
with native_compilation, schemabinding, execute as owner as begin atomic with (transaction isolation level = snapshot, language = N'us_english')
--as begin

declare @Fund_id smallint, @Curr_id tinyint, @MaxInvId bigint, @CurrModelCount bigint

declare @ModelRandomizer load.ModelRandomizer

/* Investors & Accounts */
select @Fund_id = min(Fund_id) from Fund.Fund
while @Fund_id is not null
 begin
  select @Curr_id = null, @MaxInvId = null, @CurrModelCount = null
  select @Curr_id = Curr_id from Fund.Fund where Fund_id = @Fund_id
  
  select @MaxInvId = max(Inv_id) from Investor.Investor
  select @MaxInvId = isnull(@MaxInvId, 0)
  --select @Fund_id as "@Fund_id", @Curr_id as "@Curr_id", @MaxModelRowNo as "@MaxModelRowNo", @MaxInvId as "@MaxInvId", @NoOfInvestorsPerFund as "@NoOfInvestorsPerFund"

  insert Investor.Investor (Inv_id, Inv_name)
  select n as Inv_id, N'Investor'+convert(nvarchar(256), n) as Inv_name
  from   Load.Nums n
  where  n > @MaxInvId and n <= @MaxInvId + @NoOfInvestorsPerFund
  
  select @CurrModelCount = count(*) from Advisor.Model where Curr_id = @Curr_id

  delete    @ModelRandomizer
  insert    @ModelRandomizer (id, Model_id)
  select    sum(1) as Id, m1.Model_id
  from      Advisor.Model m1
  left join Advisor.Model m2 on m1.Model_id >= m2.Model_id and m1.Curr_id = m2.Curr_id
  where     m1.Curr_id = @Curr_id
  group by  m1.Model_id

  insert  Investor.Account (Accnt_id, Inv_id, Curr_id, Fund_id, Model_id)
  select  i.Inv_id as Accnt_id, i.Inv_id, @Curr_id as Curr_id, @Fund_id as Fund_id, m.Model_Id as Model_id /* Random Model_id */
  from    Investor.Investor i
  join   (select nums.n as Inv_Id, convert(int, ((rand(n) * 10000 - floor(rand(n) * 10000)) * @CurrModelCount) + 1) as Random_Model_Id --, cast(rand(checksum(newid())) * @CurrModelCount + 1 as int) as Random_Model_Id
          from Load.Nums nums where nums.n > @MaxInvId and nums.n <= @MaxInvId + @NoOfInvestorsPerFund) rn on i.Inv_Id = rn.Inv_Id
  cross apply (select top 1 Model_id from @ModelRandomizer m where m.Id = rn.Random_Model_Id order by Model_id ) m
  where   i.Inv_id > @MaxInvId and i.Inv_id <= @MaxInvId + @NoOfInvestorsPerFund

  insert  Investor.AccountSeq (Fund_id, Accnt_id, Tran_id, Trade_id)
  select  Fund_id, Accnt_id, 0, 0
  from    Investor.Account
  where   Fund_id = @Fund_id

  declare @AccntsCount bigint, @MaxAccnt_id integer, @Rebal_id integer = 1

  insert Investor.Balance (Fund_id, Accnt_id, Rebal_id, HoldingsValue, CashBalance)
  select a.Fund_id, a.Inv_id as Accnt_id, 0, 0, 0
  from   Investor.Account a
  where  Fund_id = @Fund_id

  insert Investor.Deposit (Fund_id, Accnt_id, Deposit_id, Rebal_id, Amount, Deposit_dt)
  select a.Fund_id, a.Accnt_id, 1 as Deposit_id, null as Rebal_id, convert(int, (rand(a.Accnt_id) * 10000 - floor(rand(a.Accnt_id) * 10000)) * 100) * 100 as Amount, getdate()
  from   Investor.Account a
  where  Fund_id = @Fund_id

  select @AccntsCount = null, @MaxAccnt_id = null
  select @AccntsCount = count(*), @MaxAccnt_id = max(Accnt_id) from Investor.Account where Fund_id = @Fund_id
  update Fund.Fund set AccntsCount = @AccntsCount, MaxAccnt_id = @MaxAccnt_id where Fund_id = @Fund_id

  insert Fund.Rebalance (Rebal_id, Fund_id, Curr_id, Start_dt, AccntsCount, End_dt, InvestorHoldings, InvestorCash, Deposits, Withdrawals, FeesCharged)
  select @Rebal_id, Fund_id, Curr_id, getdate(), AccntsCount, '01-Jan-1900', 0, 0, 0, 0, 0
  from   Fund.Fund where Fund_id = @Fund_id

  insert Fund.RebalancePricing (Rebal_id, Fund_id, Asset_id, Exch_id, Curr_id, Price)
  select @Rebal_id, @Fund_id, Asset_id, Exch_id, Curr_id, Price
  from   Market.ListingPrice where Curr_id = @Curr_id

  insert    Fund.ModelAssetPrice (Fund_id, Rebal_id, Model_id, Asset_id, Exch_id, Curr_id, Weighting, Curr_Price)
  select    @Fund_id, @Rebal_id, a.Model_id, a.Asset_id, a.Exch_id, a.Curr_id, a.Weighting, rp.Price
  from      Fund.RebalancePricing rp
  join      Advisor.ModelAsset a on a.Asset_id = rp.Asset_id and a.Exch_id = rp.Exch_id and a.Curr_id = rp.Curr_id
  where     rp.Fund_id = @Fund_id and rp.Rebal_id = @Rebal_id

  select @Fund_id = min(Fund_id) from Fund.Fund where Fund_id > @Fund_id
  select @NoOfInvestorsPerFund += 4000
 end
end
go

/* [load].[ModelAssets] */
create or alter procedure [load].[ModelAssets] @MinimumAssetsPerModel bigint, @MaximumAssetsPerModel bigint
as begin
set nocount on

declare @Curr_id tinyint, @Model_id smallint, @Asset_id integer, @Exch_id smallint

/* Model Asset Weightings */
declare @ThisAllocationsCount int, @AssetNo integer, @Weighting decimal(8, 2), @WeightRemaining decimal(8, 2), @MaxListingRowNo bigint, @RandListingNo bigint
select @Model_id = null, @Asset_id = 0, @Weighting = 0
select @Model_id = min(Model_id) from Advisor.Model
while @Model_id is not null
 begin
  select @ThisAllocationsCount = null, @WeightRemaining = 100, @Curr_id = null
  select @Curr_id = Curr_id from Advisor.Model where Model_id = @Model_id
  select @ThisAllocationsCount = round((rand() * (@MaximumAssetsPerModel-@MinimumAssetsPerModel)), 0)+@MinimumAssetsPerModel
  update Advisor.Model set AssetsCount = @ThisAllocationsCount where Model_id = @Model_id
  select @AssetNo = 1
  while @AssetNo <= @ThisAllocationsCount
   begin
    select @Asset_id = null, @RandListingNo = null, @Exch_id = null
	select @MaxListingRowNo = count(*) from Market.Listing where Curr_id = @Curr_id and Asset_id not in (select Asset_id from Advisor.ModelAsset where Model_id = @Model_id)
	select @RandListingNo = (round((rand() * (@MaxListingRowNo - 1)), 0))+1

	select @Asset_id = Asset_id, @Exch_id = Exch_id
	from  (select row_number() over (order by Asset_id) as RowNo, Asset_id, Exch_id 
           from Market.Listing where Curr_id = @Curr_id and Asset_id not in (select Asset_id from Advisor.ModelAsset where Model_id = @Model_id) ) a
	where  RowNo = @RandListingNo

	if @AssetNo = @ThisAllocationsCount select @Weighting = @WeightRemaining
     else select @Weighting = (round((rand() * (@WeightRemaining - (@ThisAllocationsCount - @AssetNo + 1))), 0))+1
	select @WeightRemaining = @WeightRemaining - @Weighting

    insert into Advisor.ModelAsset (Model_id, Asset_id, Exch_id, Curr_id, Weighting) values (@Model_id, @Asset_id, @Exch_id, @Curr_id, @Weighting/100.000000)
	select @AssetNo += 1
   end

  select @Model_id = min(Model_id) from Advisor.Model where Model_id > @Model_id
 end
end
go
/* [load].[Models] */
create or alter procedure [load].[Models] @NoOfModelsPerAdvisorPerCurrency bigint
with native_compilation, schemabinding, execute as owner as begin atomic with (transaction isolation level = snapshot, language = N'us_english')
--as begin
--set nocount on

declare @Curr_id tinyint, @Model_id smallint, @Advisor_id smallint, @Advisor_name nvarchar(256)
, @Model_name nvarchar(256), @Adv_Model_count bigint, @Max_Advisor_id smallint

/* Models */
select @Max_Advisor_id = max(Advisor_id) from Advisor.Advisor
select @Advisor_id = 1, @Model_id = 1
while @Advisor_id <= @Max_Advisor_id
 begin
  select @Advisor_name = null, @Curr_id = null
  select @Advisor_name = Advisor_name from Advisor.Advisor where Advisor_id = @Advisor_id
  select @Curr_id = min(Curr_id) from Market.Currency
  while @Curr_id is not null
   begin
    select @Adv_Model_count = 1
    while @Adv_Model_count <= @NoOfModelsPerAdvisorPerCurrency
     begin
      select @Adv_Model_count += 1
      select @Model_name = N'Model'+convert(nvarchar(256), @Model_id)
      insert into Advisor.Model (Model_id, Model_name, Advisor_id, Curr_id) values (@Model_id, @Model_name, @Advisor_id, @Curr_id)  
      select @Model_id += 1
     end
    select @Curr_id = min(Curr_id) from Market.Currency where Curr_id > @Curr_id
   end
  select @Advisor_id += 1
 end
end
go
/* [load].[Advisors] */
create or alter procedure [load].[Advisors] @NoOfAdvisors bigint
with native_compilation, schemabinding, execute as owner as begin atomic with (transaction isolation level = snapshot, language = N'us_english')
--as begin

--set nocount on

declare @Advisor_id smallint, @Advisor_name nvarchar(256)

/* Advisors */
select @Advisor_id = 1
while @Advisor_id <= @NoOfAdvisors
 begin
  select @Advisor_name = N'Advisor'+convert(nvarchar(256), @Advisor_id)
  insert into Advisor.Advisor (Advisor_id, Advisor_name) values (@Advisor_id, @Advisor_name)
  select @Advisor_id += 1
 end
end
go
/* [load].[Assets] */
create or alter procedure [load].[Assets] @NoOfAssetsPerExchangeClass bigint, @MaximumAssetPrice int
--with native_compilation, schemabinding, execute as owner as begin atomic with (transaction isolation level = snapshot, language = N'us_english')
as begin

declare @Exch_id smallint, @ISIN_Prefix char(2), @Class_code varchar(64), @Asset_id integer, @ISIN_code char(12), @Asset_name nvarchar(256), @Asset_count bigint, @Price decimal(10, 2), @Curr_id tinyint

/* Assets & Listings */
select @Asset_id = 1
select @Exch_id = min(Exch_id) from Market.Exchange
while @Exch_id is not null
 begin
  select @Class_code = null, @ISIN_Prefix = null, @Curr_id = null
  select @ISIN_Prefix = ISIN_Prefix, @Curr_id = Curr_id from Market.Exchange where Exch_id = @Exch_id
  select @Class_code = min(Class_code) from Market.AssetClass
  while @Class_code is not null
   begin
    select @Asset_count = 1
    while @Asset_count <= @NoOfAssetsPerExchangeClass
     begin
      select @ISIN_code = null, @Asset_name = null, @Price = null
	  select @ISIN_Code = @ISIN_Prefix+left(('000000000'+convert(varchar(64), @Asset_id)), 12), @Asset_name = @Class_code+N'_'+convert(nvarchar(256), @Asset_id)+N'_'+convert(nvarchar(256), @ISIN_code)
	  insert into Market.Asset (Asset_id, ISIN_code, Asset_name, Class_code) values (@Asset_id, @ISIN_code, @Asset_name, @Class_code)
	  select @Price = round((rand()*(@MaximumAssetPrice-1)), 0)+1
	  insert into Market.Listing (Asset_id, Exch_id, ISIN_Code, Curr_id) values (@Asset_id, @Exch_id, @ISIN_Code, @Curr_id)
	  insert into Market.ListingPrice (Asset_id, Exch_id, Curr_id, Price) values (@Asset_id, @Exch_id, @Curr_id, @Price)
      select @Asset_count += 1, @Asset_id += 1
     end
     select @Class_code = min(Class_code) from Market.AssetClass where Class_code > @Class_code
   end
  select @Exch_id = min(Exch_id) from Market.Exchange where Exch_id > @Exch_id
 end


insert into Market.ListingPriceHistory (Asset_id, Exch_id, Price_dt, Price, Curr_id)
select Asset_id, Exch_id, sysdatetime(), Price, Curr_id
from Market.ListingPrice
end
go
/* [load].[DeleteAllData] */
create or alter procedure [load].[DeleteAllData]
with native_compilation, schemabinding, execute as owner as begin atomic with (transaction isolation level = snapshot, language = N'us_english')
--as begin

delete from Logging.Rebalance
delete from Logging.RebalanceAsset
delete from Logging.PriceUpdate
delete from Mutex.PriceSnapshot
delete from Thread.ErrorLog
delete from Thread.Queue
delete from Thread.State
delete from Investor.Deposit
delete from Investor.Holding
delete from Investor.Transact
delete from Investor.Trade
delete from Investor.AccountRebalance
delete from Investor.Balance
delete from Investor.AccountSeq
delete from Investor.Account
delete from Investor.Investor
delete from Advisor.ModelAsset
delete from Advisor.Model
delete from Advisor.Advisor
delete from Fund.ModelAssetPrice
delete from Fund.RebalancePricing
delete from Fund.HoldingHistory
delete from Fund.Holding
delete from Fund.Trade
delete from Fund.Rebalance
delete from Fund.BalanceHistory
delete from Fund.Balance
delete from Fund.Fund
delete from Market.ListingPriceHistory
delete from Market.ListingPrice
delete from Market.Listing
delete from Market.Asset
delete from Market.AssetClass
delete from Market.Exchange
delete from Market.Currency

end
go

use WealthBench

alter database WealthBench set query_store clear


go



use WealthBench
go

use master
go
