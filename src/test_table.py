import pandas as pd
import numpy as np
from asyncio import new_event_loop
from tsetmc.market_watch import MarketWatch
from sqlalchemy import text
from config import engine, TSETMC_SCHEMA
from datetime import datetime, time

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

async def listen_to_update_events():
    global market_watch
    while True:
        # بررسی زمان: اگر بعد از 14:45 باشد، حلقه را متوقف کن
        current_time = datetime.now().time()
        if current_time > time(16, 55):
            print("زمان از 14:45 گذشته است. متوقف کردن برنامه.")
            break
        
        await market_watch.update_event.wait()
        df = market_watch.df.copy()
        
        print(df.head())
        print("=" * 80)
        
        df['UpdateTime'] = pd.Timestamp.now()
        
        # محاسبه BuyCount و SellCount
        df['BuyCount'] = df[[f'zd{i}' for i in range(1,6)]].fillna(0).sum(axis=1)
        df['SellCount'] = df[[f'zo{i}' for i in range(1,6)]].fillna(0).sum(axis=1)
        
        # محاسبه Buy_Volume و Sell_Volume
        df['BuyVolume'] = df[[f'qd{i}' for i in range(1,6)]].fillna(0).sum(axis=1)
        df['SellVolume'] = df[[f'qo{i}' for i in range(1,6)]].fillna(0).sum(axis=1)
        
        # محاسبه نسبت ورود پول هوشمند (Smart Money Ratio): (BuyVolume / BuyCount) / (SellVolume / SellCount)
        df['SmartMoneyRatio'] = np.where(
            (df['BuyCount'] == 0) | (df['SellCount'] == 0),
            0,
            (df['BuyVolume'] / df['BuyCount']) / (df['SellVolume'] / df['SellCount'])
        )
        
        # فیلتر و تبدیل همه ستون‌ها به نوع مناسب (همه ستون‌ها)
        insert_df = pd.DataFrame({
            'InsCode': df.index,
            'zo1': df.get('zo1', 0).fillna(0).astype(float),
            'zo2': df.get('zo2', 0).fillna(0).astype(float),
            'zo3': df.get('zo3', 0).fillna(0).astype(float),
            'zo4': df.get('zo4', 0).fillna(0).astype(float),
            'zo5': df.get('zo5', 0).fillna(0).astype(float),
            'zd1': df.get('zd1', 0).fillna(0).astype(float),
            'zd2': df.get('zd2', 0).fillna(0).astype(float),
            'zd3': df.get('zd3', 0).fillna(0).astype(float),
            'zd4': df.get('zd4', 0).fillna(0).astype(float),
            'zd5': df.get('zd5', 0).fillna(0).astype(float),
            'pd1': df.get('pd1', 0).fillna(0).astype(float),
            'pd2': df.get('pd2', 0).fillna(0).astype(float),
            'pd3': df.get('pd3', 0).fillna(0).astype(float),
            'pd4': df.get('pd4', 0).fillna(0).astype(float),
            'pd5': df.get('pd5', 0).fillna(0).astype(float),
            'po1': df.get('po1', 0).fillna(0).astype(float),
            'po2': df.get('po2', 0).fillna(0).astype(float),
            'po3': df.get('po3', 0).fillna(0).astype(float),
            'po4': df.get('po4', 0).fillna(0).astype(float),
            'po5': df.get('po5', 0).fillna(0).astype(float),
            'qd1': df.get('qd1', 0).fillna(0).astype(float),
            'qd2': df.get('qd2', 0).fillna(0).astype(float),
            'qd3': df.get('qd3', 0).fillna(0).astype(float),
            'qd4': df.get('qd4', 0).fillna(0).astype(float),
            'qd5': df.get('qd5', 0).fillna(0).astype(float),
            'qo1': df.get('qo1', 0).fillna(0).astype(float),
            'qo2': df.get('qo2', 0).fillna(0).astype(float),
            'qo3': df.get('qo3', 0).fillna(0).astype(float),
            'qo4': df.get('qo4', 0).fillna(0).astype(float),
            'qo5': df.get('qo5', 0).fillna(0).astype(float),
            'isin': df.get('isin', pd.NA),
            'l18': df.get('l18', pd.NA),
            'l30': df.get('l30', pd.NA),
            'heven': df.get('heven', 0).fillna(0).astype(int),
            'pf': df.get('pf', 0).fillna(0).astype(float),
            'pc': df.get('pc', 0).fillna(0).astype(float),
            'pl': df.get('pl', 0).fillna(0).astype(float),
            'tno': df.get('tno', 0).fillna(0).astype(float),
            'tvol': df.get('tvol', 0).fillna(0).astype(float),
            'tval': df.get('tval', 0).fillna(0).astype(float),
            'pmin': df.get('pmin', 0).fillna(0).astype(float),
            'pmax': df.get('pmax', 0).fillna(0).astype(float),
            'py': df.get('py', 0).fillna(0).astype(float),
            'eps': df.get('eps', 0).fillna(0).astype(float),
            'bvol': df.get('bvol', 0).fillna(0).astype(float),
            'visitcount': df.get('visitcount', 0).fillna(0).astype(int),
            'flow': df.get('flow', 0).fillna(0).astype(int),
            'cs': df.get('cs', pd.NA),
            'tmax': df.get('tmax', 0).fillna(0).astype(float),
            'tmin': df.get('tmin', 0).fillna(0).astype(float),
            'z': df.get('z', 0).fillna(0).astype(float),
            'yval': df.get('yval', pd.NA),
            'predtran': df.get('predtran', 0).fillna(0).astype(float),
            'buyop': df.get('buyop', 0).fillna(0).astype(float),
            'BuyCount': df['BuyCount'].astype(float),
            'SellCount': df['SellCount'].astype(float),
            'SmartMoneyRatio': df['SmartMoneyRatio'].astype(float),
            'UpdateTime': df['UpdateTime']
        })
        
        print(f"insert_df shape: {insert_df.shape}")
        print("Sample insert_df row:", insert_df.iloc[0].to_dict())
        
        # Bulk insert با executemany
        stmt = f"""
        MERGE {TSETMC_SCHEMA}.market_watch AS target
        USING (SELECT :InsCode AS InsCode) AS source
        ON (target.InsCode = source.InsCode)
        WHEN MATCHED THEN
            UPDATE SET 
                zo1 = :zo1, zo2 = :zo2, zo3 = :zo3, zo4 = :zo4, zo5 = :zo5,
                zd1 = :zd1, zd2 = :zd2, zd3 = :zd3, zd4 = :zd4, zd5 = :zd5,
                pd1 = :pd1, pd2 = :pd2, pd3 = :pd3, pd4 = :pd4, pd5 = :pd5,
                po1 = :po1, po2 = :po2, po3 = :po3, po4 = :po4, po5 = :po5,
                qd1 = :qd1, qd2 = :qd2, qd3 = :qd3, qd4 = :qd4, qd5 = :qd5,
                qo1 = :qo1, qo2 = :qo2, qo3 = :qo3, qo4 = :qo4, qo5 = :qo5,
                isin = :isin, l18 = :l18, l30 = :l30, heven = :heven,
                pf = :pf, pc = :pc, pl = :pl, tno = :tno, tvol = :tvol, tval = :tval,
                pmin = :pmin, pmax = :pmax, py = :py, eps = :eps, bvol = :bvol,
                visitcount = :visitcount, flow = :flow, cs = :cs, tmax = :tmax, tmin = :tmin,
                z = :z, yval = :yval, predtran = :predtran, buyop = :buyop,
                BuyCount = :BuyCount, SellCount = :SellCount, SmartMoneyRatio = :SmartMoneyRatio,
                UpdateTime = :UpdateTime
        WHEN NOT MATCHED THEN
            INSERT (InsCode, zo1, zo2, zo3, zo4, zo5, zd1, zd2, zd3, zd4, zd5,
                    pd1, pd2, pd3, pd4, pd5, po1, po2, po3, po4, po5,
                    qd1, qd2, qd3, qd4, qd5, qo1, qo2, qo3, qo4, qo5,
                    isin, l18, l30, heven, pf, pc, pl, tno, tvol, tval,
                    pmin, pmax, py, eps, bvol, visitcount, flow, cs, tmax, tmin,
                    z, yval, predtran, buyop, BuyCount, SellCount, SmartMoneyRatio, UpdateTime)
            VALUES (:InsCode, :zo1, :zo2, :zo3, :zo4, :zo5, :zd1, :zd2, :zd3, :zd4, :zd5,
                    :pd1, :pd2, :pd3, :pd4, :pd5, :po1, :po2, :po3, :po4, :po5,
                    :qd1, :qd2, :qd3, :qd4, :qd5, :qo1, :qo2, :qo3, :qo4, :qo5,
                    :isin, :l18, :l30, :heven, :pf, :pc, :pl, :tno, :tvol, :tval,
                    :pmin, :pmax, :py, :eps, :bvol, :visitcount, :flow, :cs, :tmax, :tmin,
                    :z, :yval, :predtran, :buyop, :BuyCount, :SellCount, :SmartMoneyRatio, :UpdateTime);
        """
        
        with engine.begin() as conn:
            conn.execute(text(stmt), insert_df.to_dict(orient='records'))
            print(f"✅ Bulk insert/update completed for {len(insert_df)} records.")

# 🟢 ساخت شیء MarketWatch
market_watch = MarketWatch()

# 🔄 ساخت حلقه رویداد جدید
loop = new_event_loop()
watch_task = loop.create_task(listen_to_update_events())

# 🚀 شروع دریافت داده زنده
loop.run_until_complete(market_watch.start())
loop.run_forever()