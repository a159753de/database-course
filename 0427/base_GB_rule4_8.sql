CREATE OR ALTER FUNCTION [dbo].[base_GB_rule4_8]
(
    @company varchar(10),
    @positive_bias_threshold float,  -- 正乖離閾值 (例如: 5.0)
    @negative_bias_threshold float   -- 負乖離閾值 (例如: -5.0)
)
RETURNS @result TABLE
(
    date date,             -- 交易日期
    buy_or_sell int,       -- 1為買入, -1為賣出
    gb_rule int,           -- 法則編號 (4 或 8)
    bias float,            -- 當日乖離率
    price real,            -- 當日收盤價
    ma_value real,         -- 當日均線值
    trend nvarchar(20)     -- 均線趨勢 (文字)
)
AS
BEGIN
    /* 1. 宣告暫存表 */
    DECLARE @temp_table TABLE
    (
        date date,
        today_c real,
        today_MA real,
        bias real,       
        trend int        
    )

    /* 2. 將 MA_trend 資訊放入暫存表 (假設使用 MA20，往回看 8 天，決斷天數 6 天) */
    INSERT INTO @temp_table (date, today_c, today_MA, trend)
    SELECT date, today_c, today_MA, trend
    FROM dbo.find_MA_updown(@company, 'MA20', 8, 6)

    /* 3. 更新乖離率 (bias) 值至 @temp_table */
    UPDATE @temp_table
    SET bias = ((today_c - today_MA) / today_MA) * 100.0

    /* 4. 宣告游標所需參數 */
    DECLARE @date DATE
    DECLARE @today_c REAL
    DECLARE @today_MA REAL
    DECLARE @today_bias REAL
    DECLARE @trend INT
    DECLARE @trend_str NVARCHAR(20)

    /* 宣告 cursor，依日期升序排列 */
    DECLARE cur CURSOR LOCAL FOR
        SELECT date, today_c, today_MA, bias, trend
        FROM @temp_table
        ORDER BY date ASC

    OPEN cur
    FETCH NEXT FROM cur INTO @date, @today_c, @today_MA, @today_bias, @trend

    /* 5. 開啟 cursor，逐筆根據條件生成信號 */
    WHILE @@FETCH_STATUS = 0
    BEGIN
        /* 將 trend 數字轉換為可讀文字以符合輸出需求 */
        SET @trend_str = CASE 
                            WHEN @trend = 1 THEN '上漲趨勢'
                            WHEN @trend = -1 THEN '下跌趨勢'
                            ELSE '橫盤整理'
                         END

        /* 法則 4: 抄底 - 價格大幅低於均線(負乖離過大)且處於下降趨勢中 */
        IF (@trend = -1 AND @today_bias < @negative_bias_threshold)
        BEGIN
            INSERT INTO @result
            VALUES(@date, 1, 4, @today_bias, @today_c, @today_MA, @trend_str)
        END

        /* 法則 8: 反轉 - 價格大幅高於均線(正乖離過大)且處於上升趨勢中 */
        IF (@trend = 1 AND @today_bias > @positive_bias_threshold)
        BEGIN
            INSERT INTO @result
            VALUES(@date, -1, 8, @today_bias, @today_c, @today_MA, @trend_str)
        END

        FETCH NEXT FROM cur INTO @date, @today_c, @today_MA, @today_bias, @trend
    END

    CLOSE cur
    DEALLOCATE cur

    RETURN
END;