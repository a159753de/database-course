CREATE OR ALTER FUNCTION [dbo].[GB_rule4]
(
    @company varchar(10),
    @negative_bias_threshold float  -- 負乖離閾值，應該是負數，例如 -5
)
RETURNS @result TABLE
(
    date date,         -- 買入時間
    buy_or_sell int,   -- 買入為1
    gb_rule int,       -- 法則編號
    bias float,        -- 當日乖離率
    price real,        -- 當日價格
    ma_value real      -- 當日均線值
)
AS
BEGIN
    /* 宣告暫存表 */
    DECLARE @temp_table TABLE
    (
        date date,
        today_c real,
        today_MA real,
        bias real,     -- 每日乖離
        trend int      -- 判斷現在為空頭、多頭趨勢
    )

    /* 將MA_trend資訊放入暫存表 */
    INSERT INTO @temp_table (date, today_c, today_MA, trend)
    SELECT date, today_c, today_MA, trend
    FROM find_MA_updown(@company, 'MA20', 8, 6)

    /* 更新bias值至@temp_table */
    UPDATE @temp_table
    SET bias = ((today_c - today_MA) / today_MA) * 100

    /* 宣告參數 */
    DECLARE @date DATE
    DECLARE @today_c REAL
    DECLARE @today_MA REAL
    DECLARE @today_bias REAL
    DECLARE @trend INT

    /* 宣告cursor */
    DECLARE cur CURSOR LOCAL FOR
        SELECT date, today_c, today_MA, bias, trend
        FROM @temp_table
        ORDER BY date ASC

    OPEN cur
    FETCH NEXT FROM cur INTO @date, @today_c, @today_MA, @today_bias, @trend

    /* 開啟cursor，根據條件生成買入訊號 */
    WHILE @@FETCH_STATUS = 0
    BEGIN
        /* 法則4: 抄底 - 價格大幅低於均線(負乖離過大)且處於下降趨勢中 */
        IF (@trend = -1 AND @today_bias < @negative_bias_threshold)
        BEGIN
            INSERT INTO @result
            VALUES(@date, 1, 4, @today_bias, @today_c, @today_MA)
        END

        FETCH NEXT FROM cur INTO @date, @today_c, @today_MA, @today_bias, @trend
    END

    CLOSE cur
    DEALLOCATE cur

    RETURN
END