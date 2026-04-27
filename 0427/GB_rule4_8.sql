CREATE OR ALTER FUNCTION [dbo].[GB_rule4_8]
(
    @company varchar(10),
    @positive_bias_threshold float,  -- 正乖離閾值，例如 5.0
    @negative_bias_threshold float   -- 負乖離閾值，例如 -5.0
)
RETURNS @result TABLE
(
    date date,                -- 日期
    buy_or_sell int,          -- 買賣方向 (1代表買入，-1代表賣出)
    gb_rule int,              -- 葛蘭碧法則編號 (4, 44, 8, 88，均線轉向設定為 0)
    description nvarchar(100) -- 文字說明
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

    /* 2. 將 MA 趨勢資訊放入暫存表 (假設使用 MA20，往回看 8 天，決斷天數 6 天) */
    INSERT INTO @temp_table (date, today_c, today_MA, trend)
    SELECT date, today_c, today_MA, trend
    FROM dbo.find_MA_updown(@company, 'MA20', 8, 6)

    /* 3. 計算並更新 bias 乖離率值 */
    UPDATE @temp_table
    SET bias = ((today_c - today_MA) / today_MA) * 100.0

    /* 4. 宣告參數與 Cursor */
    DECLARE @date DATE
    DECLARE @today_c REAL
    DECLARE @today_MA REAL
    DECLARE @today_bias REAL
    DECLARE @trend INT

    -- 宣告用於記錄「前一天」資訊的變數，以利判斷進階條件與均線轉向
    DECLARE @prev_bias REAL = NULL
    DECLARE @prev_trend INT = NULL

    -- 開啟 cursor，依日期升序排列
    DECLARE cur CURSOR LOCAL FOR
        SELECT date, today_c, today_MA, bias, trend
        FROM @temp_table
        ORDER BY date ASC

    OPEN cur
    FETCH NEXT FROM cur INTO @date, @today_c, @today_MA, @today_bias, @trend

    /* 5. 逐筆根據條件生成信號 */
    WHILE @@FETCH_STATUS = 0
    BEGIN
        
        -- 移動均線轉向判斷：均線由下降 (-1) 轉為上升 (1) 時產生買入信號
        IF (@prev_trend = -1 AND @trend = 1)
        BEGIN
            INSERT INTO @result (date, buy_or_sell, gb_rule, description)
            VALUES (@date, 1, 0, '均線轉向: 由下降轉為上升 (產生買入信號)')
        END
        -- 移動均線轉向判斷：均線由上升 (1) 轉為下降 (-1) 時產生賣出信號
        ELSE IF (@prev_trend = 1 AND @trend = -1)
        BEGIN
            INSERT INTO @result (date, buy_or_sell, gb_rule, description)
            VALUES (@date, -1, 0, '均線轉向: 由上升轉為下降 (產生賣出信號)')
        END

        -- 法則 4 進階 (44): 在高度負乖離後，乖離率開始回升(但仍為負)時，產生買入信號
        IF (@prev_bias < @negative_bias_threshold AND @today_bias > @prev_bias AND @today_bias < 0)
        BEGIN
            INSERT INTO @result (date, buy_or_sell, gb_rule, description)
            VALUES (@date, 1, 44, '法則4進階: 抄底反彈 - 負乖離開始縮小')
        END

        -- 法則 8 進階 (88): 在高度正乖離後，乖離率開始下降(但仍為正)時，產生賣出信號
        IF (@prev_bias > @positive_bias_threshold AND @today_bias < @prev_bias AND @today_bias > 0)
        BEGIN
            INSERT INTO @result (date, buy_or_sell, gb_rule, description)
            VALUES (@date, -1, 88, '法則8進階: 反轉回落 - 正乖離開始縮小')
        END

        -- 法則 4 基本: 抄底 - 下降趨勢且價格大幅低於均線 (負乖離過大)
        IF (@trend = -1 AND @today_bias < @negative_bias_threshold)
        BEGIN
            INSERT INTO @result (date, buy_or_sell, gb_rule, description)
            VALUES (@date, 1, 4, '法則4: 抄底 - 股價大幅低於均線且處於下降趨勢')
        END

        -- 法則 8 基本: 反轉 - 上升趨勢且價格大幅高於均線 (正乖離過大)
        IF (@trend = 1 AND @today_bias > @positive_bias_threshold)
        BEGIN
            INSERT INTO @result (date, buy_or_sell, gb_rule, description)
            VALUES (@date, -1, 8, '法則8: 反轉 - 股價大幅高於均線且處於上升趨勢')
        END

        -- 於迴圈最後，將當天的數值紀錄起來，供隔天(下一筆)作為 @prev 變數使用
        SET @prev_bias = @today_bias
        SET @prev_trend = @trend

        FETCH NEXT FROM cur INTO @date, @today_c, @today_MA, @today_bias, @trend
    END

    CLOSE cur
    DEALLOCATE cur

    RETURN
END;