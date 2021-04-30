--	SYNTAX: Microsolft SQL Server

CREATE FUNCTION GET_ACTIVE_COUNT_FOR_THE_DAY (@InputDate datetime)
RETURNS int AS
BEGIN
    RETURN (
		SELECT COUNT(DISTINCT a.professional_id_anonymized)
		FROM 
		event_log a
		FULL OUTER  JOIN (
			SELECT DISTINCT professional_id_anonymized, CASE WHEN (
				SELECT Top 1 event_type FROM event_log 
                WHERE created_at <= DATEADD(minute, -1, DATEADD(Day, 1, @InputDate)) 
                AND professional_id_anonymized = ev.professional_id_anonymized  
                ORDER BY created_at DESC
			) IN ( 'became_able_to_propose', 'proposed')
			THEN 'active'
			END AS event_status
			FROM event_log ev
		) b
		ON a.professional_id_anonymized = b.professional_id_anonymized
		WHERE b.event_status = 'active'
)
END
GO

DECLARE  @CurrentDate datetime
         ,@End  datetime

SET @CurrentDate = (SELECT CONVERT(date, MIN(created_at)) FROM event_log);
SET @END = (SELECT CONVERT(date, MAX(created_at)) FROM event_log);

WITH result(entry_date, active_count) AS (
     SELECT @CurrentDate 'date', dbo.GET_ACTIVE_COUNT_FOR_THE_DAY (@CurrentDate)
     UNION ALL
     SELECT DATEADD(Day, 1, t.entry_date), dbo.GET_ACTIVE_COUNT_FOR_THE_DAY (DATEADD(Day, 1, t.entry_date))
       FROM result t
      WHERE DATEADD(Day, 1, t.entry_date) <= @End
 )
 SELECT * FROM result;

 DROP FUNCTION dbo.GET_ACTIVE_COUNT_FOR_THE_DAY;