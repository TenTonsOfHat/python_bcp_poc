DECLARE @DaysBack INT = 5	
DECLARE @ExplicitCutoffDate DATETIME = DATEADD(DAY, DATEDIFF(day, 0, (dateadd(day, datediff(day, 0, DATEADD(DAY, -@DaysBack, GETDATE())),0))),0)
SELECT * FROM CurvePoint cp WHERE cp.EffectiveToDateTime > @ExplicitCutoffDate AND cp.IsActive = 1