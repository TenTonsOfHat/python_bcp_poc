cd %~dp0
bcp "SELECT * FROM CurvePoint cp WHERE cp.EffectiveToDateTime > DATEADD(DAY, DATEDIFF(day, 0, (dateadd(day, datediff(day, 0, DATEADD(DAY, -5, GETDATE())),0))),0) AND cp.IsActive = 1;" queryout CurvePoint.Native.bcp -n -d PE_Dev -S dev-sql1.gravitate.energy -U matt.rothmeyer -P "6FG5HP?ENomF5&bT" 
bcp CurvePoint IN "CurvePoint.Native.bcp" -n -S localhost -b 10000 -U sa -P Atleast8 -d PE_local -E
