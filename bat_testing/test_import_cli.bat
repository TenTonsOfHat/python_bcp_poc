cd %~dp0
@REM bcp "SELECT TOP 5 * FROM CurvePoint cp WHERE cp.EffectiveToDateTime > DATEADD(DAY, DATEDIFF(day, 0, (dateadd(day, datediff(day, 0, DATEADD(DAY, -5, GETDATE())),0))),0) AND cp.IsActive = 1" queryout CurvePoint.bcp -c -S dev-sql1.gravitate.energy -U matt.rothmeyer -P "6FG5HP?ENomF5&bT" -d PE_Dev
bcp CurvePoint format nul -x -f -t CurvePoint_Format.xml -n  -S dev-sql1.gravitate.energy -U matt.rothmeyer -P "6FG5HP?ENomF5&bT" -d PE_Dev
@REM bcp PE_local.dbo.CurvePoint IN "CurvePoint.bcp" -t -S localhost -b 10000 -U sa -P Atleast8 -F 1 -f CurvePoint_Format.xml
