SELECT
	count(*)
FROM
	dimpayer p
WHERE
	p."Is Active" = TRUE
	AND p."Is Demo" = FALSE
	AND p."Source System" = 'hha' --133
	

SELECT
	count(*)
FROM
	dimuser p
WHERE
	p."Source System" = 'hha' AND p."Is Support User" = FALSE AND p."Vendor Type" = 'Payer' AND p."Username" IS NOT NULL --15755


SELECT
	count(*)
FROM
	DIMPATIENTADDRESS d
WHERE
	d."Source System" = 'hha'
	AND d."Is Using Google API" = TRUE
	AND "County" = 'ROCKLAND' AND d."Source System" = 'hha' --463


SELECT
	count(*)
FROM
	dimpatient p
WHERE
	p."Has Visit" = TRUE
	AND p."Status" = 'Active'
	AND p."Source System" = 'hha'
	AND p."Is Authorized" = TRUE
	AND p."Is Payer Created Patient" = TRUE
	AND p."Source System" = 'hha'
	AND p."Updated Datatimestamp" >= DATEADD(DAY, -30, CURRENT_DATE())
	--104551
	
SELECT count(*) FROM DIMCAREGIVER d WHERE  d."Registry Is Checked" = TRUE AND d."Source System" = 'hha'  AND d."Updated Datatimestamp" >= DATEADD(day, -30, CURRENT_DATE())  --43762

SELECT count(*) FROM DIMCONTRACT d WHERE d."Source System" = 'hha' AND d."Updated Datatimestamp" >= DATEADD(day, -30, CURRENT_DATE()) --13464

SELECT count(*) FROM DIMOFFICE d WHERE d."Source System" = 'hha' AND d."Updated Datatimestamp" >= DATEADD(day, -30, CURRENT_DATE()) --3302

SELECT count(*) FROM DIMPAYERPROVIDER d --66005

SELECT count(*) FROM DIMPROVIDER d WHERE d."Source System" = 'hha' AND d."Updated Datatimestamp" >= DATEADD(day, -30, CURRENT_DATE())--1351

SELECT count(*) FROM DIMSERVICECODE d WHERE d."Source System" = 'hha' AND d."Updated Datatimestamp" >= DATEADD(day, -30, CURRENT_DATE()) --5967

SELECT count(*) FROM DIMUSEROFFICES d WHERE d."Source System" = 'hha' --286092

SELECT count(*) FROM FACTCAREGIVERABSENCE f WHERE f."Active" = TRUE AND f."Source System" = 'hha' and f."Updated Date" >= DATEADD(day, -30, CURRENT_DATE()) --101990

SELECT count(*) FROM FACTCAREGIVERINSERVICE f WHERE f."Source System" = 'hha' and f."Updated Date" >= DATEADD(day, -30, CURRENT_DATE())  --33319

SELECT count(*) FROM FACTVISITCALLPERFORMANCE_CR fc WHERE fc."Payer Id" = '042cb099-168b-4717-9bd0-936848b4fab1' AND fc."Visit Updated Timestamp" >= DATEADD(day, -30, CURRENT_DATE()) --10034

SELECT count(*) FROM FACTVISITCALLPERFORMANCE_DELETED_CR fc WHERE fc."Payer Id" = '042cb099-168b-4717-9bd0-936848b4fab1' AND fc."Visit Updated Timestamp" >= DATEADD(day, -30, CURRENT_DATE())  --493