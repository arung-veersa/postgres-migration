SELECT
	count(*)
FROM
	dimpayer p
WHERE
	p."Is Active" = TRUE
	AND p."Is Demo" = FALSE
	AND p."Source System" = 'hha' --160
	

SELECT
	count(*)
FROM
	dimuser p
WHERE
	p."Source System" = 'hha' AND p."Is Support User" = FALSE AND p."Username" IS NOT NULL --352689


SELECT
	count(*)
FROM
	DIMPATIENTADDRESS d
WHERE
	d."Source System" = 'hha' --9275671


SELECT
	count(*)
FROM
	dimpatient p
WHERE
	p."Has Visit" = TRUE
	AND p."Status" = 'Active'
	AND p."Source System" = 'hha' -- 989119
	
SELECT count(*) FROM DIMCAREGIVER d WHERE d."Source System" = 'hha' --6317857

SELECT count(*) FROM DIMCONTRACT d WHERE d."Is Active" = TRUE and d."Source System" = 'hha' --67958

SELECT count(*) FROM DIMOFFICE d WHERE d."Is Active" = TRUE and d."Source System" = 'hha' --12591

SELECT count(*) FROM DIMPAYERPROVIDER d WHERE d."Source System" = 'hha' --111373

SELECT count(*) FROM DIMPROVIDER d WHERE d."Is Active" = TRUE AND d."Is Demo" = FALSE AND d."Source System" = 'hha' --4097

SELECT count(*) FROM DIMSERVICECODE d WHERE d."Source System" = 'hha' AND d."Is Active" = true --1771893

SELECT count(*) FROM DIMUSEROFFICES d WHERE d."Source System" = 'hha' --409002

SELECT count(*) FROM FACTCAREGIVERABSENCE f WHERE f."Active" = TRUE AND f."Source System" = 'hha' --8248366

SELECT count(*) FROM FACTCAREGIVERINSERVICE f WHERE f."Source System" = 'hha'  --12645499

SELECT count(*) FROM FACTVISITCALLPERFORMANCE_CR fc WHERE fc."External Source" = 'HHAX' AND fc."Permanent Deleted" = FALSE  --271840210

SELECT count(*) FROM FACTVISITCALLPERFORMANCE_DELETED_CR fc WHERE fc."External Source" = 'HHAX' AND fc."Permanent Deleted" = TRUE --25420266