-- FUNCTION: MODELING.spGetDailyCaseCount(character varying, bigint, date, date, bigint)

-- DROP FUNCTION "crdc_import"."GetEnrollmentVariables"(varValue character varying, varCount integer);

CREATE OR REPLACE FUNCTION "crdc_import"."GetEnrollmentVariables"()
    RETURNS TABLE(
		varValue character varying,
		varCount bigint
	) 
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
    ROWS 1000 
    
AS $BODY$
/*
	Function	: funGetClassroomCountsBySchool 
	Description	: Returns case data for a particular date range. 
	Input : 	sLocation = Location of interest.
				nImportExperimentID = The ID of the dataset of interest
				dtDateStart = Starting date for the range of interest.
				dtDateEnd = Ending date for the range of interest.			
	Output : 	Table with number of cases over a particular time span.
	Author : 	
	Change log:	
	Example :
				select * from "crdc_import"."GetEnrollmentVariables"();
*/
begin

	return query
	select 	cast(tot_enr_m as character varying) as varValue, 
			count(*) as varCount
	from 	crdc_import.enrollment 
	group by tot_enr_m
	order by tot_enr_m;	


end;
$BODY$;

-- ALTER FUNCTION "MODELING"."spGetDailyCaseCount"(character varying, bigint, date, date, bigint)    OWNER TO postgres;
