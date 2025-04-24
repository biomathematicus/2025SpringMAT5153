-- FUNCTION: MODELING.spGetDailyCaseCount(character varying, bigint, date, date, bigint)

-- DROP FUNCTION "MODELING"."spGetDailyCaseCount"(character varying, bigint, date, date, bigint);

CREATE OR REPLACE FUNCTION "MODELING"."spGetDailyCaseCount"(
	slocation character varying,
	nimportexperimentid bigint,
	dtdatestart date,
	dtdateend date,
	nidvariable bigint)
    RETURNS TABLE(dtdate date, amvalue double precision) 
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
    ROWS 1000 
    
AS $BODY$
/*
	Procedure : spGetDailyCaseCount 
	Description: Returns case data for a particular date range. 
	Input : 	sLocation = Location of interest.
				nImportExperimentID = The ID of the dataset of interest
				dtDateStart = Starting date for the range of interest.
				dtDateEnd = Ending date for the range of interest.			
	Output : 	Table with number of cases over a particular time span.
	Author : 	Biomathematics Research Group at UTSA
				Samuel Roberts, samuel.roberts@utsa.edu
				Yunus A. Abdussalam, yunus.abdussalam@utsa.edu
				Juan B Gutierrez, juan.gutierrez3@utsa.edu
	Change log:	Change log : Modifed the code to handle various input cases for the ID run.
				Modified the code to reflect changes to Epidata table.
				Modified code to take nImportExperimentID
				2020-11-24: Added nIDVariable as input. Changed from temp table to direct query. JBG
				12/30/2020: Corrected PK-FK relationships to know data import used in ModelRun. JBG
	Example :
				select * from "MODELING"."spGetDailyCaseCount"('45001', 61, '2020-01-01', '2020-08-17',1);
*/
begin

	return query
	SELECT dt_value, am_value
		FROM   	"MODELING"."EpiData" E
		join 	"MODELING"."ModelRun" M on M.id_run = E.id_run 
		where  	M.id_import = nImportExperimentID
				and M.cd_location = sLocation
				and E.dt_value BETWEEN dtDateStart AND dtDateEnd
				and E.am_value > 0
				and E.id_variable = nIDVariable
	order by 	E.dt_value;

end;
$BODY$;

ALTER FUNCTION "MODELING"."spGetDailyCaseCount"(character varying, bigint, date, date, bigint)
    OWNER TO postgres;
