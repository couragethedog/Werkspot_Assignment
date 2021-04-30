--	SYNTAX: BigQuery
--  	Create Dimension Tables First
--	Step 1: Split the Metadata Column with underscore as Delimiter to get service related information
--	Note: 'qp-q-looker-2020-04' is DB and 'Professionals_Base' is the name of the dataset here

CREATE TABLE  Professionals_Base.SplitTable2 AS (
    SELECT SPLIT (meta_data,'_') AS SplitArray
    FROM `qp-q-looker-2020-04.Professionals_Base.EventLog`
    WHERE meta_data IS NOT NULL 
)
-------------------------------------------------------------------------------
--	SPLIT Function in Big Query returns an array of values on the same index,
--	Step 2: Create a TABLE to store array offsets as different columns 


CREATE TABLE Professionals_Base.Output0 AS 
WITH items AS (
SELECT SplitArray AS p FROM Professionals_Base.SplitTable2 
)
SELECT
    struct(
       items.p[offset(0)] AS service_id, 
       items.p[offset(1)] AS service_name_nl,
       items.p[offset(2)] AS service_name_en
      ) item  FROM items

-------------------------------------------------------------------------------
--	As we have same combination of service_id, service_name_nl and service_name_en for multiple rows with different lead_fee
--	To Acheive No Redundancy, Keep only UNIQUE Combinations in the Final Dimension Table 
--	Dim_Service

CREATE TABLE Professionals_Base.Dim_Service AS
SELECT DISTINCT item. service_id, item.service_name_nl, item.service_name_en FROM `qp-q-looker-2020-04.Professionals_Base.Output0`	  

-------------------------------------------------------------------------------
-- Fact
CREATE TABLE Professionals_Base.Fact AS 
SELECT 
event_id,
event_type,
professional_id_anonymized,
created_at,
SPLIT(meta_data ,'_')[OFFSET (0)] AS Service_ID,
SPLIT(meta_data ,'_')[OFFSET (3)] AS Lead_fee
FROM `qp-q-looker-2020-04.Professionals_Base.EventLog`	  

-------------------------------------------------------------------------------	  
	  
