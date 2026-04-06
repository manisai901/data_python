-- SQL  File Name: ETL_60_15_01_FOTC_RWA_FACILITY_INTRA.sql

CREATE TEMPORARY FUNCTION
  FOTC_UDF_Convert_To_Usd(In_currency STRING,
    DMDS ARRAY<STRUCT<source_currency STRING,
    target_currency STRING,
    exchange_rate FLOAT64>>) AS ((
    SELECT
      exchange_rate
    FROM
      UNNEST(DMDS) source_curr
    WHERE
      source_curr.source_currency = In_currency));

WITH RWA_RD_SITE_ATTRIBUTES AS (
  SELECT
    Site_Id AS Site_Id,
    Parameter AS Parameter,
    Value AS Value,
    Regulator AS Regulator
  FROM
    [$target_dataset].FOTC_RD_SITE_ATTRIBUTES ),
	GRCA_CODES AS (SELECT DISTINCT Inclusion_Flag,Site_ID,GRCA_Code
FROM  [$target_dataset].FOTC_RD_CALC_GRCA_CODES),
FOTC_RD_SITES_UNDRAWN_CASCADE AS ( -- FINOTC-18444
  SELECT
    Site_Id AS Site_Id,
    New_Cascade_Site_Flag AS New_Cascade_Site_Flag
  FROM
    [$target_dataset].FOTC_RD_SITES_UNDRAWN_CASCADE ),

fotc_relationship_detail_intra AS	-- FINOTC-18444
(
	SELECT
		grca_entity_identifier,
		credit_facility_identifier,
		relationship_role,
		COUNT(involved_party_identifier) AS involved_party_identifier

	FROM [$target_dataset].FOTC_RELATIONSHIP_DETAIL_INTRA

	group by
		grca_entity_identifier,
		credit_facility_identifier,
		relationship_role
),

CG AS
(
SELECT
Customer_Group,
Customer_Group_Desc,
Customer_Sub_Group2_fallback

FROM [$target_dataset].FOTC_RD_CG
),

CSG2_VALIDATION AS
(
	SELECT
		Customer_Sub_Group_Level2_Code
	FROM [$target_dataset].FOTC_RD_CSG2_GROUP_VAL
),

SRC_TO_INT AS (

SELECT
Source_Type,
Parameter,
Input_Value,
Output_Value

FROM
[$target_dataset].FOTC_RD_SRC_TO_INT_MAPPING

WHERE CURRENT_TIMESTAMP() >= from_date AND CURRENT_TIMESTAMP() <= to_date
AND Source_Type = "SDI"

),

Cost_Centre_to_Customer_Mapping AS

(
SELECT
Cost_Centre,
Customer_Group,
Customer_Sub_Group_Level_1,
Customer_Sub_Group_Level_2

FROM [$target_dataset].FOTC_RD_COST_CENTRE_TO_CG

WHERE CURRENT_TIMESTAMP() >= from_date AND CURRENT_TIMESTAMP() <= to_date
),

Asset_Liability_GRCA_Code AS
(
SELECT * FROM [$target_dataset].FOTC_RD_COMMON_GRCA_CODES
WHERE CURRENT_TIMESTAMP() >= from_date AND CURRENT_TIMESTAMP() <= to_date  ---FINOTC-289784
),

 FOTC_INTRA_INTRIM_00100 AS
(
SELECT *,
--Moved code from INTERIM SQL to here by Avinash

	CASE WHEN Rwa_Run_Type = "ADJ" /*and local_credit_facility_identifier LIKE 'GL_REC%'*/ -- 04/Feb/2021 FINOTC-12385
	THEN Undrawn_Balance_in_Actual_Currency_USD_Interim
	WHEN  Undrawn_Balance_in_Actual_Currency_USD_Interim < 0 THEN 0
	ELSE Undrawn_Balance_in_Actual_Currency_USD_Interim
	END AS
  Undrawn_Balance_in_Actual_Currency_USD_Interim_GL_Recon,


	CASE
		WHEN Expiry_Date_Facility IS NOT NULL and
		Expiry_Date_Facility  >= Reporting_Date
           then Expiry_Date_Facility
		WHEN  Next_Review_Date  IS NOT NULL then Next_Review_Date
	END AS Facility_End_Date,

	CASE
		WHEN Expiry_Date_Facility IS NOT NULL and
			Expiry_Date_Facility  >= Reporting_Date
           then "Expiry_Date_Facility"
			WHEN Next_Review_Date  IS NOT NULL
               then "Next_Review_Date"
			  END AS Facility_End_Date_Source,



	SAFE_CAST(CARM_Unweighted_Limit_USD / GBP_TO_USD_EXCH_RATE  AS FLOAT64) AS CARM_Unweighted_Limit_GBP,
	  COALESCE(grca_Entity_Identifier,
	    CARM_Site_Saracen_ID) AS Legal_Entity_RWA,

    COALESCE(cost_centre_number,
      CARM_Organisational_Unit) AS Cost_Centre_RWA,
    CASE
      WHEN Site_Eligible_For_UK_Ret_SME_Reclass_Flag = 'Y' THEN CONCAT(SAFE_CAST(CARM_Credit_Proposal_Serial_Number AS STRING),"_",CARM_Relationship_Identifier)
  END
    AS UK_Ret_SME_Agg_ID,
    CASE
      WHEN Parent_Facility_Flag ="N" AND Facility_Committed_Indicator_RWA = "Y" THEN "Y"
    ELSE
    "N"
  END
    AS Leaf_Facility_Flag_Interim,

    CASE
	  WHEN Coalesce(unconditionally_cancellable_indicator, CARM_Unconditionally_Cancellable_Indicator) = "Y" THEN "Y"
      WHEN Coalesce(unconditionally_cancellable_indicator,CARM_Unconditionally_Cancellable_Indicator) = "N" THEN "N"
      WHEN Coalesce(unconditionally_cancellable_indicator, CARM_Unconditionally_Cancellable_Indicator) = "1" THEN "Y"
      WHEN Coalesce(unconditionally_cancellable_indicator,CARM_Unconditionally_Cancellable_Indicator) = "0" THEN "N"
	  -- WHEN Unconditionally_Cancellable_Indicator_Fallback_Flag = "Y" THEN "Y"   --FINOTC-171149
  END
    AS Unconditionally_Cancellable_Indicator_RWA,

    CASE
      WHEN Facility_Type_Code_Fallback_Flag ="Y" THEN Facility_Type_Code_Fallback
    ELSE
    Facility_Type_code_Group
  END
    AS Facility_Type_Code_RWA,
	---end of code movement

	CASE WHEN UNDRAWN_CASCADE.New_Cascade_Site_Flag IS NOT NULL
		 THEN UNDRAWN_CASCADE.New_Cascade_Site_Flag
		 ELSE "N"
	END AS Undrawn_Cascade_Process_Flag ,-- FINOTC-18444

	CASE WHEN Undrawn_Balance_in_Actual_Currency_USD_Interim IS NOT NULL
	and undrawn_commitment_grca_reconciliation_key IS NULL  --FINOTC-118464 Defect
	THEN "GRCA MISSING"
	     WHEN Asset_Liability_GRCA_Code_JOIN.B_S_Control IS NOT NULL THEN UPPER(Asset_Liability_GRCA_Code_JOIN.B_S_Control)
		 WHEN undrawn_commitment_grca_reconciliation_key Like  "A%" OR undrawn_commitment_grca_reconciliation_key Like "M%" THEN "ASSET"
		 WHEN Undrawn_Balance_in_Actual_Currency_USD_Interim < 0 THEN "LIABILITY"
	     ELSE "ASSET"
	END AS undrawn_commitment_grca_reconciliation_key_BS_Type_Flag --FINOTC-59410

FROM [$target_dataset].FOTC_RWA_FACILITY_INTRA_INTERIM

	LEFT OUTER JOIN FOTC_RD_SITES_UNDRAWN_CASCADE AS UNDRAWN_CASCADE -- FINOTC-18444
		ON UNDRAWN_CASCADE.Site_Id = FOTC_RWA_FACILITY_INTRA_INTERIM.grca_entity_identifier

	LEFT OUTER JOIN Asset_Liability_GRCA_Code Asset_Liability_GRCA_Code_JOIN --FINOTC-59410
	    ON Asset_Liability_GRCA_Code_JOIN.GRCA_CODE =undrawn_commitment_grca_reconciliation_key

),
DMDS AS (
  SELECT
    CCY_CODE AS source_currency,
    EXCH_CCY AS target_Currency,
    EXCH_RATE AS exchange_rate

  FROM
    [$target_dataset].DMDS_USD_FX_RATES ),
SUM_CARM_Unweighted_Limit_BY_UK_Ret_SME_Agg_ID AS
(
SELECT UK_Ret_SME_Agg_ID,
       SUM(SAFE_CAST(CARM_Unweighted_Limit_GBP AS FLOAT64)) AS SUM_LIMIT

FROM  FOTC_INTRA_INTRIM_00100   --Avinash changed as part of SPLIT of INTERIM

WHERE  Site_Eligible_For_UK_Ret_SME_Reclass_Flag = 'Y' AND CARM_Ultimate_Parent_Facility_Flag = 'Y'
	         AND Cat_B_S_Facility_Flag ="N"

GROUP BY UK_Ret_SME_Agg_ID
),

--FINOTC-58708: Added this Table Derivation
SUM_MAX_USD_Interim_GL_Recon_BY_credit_facility AS (
select undrawn_commitment_grca_reconciliation_key,
SUM(SAFE_CAST(Undrawn_Balance_in_Actual_Currency_USD_Interim_GL_Recon AS FLOAT64)) as Sum_Undrawn_Balance_in_Actual_Currency_USD_On_GL_Key,
MAX(abs(Undrawn_Balance_in_Actual_Currency_USD_Interim_GL_Recon)) as Max_Undrawn_Balance_in_Actual_Currency_USD_On_GL_Key

from FOTC_INTRA_INTRIM_00100

where credit_facility_system_identifier like "ADJ_RECON%" and Batch_Run_Type IN ("FOTC") and
undrawn_commitment_grca_reconciliation_key_BS_Type_Flag = 'ASSET' --FINOTC-59410

group by undrawn_commitment_grca_reconciliation_key
),
--FINOTC-58708: Added this Table Derivation
MAX_NUM as (
select max(num) as max_row_num, Undrawn_Balance_in_Actual_Currency_USD_Interim_GL_Recon from (
select Max_Undrawn_Balance_in_Actual_Currency_USD_On_GL_Key,Undrawn_Balance_in_Actual_Currency_USD_Interim_GL_Recon,
ROW_NUMBER() OVER(PARTITION BY SAFE_CAST(Undrawn_Balance_in_Actual_Currency_USD_Interim_GL_Recon as numeric) ORDER BY Undrawn_Balance_in_Actual_Currency_USD_Interim_GL_Recon ASC)  AS num

from FOTC_INTRA_INTRIM_00100 INTRIM_00100

left join SUM_MAX_USD_Interim_GL_Recon_BY_credit_facility SUM_MAX

ON SUM_MAX.undrawn_commitment_grca_reconciliation_key = INTRIM_00100.undrawn_commitment_grca_reconciliation_key
AND undrawn_commitment_grca_reconciliation_key_BS_Type_Flag = 'ASSET' --FINOTC-59410

where Undrawn_Balance_in_Actual_Currency_USD_Interim_GL_Recon = Max_Undrawn_Balance_in_Actual_Currency_USD_On_GL_Key

)yvs group by Undrawn_Balance_in_Actual_Currency_USD_Interim_GL_Recon
),


FOTC_INTRA_INTRIM_00200 AS
(
SELECT  CDM_CREDIT_REPAIR.*,
CASE
      WHEN (COALESCE(CDM_CREDIT_REPAIR.Facility_Type_Code_RWA, "NA")) IN ("FNIH", "FNII", "FNIT") THEN "N"
      ELSE "Y"
  END
    AS Is_NNeg_Facility_Flag,
--FINOTC-236758 and FINOTC-183192, swap the value under these 2 conditions as per the correction mail.
CASE WHEN  COALESCE(CDM_CREDIT_REPAIR.advised_indicator_SDI,CDM_CREDIT_REPAIR.CARM_Advised_Indicator)  = "0"
          Then   "N"
          WHEN COALESCE(CDM_CREDIT_REPAIR.advised_indicator_SDI,CDM_CREDIT_REPAIR.CARM_Advised_Indicator)   =  "1"
          Then   "Y"
Else COALESCE(CDM_CREDIT_REPAIR.advised_indicator_SDI,CDM_CREDIT_REPAIR.CARM_Advised_Indicator) END AS Advised_Indicator_RWA,

SUM_CARM_Unweighted_Limit_BY_UK_Ret_SME_Agg_ID.SUM_LIMIT AS Total_Limit_GBP_for_UK_Retail_SME_Check,
Cost_Centre_to_Customer_Mapping.Customer_Group AS Customer_Group_Mapping,
Cost_Centre_to_Customer_Mapping.Customer_Sub_Group_Level_2 AS Customer_Sub_Group_Level_2_Mapping,
Cost_Centre_to_Customer_Mapping.Customer_Sub_Group_Level_1 AS Customer_Sub_Group_Level_1_Mapping,

CASE WHEN RWA_RD_SITE_ATTRIBUTES.Value IS NOT NULL THEN "Y"
ELSE "N"
END AS Private_Banking_Site_Flag,

CASE WHEN (COALESCE(fotc_relationship_detail_intra.involved_party_identifier,0.0) > 1)
	 THEN "Y"
	 ELSE "N"
END AS Borrowing_Group_Facility, --FINOTC-18444

----------------------------------------------------
Sum_Undrawn_Balance_in_Actual_Currency_USD_On_GL_Key,		--FINOTC-58708
Max_Undrawn_Balance_in_Actual_Currency_USD_On_GL_Key,		--FINOTC-58708

CASE
when
     credit_facility_system_identifier like "ADJ_RECON%" and Batch_Run_Type   IN ("FOTC")
Then
  CASE
	when
	Max_Undrawn_Balance_in_Actual_Currency_USD_On_GL_Key = abs(CDM_CREDIT_REPAIR.Undrawn_Balance_in_Actual_Currency_USD_Interim_GL_Recon)
	   and undrawn_commitment_grca_reconciliation_key_BS_Type_Flag = 'ASSET' --FINOTC-59410
	THEN
          case when
		  ROW_NUMBER() OVER(PARTITION BY SAFE_CAST(CDM_CREDIT_REPAIR.Undrawn_Balance_in_Actual_Currency_USD_Interim_GL_Recon as numeric) ORDER BY CDM_CREDIT_REPAIR.Undrawn_Balance_in_Actual_Currency_USD_Interim_GL_Recon ASC) = max_row_num
          Then "Y" else "N"
		  end
    else "N"
	end
else "NA"
end as Valid_rows_for_Undrawn_Balance_in_Actual_Currency_USD_GL_Recon,       --FINOTC-58708 : Added this case statement
CASE WHEN SITE_ATTRIBUTE.Value IS NOT NULL AND  SITE_ATTRIBUTE.Value= "Y" THEN  "Y" Else "N" END Undrawn_Commitment_GRCA_Exclusion_Not_To_Be_Applied, --FINOTC-110826
CASE WHEN SITE_ATTRIBUTE1.Value IS NOT NULL AND  SITE_ATTRIBUTE1.Value= "Y" THEN  "Y" Else "N" END SITE_Has_Own_GRCA_Exclusion_Codes_Flag,	--FINOTC-110826
CASE WHEN SITE_ATTRIBUTE2.Value IS NOT NULL AND  SITE_ATTRIBUTE2.Value= "Y" THEN  "Y" Else "N" END Sites_To_Unmask_PCIF_Flag	--FINOTC-139156, FINOTC-139809, FINOTC-147919

FROM    FOTC_INTRA_INTRIM_00100 CDM_CREDIT_REPAIR
	LEFT OUTER JOIN SUM_CARM_Unweighted_Limit_BY_UK_Ret_SME_Agg_ID
		ON SUM_CARM_Unweighted_Limit_BY_UK_Ret_SME_Agg_ID.UK_Ret_SME_Agg_ID =CDM_CREDIT_REPAIR.UK_Ret_SME_Agg_ID

	---------------------------------------------------------------------------------------
	LEFT OUTER JOIN Cost_Centre_to_Customer_Mapping ON
		CDM_CREDIT_REPAIR.Cost_Centre_RWA = Cost_Centre_to_Customer_Mapping.Cost_Centre

	LEFT OUTER JOIN RWA_RD_SITE_ATTRIBUTES ON
		CDM_CREDIT_REPAIR.grca_entity_identifier = RWA_RD_SITE_ATTRIBUTES.Site_ID		----FINOTC-14155
		AND RWA_RD_SITE_ATTRIBUTES.Parameter = "PRIVATE_BANKING_SITE"
	LEFT OUTER JOIN RWA_RD_SITE_ATTRIBUTES  SITE_ATTRIBUTE --FINOTC-110826
		ON 	SITE_ATTRIBUTE.SITE_ID=CDM_CREDIT_REPAIR.grca_entity_identifier
		AND SITE_ATTRIBUTE.Parameter = "GRCA_EXCLUSION_NOT_APPLIED"
	LEFT OUTER JOIN RWA_RD_SITE_ATTRIBUTES  SITE_ATTRIBUTE1 --FINOTC-110826
		ON 	SITE_ATTRIBUTE1.SITE_ID=CDM_CREDIT_REPAIR.grca_entity_identifier
		AND SITE_ATTRIBUTE1.Parameter = "SITE_HAS_OWN_GRCA_EXCLUSION_CODES_FLAG"
LEFT OUTER JOIN RWA_RD_SITE_ATTRIBUTES  SITE_ATTRIBUTE2 --FINOTC-110826, FINOTC-147919
		ON 	SITE_ATTRIBUTE2.SITE_ID=CDM_CREDIT_REPAIR.grca_entity_identifier
		AND SITE_ATTRIBUTE2.Parameter = "MASKED"

	LEFT OUTER JOIN fotc_relationship_detail_intra  -- FINOTC-18444
			ON
			fotc_relationship_detail_intra.grca_entity_identifier = CDM_CREDIT_REPAIR.grca_entity_identifier
			AND fotc_relationship_detail_intra.credit_facility_identifier = CDM_CREDIT_REPAIR.local_credit_facility_identifier
			AND fotc_relationship_detail_intra.relationship_role = "R_FAC_IP"

			--FINOTC-58708: Added this Left join
    LEFT OUTER JOIN SUM_MAX_USD_Interim_GL_Recon_BY_credit_facility
			ON SUM_MAX_USD_Interim_GL_Recon_BY_credit_facility.undrawn_commitment_grca_reconciliation_key =CDM_CREDIT_REPAIR.undrawn_commitment_grca_reconciliation_key
			--FINOTC-58708: Added this Left join
	LEFT OUTER JOIN max_num
	ON CDM_CREDIT_REPAIR.Undrawn_Balance_in_Actual_Currency_USD_Interim_GL_Recon=max_num.Undrawn_Balance_in_Actual_Currency_USD_Interim_GL_Recon
	AND undrawn_commitment_grca_reconciliation_key_BS_Type_Flag = 'ASSET' --FINOTC-59410

-----------------------------------------------------------------------------------------
),
FOTC_INTRA_INTRIM_00300 as
(
SELECT		CDM_CREDIT_REPAIR.*,
       CASE WHEN  CDM_CREDIT_REPAIR.Site_Eligible_For_UK_Ret_SME_Reclass_Flag= "Y"
      		 THEN  CASE WHEN  CDM_CREDIT_REPAIR.Total_Limit_GBP_for_UK_Retail_SME_Check > 0 AND CDM_CREDIT_REPAIR.Total_Limit_GBP_for_UK_Retail_SME_Check <= CDM_CREDIT_REPAIR.UK_Ret_SME_Limit_Threshold_GBP
                THEN "Y"
                ELSE "N" END
               ELSE "NA"
             END AS Limit_Eligible_For_UK_Ret_SME_Reclass_Flag,
		 Coalesce(CDM_CREDIT_REPAIR.Advised_Credit_Limit_USD,CDM_CREDIT_REPAIR.CARM_Unweighted_Approved_Limit_Amount_USD) AS Unweighted_Limit_RWA, --FINTOC-9407

       CASE WHEN CDM_CREDIT_REPAIR.Advised_Credit_Limit_USD is NOT NULL THEN "SDI"
            WHEN CDM_CREDIT_REPAIR.CARM_Unweighted_Approved_Limit_Amount_USD IS NOT NULL THEN "CARM" END AS Unweighted_Limit_Source,--FINOTC-9407
       CASE WHEN CDM_CREDIT_REPAIR.credit_facility_first_drawdown_date IS NOT NULL THEN "SDI"
            WHEN CDM_CREDIT_REPAIR.CARM_Application_Approval_Date is NOT NULL THEN "CARM" END AS Facility_Start_Date_Source,
       CASE WHEN CDM_CREDIT_REPAIR.governing_credit_facility_identifier IS NULL AND CDM_CREDIT_REPAIR.Parent_Facility_Flag ="N" THEN "Y" ELSE "N" END AS Standalone_Facility_Indicator,

-----------------------------------------------------------------------
CASE WHEN CDM_CREDIT_REPAIR.Batch_Run_Type = "FOTC" THEN
		  COALESCE(SRC_TO_INT.Output_Value,CDM_CREDIT_REPAIR.Customer_Group_Mapping)
	 ELSE CDM_CREDIT_REPAIR.Customer_Group_Mapping
END AS Customer_Group_Mapping_converted,

CASE WHEN CDM_CREDIT_REPAIR.Batch_Run_Type = "FOTC" THEN
     COALESCE(SRC_TO_INT_2.Output_Value,CDM_CREDIT_REPAIR.Customer_Sub_Group_Level_2_Mapping)
	 ELSE CDM_CREDIT_REPAIR.Customer_Sub_Group_Level_2_Mapping
END AS customer_sub_group_level_2_code_converted,

CASE WHEN CDM_CREDIT_REPAIR.Batch_Run_Type = "FOTC" THEN
          COALESCE(SRC_TO_INT_1.Output_Value, CDM_CREDIT_REPAIR.Customer_Sub_Group_Level_1_Mapping)
ELSE CDM_CREDIT_REPAIR.Customer_Sub_Group_Level_1_Mapping
END AS Customer_Sub_Group_Level_1_Code_Converted,

-----------------------------------------------------------------------
CASE
     When Valid_rows_for_Undrawn_Balance_in_Actual_Currency_USD_GL_Recon = "Y"
             Then Sum_Undrawn_Balance_in_Actual_Currency_USD_On_GL_Key
     When Valid_rows_for_Undrawn_Balance_in_Actual_Currency_USD_GL_Recon = "N"
             Then 0
     When Valid_rows_for_Undrawn_Balance_in_Actual_Currency_USD_GL_Recon = "NA"
             Then Undrawn_Balance_in_Actual_Currency_USD_Interim_GL_Recon
         else null
end as Undrawn_Balance_in_Actual_Currency_USD,                            --FINOTC-58708
CASE WHEN CDM_CREDIT_REPAIR.Undrawn_Commitment_GRCA_Exclusion_Not_To_Be_Applied = "Y"   THEN  "N"
	WHEN   CDM_CREDIT_REPAIR.SITE_Has_Own_GRCA_Exclusion_Codes_Flag ="Y" AND CDM_CREDIT_REPAIR.undrawn_commitment_grca_reconciliation_key <>" " AND CDM_CREDIT_REPAIR.undrawn_commitment_grca_reconciliation_key IS NOT NULL
		THEN
			(CASE WHEN GRCA_CODES.Inclusion_Flag IS NOT NULL and GRCA_CODES.Inclusion_Flag  IN (  "N" , " " ) THEN  "Y"
			Else
				(CASE WHEN GRCA_CODES.Inclusion_Flag IS  NULL and  CDM_CREDIT_REPAIR.undrawn_commitment_grca_reconciliation_key IS NOT NULL THEN "Y"   Else "N" END )
				END)
	WHEN  CDM_CREDIT_REPAIR.SITE_Has_Own_GRCA_Exclusion_Codes_Flag  <> "Y"  AND CDM_CREDIT_REPAIR.undrawn_commitment_grca_reconciliation_key <>" " AND  CDM_CREDIT_REPAIR.undrawn_commitment_grca_reconciliation_key IS NOT NULL
		THEN  (CASE WHEN GRCA_CODES.Inclusion_Flag IS NOT NULL and GRCA_CODES.Inclusion_Flag  IN (  "N" , " " ) THEN  "Y"
					WHEN GRCA_CODES1.Inclusion_Flag IS NOT NULL and GRCA_CODES1.Inclusion_Flag  IN (  "N" , " " ) THEN  "Y"
					WHEN GRCA_CODES1.Inclusion_Flag IS  NULL and  CDM_CREDIT_REPAIR.undrawn_commitment_grca_reconciliation_key IS NOT NULL THEN "Y"
					Else "N" END )
			END	  AS Undrawn_Commitment_GRCA_Exclusion_For_Undrawn_Cascade_Flag ,  --     FINOTC-145310

CASE WHEN CDM_CREDIT_REPAIR.Undrawn_Commitment_GRCA_Exclusion_Not_To_Be_Applied = "Y" THEN  "N"
 WHEN SITE_Has_Own_GRCA_Exclusion_Codes_Flag ="Y" AND undrawn_commitment_grca_reconciliation_key  <>" "  AND undrawn_commitment_grca_reconciliation_key IS NOT NULL
    THEN
			(CASE WHEN GRCA_CODES.Inclusion_Flag IS NOT NULL and GRCA_CODES.Inclusion_Flag  IN (  "N" , " " )  THEN "SITE_Has_Own_GRCA_Exclusion_Codes_Flag_AS_Y_EXCLUDED_BECAUSE_OF_INCLUSION_FLAG_FROM_REF_DATA_IS_N_OR_SPACE"
				Else
        (CASE WHEN GRCA_CODES.Inclusion_Flag IS  NULL and  CDM_CREDIT_REPAIR.undrawn_commitment_grca_reconciliation_key IS NOT NULL
        THEN  "SITE_Has_Own_GRCA_Exclusion_Codes_Flag_AS_Y_EXCLUDED_BECAUSE_OF_GRCA_KEY_NOT_PRESENT_IN_REF_DATA"
					Else "SITE_Has_Own_GRCA_Exclusion_Codes_Flag_AS_Y_NOT_TO_BE_EXCLUDED" END)
					END)

 Else
 	(CASE WHEN  CDM_CREDIT_REPAIR.SITE_Has_Own_GRCA_Exclusion_Codes_Flag  <> "Y"  AND CDM_CREDIT_REPAIR.undrawn_commitment_grca_reconciliation_key <>" " AND  CDM_CREDIT_REPAIR.undrawn_commitment_grca_reconciliation_key IS NOT NULL
      THEN
	  (CASE WHEN GRCA_CODES.Inclusion_Flag IS NOT NULL AND GRCA_CODES.Inclusion_Flag IN ("N","")
	  THEN "SITE_Has_Own_GRCA_Exclusion_Codes_Flag_AS_Y_EXCLUDED_BECAUSE_OF_INCLUSION_FLAG_FROM_REF_DATA_IS_N_OR_SPACE"
	 	  WHEN GRCA_CODES1.Inclusion_Flag IS NOT NULL and GRCA_CODES1.Inclusion_Flag  IN (  "N" , " " )
		  	THEN  "SITE_Has_Own_GRCA_Exclusion_Codes_Flag_AS_NOT_Y_EXCLUDED_BECAUSE_OF_INCLUSION_FLAG_FROM_REF_DATA_IS_N_OR_SPACE"
			Else
			  	(CASE WHEN GRCA_CODES1.Inclusion_Flag IS  NULL and  CDM_CREDIT_REPAIR.undrawn_commitment_grca_reconciliation_key IS NOT NULL
				  THEN "SITE_Has_Own_GRCA_Exclusion_Codes_Flag_AS_NOT_Y_EXCLUDED_BECAUSE_OF_GRCA_KEY_NOT_PRESENT_IN_REF_DATA"
          		  ELSE "SITE_Has_Own_GRCA_Exclusion_Codes_Flag_AS_NOT_Y_NOT_TO_BE_EXCLUDED"	END)
					END)
					 END)
          END AS Undrawn_Commitment_GRCA_Exclusion_For_Undrawn_Cascade_Flag_Indicator  --FINOTC-110826 --FINOTC-143388-V0.88

FROM FOTC_INTRA_INTRIM_00200 CDM_CREDIT_REPAIR
LEFT OUTER JOIN GRCA_CODES ON
UPPER(GRCA_CODES.GRCA_Code) = UPPER(CDM_CREDIT_REPAIR.undrawn_commitment_grca_reconciliation_key) AND GRCA_CODES.Site_ID = CDM_CREDIT_REPAIR.grca_entity_identifier --FINOTC-110826
LEFT OUTER JOIN GRCA_CODES GRCA_CODES1 ON
UPPER(GRCA_CODES1.GRCA_Code) = UPPER(CDM_CREDIT_REPAIR.undrawn_commitment_grca_reconciliation_key) AND GRCA_CODES1.Site_ID = "*"  --FINOTC-110826

LEFT OUTER JOIN SRC_TO_INT SRC_TO_INT
ON SRC_TO_INT.Source_Type = "SDI"
AND SRC_TO_INT.Parameter = "CG"
AND upper(CDM_CREDIT_REPAIR.Customer_Group_Mapping) = SRC_TO_INT.Input_Value

LEFT OUTER JOIN SRC_TO_INT SRC_TO_INT_2
ON SRC_TO_INT_2.Source_Type = "SDI"
AND SRC_TO_INT_2.Parameter = "CG2"
AND upper(CDM_CREDIT_REPAIR.Customer_Sub_Group_Level_2_Mapping) = SRC_TO_INT_2.Input_Value

LEFT OUTER JOIN SRC_TO_INT SRC_TO_INT_1
ON SRC_TO_INT_1.Source_Type = "SDI"
AND SRC_TO_INT_1.Parameter = "CG1"
AND upper(CDM_CREDIT_REPAIR.Customer_Sub_Group_Level_1_Mapping) = SRC_TO_INT_1.Input_Value
),
FOTC_INTRA_INTRIM_400 AS
(
SELECT FOTC_INTRA_INTRIM.*,

CASE WHEN   FOTC_INTRA_INTRIM.Standalone_Facility_Indicator="Y" THEN "LEAF_NODE_BECAUSE_OF_STANDALONE_FACILITY"
     WHEN   FOTC_INTRA_INTRIM.Parent_Facility_Flag = "N" and FOTC_INTRA_INTRIM.Facility_Committed_Indicator_RWA = "Y" THEN  "COMMITED_LEAF_NODE"
     WHEN   FOTC_INTRA_INTRIM.Parent_Facility_Flag = "N" and  FOTC_INTRA_INTRIM.Facility_Committed_Indicator_RWA ="N" THEN "NOT_A_LEAF_NODE_BECAUSE_OF_UNCOMMITED_CHILD"
	  WHEN   FOTC_INTRA_INTRIM.Parent_Facility_Flag = "Y" and  FOTC_INTRA_INTRIM.Facility_Committed_Indicator_RWA ="Y" THEN "LEAF_NODE_BECAUSE_OF_UNCOMMITTED_CHILD"
 Else "NOT_A_LEAF_NODE" END AS Leaf_Facility_Flag_Indicator,
--FINOTC-7623
 CASE WHEN FOTC_INTRA_INTRIM.Standalone_Facility_Indicator="Y" THEN "Y"
           WHEN FOTC_INTRA_INTRIM.Leaf_Facility_Flag_Interim ="Y" THEN "Y"
           WHEN FOTC_INTRA_INTRIM.Leaf_facility_Cos_Of_Non_Commited_Child_Facilities_Flag ="Y" THEN "Y"
      ELSE "N"
      END AS Leaf_Facility_Flag
	  	,FOTC_UDF_Convert_To_Usd(Functional_CCY.Local_Currency_code ,
      ARRAY(
      SELECT
        STRUCT(source_currency,
          target_Currency,
          exchange_rate)
      FROM
        DMDS)) AS Local_Currency_code_EXCH_RATE

		,Functional_CCY.Local_Currency_code as Functional_Currency_Code,

COALESCE(SRC_TO_INT.Output_Value,FOTC_INTRA_INTRIM.Customer_Group_Mapping_converted) AS Customer_Group_Intermediate,
COALESCE(SRC_TO_INT_2.Output_Value,FOTC_INTRA_INTRIM.customer_sub_group_level_2_code_converted) AS Customer_Sub_Group_Level_2_Intermediate

FROM FOTC_INTRA_INTRIM_00300 FOTC_INTRA_INTRIM

LEFT OUTER JOIN SRC_TO_INT SRC_TO_INT ON
SRC_TO_INT.Source_Type = "SDI"
AND SRC_TO_INT.Parameter = "CUSTOMER_GROUP"
AND upper(FOTC_INTRA_INTRIM.Customer_Group_Mapping_converted) = SRC_TO_INT.Input_Value

LEFT OUTER JOIN SRC_TO_INT SRC_TO_INT_2 ON
SRC_TO_INT_2.Source_Type = "SDI"
AND SRC_TO_INT_2.Parameter = "CUSTOMER_SUB_GROUP2"
AND upper(FOTC_INTRA_INTRIM.customer_sub_group_level_2_code_converted) = SRC_TO_INT_2.Input_Value

	LEFT JOIN [$target_dataset].FOTC_RD_REF_SITE_LOCAL_CCY Functional_CCY
ON FOTC_INTRA_INTRIM.grca_entity_identifier = Functional_CCY.Site_ID			----FINOTC-14155

),

FOTC_INTRA_INTRIM_00425 AS
(
SELECT FOTC_INTRA_INTRIM.*,

CASE WHEN FOTC_INTRA_INTRIM.Customer_Sub_Group_Level_2_Intermediate = "OT2" AND FOTC_INTRA_INTRIM.Private_Banking_Site_Flag= "Y"
THEN "BSMGPB"
ELSE  Customer_Sub_Group_Level_2_Intermediate
END AS Customer_sub_group_level_2

FROM FOTC_INTRA_INTRIM_400 FOTC_INTRA_INTRIM
),

FOTC_INTRA_INTRIM_00450 AS
(
SELECT FOTC_INTRA_INTRIM.*,

CASE WHEN FOTC_INTRA_INTRIM.Customer_Group_Intermediate = "CIBM" AND FOTC_INTRA_INTRIM.Customer_sub_group_level_2 = "BSMGBM"
THEN "CC"
ELSE FOTC_INTRA_INTRIM.Customer_Group_Intermediate
END AS Customer_Group,

CASE WHEN CSG2_VALIDATION.Customer_Sub_Group_Level2_Code IS NOT NULL THEN "Y"
ELSE "N"
END AS Customer_sub_group_level_2_Valid_Flag

FROM FOTC_INTRA_INTRIM_00425 FOTC_INTRA_INTRIM

LEFT OUTER JOIN CSG2_VALIDATION ON
upper(FOTC_INTRA_INTRIM.Customer_sub_group_level_2) = CSG2_VALIDATION.Customer_Sub_Group_Level2_Code
),


FOTC_INTRA_INTRIM_500 as (

	SELECT FOTC_INTRA_INTRIM.* ,
  CASE
    WHEN FOTC_INTRA_INTRIM.Local_Currency_code_EXCH_RATE > 1 THEN FOTC_INTRA_INTRIM.Advised_Credit_Limit_USD * FOTC_INTRA_INTRIM.Local_Currency_code_EXCH_RATE
    ELSE
	FOTC_INTRA_INTRIM.Advised_Credit_Limit_USD/Local_Currency_code_EXCH_RATE
END as Advised_Credit_Limit_Functional_Currency,

CASE
    WHEN FOTC_INTRA_INTRIM.Local_Currency_code_EXCH_RATE > 1 THEN FOTC_INTRA_INTRIM.Undrawn_Balance_in_Actual_Currency_USD * FOTC_INTRA_INTRIM.Local_Currency_code_EXCH_RATE
  ELSE
  FOTC_INTRA_INTRIM.Undrawn_Balance_in_Actual_Currency_USD/Local_Currency_code_EXCH_RATE
END as Undrawn_Balance_in_Functional_Currency_RWA

  ,CASE
    WHEN FOTC_INTRA_INTRIM.Local_Currency_code_EXCH_RATE > 1 THEN FOTC_INTRA_INTRIM.CARM_Unweighted_Approved_Limit_Amount_USD * FOTC_INTRA_INTRIM.Local_Currency_code_EXCH_RATE
  ELSE
  FOTC_INTRA_INTRIM.CARM_Unweighted_Approved_Limit_Amount_USD/FOTC_INTRA_INTRIM.Local_Currency_code_EXCH_RATE
END as CARM_Unweighted_Approved_Limit_Amount_Functional_Currency,

CASE WHEN CG.Customer_Group IS NOT NULL THEN "Y"
ELSE "N"
END AS Customer_Group_Valid_Flag,

CASE WHEN Borrowing_Group_Facility = "Y" AND Undrawn_Cascade_Process_Flag = "Y"
	 THEN "The_Cascade_Process_through_Borrowing_Group_and_New_Headroom_Allocation"
	 WHEN Borrowing_Group_Facility = "N" AND Undrawn_Cascade_Process_Flag = "Y"
	 THEN "The_Cascade_Process_through_New_Headroom_Allocation"
	 ELSE "The_Cascade_Process_through_Existing_Headroom_Allocation"

END AS Undrawn_Cascade_Process_Indicator -- FINOTC-18444


FROM FOTC_INTRA_INTRIM_00450 FOTC_INTRA_INTRIM

LEFT OUTER JOIN CG ON
upper(FOTC_INTRA_INTRIM.Customer_Group) = CG.Customer_Group


)
select
	Unique_Facility_ID,
	Advised_Credit_Limit_USD,
	Advised_Indicator_RWA,
	Batch_Run_Type,
	CARM_Advised_Indicator,
	CARM_Application_Approval_Date,
	CARM_Availability_Expiration_Date,
	CARM_Commited_Indicator,
	CARM_Credit_Proposal_Serial_Number,
	CARM_DLGD,
	CARM_Downturn_Loss_Given_Default_Percentage,
	CARM_ELGD,
	CARM_Facility_CCY_Code_Group,
	CARM_Facility_Currency_Code,
	CARM_Facility_Customer_ID,
	CARM_Facility_Expiry_Date,
	CARM_Facility_Expiry_Date_Valid_Flag,
	CARM_Facility_Scorecard,
	CARM_Facility_Type_Code_Group,
	CARM_Facility_Type_Code_Local,
	CARM_Forbearance_Indicator,
	CARM_GBCDU_Site_Code,
	CARM_Group_system_ID,
	CARM_Involved_Party_Alternative_ID_Type,
	CARM_LGD,
	CARM_LGD_Indicator,
	CARM_Limit_CCY,
	CARM_Next_Review_Date,
	CARM_Organisational_Unit,
	CARM_Overridden_Downturn_Loss_Given_Default_Percentage,
	CARM_Overridden_Loss_Given_Default_Override_Category_Code,
	CARM_Overridden_Loss_Given_Default_Percentage,
	CARM_Overridden_Regulatory_Specialised_Lending_PD,
	CARM_Overridden_Supervisory_Category,
	CARM_Parent_Facility_ID,
	CARM_Proposed_Loss_Given_Default_Percentage,
	CARM_Proposed_Regulatory_Specialised_Lending_PD,
	CARM_Proposed_Supervisory_Category,
	CARM_Regulatory_Specialised_Lending_Scorecard,
	CARM_Regulatory_Specialised_Lending_Type,
	CARM_Relationship_Identifier,
	CARM_Site_Saracen_ID,
	CARM_Supervisory_Category,
	CARM_Ultimate_Parent_Facility_Flag,
	CARM_Unconditionally_Cancellable_Indicator,
	CARM_Unweighted_Approved_Limit_Amount,
	CARM_Unweighted_Limit_GBP,
	Cat_B_S_Facility_Flag,
	Committed_Indicator_Fallback,
	Committed_Indicator_Fallback_Flag,
	Cost_Centre_RWA,
	Expiry_Date_Facility,
	Expiry_Date_Facility_Source,
	Facility_Classification,
	Facility_Committed_Indicator,
	Facility_Committed_Indicator_RWA,
	Facility_Expiry_Date_Valid_Flag,
	Facility_Start_Date,
	Facility_Start_Date_Source,
	Facility_Type_Code,
	Facility_Type_Code_Fallback,
	Facility_Type_Code_Group,
	Facility_Type_Code_Local,
	Facility_Type_Code_RWA,
	GBCDU_Site_Code_SDI,
	Group_System_ID,
	Is_NNeg_Facility_Flag,
	Leaf_facility_Cos_Of_Non_Commited_Child_Facilities_Flag,
	Leaf_Facility_Flag,
	Leaf_Facility_Flag_Indicator,
	Leaf_Facility_Flag_Interim,
	Legal_Entity_RWA,
	Limit_Eligible_For_UK_Ret_SME_Reclass_Flag,
	Next_Review_Date,
	Next_Review_Date_Source,
	Parent_Facility_Flag,
	PCIF_Identifier,
	Site_Eligible_For_UK_Ret_SME_Reclass_Flag,
	Source,
	Standalone_Facility_Indicator,
	Sys_Country_Code,
	Total_Limit_GBP_for_UK_Retail_SME_Check,
	UK_Ret_SME_Agg_ID,
	UK_Ret_SME_Limit_Threshold_GBP,
	Unconditionally_Cancellable_Indicator_RWA,
	Undrawn_Balance_in_Actual_Currency_USD,
	Undrawn_Balance_in_Actual_Currency_USD_Interim_GL_Recon,			--FINOTC-58708
	Undrawn_Balance_in_Actual_Currency_USD_Interim,
	Unweighted_Limit_RWA,
	Unweighted_Limit_Source,
	CARM_Unweighted_Approved_Limit_Amount_USD,
	credit_officer_identifier AS RWA_credit_officer_identifier,
	credit_facility_limit_next_review_date_Valid_Flag,
	CARM_Next_Review_Date_Valid_Flag,
	Facility_End_Date,
	Facility_End_Date_Source,
	Advised_Indicator_Validation_Flag,
	Advised_Indicator_Fallback,
	Advised_Indicator_SDI,
	SL_Grade_Description,
	Facility_Type,
	Facility_Limit_Lcl_Rep_Ccy,
	CRE_Type_Identifier,
	Aggregated_MIF_Flag,
	--newly added override fields
	CARM_Next_Review_Date_Pre_Override,
	CARM_Next_Review_Date_Override,
	CARM_Facility_Expiry_Date_Pre_Override,
	CARM_Facility_Expiry_Date_Override,
	CARM_Next_Review_Date_Override_Type_Indicator,
	CARM_Facility_Expiry_Date_Override_Type_Indicator
	--FINOTC-7623
	,Functional_Currency_Code
	,Advised_Credit_Limit_Functional_Currency
	,Undrawn_Balance_in_Functional_Currency_RWA
	,CARM_Unweighted_Approved_Limit_Amount_Functional_Currency,
	Adjustment_Flag,
	Customer_Group_Mapping_converted,
	Customer_Group_Intermediate,
	Customer_Group,
	Customer_Group_Valid_Flag,
	Customer_Sub_Group_Level_2_Mapping,
	customer_sub_group_level_2_code_converted,
	Customer_Sub_Group_Level_2_Intermediate,
	Customer_sub_group_level_2,
	Private_Banking_Site_Flag,
	Customer_sub_group_level_2_Valid_Flag,
	Customer_Sub_Group_Level_1_Mapping,
	Customer_Sub_Group_Level_1_Code_Converted,
	unweighted_utilisation_amount_USD,--5946
	--10330
	Arrears_Interest_Fees_Penalty_Payment_Capitalisation_Date,
	Refinancing_or_New_Credit_Facilities_Date,
	Interest_Only_Conversion_Date,
	Reduced_Payments_Date,
	Rescheduled_Payments_Date,
	Conversion_of_Currency_Date,
	Grace_Period_or_Payment_Moratorium_Date,
	Sales_by_Agreement_Date,
	Debt_Consolidation_Date,
	Debt_for_Equity_Swaps_Date,
	Reduction_In_Margin_Date,
	Principle_Payment_Forgiveness_Date,
	Interest_Fees_Penalty_Date,
	Repayment_Profile_Date,
	Other_Payment_Date,
	Covenaint_Waiver_Date,
	Restructuring_of_Collaterals_Date,
	Other_Non_Payment_Date,
	CARM_L_Credit_Proposal_Serial_Number,
	CARM_L_Forebearance_Flag,
	CARM_Date_of_Forborne,
	CARM_Date_of_First_Forborne,
	CARM_Number_of_Forbearance_Measures,--10330
	Facility_Type_Code_Fallback_Flag, --FINOTC-25842,
	RWA_Secured_Indicator,  ------FINOTC-31583
	End_of_period_balance_transaction_currency, ------FINOTC-31583
	Master_Category_A_Exposure_Limit,    ------FINOTC-31583
	Utilisation,-------FINOTC-31583
	Utilisation_Indicator,------FINOTC-31583
	Secured_Unsecured_Indicator, ------FINOTC-31583
	Undrawn_Cascade_Process_Flag, -- FINOTC-18444
	Borrowing_Group_Facility, -- FINOTC-18444
	Undrawn_Cascade_Process_Indicator, -- FINOTC-18444
	Local_CARM_Overridden_Loss_Given_Default_Override_Category_Code, --FINOTC-54280
	Local_CARM_Overridden_Downturn_Loss_Given_Default_Percentage,	 --FINOTC-54280
	Local_CARM_Downturn_Loss_Given_Default_Percentage,				 --FINOTC-54280
	Local_CARM_Overridden_Loss_Given_Default_Percentage,			 --FINOTC-54280
	Local_CARM_Proposed_Loss_Given_Default_Percentage,				 --FINOTC-54280
	Local_CARM_DLGD,			--FINOTC-54280
	Local_CARM_ELGD,			--FINOTC-54280
	Local_CARM_LGD,				--FINOTC-54280
	Local_CARM_LGD_Indicator,	--FINOTC-54280
	Sum_Undrawn_Balance_in_Actual_Currency_USD_On_GL_Key,	--FINOTC-58708
	Max_Undrawn_Balance_in_Actual_Currency_USD_On_GL_Key,	--FINOTC-58708
	Valid_rows_for_Undrawn_Balance_in_Actual_Currency_USD_GL_Recon,  --FINOTC-58708
	ADC_Indicator,  --FINOTC-60737
	Level_of_ADC_Pre_Sale_Or_Pre_Lease_Contracts_Value, --FINOTC-60737
	Level_of_ADC_Equity_At_Risk_Value, --FINOTC-60737
	undrawn_commitment_grca_reconciliation_key_BS_Type_Flag
	,Real_Estate_Project_Indicator --FINOTC-111521
	,High_Rise_Indicator --FINOTC-111521
	,Construction_Project_Indicator --FINOTC-111521
	,Land_Acquisition_Indicator --FINOTC-111521
	,Purpose_Built_Rent_Construction_Indicator --FINOTC-111521
	,ADC_Social_Housing_Indicator --FINOTC-111521
	,Undrawn_Commitment_GRCA_Exclusion_For_Undrawn_Cascade_Flag --FINOTC-110826
	,Undrawn_Commitment_GRCA_Exclusion_For_Undrawn_Cascade_Flag_Indicator --FINOTC-110826
	,Undrawn_Commitment_GRCA_Exclusion_Not_To_Be_Applied --FINOTC-110826
	,SITE_Has_Own_GRCA_Exclusion_Codes_Flag   --FINOTC-110826
	,Sites_To_Unmask_PCIF_Flag --FINOTC-139156 _ FINOTC-139809
    -- ,Unconditionally_Cancellable_Indicator_Fallback_Flag	 --FINOTC-171149
    ,Facility_Grade  --FINOTC-186054
	,Seniority_Code --FINOTC-300631
	,SAFE_CAST(Facility_Identifier AS STRING) AS Facility_Identifier									--FINOTC-292309
	,SAFE_CAST(Syndicated_Facilities_Type_Code AS STRING) AS Syndicated_Facilities_Type_Code			--FINOTC-292309
	,SAFE_CAST(Facility_Riskiness AS NUMERIC) AS Facility_Riskiness										--FINOTC-292309
FROM FOTC_INTRA_INTRIM_500
