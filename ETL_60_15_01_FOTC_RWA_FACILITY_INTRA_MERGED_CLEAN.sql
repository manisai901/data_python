

-- ================= MERGED FROM: /mnt/data/1_ETL_60_15_01_FOTC_RWA_FACILITY_INTRA_INTERIM_1.txt =================

    FILE Name: ETL_60_15_01_FOTC_RWA_FACILITY_INTRA_INTERIM_1

CREATE TEMPORARY FUNCTION
  FOTC_UDF_TO_DATE(str_date STRING,
    pattern STRING) AS (CASE
      WHEN str_date IS NULL OR TRIM(str_date)='' THEN NULL
      WHEN UPPER(pattern)='YYD' THEN DATE_ADD(parse_DATE('%y',
        SUBSTR(str_date, 1, 2)), INTERVAL SAFE_CAST(SUBSTR(str_date, 3, 3) AS INT64)-1 DAY)
    ELSE
    PARSE_DATE(CASE
        WHEN UPPER(pattern)='DDMMYYYY' THEN '%d%m%Y'
        WHEN UPPER(pattern)='YYYY-MM-DD' THEN '%Y-%m-%d'
        WHEN UPPER(pattern)='DD-MMM-YY' THEN '%d-%b-%y'
		WHEN UPPER(pattern) = 'MM/DD/YYYY' THEN '%m/%d/%Y'
      ELSE
      '%Y%m%d'
    END
      ,
      str_date)
  END
    );

WITH
FOTC_RD_OVERRIDE_FAC_Adjustable As
(
Select * from [$target_dataset].FOTC_RD_OVERRIDE_FAC
  WHERE CURRENT_TIMESTAMP() >= from_date AND CURRENT_TIMESTAMP() <= to_date
AND "[$run_type]" NOT LIKE '%UNADJ%'

),

ESR_GRP_GLMFACIL AS (
  SELECT
    TRIM(Facility_Class) AS Facility_Class,
    TRIM(Facility_code) AS Local_Site_Code,
    SAFE_CAST('[$reporting_date]' AS DATE) AS Reporting_Date  --FINOTC-15994
  FROM
    [$target_dataset].ESR_GRP_GLMFACIL ),
  RWA_RD_REGULATOR_SPECIFIC_VALUES AS (
  SELECT
    Regulator AS Regulator,
    Parameter_Name AS Parameter_Name,
    Parameter_Value,
	Parameter_Value_String
  FROM
    [$target_dataset].FOTC_RD_REGULATOR_SPECIFIC_VALUES ),
  RWA_RD_SITE_ATTRIBUTES AS (
  SELECT
    Site_Id AS Site_Id,
    Parameter AS Parameter,
    Value AS Value,
    Regulator AS Regulator
  FROM
    [$target_dataset].FOTC_RD_SITE_ATTRIBUTES ),

	FOTC_CARM_FACILITY_INTRA AS
	(
	Select Distinct *  EXCEPT (__UUID)
	FROM [$target_dataset].FOTC_CARM_FACILITY_INTRA
	),


	FOTC_RD_LOCAL_TO_GROUP_FACILITY_MAP AS
	(
		SELECT
		Category ,
		Sub_Category ,
		Legal_Entity,
		System_ID,
		Local_Code,
		Group_Code,
		Comments
		FROM [$target_dataset].FOTC_RD_LOCAL_TO_GROUP_FACILITY_MAP),

  CDM_CREDIT_REPAIR_00100 AS (
  SELECT
		FAC_COMMON.local_credit_facility_identifier,
		CASE WHEN "[$batch_run_type]" = "B2RWA" THEN NULL
				ELSE FAC_COMMON.booking_entity_identifier  ----FINOTC-14155
			END AS booking_entity_identifier,
		FAC_COMMON.grca_Entity_Identifier, ---Added New column
		FAC_COMMON.credit_facility_system_identifier,
		FAC_COMMON.Unique_Facility_ID,
		FAC_COMMON.Unique_CARM_Facility_ID,
		FAC_COMMON.Unique_Governing_Facility_ID,
		FAC_COMMON.Unique_IP_ID,
		FAC_COMMON.Entity_Saracen_Code_1,
		FAC_COMMON.booking_entity_identifier AS booking_entity_identifier_1,
		FAC_COMMON.governing_credit_facility_identifier,
		FAC_COMMON.primary_obligor_party_system_identifier,
		FAC_COMMON.primary_obligor_party_identifier,
		FAC_COMMON.primary_obligor_party_identifier_type_code,
		FAC_COMMON.secondary_obligor_party_identifier,
		FAC_COMMON.local_credit_facility_type_code,
		FAC_COMMON.banking_or_trading_book_code,
		FAC_COMMON.committed_indicator,
		FAC_COMMON.unconditionally_cancellable_indicator,
		FAC_COMMON.credit_facility_revolving_type_indicator,
		FAC_COMMON.credit_facility_offer_date,
		FAC_COMMON.facility_offer_acceptance_date,
		FAC_COMMON.credit_facility_first_drawdown_date,
		FAC_COMMON.credit_facility_final_drawdown_date, ---Corrected spelling
		FAC_COMMON.facility_expiry_date,
		FAC_COMMON.advised_credit_limit_amount_in_transaction_currency,
		FAC_COMMON.advised_credit_limit_amount_transaction_currency_code,
		FAC_COMMON.undrawn_balance_in_transaction_currency,
		FAC_COMMON.undrawn_balance_transaction_currency_code,
		FAC_COMMON.undrawn_balance_in_functional_currency,
		FAC_COMMON.undrawn_balance_functional_currency_code,
		FAC_COMMON.cost_centre_number,
		FAC_COMMON.local_gl_reconciliation_key,
		FAC_COMMON.undrawn_commitment_grca_reconciliation_key,
		FAC_COMMON.credit_officer_identifier as credit_officer_identifier_Interim,
		FAC_COMMON.local_relationship_manager_identifier,
		FAC_COMMON.credit_facility_limit_next_review_date,
		FAC_COMMON.standby_facility_indicator,
		FAC_COMMON.facility_notice_period_days_count,
		FACILITY_BESPOKE_REPAIR.Secured_Indicator AS RWA_Secured_Indicator,                        --FINOTC-31583
		FACILITY_BESPOKE_REPAIR.End_of_period_balance_transaction_currency,                        --FINOTC-31583
		FACILITY_BESPOKE_REPAIR.Master_Category_A_Exposure_Limit,                                  --FINOTC-31583
		FAC_COMMON.proportion_secured_percentage,
		FAC_COMMON.liquidity_or_credit_facility_code,
		FAC_COMMON.availability_expiration_date,
		FAC_COMMON.recognition_or_de_recognition_date,
		FAC_COMMON.advised_indicator,
		FAC_COMMON.unweighted_utilisation_amount_in_transaction_currency,
		FAC_COMMON.unweighted_utilisation_amount_transaction_currency_code,
		FAC_COMMON.credit_facility_arrangement_source_system_code,
		FAC_COMMON.credit_facility_arrangement_local_number,
		FAC_COMMON.credit_facility_arrangement_suffix_number,
		FAC_COMMON.undrawn_balance_nominal_account_number,
		FAC_COMMON.undrawn_balance_source_chartfield_code,
		FAC_COMMON.detailed_facility_code,
		FAC_COMMON.credit_facility_or_product_chartfield_code,
		FAC_COMMON.affiliate_chartfield_code,
		FAC_COMMON.summary_customer_type_chartfield_code,
		FAC_COMMON.detailed_customer_type_chartfield_code,
		FAC_COMMON.recurring_fee_or_interest_percentage,
		FAC_COMMON.non_usage_fee_amount_in_transaction_currency,
		FAC_COMMON.non_usage_fee_amount_transaction_currency_code,
		FAC_COMMON.up_front_fee_amount_in_transaction_currency,
		FAC_COMMON.up_front_fee_amount_transaction_currency_code,

		--- New attribute added due to override
		RWA_Run_Type,
		committed_indicator_Pre_Override,
		Committed_Indicator_Override,
		unconditionally_cancellable_indicator_Pre_Override,
		Unconditionally_Cancellable_Indicator_Override,
		facility_expiry_date_Pre_Override,
		facility_expiry_date_Override,
		undrawn_balance_in_transaction_currency_Pre_Override,
		undrawn_balance_in_transaction_currency_Override,
		undrawn_balance_transaction_currency_code_Pre_Override,
		undrawn_balance_transaction_currency_code_Override,
		undrawn_commitment_grca_reconciliation_key_Pre_Override,
		Undrawn_Commitment_Grca_Reconciliation_Key_Override,
		credit_facility_limit_next_review_date_Pre_Override,
		credit_facility_limit_next_review_date_Override,
		Committed_Indicator_Override_Type_Indicator,
		Unconditionally_Cancellable_Indicator_Override_Type_Indicator,
		facility_expiry_date_Override_Type_Indicator,
		undrawn_balance_in_transaction_currency_Override_Type_Indicator,
		undrawn_balance_transaction_currency_code_Override_Type_Indicator,
		Undrawn_Commitment_Grca_Reconciliation_Key_Override_Type_Indicator,
		credit_facility_limit_next_review_date_Override_Type_Indicator,


		CARM_F_FAC.Involved_Party_Alternative_ID_Type AS CARM_Involved_Party_Alternative_ID_Type,
		CARM_F_FAC.Facility_Customer_ID AS CARM_Facility_Customer_ID,
		CARM_F_FAC.Group_System_ID AS CARM_Group_system_ID,
		CARM_F_FAC.Credit_Proposal_Serial_Number AS CARM_Credit_Proposal_Serial_Number,
		CARM_F_FAC.Facility_Currency_Code AS CARM_Facility_Currency_Code,
		CARM_F_FAC.Relationship_Identifier AS CARM_Relationship_Identifier,
		CARM_F_FAC.Ultimate_Parent_Facility_Flag AS CARM_Ultimate_Parent_Facility_Flag,
		CARM_F_FAC.DLGD AS CARM_DLGD,
		CARM_F_FAC.ELGD AS CARM_ELGD,
		CARM_F_FAC.LGD AS CARM_LGD,
		CARM_F_FAC.LGD_Indicator AS CARM_LGD_Indicator,
		CARM_F_FAC.availability_expiration_date AS CARM_Availability_Expiration_Date,
		CARM_F_FAC.Application_Approval_Date AS CARM_Application_Approval_Date,
		CARM_F_FAC.Committed_Indicator_Fallback AS CARM_Commited_Indicator,
		CARM_F_FAC.Regulatory_Specialised_Lending_Type AS CARM_Regulatory_Specialised_Lending_Type,
		CARM_F_FAC.Supervisory_Category AS CARM_Supervisory_Category,
		CARM_F_FAC.Proposed_Supervisory_Category AS CARM_Proposed_Supervisory_Category,
		CARM_F_FAC.Overridden_Supervisory_Category AS CARM_Overridden_Supervisory_Category,
		CARM_F_FAC.Proposed_Regulatory_Specialised_Lending_PD AS CARM_Proposed_Regulatory_Specialised_Lending_PD,
		CARM_F_FAC.Overridden_Regulatory_Specialised_Lending_PD AS CARM_Overridden_Regulatory_Specialised_Lending_PD,
		CARM_F_FAC.Regulatory_Specialised_Lending_Scorecard AS CARM_Regulatory_Specialised_Lending_Scorecard,
		CARM_F_FAC.Facility_Scorecard AS CARM_Facility_Scorecard,
		CARM_F_FAC.Overridden_Loss_Given_Default_Override_Category_Code AS CARM_Overridden_Loss_Given_Default_Override_Category_Code,
		CARM_F_FAC.Overridden_Downturn_Loss_Given_Default_Percentage AS CARM_Overridden_Downturn_Loss_Given_Default_Percentage,
		CARM_F_FAC.Downturn_Loss_Given_Default_Percentage AS CARM_Downturn_Loss_Given_Default_Percentage,
		CARM_F_FAC.Overridden_Downturn_Loss_Given_Default_Percentage AS CARM_Overridden_Loss_Given_Default_Percentage,
		CARM_F_FAC.Proposed_Loss_Given_Default_Percentage AS CARM_Proposed_Loss_Given_Default_Percentage,
		CARM_F_FAC.Unweighted_Approved_Limit_Amount AS CARM_Unweighted_Approved_Limit_Amount,
		CARM_F_FAC.Parent_Facility_1 AS CARM_Parent_Facility_ID,
		CARM_F_FAC.advised_indicator AS CARM_Advised_Indicator,
		CARM_F_FAC.GBCDU_Site_Code AS CARM_GBCDU_Site_Code,
		CARM_F_FAC.Unconditionally_Cancellable_Indicator_Fallback AS CARM_Unconditionally_Cancellable_Indicator,
		CARM_F_FAC.Next_Review_Date AS CARM_Next_Review_Date,
		CARM_F_FAC.Facility_Type_Code AS CARM_Facility_Type_Code_Local,
		CARM_F_FAC.Facility_Type_Code_Group AS CARM_Facility_Type_Code_Group,
		CARM_F_FAC.Facility_CCY_Code_Group AS CARM_Facility_CCY_Code_Group,
		CARM_F_FAC.Forbearance_Indicator AS CARM_Forbearance_Indicator,
		CARM_F_FAC.Organisational_Unit AS CARM_Organisational_Unit,
		CARM_F_FAC.Facility_CCY_Code_Group AS CARM_Limit_CCY,
		CARM_F_FAC.Facility_Expiration_Date AS CARM_Facility_Expiry_Date,
		CARM_F_FAC.Site_Saracen_ID AS CARM_Site_Saracen_ID,
		"CREDIT_FACILITY" AS Source,
		FOTC_UDF_TO_DATE(FAC_COMMON.reporting_date,'YYYY-MM-DD') AS Reporting_Date,
		"[$sys_country_code]" AS Sys_Country_Code,
		"[$group_sys_id]" AS Group_System_ID,
		"[$batch_run_type]" AS Batch_Run_Type,
		FACILITY_BESPOKE_REPAIR.SL_Grade_Description AS SL_Grade_Description,
		--FACILITY_BESPOKE_REPAIR.Facility_Type AS Facility_Type,
		facility_uses AS Facility_Type, --FINOTC-37442 --V0.59
		--FACILITY_BESPOKE_REPAIR.Facility_Limit_Lcl_Rep_Ccy AS Facility_Limit_Lcl_Rep_Ccy,
		advised_credit_limit_amount_in_transaction_currency AS Facility_Limit_Lcl_Rep_Ccy,  --FINOTC-37442 --V0.59
		FACILITY_BESPOKE_REPAIR.CRE_Type_Identifier AS CRE_Type_Identifier,
		FAC_COMMON.Adjustment_type AS Adjustment_type,
		--FINOTC-10330
		CARM_F_FAC.Arrears_Interest_Fees_Penalty_Payment_Capitalisation_Date,
		CARM_F_FAC.Refinancing_or_New_Credit_Facilities_Date,
		CARM_F_FAC.Interest_Only_Conversion_Date,
		CARM_F_FAC.Reduced_Payments_Date,
		CARM_F_FAC.Rescheduled_Payments_Date,
		CARM_F_FAC.Conversion_of_Currency_Date,
		CARM_F_FAC.Grace_Period_or_Payment_Moratorium_Date,
		CARM_F_FAC.Sales_by_Agreement_Date,
		CARM_F_FAC.Debt_Consolidation_Date,
		CARM_F_FAC.Debt_for_Equity_Swaps_Date,
		CARM_F_FAC.Reduction_In_Margin_Date,
		CARM_F_FAC.Principle_Payment_Forgiveness_Date,
		CARM_F_FAC.Interest_Fees_Penalty_Date,
		CARM_F_FAC.Repayment_Profile_Date,
		CARM_F_FAC.Other_Payment_Date,
		CARM_F_FAC.Covenaint_Waiver_Date,
		CARM_F_FAC.Restructuring_of_Collaterals_Date,
		CARM_F_FAC.Other_Non_Payment_Date,
		CARM_F_FAC.CARM_L_Credit_Proposal_Serial_Number,
		CARM_F_FAC.CARM_L_Forebearance_Flag,
		CARM_F_FAC.CARM_Date_of_Forborne,
		CARM_F_FAC.CARM_Date_of_First_Forborne,
		CARM_F_FAC.CARM_Number_of_Forbearance_Measures,
		--FINOTC-10330 end
		--FINOTC-54280:Uplift for V0.65 Started here
		CARM_F_FAC.Local_Overridden_Loss_Given_Default_Override_Category_Code AS Local_CARM_Overridden_Loss_Given_Default_Override_Category_Code,
		CARM_F_FAC.Local_Overridden_Downturn_Loss_Given_Default_Percentage AS Local_CARM_Overridden_Downturn_Loss_Given_Default_Percentage,
		CARM_F_FAC.Local_Downturn_Loss_Given_Default_Percentage AS Local_CARM_Downturn_Loss_Given_Default_Percentage,
		CARM_F_FAC.Local_Overridden_Downturn_Loss_Given_Default_Percentage AS Local_CARM_Overridden_Loss_Given_Default_Percentage,
		CARM_F_FAC.Local_Proposed_Loss_Given_Default_Percentage AS Local_CARM_Proposed_Loss_Given_Default_Percentage,
		CARM_F_FAC.Local_DLGD AS Local_CARM_DLGD,
		CARM_F_FAC.Local_ELGD AS Local_CARM_ELGD,
		CARM_F_FAC.Local_LGD AS Local_CARM_LGD,
		CARM_F_FAC.Local_LGD_Indicator AS Local_CARM_LGD_Indicator,
		--FINOTC-111521
		WDR_ADC_REPAIR.ADC_Indicator AS ADC_Indicator,   --FINOTC-60737
		WDR_ADC_REPAIR.Level_of_ADC_Pre_Sale_Pre_Lease_Contract AS Level_of_ADC_Pre_Sale_Or_Pre_Lease_Contracts_Value,  --FINOTC-60737
		WDR_ADC_REPAIR.Level_of_ADC_Equity_at_Risk AS Level_of_ADC_Equity_At_Risk_Value --FINOTC-60737
		--Ended here
		,WDR_ADC_REPAIR.Real_Estate_Project_Indicator AS Real_Estate_Project_Indicator
		,WDR_ADC_REPAIR.High_Rise_Indicator AS High_Rise_Indicator
		,WDR_ADC_REPAIR.Construction_Project_Indicator AS Construction_Project_Indicator
		,WDR_ADC_REPAIR.Land_Acquisition_Indicator AS Land_Acquisition_Indicator
		,WDR_ADC_REPAIR.Purpose_Built_Rent_Construction_Indicator AS Purpose_Built_Rent_Construction_Indicator
		,WDR_ADC_REPAIR.ADC_Social_Housing_Indicator AS ADC_Social_Housing_Indicator
		--FINOTC-111521
		-- ,CASE WHEN unconditionally_cancellable_indicator_Pre_Override IS NOT NULL
		      -- THEN "N"
			  -- WHEN CARM_F_FAC.Unconditionally_Cancellable_Indicator_Fallback_Flag IS NOT NULL
			  -- THEN CARM_F_FAC.Unconditionally_Cancellable_Indicator_Fallback_Flag
			  -- ELSE "Y"
         -- END AS Unconditionally_Cancellable_Indicator_Fallback_Flag			  --FINOTC-171149
        ,CARM_F_FAC.Facility_Grade                                                 --FINOTC-186054
		,SAFE_CAST(CARM_F_FAC.Facility_Identifier AS STRING) AS Facility_Identifier									--FINOTC-292309
		,SAFE_CAST(CARM_F_FAC.Syndicated_Facilities_Type_Code AS STRING) AS Syndicated_Facilities_Type_Code			--FINOTC-292309
		,SAFE_CAST(CARM_F_FAC.Facility_Riskiness AS NUMERIC) AS Facility_Riskiness									--FINOTC-292309
		,Seniority_Code --FINOTC-300631
  FROM
    [$target_dataset].FOTC_COMMON_FACILITY_INTRA FAC_COMMON
  LEFT OUTER JOIN
    FOTC_CARM_FACILITY_INTRA CARM_F_FAC
  ON
	CARM_F_FAC.Facility_Identifier  = CASE WHEN "[$batch_run_type]" = "B2RWA" and FAC_COMMON.HNAH_ENTITY_FLAG = "Y"              -- FINOTC-263681
                                               THEN FAC_COMMON.local_credit_facility_identifier
                                           ELSE FAC_COMMON.credit_facility_arrangement_local_number END
    AND CARM_F_FAC.Site_Saracen_ID  =FAC_COMMON.Grca_entity_identifier
  LEFT OUTER JOIN
    [$target_dataset].HNAH_RWA_FACILITY_BESPOKE_REPAIR FACILITY_BESPOKE_REPAIR
  ON
	FACILITY_BESPOKE_REPAIR.local_credit_facility_identifier =FAC_COMMON.local_credit_facility_identifier
    AND FAC_COMMON.credit_facility_arrangement_local_number  =FACILITY_BESPOKE_REPAIR.credit_facility_arrangement_local_number
	AND "[$batch_run_type]" = "B2RWA" and FAC_COMMON.HNAH_Entity_Flag = "Y" --FINOTC_213409

LEFT OUTER JOIN
		[$target_dataset].WDR_ADC_REPAIR WDR_ADC_REPAIR --added join as per FINOTC-111521
		ON
		Facility_ID   =  FAC_COMMON.local_credit_facility_identifier
--AND  System_ID     =   FAC_COMMON.credit_facility_system_identifier  --FINOTC-223702
AND    safe_cast(Saracen_ID As STRING)  =  FAC_COMMON.grca_entity_identifier
	),

	CDM_CREDIT_REPAIR_00110 AS
	(
		SELECT CDM_CREDIT_FACILITY_REPAIR.*,


		CASE WHEN (local_credit_facility_identifier like '%ADJ_RECON%' or local_credit_facility_identifier like '%ADJ_RECON%')
       then 'R'
       WHEN (local_credit_facility_identifier like  '%OFFSYS% '  or local_credit_facility_identifier like  '%OFFSYS%' )
        then 'O'
       WHEN  Adjustment_type like '%Manual%' and Adjustment_type like '%Auto%'
        then 'B'
       WHEN Adjustment_type like '%Manual%'
        then 'M'
       WHEN Adjustment_type like '%Auto%'
        then 'A'
WHEN Adjustment_type is Null then  'N' END AS Adjustment_Flag,

		CDM_CREDIT_FACILITY_REPAIR.CARM_Next_Review_Date AS CARM_Next_Review_Date_Pre_Override,

		CASE
		WHEN RWA_Run_Type IN ( "ADJ") THEN FOTC_UDF_TO_DATE(FACILITY_OVERRIDE_1.Override_Value ,'MM/DD/YYYY')
		END AS CARM_Next_Review_Date_Override,

		CDM_CREDIT_FACILITY_REPAIR.CARM_Facility_Expiry_Date AS CARM_Facility_Expiry_Date_Pre_Override,

		CASE
		WHEN RWA_Run_Type IN ( "ADJ") THEN FOTC_UDF_TO_DATE(FACILITY_OVERRIDE_2.Override_Value ,'MM/DD/YYYY')
		END AS CARM_Facility_Expiry_Date_Override

		FROM CDM_CREDIT_REPAIR_00100 CDM_CREDIT_FACILITY_REPAIR
		LEFT OUTER JOIN FOTC_RD_OVERRIDE_FAC_Adjustable FACILITY_OVERRIDE_1
		ON   FACILITY_OVERRIDE_1.SITE_ID  = CDM_CREDIT_FACILITY_REPAIR.grca_Entity_Identifier			----FINOTC-14155
		AND FACILITY_OVERRIDE_1.Facility_ID  = CDM_CREDIT_FACILITY_REPAIR.local_credit_facility_identifier
		AND FACILITY_OVERRIDE_1.Override_Type = "CARM_NEXT_REVIEW_DATE"
		LEFT OUTER JOIN FOTC_RD_OVERRIDE_FAC_Adjustable FACILITY_OVERRIDE_2
		ON   FACILITY_OVERRIDE_2.SITE_ID  = CDM_CREDIT_FACILITY_REPAIR.grca_Entity_Identifier		----FINOTC-14155
		AND FACILITY_OVERRIDE_2.Facility_ID  = CDM_CREDIT_FACILITY_REPAIR.local_credit_facility_identifier
		AND FACILITY_OVERRIDE_2.Override_Type = "CARM_FACILITY_EXPIRY_DATE"

	),

	CDM_CREDIT_REPAIR_00120 AS
	(
		SELECT CDM_CREDIT_FACILITY_REPAIR.* EXCEPT(CARM_Next_Review_Date,CARM_Facility_Expiry_Date),

		CASE WHEN CARM_Next_Review_Date_Override IS NOT NULL THEN
             "Facility"
		END AS CARM_Next_Review_Date_Override_Type_Indicator,

		CASE WHEN  CARM_Next_Review_Date_Override IS NOT NULL
			THEN CARM_Next_Review_Date_Override
			ELSE CARM_Next_Review_Date_Pre_Override
		END AS  CARM_Next_Review_Date,

		CASE WHEN CARM_Facility_Expiry_Date_Override IS NOT NULL THEN
             "Facility"
		END AS CARM_Facility_Expiry_Date_Override_Type_Indicator,

		CASE WHEN  CARM_Facility_Expiry_Date_Override IS NOT NULL
			THEN CARM_Facility_Expiry_Date_Override
			ELSE CARM_Facility_Expiry_Date_Pre_Override
		END AS  CARM_Facility_Expiry_Date

		FROM CDM_CREDIT_REPAIR_00110 CDM_CREDIT_FACILITY_REPAIR
	),

	CDM_CREDIT_REPAIR_00150 AS
	(
		SELECT CDM_REPAIR.*,


			CASE WHEN Batch_Run_Type = "B2RWA" THEN  CDM_REPAIR.booking_entity_identifier_1
				ELSE NULL
			END AS GBCDU_Site_Code_SDI,

			 CASE
				WHEN credit_facility_limit_next_review_date NOT IN ( "9999-12-31")     and
					credit_facility_limit_next_review_date IS NOT NULL                         and
					credit_facility_limit_next_review_date  NOT IN ( "1901-01-01")   and
					credit_facility_limit_next_review_date >= Reporting_Date
					then   "Y"
					else "N"
				END AS credit_facility_limit_next_review_date_Valid_Flag,

				 CASE
					WHEN CARM_Next_Review_Date NOT IN ( "9999-12-31")     and
					CARM_Next_Review_Date IS NOT NULL                       and
					CARM_Next_Review_Date  NOT IN ( "1901-01-01")  and
					CARM_Next_Review_Date >= Reporting_Date
					then   "Y"
					else "N"
				END AS CARM_Next_Review_Date_Valid_Flag,



			CASE
				WHEN Batch_Run_Type  NOT IN ("FOTC")
				then  Null
				Else CDM_REPAIR.credit_officer_identifier_Interim
			END AS credit_officer_identifier,

		FROM CDM_CREDIT_REPAIR_00120 CDM_REPAIR
	),

  CDM_CREDIT_REPAIR_00200 AS (
  SELECT
    CDM_REPAIR.* EXCEPT(booking_entity_identifier_1),
   CASE
		WHEN credit_facility_limit_next_review_date_valid_Flag = "Y"
		then  credit_facility_limit_next_review_date
		WHEN   CARM_Next_Review_Date_Valid_Flag  = "Y"
        then  CARM_Next_Review_Date
		END AS Next_Review_Date,

		CASE
			WHEN credit_facility_limit_next_review_date_valid_Flag = "Y"
			then  "SDI_Based_credit_facility_limit_next_review_date"
			WHEN  CARM_Next_Review_Date_Valid_Flag  = "Y"
			then  "CARM_Based_Next_Review_Date"
		END AS Next_Review_Date_Source,

	CASE WHEN Batch_Run_Type = "B2RWA" THEN LOCAL_TO_GROUP_FACILITY_MAP_ESR.Group_Code
			WHEN Batch_Run_Type = "FOTC" THEN LOCAL_TO_GROUP_FACILITY_MAP_NONESR.Group_Code
	END AS Facility_Type_Code,

			CASE
				WHEN Batch_Run_Type NOT IN ( "FOTC")
				then
					CASE
						WHEN  CDM_REPAIR.credit_officer_identifier_Interim IS NOT NULL
						then CDM_REPAIR.credit_officer_identifier_Interim
					Else "N"
					END
					ELSE "N"
					END AS Aggregated_MIF_Flag,

    ESR_GLMFACIL.Facility_Class AS Facility_Classification,
    COALESCE(SAFE_CAST(credit_facility_first_drawdown_date AS DATE),
      SAFE_CAST(CARM_Application_Approval_Date AS DATE)) AS Facility_Start_Date,
    COALESCE(SAFE_CAST(committed_indicator AS STRING),
      SAFE_CAST(CARM_Commited_Indicator AS STRING)) AS Facility_Committed_Indicator,
    COALESCE(SAFE_CAST(local_credit_facility_type_code AS STRING),
      SAFE_CAST(CARM_Facility_Type_Code_Local AS STRING)) AS Facility_Type_Code_Local,
    CASE
      WHEN SITE_ATTRIBUTES.Parameter IS NOT NULL THEN CASE
      WHEN SITE_ATTRIBUTES.VALUE IS NOT NULL THEN SITE_ATTRIBUTES.VALUE
    ELSE
    "N"
  END
    ELSE
    "NA"
  END
    AS Site_Eligible_For_UK_Ret_SME_Reclass_Flag,

	CASE WHEN "[$run_group]" = "HBUK"
	THEN SITE_ATTRIBUTES_PCIF_HBUK.Value
	ELSE SITE_ATTRIBUTES_PCIF.Value END AS PCIF_Identifier, --FINOTC-276185

    SAFE_CAST(REGULATOR_SPECIFIC.Parameter_Value AS FLOAT64) AS UK_Ret_SME_Limit_Threshold_GBP,

	 CASE WHEN  advised_indicator IN ("Y", "N", "1", "0")			--FINOTC-61001 --FINOTC-135812
THEN "Y"
ELSE "N" END AS Advised_Indicator_Validation_Flag,
SITE_ATTRIBUTES_1.value AS HNAH_Entity_Flag                                                 --FINOTC-31583
  FROM
    CDM_CREDIT_REPAIR_00150 CDM_REPAIR
  LEFT OUTER JOIN FOTC_RD_LOCAL_TO_GROUP_FACILITY_MAP LOCAL_TO_GROUP_FACILITY_MAP_ESR
		ON LOCAL_TO_GROUP_FACILITY_MAP_ESR.Category = "ESR"
		AND LOCAL_TO_GROUP_FACILITY_MAP_ESR.Sub_Category = "ESR"
		AND CDM_REPAIR.GBCDU_Site_Code_SDI = LOCAL_TO_GROUP_FACILITY_MAP_ESR.System_ID
		AND CDM_REPAIR.local_credit_facility_type_code = LOCAL_TO_GROUP_FACILITY_MAP_ESR.Local_Code

		LEFT OUTER JOIN FOTC_RD_LOCAL_TO_GROUP_FACILITY_MAP LOCAL_TO_GROUP_FACILITY_MAP_NONESR
		ON LOCAL_TO_GROUP_FACILITY_MAP_NONESR.Category = "NON_ESR"
		AND LOCAL_TO_GROUP_FACILITY_MAP_NONESR.Sub_Category = "RWA"
		AND CDM_REPAIR.grca_Entity_Identifier = LOCAL_TO_GROUP_FACILITY_MAP_NONESR.Legal_Entity			----FINOTC-14155
		AND LOCAL_TO_GROUP_FACILITY_MAP_NONESR.System_ID = CDM_REPAIR.credit_facility_system_identifier
		AND CDM_REPAIR.local_credit_facility_type_code = LOCAL_TO_GROUP_FACILITY_MAP_NONESR.Local_Code

  LEFT OUTER JOIN
    ESR_GRP_GLMFACIL ESR_GLMFACIL
  ON
    ESR_GLMFACIL.Local_Site_Code=CDM_REPAIR.CARM_Facility_Type_Code_Group
    AND ESR_GLMFACIL.Reporting_Date=CDM_REPAIR.Reporting_Date
  LEFT OUTER JOIN
    RWA_RD_SITE_ATTRIBUTES SITE_ATTRIBUTES
  ON
	SITE_ATTRIBUTES.Site_Id=CDM_REPAIR.grca_Entity_Identifier			----FINOTC-14155
	AND SITE_ATTRIBUTES.Parameter="Site_Eligible_For_UK_Ret_SME_Reclass_Flag"
	LEFT OUTER JOIN                                                                              --FINOTC-31583
    RWA_RD_SITE_ATTRIBUTES SITE_ATTRIBUTES_1
    ON
    SITE_ATTRIBUTES_1.Site_Id=CDM_REPAIR.grca_entity_identifier
    AND SITE_ATTRIBUTES_1.Parameter="HNAH_ENTITY_FLAG"

	LEFT OUTER JOIN
    RWA_RD_SITE_ATTRIBUTES SITE_ATTRIBUTES_PCIF_HBUK
   ON
    SITE_ATTRIBUTES_PCIF_HBUK.Site_Id=CDM_REPAIR.grca_entity_identifier
    AND SITE_ATTRIBUTES_PCIF_HBUK.Parameter="PCIF_IMPORT_SOURCE_HBUK"  --FINOTC-276185

	LEFT OUTER JOIN
    RWA_RD_SITE_ATTRIBUTES SITE_ATTRIBUTES_PCIF
	  ON
	SITE_ATTRIBUTES_PCIF.Site_Id=CDM_REPAIR.grca_entity_identifier		----FINOTC-14155
    AND SITE_ATTRIBUTES_PCIF.Parameter="PCIF_IMPORT_SOURCE"


  LEFT OUTER JOIN
    RWA_RD_REGULATOR_SPECIFIC_VALUES REGULATOR_SPECIFIC
  ON
    REGULATOR_SPECIFIC.Regulator="PRA"
    AND REGULATOR_SPECIFIC.Parameter_Name="UK_RET_SME_LIMIT_THRESHOLD_GBP" ),

CDM_CREDIT_REPAIR_00210 AS (
	SELECT
    CDM_REPAIR.*,
	CASE WHEN Advised_Indicator_Validation_Flag <> "Y"
    OR
    Advised_Indicator_Validation_Flag IS NULL
THEN "Y"
ELSE "N" END AS Advised_Indicator_Fallback,

	CASE
         	WHEN Committed_Indicator_Override IS NOT NULL THEN "N"	--FINOTC-9225
      WHEN Facility_Committed_Indicator IS NULL THEN "Y"
    ELSE
    "N"
	END AS Committed_Indicator_Fallback_Flag,

	CASE WHEN HNAH_Entity_Flag = "Y" AND Batch_Run_Type = "B2RWA" THEN                                                                   --FINOTC-31583
     CASE WHEN COALESCE(SAFE_CAST(Master_Category_A_Exposure_Limit AS FLOAT64),0) = 0 THEN 0
	    WHEN (COALESCE(SAFE_CAST(End_of_period_balance_transaction_currency AS FLOAT64),0) / COALESCE(SAFE_CAST(Master_Category_A_Exposure_Limit AS FLOAT64),0)) > 1 THEN 1
		WHEN (COALESCE(SAFE_CAST(End_of_period_balance_transaction_currency AS FLOAT64),0) / COALESCE(SAFE_CAST(Master_Category_A_Exposure_Limit AS FLOAT64),0)) < 0  THEN 0
     ELSE (COALESCE(SAFE_CAST(End_of_period_balance_transaction_currency AS FLOAT64),0) / COALESCE(SAFE_CAST(Master_Category_A_Exposure_Limit AS FLOAT64),0)) END
	ELSE NULL
	END AS Utilisation,     ----- FINOTC_213409

  CASE WHEN HNAH_Entity_Flag = "Y"  AND Batch_Run_Type = "B2RWA"  THEN                                                                         --FINOTC-31583
    CASE WHEN COALESCE(SAFE_CAST(Master_Category_A_Exposure_Limit as FLOAT64),0) =0  THEN "Limit_For_Utilisation_Not_Available"
	    WHEN (COALESCE(SAFE_CAST(End_of_period_balance_transaction_currency as FLOAT64),0) / COALESCE(SAFE_CAST(Master_Category_A_Exposure_Limit AS FLOAT64),0)) > 1 		      THEN "Utilisation_Greater_Than_One"
        WHEN (COALESCE(SAFE_CAST(End_of_period_balance_transaction_currency as FLOAT64),0) / COALESCE(SAFE_CAST(Master_Category_A_Exposure_Limit AS FLOAT64),0)) < 0 		      THEN "Utilisation_Less_Than_Zero"
    ELSE  "Utilisation_Within_Valid_Range" END
  ELSE "N/A"
  END AS Utilisation_Indicator,      ---- FINOTC_213409

   CASE WHEN  RWA_Secured_Indicator in ("1")  THEN "Secured"                                                          --FINOTC-31583
       WHEN  RWA_Secured_Indicator in ("2")  THEN "Unsecured"
	    END AS Secured_Unsecured_Indicator

	FROM
    CDM_CREDIT_REPAIR_00200 CDM_REPAIR )


select *
from CDM_CREDIT_REPAIR_00210



-- ================= MERGED FROM: /mnt/data/2_ETL_60_15_01_FOTC_RAW_FACILITY_INTRA_INTERIM.txt =================


WITH
CDM_CREDIT_REPAIR_For_Parent_Check AS (
  SELECT
    governing_credit_facility_identifier,
    grca_entity_identifier,
    credit_facility_system_identifier
  FROM
    [$target_dataset].FOTC_COMMON_FACILITY_INTRA
  GROUP BY
    governing_credit_facility_identifier,
    grca_entity_identifier,
    credit_facility_system_identifier
),

DMDS AS (
  SELECT
    CCY_CODE  AS source_currency,
    EXCH_CCY AS target_currency,
    EXCH_RATE AS exchange_rate
  FROM
    [$target_dataset].DMDS_USD_FX_RATES
),

RWA_RD_REGULATOR_SPECIFIC_VALUES AS (
  SELECT
    Regulator,
    Parameter_Name,
    Parameter_Value,
    Parameter_Value_String
  FROM
    [$target_dataset].FOTC_RD_REGULATOR_SPECIFIC_VALUES
),


CDM_CREDIT_REPAIR_00230 AS (
  SELECT
    r.*,

    CASE
      WHEN Committed_Indicator_Fallback_Flag = 'Y' THEN 'Y'
      ELSE 'N'
    END AS Committed_Indicator_Fallback,

    CASE
      WHEN Advised_Indicator_Validation_Flag <> 'Y'
        THEN Advised_Indicator_Fallback
      ELSE advised_indicator
    END AS Advised_Indicator_SDI,

    CASE
      WHEN Facility_Committed_Indicator IS NOT NULL THEN
        CASE
          WHEN Facility_Committed_Indicator IN ('Y', '1') THEN 'Y'
          WHEN Facility_Committed_Indicator IN ('N', '0') THEN 'N'
        END
      ELSE
        CASE
          WHEN Committed_Indicator_Fallback_Flag = 'Y' THEN 'Y'
          ELSE 'N'
        END
    END AS Facility_Committed_Indicator_RWA,

    CASE
      WHEN CARM_Commited_Indicator IS NOT NULL THEN CARM_Commited_Indicator
      ELSE
        CASE
          WHEN Committed_Indicator_Fallback_Flag = 'Y' THEN 'Y'
          ELSE 'N'
        END
    END AS Commited_Indicator_Final

  FROM
    [$target_dataset].FOTC_RWA_FACILITY_INTRA_INTERIM_1 r
),

MIF_L_GOV AS (
  SELECT
    r.*,
    r.local_credit_facility_identifier,
    r.governing_credit_facility_identifier,
    r.grca_entity_identifier,
    r.CARM_Site_Saracen_ID
  FROM
    CDM_CREDIT_REPAIR_00230 r
),

CHECK_ALL_CHILD_ARE_UNCOMMITED AS (
  SELECT
    local_credit_facility_identifier AS Facility_ID,
    Site_Saracen_ID,
    Commited_Indicator AS Child_Committed_Indicator
  FROM (
    SELECT
      p.local_credit_facility_identifier,
      p.grca_entity_identifier AS Site_Saracen_ID,
      c.Facility_Committed_Indicator_RWA AS Commited_Indicator
    FROM
      CDM_CREDIT_REPAIR_00230 p
    LEFT JOIN
      MIF_L_GOV g
      ON p.local_credit_facility_identifier = g.governing_credit_facility_identifier
     AND p.grca_entity_identifier = g.grca_entity_identifier
    LEFT JOIN
      CDM_CREDIT_REPAIR_00230 c
      ON g.local_credit_facility_identifier = c.local_credit_facility_identifier
     AND g.grca_entity_identifier = c.grca_entity_identifier
    WHERE
      g.local_credit_facility_identifier IS NOT NULL
  )
  GROUP BY
    Facility_ID,
    Site_Saracen_ID,
    Child_Committed_Indicator
),

CDM_CREDIT_REPAIR_00300 AS (
  SELECT
    r.*,
    CASE
      WHEN cc.Child_Committed_Indicator = 'Y' THEN 'N'
      WHEN cu.Child_Committed_Indicator = 'N' THEN 'Y'
      ELSE 'N'
    END AS Leaf_facility_Cos_Of_Non_Commited_Child_Facilities_Flag
  FROM
    CDM_CREDIT_REPAIR_00230 r
  LEFT JOIN
    CHECK_ALL_CHILD_ARE_UNCOMMITED cc
    ON r.local_credit_facility_identifier = cc.Facility_ID
   AND r.grca_entity_identifier = cc.Site_Saracen_ID
   AND cc.Child_Committed_Indicator = 'Y'
  LEFT JOIN
    CHECK_ALL_CHILD_ARE_UNCOMMITED cu
    ON r.local_credit_facility_identifier = cu.Facility_ID
   AND r.grca_entity_identifier = cu.Site_Saracen_ID
   AND cu.Child_Committed_Indicator = 'N'
),

CDM_CREDIT_REPAIR_00400 AS (
  SELECT
    r.*,

    COALESCE(
      SAFE_CAST(Facility_Type_Code AS STRING),
      SAFE_CAST(CARM_Facility_Type_Code_Group AS STRING)
    ) AS Facility_Type_Code_Group,

    CASE
      WHEN COALESCE(SAFE_CAST(Facility_Classification AS STRING), 'NA') IN ('B', 'S')
      THEN 'Y' ELSE 'N'
    END AS Cat_B_S_Facility_Flag,

    fx_gbp.exchange_rate  AS GBP_TO_USD_EXCH_RATE,
    fx_adv.exchange_rate  AS Advised_Credit_Limit_EXCH_RATE,
    fx_unw.exchange_rate  AS Unweighted_Utilisation_EXCH_RATE,
    fx_und.exchange_rate  AS Undrawn_Balance_in_Actual_Currency_EXCH_RATE,
    fx_carm.exchange_rate AS CARM_Unweighted_Approved_Limit_EXCH_RATE,

    CASE
      WHEN facility_expiry_date IS NOT NULL
       AND facility_expiry_date NOT IN ('9999-12-31', '1901-01-01')
       AND facility_expiry_date >= Reporting_Date
      THEN 'Y' ELSE 'N'
    END AS Facility_Expiry_Date_Valid_Flag

  FROM
    CDM_CREDIT_REPAIR_00300 r
  LEFT JOIN DMDS fx_gbp
    ON fx_gbp.source_currency = 'GBP'
  LEFT JOIN DMDS fx_adv
    ON fx_adv.source_currency = r.advised_credit_limit_amount_transaction_currency_code
  LEFT JOIN DMDS fx_unw
    ON fx_unw.source_currency = r.unweighted_utilisation_amount_transaction_currency_code
  LEFT JOIN DMDS fx_und
    ON fx_und.source_currency = r.undrawn_balance_transaction_currency_code
  LEFT JOIN DMDS fx_carm
    ON fx_carm.source_currency = r.CARM_Limit_CCY
)

SELECT
  r.*,

  r.advised_credit_limit_amount_in_transaction_currency
    * r.Advised_Credit_Limit_EXCH_RATE
    AS Advised_Credit_Limit_USD,

  r.undrawn_balance_in_transaction_currency
    * r.Undrawn_Balance_in_Actual_Currency_EXCH_RATE
    AS Undrawn_Balance_in_Actual_Currency_USD_Interim,

  r.CARM_Unweighted_Approved_Limit_Amount
    * r.CARM_Unweighted_Approved_Limit_EXCH_RATE
    AS CARM_Unweighted_Limit_USD,

  CASE
    WHEN r.Facility_Type_Code_Group IS NULL
    THEN SAFE_CAST(p.Parameter_Value_String AS STRING)
  END AS Facility_Type_Code_Fallback,

  CASE
    WHEN pc.governing_credit_facility_identifier IS NOT NULL THEN 'Y'
    ELSE 'N'
  END AS Parent_Facility_Flag

FROM
  CDM_CREDIT_REPAIR_00400 r
LEFT JOIN
  RWA_RD_REGULATOR_SPECIFIC_VALUES p
  ON p.Regulator = 'PRA'
 AND p.Parameter_Name = 'FACILITY_TYPE_CODE_FALLBACK'
LEFT JOIN
  CDM_CREDIT_REPAIR_For_Parent_Check pc
  ON r.local_credit_facility_identifier = pc.governing_credit_facility_identifier
 AND r.grca_entity_identifier = pc.grca_entity_identifier
 AND r.credit_facility_system_identifier = pc.credit_facility_system_identifier;

-- ================= MERGED FROM: /mnt/data/3_ETL_60_15_01_FOTC_RWA_FACILITY_INTRA.txt =================

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