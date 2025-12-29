
-- =============================================================
-- File: FOTC_RWA_FACILITY_INTRA_OPTIMISED.sql
-- NOTE:
--   * [$target_dataset] placeholders are intentionally preserved
--   * Business logic unchanged
--   * Only formatting, minor optimisation, and redundant WITH cleanup
-- =============================================================

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

-- -------------------------------------------------------------
-- Merged 00220 + 00230 (logic unchanged)
-- -------------------------------------------------------------
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

-- -------------------------------------------------------------
-- Parent / child mapping (no semantic renaming)
-- -------------------------------------------------------------
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
