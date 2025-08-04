/*CREATE INPUT TABLE  FOR DIFFERENT MARKETS -OBESITY RX/DX/PX (Pivoting to Master 2.0)*/
DROP TABLE IF EXISTS DBU.DT_INPUT_CODES_OBS_ZEP;
CREATE TABLE DBU.DT_INPUT_CODES_OBS_ZEP DISTSTYLE EVEN AS
(
	SELECT DISTINCT 'RX_ALL_AOM' AS COHORT, NDC AS CODE FROM APLD_EX.LAAD_OBESITY_MASTERDATA
	UNION
	SELECT DISTINCT 'RX_INJ_AOM' AS COHORT, NDC AS CODE FROM APLD_EX.LAAD_OBESITY_MASTERDATA WHERE BRAND_NAME IN ('WEGOVY','ZEPBOUND','SAXENDA')
	UNION 
	SELECT DISTINCT 'RX_ZEP' AS COHORT, NDC AS CODE FROM APLD_EX.LAAD_OBESITY_MASTERDATA WHERE BRAND_NAME IN ('ZEPBOUND')
	UNION 
	SELECT DISTINCT 'RX_WEGSAX' AS COHORT, NDC AS CODE FROM APLD_EX.LAAD_OBESITY_MASTERDATA WHERE BRAND_NAME IN ('WEGOVY','SAXENDA')
	UNION 
	SELECT DISTINCT 'RX_GLP1' AS COHORT,NDC AS CODE FROM APLD_EX.LAAD_DIABETES_MASTERDATA WHERE CLASS IN ('GLP1')
	UNION
	SELECT DISTINCT 'RX_T2D' AS COHORT,NDC AS CODE FROM APLD_EX.LAAD_DIABETES_MASTERDATA WHERE PATIENT_TYPE='TYPE 2' AND CLASS NOT IN ('GLUCAGON_MKT','OTHERS')
	UNION
	SELECT DISTINCT 'RX_MNJ' AS COHORT,NDC AS CODE FROM APLD_EX.LAAD_DIABETES_MASTERDATA WHERE BRAND_NAME IN ('MOUNJARO')
	UNION
	SELECT DISTINCT 'RX_OZM' AS COHORT,NDC AS CODE FROM APLD_EX.LAAD_DIABETES_MASTERDATA WHERE BRAND_NAME IN ('OZEMPIC')
	UNION
	SELECT DISTINCT 'RX_ORAL_BRAND_AOM' AS COHORT, NDC AS CODE FROM APLD_EX.LAAD_OBESITY_MASTERDATA WHERE BRAND_NAME NOT IN ('WEGOVY','ZEPBOUND','SAXENDA') AND BRAND_GENERIC_CODE IN ('Y')
	UNION 
	SELECT DISTINCT 'RX_GENERIC_AOM' AS COHORT, NDC AS CODE FROM APLD_EX.LAAD_OBESITY_MASTERDATA WHERE BRAND_GENERIC_CODE IN ('N')
	UNION
	SELECT DISTINCT 'DX_AOM' AS COHORT, DIAGNOSIS_CODE  FROM DBU.OBS_DX_CODES_AG
	UNION
	SELECT DISTINCT 'DX_AOM' AS COHORT, DIAGNOSIS_CODE FROM APLD_EX.DIAB_APLD_LAAD_DIAG_CODE_DIM WHERE DIAGNOSIS_CODE IN
	('O99.210', 'O99.211', 'O99.212', 'O99.213', 'O99.214', 'O99.215',
	'O99.840', 'O99.841', 'O99.842', 'O99.843', 'O99.844', 'O99.845')
	UNION
	SELECT DISTINCT 'PX_AOM' AS COHORT, PRC_CD  FROM DBU.OBS_PX_CODES_AG
	UNION
  SELECT COHORT, DIAGNOSIS_CODE FROM
	(
    SELECT DISTINCT 'DX_DIAB' AS COHORT
      ,dx_code as DIAGNOSIS_CODE
      ,CASE
        WHEN UPPER(dx_desc) LIKE '%UNSPECIFIED TYPE%' THEN 'OTHERS'
        WHEN (UPPER(dx_desc) LIKE '%TYPE2%'
        OR UPPER(dx_desc) LIKE '%TYPE 2 %'
        OR UPPER(dx_desc) LIKE '%TYPE II %'
        OR UPPER(dx_desc) LIKE '%TYPEII %'
        OR UPPER(dx_desc) LIKE '%TYPE||%'
        OR UPPER(dx_desc) LIKE '%TYPE || %')
        AND UPPER(dx_desc) LIKE '%DIABETES%' THEN 'TYPE 2'
        WHEN (UPPER(dx_desc) LIKE '%TYPE1%'
        OR UPPER(dx_desc) LIKE '%TYPE 1 %'
        OR UPPER(dx_desc) LIKE '%TYPE I %'
        OR UPPER(dx_desc) LIKE '%TYPEI%'
        OR UPPER(dx_desc) LIKE '%TYPE|%'
        OR UPPER(dx_desc) LIKE '%TYPE | %')
        AND UPPER(dx_desc) LIKE '%DIABETES%' THEN 'TYPE 1'
        ELSE 'OTHERS'
        END AS TYPE_FLAG
      FROM
      APLD_EX.laad_diabetes_dim_dx_code_plus
      WHERE
      TYPE_FLAG <> 'OTHERS' AND TYPE_FLAG<>'TYPE 1' AND dx_code NOT LIKE ('O%')
	)
);


/*2 year run - using the stored procedure to create obesity Rx flags*/
CALL BIA.NEW_MKT_LAUNCH_MASTER_SS(
'OBESITY', 
'RX_INJ_AOM,RX_ALL_AOM,RX_ZEP,DX_AOM,RX_GLP1,PX_AOM,DX_DIAB',
'(SELECT MAX(SERVICE_DATE)- 730 FROM APLD_EX.LAAD_OBESITY_FCT_RX_PLUS)', 
'(SELECT MAX(SERVICE_DATE) FROM APLD_EX.LAAD_OBESITY_FCT_RX_PLUS)', 
'COMBINED', 
'(SELECT * FROM SAS.CUSTOMER)',
'NA',
'NA',
'DBU.DT_INPUT_CODES_OBS_ZEP'
);

DROP TABLE IF EXISTS DT_2_yr_ZEP_PATIENT_FUNNEL_1_OMM_FLAGS;
CREATE temp TABLE DT_2_yr_ZEP_PATIENT_FUNNEL_1_OMM_FLAGS DISTSTYLE EVEN AS
(
SELECT * FROM NEW_LAUNCH_MKT_MASTER
);



DROP TABLE IF EXISTS DT_2_yr_ZEP_PATIENT_FUNNEL_3_OMM_T2D_FLAGS;
CREATE temp TABLE DT_2_yr_ZEP_PATIENT_FUNNEL_3_OMM_T2D_FLAGS DISTSTYLE EVEN AS
(
  SELECT PATIENT_ID,prsn_id,
    MAX(RX_INJ_AOM_FLAG) RX_INJ_AOM_FLAG,
	MAX(rx_all_aom_flag) rx_all_aom_flag,
    MAX(RX_ZEP_FLAG)	RX_ZEP_FLAG,
    MAX(DX_AOM_FLAG) DX_AOM_FLAG,	
    MAX(PX_AOM_FLAG) PX_AOM_FLAG,
    MAX(RX_GLP1_FLAG) RX_GLP1_FLAG,
    MAX(DX_DIAB_FLAG) DX_DIAB_FLAG
  FROM
    DT_2_yr_ZEP_PATIENT_FUNNEL_1_OMM_FLAGS
  GROUP BY 1, 2
);



drop table if exists DT_2_yr_ZEP_PATIENT_FUNNEL_6_INTEG_UNIV_MOP;
create temp table DT_2_yr_ZEP_PATIENT_FUNNEL_6_INTEG_UNIV_MOP diststyle even as
(
  select a.*,
    most_freq_rx_payer_pln_id,
    c.method_of_payment as most_freq_rx_mop_ccy
  from 
    DT_2_yr_ZEP_PATIENT_FUNNEL_3_OMM_T2D_FLAGS a 
    left join 
    (
      select *,
      case 
        when most_freq_rx_payer_pln_id_ccy is not null then most_freq_rx_payer_pln_id_ccy
        when most_freq_rx_payer_pln_id_pcy is not null then most_freq_rx_payer_pln_id_pcy
        else most_freq_rx_payer_pln_id_p2cy
      end as most_freq_rx_payer_pln_id,
      case 
        when most_rcnt_rx_payer_pln_id_ccy is not null then most_rcnt_rx_payer_pln_id_ccy
        when most_rcnt_rx_payer_pln_id_pcy is not null then most_rcnt_rx_payer_pln_id_pcy
        else most_rcnt_rx_payer_pln_id_p2cy
      end as most_rcnt_rx_payer_pln_id
      from apld_ex.obs_laad_patient_ref
    ) b 
    on lpad(a.patient_id,15,0) = lpad(b.pat_id,15,0)
    left join 
    apld_ex.laad_obesity_dim_plan c 
    on b.most_freq_rx_payer_pln_id = c.payer_plan_id 
);


drop table if exists DT_2_yr_ZEP_PATIENT_FUNNEL_7_MOP_BEST_GUESS_data_final;
create temp table DT_2_yr_ZEP_PATIENT_FUNNEL_7_MOP_BEST_GUESS_data_final as
(
with combined_claims as (
  select patient_id, claim_id, payer_plan_id, method_of_payment, service_date
  from (
    select b.patient_id, b.claim_id, b.payer_plan_id, c.method_of_payment, b.service_date
    from apld_ex.laad_obesity_fct_rx_plus b
    left join apld_ex.laad_obesity_dim_plan c 
    on b.payer_plan_id = c.payer_plan_id
    where extract(year from b.service_date) >= 2022

    union all

    select b.patient_id, b.claim_id, b.payer_plan_id, c.method_of_payment, b.service_date
    from apld_ex.laad_diabetes_fct_rx_plus b
    left join apld_ex.laad_obesity_dim_plan c 
    on b.payer_plan_id = c.payer_plan_id
    where extract(year from b.service_date) >= 2022

    union all

    select distinct a.patient_id, a.claim_id, a.payer_plan_id, b.method_of_payment, a.service_date
    from apld_ex.laad_obesity_fct_mx a
    left join apld_ex.laad_obesity_dim_plan b
    on a.payer_plan_id = b.payer_plan_id
    where extract(year from a.service_date) >= 2022

    union all

    select distinct a.patient_id, a.claim_id, a.payer_plan_id, b.method_of_payment, a.service_date
    from apld_ex.laad_diabetes_fct_dx a
    left join apld_ex.laad_diabetes_dim_plan b
    on a.payer_plan_id = b.payer_plan_id
    where extract(year from a.service_date) >= 2022
  )
  where patient_id in (select distinct patient_id from DT_2_yr_ZEP_PATIENT_FUNNEL_3_OMM_T2D_FLAGS)
),
claims_per_patient_plan as (
  select patient_id, payer_plan_id, method_of_payment, count(distinct claim_id) as claims
  from combined_claims
  group by 1,2,3
),
ranked_methods as (
  select *,
         row_number() over (partition by patient_id order by claims desc) as mop_rank
  from claims_per_patient_plan
)
select distinct patient_id, payer_plan_id, method_of_payment
from ranked_methods
where mop_rank = 1
);


/*final mop and plan*/
drop table if exists DT_2_yr_ZEP_PATIENT_FUNNEL_8_MOP_PLAN_FIN;
create temp table DT_2_yr_ZEP_PATIENT_FUNNEL_8_MOP_PLAN_FIN as
(
  select 
    a.*, b.method_of_payment, 
    case when a.most_freq_rx_mop_ccy is null then b.method_of_payment else a.most_freq_rx_mop_ccy end as final_mop ,
    case when a.most_freq_rx_payer_pln_id is null then b.payer_plan_id else a.most_freq_rx_payer_pln_id end as final_plan
  from 
  DT_2_yr_ZEP_PATIENT_FUNNEL_6_INTEG_UNIV_MOP a
  left join 
  DT_2_yr_ZEP_PATIENT_FUNNEL_7_MOP_BEST_GUESS_data_final b
  on a.patient_id = b.patient_id
);

/*Create comborb & bmi info base table starts*/

/*Dx code category decided based on diagnosis description - check excel wb for more info*/
drop table if exists dbu.DT_2_yr_comorb_other_obesity_diag_code_cohort_ZEP;
create table dbu.DT_2_yr_comorb_other_obesity_diag_code_cohort_ZEP as
(
	SELECT DISTINCT 'DX_OMM_COMORB' AS COHORT, code  FROM 
	(select distinct code from dbu.INPUT_CODES_NHS_CWM where cohort in ('DX_CV', 'DX_OSA','DX_HYP','DX_DYS'))
	union
	SELECT DISTINCT 'DX_OMM_INDUCED_OTHER' AS COHORT, DIAGNOSIS_CODE as code  from
	(select distinct diagnosis_code from dbu.OBS_DX_CODES_AG 
	where diagnosis_code in ('E66.1', 'E66.0', 'E66.9', 'E66.8', 'E66.09'))
	union 
	SELECT DISTINCT 'DX_OMM_MORBID_SEVERE' AS COHORT, DIAGNOSIS_CODE as code  from
	(select distinct diagnosis_code from dbu.OBS_DX_CODES_AG 
	where diagnosis_code in ('E66.01', 'E66.2'))
	union 
	SELECT DISTINCT 'DX_OMM_OVERWEIGHT' AS COHORT, DIAGNOSIS_CODE as code  from
	(select distinct diagnosis_code from dbu.OBS_DX_CODES_AG 
	where diagnosis_code in ('E66'))
	union 
	SELECT DISTINCT 'DX_OMM_PED' AS COHORT, DIAGNOSIS_CODE as code  from
	(select distinct diagnosis_code from dbu.OBS_DX_CODES_AG 
	where diagnosis_code in ('Z68.53','Z68.54'))
	Union
	SELECT DISTINCT 'DX_O_PREG_BAR_ADDITIONAL' AS COHORT, DIAGNOSIS_CODE as code 
	FROM
	(select distinct diagnosis_code from apld_ex.diab_apld_laad_diag_code_dim daldcd where diagnosis_code in
	('O99.210', 'O99.211', 'O99.212', 'O99.213', 'O99.214', 'O99.215',
	'O99.840', 'O99.841', 'O99.842', 'O99.843', 'O99.844', 'O99.845')
	and diagnosis_code not in (select diagnosis_code from dbu.OBS_DX_CODES_AG))
);

/*Divided into 2 cohorts for ease of run*/
CALL BIA.NEW_MKT_LAUNCH_MASTER_SS(
'OBESITY', 
'DX_OMM_PED,DX_OMM_MORBID_SEVERE,DX_OMM_OVERWEIGHT,DX_OMM_INDUCED_OTHER,DX_OMM_COMORB,DX_O_PREG_BAR_ADDITIONAL',
'(SELECT MAX(SERVICE_DATE)-730 FROM APLD_EX.LAAD_OBESITY_FCT_RX_PLUS)', 
'(SELECT MAX(SERVICE_DATE) FROM APLD_EX.LAAD_OBESITY_FCT_RX_PLUS)', 
'COMBINED', 
'(SELECT * FROM SAS.CUSTOMER)',
'NA',
'NA',
'DBU.DT_2_yr_comorb_other_obesity_diag_code_cohort_ZEP'
);

DROP TABLE IF EXISTS DT_2_yr_ZEP_PATIENT_FUNNEL_9_OTHER_COMORB_1;
CREATE temp TABLE DT_2_yr_ZEP_PATIENT_FUNNEL_9_OTHER_COMORB_1 DISTSTYLE EVEN AS
(
SELECT * FROM NEW_LAUNCH_MKT_MASTER
);


/*Patient level flagging - historical Dx is considered here, not the latest status*/
DROP TABLE IF EXISTS DT_2_yr_ZEP_PATIENT_FUNNEL_9_OTHER_COMORB_FIN;
CREATE temp TABLE DT_2_yr_ZEP_PATIENT_FUNNEL_9_OTHER_COMORB_FIN DISTSTYLE EVEN AS
(
  SELECT PATIENT_ID,
  max(DX_OMM_COMORB_FLAG) DX_OMM_COMORB_FLAG,
  MAX(DX_OMM_INDUCED_OTHER_FLAG) DX_OMM_INDUCED_OTHER_FLAG,
  MAX(DX_OMM_MORBID_SEVERE_FLAG) DX_OMM_MORBID_SEVERE_FLAG,
  MAX(DX_OMM_PED_FLAG) DX_OMM_PED_FLAG,
  MAX(DX_OMM_OVERWEIGHT_FLAG)	DX_OMM_OVERWEIGHT_FLAG,
  MAX(DX_O_PREG_BAR_ADDITIONAL_FLAG) DX_O_PREG_BAR_ADDITIONAL_FLAG
  FROM
  (
    select * FROM DT_2_yr_ZEP_PATIENT_FUNNEL_9_OTHER_COMORB_1
    
  ) A 
  
  GROUP BY 1
);



drop table if exists DT_br_eff_access_opt_in_data;
create temp table DT_br_eff_access_opt_in_data as
(
  select 
    distinct plan_id, 
    case when plan_id = '0013390001' then 'OPT-IN' else "final opt in" end as final_opt_in 
  from 
  (
    select
    a.*,
    plan_id
    from
    (
      select
        "final plan name",
        relation,
        "final opt in"
      from
        DBU.DND_TAB_OBS_EMP_OPT_IN_SHEET1
    ) A
    left join
    (
      select
        distinct plan_id,
        vendor_plan_desc,
        mmit_org_altrnt_nm,
        zepbound_formulary_sts,
        plan_to_org_relationship_granular,
        case
        when plan_to_org_relationship_granular = 'ONE IQVIA PLAN TO ONE MMIT PLAN' then mmit_org_altrnt_nm
        when plan_to_org_relationship_granular = 'MANY IQVIA PLANS TO MANY MMIT PLANS' then plan_name
        when plan_to_org_relationship_granular = 'ONE IQVIA PLAN TO MANY MMIT PLANS' then plan_name
        when plan_to_org_relationship_granular = 'MANY IQVIA PLANS TO ONE MMIT PLAN' then mmit_org_altrnt_nm
        else mmit_org_altrnt_nm
        end as final_plan_name
      from
        dbu.tzp_emp_base4_dt
    ) B
    on upper(a."final plan name") = upper(b.final_plan_name)
    order by plan_id
  )
  where plan_id is not null
);



drop table if exists DT_2_yr_ZEP_PATIENT_FUNNEL_11_FIN;
create temp table DT_2_yr_ZEP_PATIENT_FUNNEL_11_FIN as
(
  select 
    a.patient_id, a.prsn_id,
    /*Rx, Px, Dx Flags*/
    NVL(a.rx_inj_aom_flag, 0) AS rx_inj_aom_flag,
	NVL(a.rx_all_aom_flag, 0) AS rx_all_aom_flag,
    NVL(a.rx_zep_flag, 0) AS rx_zep_flag,
    NVL(a.dx_aom_flag, 0) AS dx_aom_flag,
    NVL(a.px_aom_flag, 0) AS px_aom_flag,
    NVL(a.rx_glp1_flag, 0) AS rx_glp1_flag,
    NVL(a.dx_diab_flag, 0) AS dx_diab_flag,
    /*Plan, MoP, Status*/
    NVL(a.most_freq_rx_payer_pln_id, '-') AS most_freq_rx_payer_pln_id,
    NVL(a.most_freq_rx_mop_ccy, '-') AS most_freq_rx_mop_ccy,
    NVL(a.method_of_payment, '-') AS method_of_payment,
    NVL(a.final_mop, '-') AS final_mop,
    NVL(a.final_plan, '-') AS final_plan,
    nvl(d.dominant_status,'-') as zepbound_final_status_latest_mon,
    nvl(E.dominant_status,'-') as WEGOVY_final_status_latest_mon,
    /*Age*/
    nvl(age_data.pat_age_group,'-') as pat_age_group,
    nvl(age_data.pat_gender_cd,'-') as pat_gender_cd,
    /*Opt In*/
    nvl(opt_in.final_opt_in,'-') as final_opt_in,
    /*Other Comorb, BMI Raw*/
    nvl(bmi_data.diagnosis_desc_new,'-') as diagnosis_desc_new,
    nvl(new_obes.DX_OMM_PED_FLAG, 0) as DX_OMM_PED_FLAG,
    nvl(new_obes.DX_OMM_MORBID_SEVERE_FLAG, 0) as DX_OMM_MORBID_SEVERE_FLAG,
    nvl(new_obes.DX_OMM_OVERWEIGHT_FLAG, 0) as DX_OMM_OVERWEIGHT_FLAG,
    nvl(new_obes.DX_OMM_INDUCED_OTHER_FLAG, 0) as DX_OMM_INDUCED_OTHER_FLAG,
    nvl(new_obes.DX_OMM_COMORB_FLAG, 0) as DX_OMM_COMORB_FLAG,
	nvl(new_obes.DX_O_PREG_BAR_ADDITIONAL_FLAG,0) as DX_O_PREG_BAR_ADDITIONAL_FLAG
    
  from 
    DT_2_yr_ZEP_PATIENT_FUNNEL_8_MOP_PLAN_FIN a 
    /*Zep Status*/
    left join 
    (
      select * from 
      (
        select distinct iqvia_plan_id,dominant_status,lives,
        rank() over (partition by iqvia_plan_id order by lives desc) as lives_rank
        from payer.mmit_frc 
        where "month" = (select max("month") from payer.mmit_frc) and brand='ZEPBOUND' 
      ) where lives_rank=1 and iqvia_plan_id is not null
    ) d
    on a.final_plan = d.iqvia_plan_id
    /*Wegovy Status*/
    left join 
    (
      select * from 
      (
        select distinct iqvia_plan_id,dominant_status,lives,
        rank() over (partition by iqvia_plan_id order by lives desc) as lives_rank
        from payer.mmit_frc 
        where "month" = (select max("month") from payer.mmit_frc) and brand='WEGOVY' 
      ) where lives_rank=1 and iqvia_plan_id is not null
    ) E
    on a.final_plan = e.iqvia_plan_id
    /*Age*/
    left join
    (
      select distinct pat_id, 
      case when pat_age is null then '-' 
      when pat_age >=0 and pat_age <13 then '0-12'
      when pat_age >=13 and pat_age <18 then '13-17'
      when pat_age >=18 and pat_age <26 then '18-25' 
      when pat_age >=26 and pat_age <36 then '26-35'
      when pat_age >=36 and pat_age <46 then '36-45'
      when pat_age >=46 and pat_age <56 then '46-55'
      when pat_age >=56 and pat_age <66 then '56-65'
      when pat_age >=66 then '65+' end as pat_age_group,
      nvl(pat_gender_cd,'-') as pat_gender_cd
      from apld_ex.obs_laad_patient_ref
    ) age_data
    on a.patient_id = age_data.pat_id
    /*Opt In*/
    left join
    (select * from DT_br_eff_access_opt_in_data) opt_in
    on a.final_plan = opt_in.plan_id
    /*BMI*/
    left join
    (select distinct patient_id, diagnosis_desc_new  from dbu.obesity_patients_bmi_mapping) bmi_data
    on a.patient_id = bmi_data.patient_id
    /*Other Comorb*/
    left join
    (select * from DT_2_yr_ZEP_PATIENT_FUNNEL_9_OTHER_COMORB_FIN) new_obes
    on a.patient_id = new_obes.patient_id
);


drop table if exists dbu.DT_2_yr_ZEP_PATIENT_FUNNEL_12_FIN;
create table dbu.DT_2_yr_ZEP_PATIENT_FUNNEL_12_FIN as
(
  select *,
    case when dx_aom_flag = 1 and diagnosis_desc_new in ('BMI 30-34.9', 'BMI 35-39.9','BMI 40+') then 1 else 0 
    end as bmi_incl_flg,
    case when dx_aom_flag = 1 and diagnosis_desc_new in ('BMI 27-29.9') and dx_omm_comorb_flag = 1 then 1 else 0 
    end as bmi_27_29_incl_flg,
    case when dx_aom_flag = 1 and diagnosis_desc_new in ('-','Other or unspecified obesity') 
    and (dx_omm_overweight_flag = 1 or dx_omm_induced_other_flag = 1) and dx_omm_comorb_flag = 1 then 1 else 0 
    end as bmi_unspec_overwt_induc_flg,
    case when DX_O_PREG_BAR_ADDITIONAL_FLAG = 1 and dx_omm_comorb_flag = 1 then 1 else 0 
    end as bmi_o_preg_bar_flg,
    case when dx_aom_flag = 1 and diagnosis_desc_new in ('-','Other or unspecified obesity') 
    and dx_omm_morbid_severe_flag = 1  then 1 else 0 
    end as bmi_unspec_morbid_flg,
    case when dx_aom_flag = 1 and diagnosis_desc_new in ('-','Other or unspecified obesity') 
    and dx_omm_ped_flag = 1  then 1 else 0 
    end as bmi_unspec_ped_flg,
    case when (bmi_27_29_incl_flg=0 and bmi_unspec_overwt_induc_flg=0 and bmi_unspec_morbid_flg=0) 
    and bmi_unspec_ped_flg then 1 else 0 end as ped_incl_flag,
    /*Final incl/excl flags*/
    case when (bmi_incl_flg = 1 or bmi_27_29_incl_flg = 1 or bmi_unspec_overwt_induc_flg = 1 or bmi_unspec_morbid_flg = 1 or bmi_o_preg_bar_flg = 1) 
    and (ped_incl_flag = 0) and (pat_age_group not in ('0-12','13-17'))
    then 1 else 0 end as pre_final_work_final_incl,
    case when (bmi_incl_flg = 1 or bmi_27_29_incl_flg = 1 or bmi_unspec_overwt_induc_flg = 1 or bmi_unspec_morbid_flg = 1 or bmi_o_preg_bar_flg = 1) 
    and (ped_incl_flag = 0) and (rx_glp1_flag = 0)  and (pat_age_group not in ('0-12','13-17'))
    then 1 else 0 end as work_final_incl,
    case when (bmi_incl_flg = 1 or bmi_27_29_incl_flg = 1 or bmi_unspec_overwt_induc_flg = 1 or bmi_unspec_morbid_flg = 1 or bmi_o_preg_bar_flg = 1) 
    and (bmi_unspec_ped_flg = 0) 
    then 1 else 0 end as final_incl_flg
  from 
    DT_2_yr_ZEP_PATIENT_FUNNEL_11_FIN
);