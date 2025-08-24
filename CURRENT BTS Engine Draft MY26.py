# Databricks notebook source
# MAGIC %md
# MAGIC ### Inputs

# COMMAND ----------

# DBTITLE 1,Import Packages
!pip install fuzzywuzzy
import pandas as pd
import numpy as np
from fuzzywuzzy import process

# COMMAND ----------

# DBTITLE 1,USE: User Inputs
new_builds = {
  'R1T Dual Standard': 0,
  'R1S Dual Standard': 37, 
  'R1S Dual Max': 97, 
  'R1T Dual Max': 0, 
  'R1S Dual Large': 251, 
  'R1T Dual Large': 0, 
  'R1S Tri Max': 383, 
  'R1T Tri Max': 0, 
  'R1S Quad Max': 241, 
  'R1T Quad Max': 0 
}


as_of_date = '2025-08-11'
inventory_target_date = '2025-12-31'
#need a timeframe for orders (how many orders of each config_string will we have up until X date) and inventory (how much inventory will we have up until X date)
forecast_id = 342
daily_rate = 200

# COMMAND ----------

# DBTITLE 1,freeze_model_powertrain_list
freeze_model_powertrain_list = []
for key,value in new_builds.items():
  if value != 0:
    freeze_model_powertrain_list.append(key)
print(freeze_model_powertrain_list)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Data Sources

# COMMAND ----------

# DBTITLE 1,DOWNLOAD: Inventory Policy Compliance
policy_df = spark.sql(f"""
select config_string,
model_powertrain,
CASE WHEN is_inventory_policy_eligible = 1 THEN 'Inventory Policy Compliant' ELSE 'Not Compliant' END AS inventory_policy_compliance
from commercial.demand_planning.rep_item_master
where model_year = 2026
and is_inventory_policy_eligible = 1
and model_powertrain in ({', '.join(f"'{item}'" for item in freeze_model_powertrain_list)})
order by model_powertrain asc, config_string asc
""").toPandas()

#and model_powertrain in ({', '.join(f"'{item}'" for item in freeze_model_powertrain_list)})
#197 non-CA configs available


#36 rows of CA Compliant Configs
display(policy_df)

# COMMAND ----------

# DBTITLE 1,NOT DISPLAYED Existing Demand and DOS
# how much demand between as_of_date and inventory_target_date (user inputs) grouped by config_string
# what is the velocity for each config_string between as_of_date and inventory_target_date (user inputs)

demand_df = spark.sql(f"""
with ip_table as (
select config_string,
CASE WHEN is_inventory_policy_eligible = 1 THEN 'Inventory Policy Compliant' ELSE 'Not Compliant' END AS inventory_policy_compliance
from commercial.demand_planning.rep_item_master
where model_year = 2026
and is_inventory_policy_eligible = 1
and model_powertrain in ({', '.join(f"'{item}'" for item in freeze_model_powertrain_list)})
)

select replace(fv.config_string, '2025', '2026') as updated_config_string, 
fv.model_powertrain, 
coalesce(ip.inventory_policy_compliance, 'Not Compliant') as inventory_policy_compliance,
count(fv.order_id) as demand,
count(fv.order_id) / datediff('{inventory_target_date}', '{as_of_date}') as velocity
from sandbox.integrated_planning_poc.forecast_view fv
-- from sandbox.integrated_planning_poc.forecast_burn_down_test fv
left join ip_table ip on replace(fv.config_string, '2025', '2026') = ip.config_string
where fv.model_year = 2026
and fv.model_powertrain in ({', '.join(f"'{item}'" for item in freeze_model_powertrain_list)})
and id = {forecast_id} 
and fv.order_date > '{as_of_date}' 
and fv.order_date <= '{inventory_target_date}'
-- and ip.inventory_policy_compliance = 'Inventory Policy Compliant'
group by all
order by model_powertrain desc, updated_config_string desc, inventory_policy_compliance desc, count(fv.order_id) desc
""").toPandas()

display(demand_df)
#63 rows of canadian orders



# COMMAND ----------

# DBTITLE 1,NOT DISPLAYED Existing Inventory
# how much supply of each config_string exists between as_of_date and inventory_target_date (user inputs)
#based off of actual_factory_gate_date and expected_factory_gate_date

supply_df = spark.sql(f"""
with ip_table as (
select config_string,
CASE WHEN is_inventory_policy_eligible = 1 THEN 'Inventory Policy Compliant' ELSE 'Not Compliant' END AS inventory_policy_compliance
from commercial.demand_planning.rep_item_master
where model_year = 2026
and is_inventory_policy_eligible = 1
and model_powertrain in ({', '.join(f"'{item}'" for item in freeze_model_powertrain_list)})
),

charging_port_table as (
select vehicle_id as cp_vehicle_id, charging_port_option_id
from commercial.digital_commerce.fct_vehicles
)

select
    CASE
        WHEN cp.charging_port_option_id = 'CHRG-NACS01'
        THEN replace(rv.config_string, '2025', '2026')
        ELSE rv.config_string
    END as config_string,
    CASE
        WHEN cp.charging_port_option_id = 'CHRG-NACS01'
        THEN replace(rv.model_year, '2025', '2026')
        ELSE rv.model_year
    END as model_year,

--shouldn't matter anymore with updated fixes
    rv.model_powertrain,
    coalesce(ip.inventory_policy_compliance, 'Not Compliant') as inventory_policy_compliance,
    count(rv.vehicle_id) as supply,
    fv.metadata_source_order
from commercial.demand_planning.rep_vehicles rv
left join charging_port_table cp on rv.vehicle_id = cp.cp_vehicle_id
left join ip_table ip on
    CASE
        WHEN cp.charging_port_option_id = 'CHRG-NACS01'
        THEN replace(rv.config_string, '2025', '2026')
        ELSE rv.config_string
    END = ip.config_string
left join commercial.digital_commerce.fct_vehicles fv on fv.vehicle_id = rv.vehicle_id
where CASE
        WHEN cp.charging_port_option_id = 'CHRG-NACS01'
        THEN replace(rv.model_year, '2025', '2026')
        ELSE rv.model_year
    END = 2026
    and fv.metadata_source_order not like ('%CALTRANS%')
  and rv.match_status is null --unmatched vehicles
  and rv.is_delivered = 0 -- not delivered
  and rv.vin_salability <> 'NON_SALEABLE' --saleable vehicles
  and rv.vehicle_status not in ('CREATED','DECOMMISSIONED','RETURNED','INTERNAL_RECEIVED', 'DELIVERED') -- not liquid, not delivered, not internal
    and rv.vehicle_record_status in ('ACTIVE')
    and rv.vehicle_usage in ('CUSTOMER_DELIVERY')
  and (rv.actual_factory_gate_date is not null or rv.expected_factory_gate_date is not null)
  and coalesce(rv.actual_factory_gate_date, rv.expected_factory_gate_date) < '{inventory_target_date}' 
  and rv.model_powertrain in ({', '.join(f"'{item}'" for item in freeze_model_powertrain_list)})
  -- and ip.inventory_policy_compliance = 'Inventory Policy Compliant'
  group by all
  order by rv.model_powertrain desc, config_string desc, count(rv.vehicle_id) desc
  

""").toPandas()

display(supply_df)

# COMMAND ----------

# DBTITLE 1,Full Demand and Supply DF
#merge all compliant demand and supply pictures
#calculate supply, demand, velocity, and dos

inventory_df = spark.sql(f"""
with ip_table as (
select config_string,
model_powertrain, 
CASE WHEN is_inventory_policy_eligible = 1 THEN 'Inventory Policy Compliant' ELSE 'Not Compliant' END AS inventory_policy_compliance
from commercial.demand_planning.rep_item_master
where model_year = 2026
and is_inventory_policy_eligible = 1
and model_powertrain in ({', '.join(f"'{item}'" for item in freeze_model_powertrain_list)})
and country not in ('CA')
),

charging_port_table as (
select vehicle_id as cp_vehicle_id, charging_port_option_id
from commercial.digital_commerce.fct_vehicles
),


supply_df as (  
select
    CASE
        WHEN cp.charging_port_option_id = 'CHRG-NACS01'
        THEN replace(rv.config_string, '2025', '2026')
        ELSE rv.config_string
    END as updated_config_string,
    CASE
        WHEN cp.charging_port_option_id = 'CHRG-NACS01'
        THEN replace(rv.model_year, '2025', '2026')
        ELSE rv.model_year
    END as model_year,
    rv.model_powertrain,
    coalesce(ip.inventory_policy_compliance, 'Not Compliant') as inventory_policy_compliance,
    count(rv.vehicle_id) as supply
from commercial.demand_planning.rep_vehicles rv
left join charging_port_table cp on rv.vehicle_id = cp.cp_vehicle_id
left join ip_table ip on
    CASE
        WHEN cp.charging_port_option_id = 'CHRG-NACS01'
        THEN replace(rv.config_string, '2025', '2026')
        ELSE rv.config_string
    END = ip.config_string
left join commercial.digital_commerce.fct_vehicles fv on fv.vehicle_id = rv.vehicle_id
where CASE
        WHEN cp.charging_port_option_id = 'CHRG-NACS01'
        THEN replace(rv.model_year, '2025', '2026')
        ELSE rv.model_year
    END = 2026
and fv.metadata_source_order not like ('%CALTRANS%')
  -- new below
  and coalesce(ip.inventory_policy_compliance, 'Not Compliant') = 'Inventory Policy Compliant'
  and rv.match_status is null --unmatched vehicles
  and rv.is_delivered = 0 -- not delivered
  and rv.vin_salability <> 'NON_SALEABLE' --saleable vehicles
  and rv.vehicle_status not in ('CREATED','DECOMMISSIONED','RETURNED','INTERNAL_RECEIVED', 'DELIVERED') -- not liquid, not delivered, not internal
  and rv.vehicle_record_status in ('ACTIVE')
  and rv.vehicle_usage in ('CUSTOMER_DELIVERY')
  and (rv.actual_factory_gate_date is not null or rv.expected_factory_gate_date is not null)
  and coalesce(rv.actual_factory_gate_date, rv.expected_factory_gate_date) < '{inventory_target_date}' 
  and rv.model_powertrain in ({', '.join(f"'{item}'" for item in freeze_model_powertrain_list)})
  and rv.country not in ('CA')
  -- filter out vehicles that are launch edition
  group by all
  order by rv.model_powertrain desc, updated_config_string desc, count(rv.vehicle_id) desc
),

demand_df as (
select fv.config_string,
-- replace(fv.config_string, '2025', '2026') as updated_config_string, 
fv.model_powertrain, 
coalesce(ip.inventory_policy_compliance, 'Not Compliant') as inventory_policy_compliance,
count(fv.order_id) as demand,
count(fv.order_id) / datediff('{inventory_target_date}', '{as_of_date}') as velocity
from sandbox.integrated_planning_poc.forecast_view fv
-- from sandbox.integrated_planning_poc.forecast_burn_down_test fv
left join ip_table ip on fv.config_string = ip.config_string
-- left join ip_table ip on replace(fv.config_string, '2025', '2026') = ip.config_string
where fv.model_year = 2026
-- new below
and coalesce(ip.inventory_policy_compliance, 'Not Compliant') = 'Inventory Policy Compliant'
and fv.model_powertrain in ({', '.join(f"'{item}'" for item in freeze_model_powertrain_list)})
and id = {forecast_id} and fv.order_date > '{as_of_date}' 
and fv.order_date <= '{inventory_target_date}'
and fv.country not in ('CA')
group by all
),

supply_demand_df as (
select coalesce(sd.updated_config_string, dd.config_string) as config_string, 
coalesce(sd.model_powertrain, dd.model_powertrain) as model_powertrain,
coalesce(sd.inventory_policy_compliance, dd.inventory_policy_compliance) as inventory_policy_compliance,
coalesce(sd.supply,0) as supply,
coalesce(dd.demand,0) as demand,
coalesce(dd.velocity,0) as velocity,
coalesce(sd.supply,0) - coalesce(dd.demand,0) as net_supply, 
-- (coalesce(sd.supply,0) - coalesce(dd.demand,0)) / coalesce(dd.velocity,0) as dos
coalesce(coalesce(sd.supply,0) / coalesce(dd.velocity,0),0) as dos
from supply_df sd
full outer join demand_df dd on sd.updated_config_string = dd.config_string
where coalesce(sd.inventory_policy_compliance, dd.inventory_policy_compliance) = 'Inventory Policy Compliant'
order by model_powertrain desc, (coalesce(sd.supply,0) - coalesce(dd.demand,0)) / velocity asc
)

-- we need to left join onto item_master because there may be a config that has not appeared in forecasted demand or in supply picture
-- but does exist in item_master, leaving to SKUs being excluded from the full picture 
-- in current supply and demand df, there are 128 unique SKUs, 
-- but there are 172 unique skus in item_master
select im.config_string, 
coalesce(im.model_powertrain, sdd.model_powertrain) as model_powertrain,
im.inventory_policy_compliance,
coalesce(sdd.supply,0) as supply,
coalesce(sdd.demand,0) as demand,
coalesce(sdd.velocity,0) as velocity,
coalesce(sdd.net_supply,0) as net_supply, 
coalesce(sdd.dos,0) as dos
from ip_table im
left join supply_demand_df sdd on sdd.config_string = im.config_string
order by coalesce(im.model_powertrain, sdd.model_powertrain) desc, coalesce(sdd.dos,0) asc


""").toPandas()

display(inventory_df)

# COMMAND ----------

# DBTITLE 1,DOS - Both Compliant and Non-Compliant
# #merge all compliant demand and supply pictures
# #calculate supply, demand, velocity, and dos

# inventory_df = spark.sql(f"""
# with ip_table as (
# select config_string,
# model_powertrain, 
# CASE WHEN is_inventory_policy_eligible = 1 THEN 'Inventory Policy Compliant' ELSE 'Not Compliant' END AS inventory_policy_compliance
# from commercial.demand_planning.rep_item_master
# where model_year = 2026
# -- and is_inventory_policy_eligible = 1
# and model_powertrain in ('R1S Dual Standard', 'R1T Dual Standard', 'R1S Dual Large', 'R1T Dual Large', 'R1S Dual Max', 'R1T Dual Max', 'R1S Tri Max', 'R1T Tri Max', 'R1S Quad Max', 'R1T Quad Max')
# ),

# charging_port_table as (
# select vehicle_id as cp_vehicle_id, charging_port_option_id
# from commercial.digital_commerce.fct_vehicles
# ),


# supply_df as (  
# select
#     CASE
#         WHEN cp.charging_port_option_id = 'CHRG-NACS01'
#         THEN replace(rv.config_string, '2025', '2026')
#         ELSE rv.config_string
#     END as updated_config_string,
#     CASE
#         WHEN cp.charging_port_option_id = 'CHRG-NACS01'
#         THEN replace(rv.model_year, '2025', '2026')
#         ELSE rv.model_year
#     END as model_year,
#     rv.model_powertrain,
#     coalesce(ip.inventory_policy_compliance, 'Not Compliant') as inventory_policy_compliance,
#     count(rv.vehicle_id) as supply
# from commercial.demand_planning.rep_vehicles rv
# left join charging_port_table cp on rv.vehicle_id = cp.cp_vehicle_id
# left join ip_table ip on
#     CASE
#         WHEN cp.charging_port_option_id = 'CHRG-NACS01'
#         THEN replace(rv.config_string, '2025', '2026')
#         ELSE rv.config_string
#     END = ip.config_string
# where CASE
#         WHEN cp.charging_port_option_id = 'CHRG-NACS01'
#         THEN replace(rv.model_year, '2025', '2026')
#         ELSE rv.model_year
#     END = 2026
#   -- new below
#   -- and coalesce(ip.inventory_policy_compliance, 'Not Compliant') = 'Inventory Policy Compliant'
#   and rv.match_status is null --unmatched vehicles
#   and rv.is_delivered = 0 -- not delivered
#   and rv.vin_salability <> 'NON_SALEABLE' --saleable vehicles
#   and rv.vehicle_status not in ('CREATED','DECOMMISSIONED','RETURNED','INTERNAL_RECEIVED', 'DELIVERED') -- not liquid, not delivered, not internal
#   and (rv.actual_factory_gate_date is not null or rv.expected_factory_gate_date is not null)
#   and coalesce(rv.actual_factory_gate_date, rv.expected_factory_gate_date) < '{inventory_target_date}' 
#   and rv.model_powertrain in ({', '.join(f"'{item}'" for item in freeze_model_powertrain_list)})
#   group by all
#   order by rv.model_powertrain desc, updated_config_string desc, count(rv.vehicle_id) desc
# ),

# demand_df as (
# select fv.config_string,
# -- replace(fv.config_string, '2025', '2026') as updated_config_string, 
# fv.model_powertrain, 
# coalesce(ip.inventory_policy_compliance, 'Not Compliant') as inventory_policy_compliance,
# count(fv.order_id) as demand,
# count(fv.order_id) / datediff('{inventory_target_date}', '{as_of_date}') as velocity
# from sandbox.integrated_planning_poc.forecast_view fv
# -- from sandbox.integrated_planning_poc.forecast_burn_down_test fv
# left join ip_table ip on fv.config_string = ip.config_string
# -- left join ip_table ip on replace(fv.config_string, '2025', '2026') = ip.config_string
# where fv.model_year = 2026
# -- new below
# -- and coalesce(ip.inventory_policy_compliance, 'Not Compliant') = 'Inventory Policy Compliant'
# and fv.model_powertrain in ({', '.join(f"'{item}'" for item in freeze_model_powertrain_list)})
# and id = {forecast_id} and fv.order_date > '{as_of_date}' 
# and fv.order_date <= '{inventory_target_date}'
# group by all
# ),

# supply_demand_df as (
# select coalesce(sd.updated_config_string, dd.config_string) as config_string, 
# coalesce(sd.model_powertrain, dd.model_powertrain) as model_powertrain,
# coalesce(sd.inventory_policy_compliance, dd.inventory_policy_compliance) as inventory_policy_compliance,
# coalesce(sd.supply,0) as supply,
# coalesce(dd.demand,0) as demand,
# coalesce(dd.velocity,0) as velocity,
# coalesce(sd.supply,0) - coalesce(dd.demand,0) as net_supply, 
# -- (coalesce(sd.supply,0) - coalesce(dd.demand,0)) / coalesce(dd.velocity,0) as dos
# coalesce(coalesce(sd.supply,0) / coalesce(dd.velocity,0),0) as dos
# from supply_df sd
# full outer join demand_df dd on sd.updated_config_string = dd.config_string
# -- where coalesce(sd.inventory_policy_compliance, dd.inventory_policy_compliance) = 'Inventory Policy Compliant'
# order by model_powertrain desc, (coalesce(sd.supply,0) - coalesce(dd.demand,0)) / velocity asc
# )

# -- we need to left join onto item_master because there may be a config that has not appeared in forecasted demand or in supply picture
# -- but does exist in item_master, leaving to SKUs being excluded from the full picture 
# -- in current supply and demand df, there are 128 unique SKUs, 
# -- but there are 172 unique skus in item_master
# select im.config_string, 
# coalesce(im.model_powertrain, sdd.model_powertrain) as model_powertrain,
# im.inventory_policy_compliance,
# coalesce(sdd.supply,0) as supply,
# coalesce(sdd.demand,0) as demand,
# coalesce(sdd.velocity,0) as velocity,
# coalesce(sdd.net_supply,0) as net_supply, 
# coalesce(sdd.dos,0) as dos
# from ip_table im
# left join supply_demand_df sdd on sdd.config_string = im.config_string
# order by coalesce(im.model_powertrain, sdd.model_powertrain) desc, coalesce(sdd.dos,0) asc


# """).toPandas()

# display(inventory_df)

# COMMAND ----------

# DBTITLE 1,Calculate net_supply, velocity, dos
inventory_df['velocity'] = [(1/(daily_rate)) if x == 0 else x for x in inventory_df['velocity']]
#because we recalculated velocity if it is 0 to be 1/daily_rate we need to recalculate dos as well
inventory_df['dos'] = inventory_df['supply']/inventory_df['velocity']

# inventory_df['dos'] = inventory_df['net_supply']/inventory_df['velocity']
display(inventory_df)

# COMMAND ----------

# DBTITLE 1,Excluded Config Strings
launch_edition_quad_excluded_configs = [
'2026_US_R1S Quad Max_EXP-LGR_INT-PBMP_WHL-2SP_AUD-P01_ROOF-DGP_TON-NULL_ACTBDG-DRK_UTL-S01',
'2026_US_R1T Quad Max_EXP-LGR_INT-PBMP_WHL-0DD_AUD-P01_ROOF-DGP_TON-P02_ACTBDG-DRK_UTL-T01',
'2026_US_R1T Quad Max_EXP-LGR_INT-PBMP_WHL-0AD_AUD-P01_ROOF-DGP_TON-P02_ACTBDG-DRK_UTL-T01',
'2026_US_R1T Quad Max_EXP-LGR_INT-PBMP_WHL-2SB_AUD-P01_ROOF-DGP_TON-P02_ACTBDG-DRK_UTL-T01',
'2026_US_R1S Quad Max_EXP-LGR_INT-PBMP_WHL-2SB_AUD-P01_ROOF-DGP_TON-NULL_ACTBDG-DRK_UTL-S01',
'2026_US_R1S Quad Max_EXP-LGR_INT-PBMP_WHL-0AD_AUD-P01_ROOF-DGP_TON-NULL_ACTBDG-DRK_UTL-S01',
'2026_US_R1T Quad Max_EXP-LGR_INT-PBMP_WHL-2SP_AUD-P01_ROOF-DGP_TON-P02_ACTBDG-DRK_UTL-T01',
'2026_CA_R1S Quad Max_EXP-LGR_INT-PBMP_WHL-0AD_AUD-P01_ROOF-DGP_TON-NULL_ACTBDG-DRK_UTL-S01',
'2026_US_R1S Quad Max_EXP-LGR_INT-PBMP_WHL-0DD_AUD-P01_ROOF-DGP_TON-NULL_ACTBDG-DRK_UTL-S01',
'2026_CA_R1S Quad Max_EXP-LGR_INT-PBMP_WHL-2SB_AUD-P01_ROOF-DGP_TON-NULL_ACTBDG-DRK_UTL-S01',
'2026_CA_R1T Quad Max_EXP-LGR_INT-PBMP_WHL-0AD_AUD-P01_ROOF-DGP_TON-P02_ACTBDG-DRK_UTL-T01',
'2026_CA_R1T Quad Max_EXP-LGR_INT-PBMP_WHL-2SB_AUD-P01_ROOF-DGP_TON-P02_ACTBDG-DRK_UTL-T01',
'2026_CA_R1S Quad Max_EXP-LGR_INT-PBMP_WHL-2SP_AUD-P01_ROOF-DGP_TON-NULL_ACTBDG-DRK_UTL-S01'
]

# non_le_quad_excluded_configs = ['2026_US_R1S Quad Max_EXP-GWT_INT-SSWW_WHL-2SP_AUD-P01_ROOF-DGP_TON-NULL_ACTBDG-DRK_UTL-S01',
# '2026_US_R1T Quad Max_EXP-ELG_INT-PBMP_WHL-0AD_AUD-P01_ROOF-DGP_TON-P02_ACTBDG-DRK_UTL-T01',
# '2026_US_R1T Quad Max_EXP-MDN_INT-PBMP_WHL-2SP_AUD-P01_ROOF-DGP_TON-P02_ACTBDG-DRK_UTL-T01',
# '2026_US_R1S Quad Max_EXP-FGR_INT-OCDW_WHL-2SP_AUD-P01_ROOF-DGP_TON-NULL_ACTBDG-DRK_UTL-S01',
# '2026_US_R1S Quad Max_EXP-ELG_INT-PBMP_WHL-0AD_AUD-P01_ROOF-DGP_TON-NULL_ACTBDG-DRK_UTL-S01',
# '2026_US_R1S Quad Max_EXP-SBL_INT-PBMP_WHL-0DD_AUD-P01_ROOF-DGP_TON-NULL_ACTBDG-DRK_UTL-S01',
# '2026_US_R1T Quad Max_EXP-SBL_INT-PBMP_WHL-0DD_AUD-P01_ROOF-DGP_TON-P02_ACTBDG-DRK_UTL-T01',
# '2026_US_R1T Quad Max_EXP-GWT_INT-SSWW_WHL-2SP_AUD-P01_ROOF-DGP_TON-P02_ACTBDG-DRK_UTL-T01',
# '2026_US_R1S Quad Max_EXP-MDN_INT-PBMP_WHL-2SP_AUD-P01_ROOF-DGP_TON-NULL_ACTBDG-DRK_UTL-S01',
# '2026_US_R1S Quad Max_EXP-SBL_INT-OCDW_WHL-2SP_AUD-P01_ROOF-DGP_TON-NULL_ACTBDG-DRK_UTL-S01',
# '2026_US_R1S Quad Max_EXP-FGR_INT-PBMP_WHL-0AD_AUD-P01_ROOF-DGP_TON-NULL_ACTBDG-DRK_UTL-S01'
# ]


# COMMAND ----------

# DBTITLE 1,DOWNLOAD - DOS ANALYSIS: Exclude Configs from Inventory DF
inventory_df = inventory_df.loc[~inventory_df['config_string'].isin(launch_edition_quad_excluded_configs)]

inventory_df['dos'].fillna(0, inplace=True)

display(inventory_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### BTS Engine

# COMMAND ----------

# MAGIC %md
# MAGIC ### BTS Logic
# MAGIC For each model_powertrain
# MAGIC - Look at the config_string with the smallest days of supply
# MAGIC - Add 1 unit to the config with smallest days of supply
# MAGIC - subtract from the added_volume from user input
# MAGIC - run this loop until added_volume is zero
# MAGIC - move on to the next model_powertrain
# MAGIC
# MAGIC
# MAGIC - add TypeError and add a catch for if there is zero supply (take the take rate pct for each config string and just multiply by the added volume)

# COMMAND ----------

# DBTITLE 1,BTS Engine Logic
#this will be skewed towards inventory policy take rates
#it may not align with actual take rates


builds = {}
output_df = pd.DataFrame(columns=['config_string', 'net_supply', 'velocity','dos','added_volume','new_dos'])
df_list = []
#segment by model powertrain
for model_powertrain, additional_volume in new_builds.items():
    print(f"Model: {model_powertrain}, Count: {additional_volume}")
    #set up columns for supply, demand, dos, added_volume, etc
    mp_config_df = inventory_df[inventory_df['model_powertrain'] == model_powertrain].copy()
    mp_config_df['added_volume'] = 0.0
    mp_config_df['new_net_supply'] = mp_config_df['added_volume'] + mp_config_df['net_supply']
    mp_config_df['new_dos'] = mp_config_df['new_net_supply'] / mp_config_df['velocity']
    # mp_config_df['new_dos'] = mp_config_df['demand'] / mp_config_df['velocity']
    # mp_config_df['new_dos'] = mp_config_df['supply'] / mp_config_df['velocity']




    #give a new index
    mp_config_df.reset_index(inplace=True, drop=True)
    while additional_volume > 0:
        #ie) while additional_volume is 38, greater than 0
        #pick out the index with lowest days of supply
        index = mp_config_df['new_dos'].idxmin()
        # print(mp_config_df.loc[index, 'new_dos']) #Comment this out 

        #remove one from the additional_volume (vehicles we need to add) and add to added_volume
        #acts as a counter
        additional_volume -= 1

        #at row where the index has the lowest dos, add one vehicle
        mp_config_df.at[index, 'added_volume'] += 1

        #recalculate your net supply
        mp_config_df['new_net_supply'] = mp_config_df['added_volume'] + mp_config_df['net_supply']

        #recalculate your dos
        mp_config_df['new_dos'] = mp_config_df['new_net_supply'] / mp_config_df['velocity']
    df_list.append(mp_config_df)
output_df = pd.concat(df_list)
    

# COMMAND ----------

# DBTITLE 1,Row Generation
def expand_rows_based_on_volume(df):
    # Create a list to hold the expanded rows
    expanded_rows = []
    
    # Iterate over each row in the DataFrame
    for _, row in df.iterrows():
        # Repeat the 'config_string' based on 'added_volume'
        expanded_rows.extend([row['config_string']] * int(row['added_volume']))
    
    # Create a new DataFrame from the expanded rows
    expanded_df = pd.DataFrame(expanded_rows, columns=['config_string'])
    
    return expanded_df

# Use the method on output_df
expanded_df = expand_rows_based_on_volume(output_df)

# Display the expanded DataFrame
display(expanded_df)

# COMMAND ----------

# DBTITLE 1,Extracting the necessary columns
config_attributes_df = spark.sql(
    f""" select 
    config_string,
    model_powertrain, 
    model as build_line_items_sku,
    powertrain,
    case when country = 'US' then 'RGN-USA' else 'RGN-CAN'
    end as build_country,
    paint_id as build_items_exp_exterior_id,
    interior_id as build_items_int_interior_id,
    wheels_id as build_items_whl_wheels_id,
    tonneau_id as build_items_accessories_tonneau_cover_id,
    accents_and_badging_id as build_items_badging_id,
    roof_id as build_items_roof_glass_id,
    audio_id as build_items_audio_id,
    utility_panel_id as build_items_utl_utility_panel_id
    from commercial.demand_planning.rep_item_master
    where is_inventory_policy_eligible = 1
                   """
).toPandas()
#    package_id as build_items_pkg_trim_id, 
#    camp_speaker_id as build_camp_speaker_id



expanded_df = expanded_df.merge(config_attributes_df, left_on='config_string', right_on='config_string', how='left')
expanded_df['production_zone'] = 'Liquid/To Freeze'
expanded_df['vo_vehicle_id'] = ''


column_order = ['vo_vehicle_id', 'production_zone', 'config_string', 'model_powertrain', 'build_line_items_sku', 'powertrain', 'build_country', 'build_items_exp_exterior_id', 'build_items_int_interior_id', 
                'build_items_whl_wheels_id', 'build_items_accessories_tonneau_cover_id', 'build_items_badging_id',
                'build_items_roof_glass_id', 'build_items_audio_id', 'build_items_utl_utility_panel_id',
                'production_zone'
                ]
expanded_df = expanded_df[column_order]
#'build_items_pkg_trim_id',
#'build_camp_speaker_id', 

# display(expanded_df)

# COMMAND ----------

# DBTITLE 1,Build Option ID Definition
def add_build_option_id_column (df:pd.DataFrame):
    df['build_option_id'] = pd.NA
    df.loc[df['model_powertrain'].isin(['R1S Dual Standard']), 'build_option_id'] = 'BLD-DSR1S'
    df.loc[df['model_powertrain'].isin(['R1S Dual Large', 'R1S Dual Max']), 'build_option_id'] = 'BLD-DR1S'
    df.loc[df['model_powertrain'].isin(['R1S Tri Max']), 'build_option_id'] = 'BLD-TR1S'
    df.loc[df['model_powertrain'].isin(['R1S Quad Max']), 'build_option_id'] = 'BLD-QR1S'
    df.loc[df['model_powertrain'].isin(['R1S Quad Launch']), 'build_option_id'] = 'BLD-QLR1S'
    df.loc[df['build_line_items_sku'].isin(['R1S']) & df['build_items_exp_exterior_id'].isin(['EXP-CDN']), 'build_option_id'] = 'BLD-TDR1S'


    	
    df.loc[df['model_powertrain'].isin(['R1T Dual Standard']), 'build_option_id'] = 'BLD-DSR1T'
    df.loc[df['model_powertrain'].isin(['R1T Dual Large', 'R1T Dual Max']), 'build_option_id'] = 'BLD-DR1T'
    df.loc[df['model_powertrain'].isin(['R1T Tri Max']), 'build_option_id'] = 'BLD-TR1T'
    df.loc[df['model_powertrain'].isin(['R1T Quad Max']), 'build_option_id'] = 'BLD-QR1T'
    df.loc[df['model_powertrain'].isin(['R1T Quad Launch']), 'build_option_id'] = 'BLD-QLR1T'
    df.loc[df['build_line_items_sku'].isin(['R1T']) & df['build_items_exp_exterior_id'].isin(['EXP-CDN']), 'build_option_id'] = 'BLD-TDR1T'

    return df

def add_sku_string_column (df:pd.DataFrame):
    df['sku_string'] = pd.NA
    df['sku_string'] = df['model_powertrain'] + " - " + df['build_items_exp_exterior_id'] + " - " + df['build_items_int_interior_id'] + " - " + df['build_items_whl_wheels_id'] + " - " + df['build_items_accessories_tonneau_cover_id'] + " - " + df['build_items_badging_id'] + " - " + df['build_items_roof_glass_id'] + " - " + df['build_items_audio_id'] + " - " + df['build_items_utl_utility_panel_id']

    return df

add_build_option_id_column(expanded_df)
add_sku_string_column(expanded_df)
# display(expanded_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Liquid Zone

# COMMAND ----------

# DBTITLE 1,Ingest Liquid Zone
liquid_zone_df = spark.sql(
    f""" with charging_port_table as (
select vehicle_id as cp_vehicle_id, charging_port_option_id
from commercial.digital_commerce.fct_vehicles
),

    vehicle_table as (
    select
        rv.vehicle_id as vo_vehicle_id,
    CASE
        WHEN cp.charging_port_option_id = 'CHRG-NACS01'
        THEN replace(rv.config_string, '2025', '2026')
        ELSE rv.config_string
    END as config_string,
    CASE
        WHEN cp.charging_port_option_id = 'CHRG-NACS01'
        THEN replace(rv.model_year, '2025', '2026')
        ELSE rv.model_year
    END as model_year,
    vin,
        model as build_line_items_sku,
        model_powertrain,
        motor_id as build_items_mot_drivesystem_id,
        battery_id as build_items_bat_range_id,
        paint_id as build_items_exp_exterior_id,
        interior_id as build_items_int_interior_id,
        wheels_id as build_items_whl_wheels_id,
        tonneau_id as build_items_accessories_tonneau_cover_id,
        accents_and_badging_id as build_items_badging_id,
        roof_id as build_items_roof_glass_id,
        audio_id as build_items_audio_id,
        utility_panel_id as build_items_utl_utility_panel_id,
        camp_speaker_id as build_camp_speaker_id,
        distribution_center,
        vehicle_generation,
        match_status,
        vehicle_usage,
        vehicle_status,
        vehicle_record_status,
        expected_ga_in_date,
        actual_ga_in_date,
        expected_ga_in_date,
        actual_factory_gate_date,
        expected_build_date_ip,
        case
            when
                vehicle_status in ('PRODUCTION_CONTROL_FROZEN') and vin is null
                then 'Frozen'
            when
                vehicle_status in ('PRODUCTION_CONTROL_FROZEN')
                and vin is not null
                then 'Firm/ VINned'
            when vehicle_status in ('DELIVERED') then 'Delivered'
            when vehicle_status in (
                'PRODUCTION_STARTED',
                'ARRIVED_AT_DISTRIBUTION_CENTER',
                'FACTORY_GATED',
                'FACTORY_OK',
                'GENERAL_ASSEMBLY_COMPLETED',
                'IN_TRANSIT_TO_DISTRIBUTION_CENTER',
                'INTERNAL_RECEIVED',
                'PDI_COMPLETED',
                'PDI_STARTED',
                'RETURNED',
                'STAGED_FOR_PDI',
                'VEHICLE_PREPPED'
            ) and vin is not null then 'Already Built/ WIP'
            when vehicle_status in ('DECOMMISSIONED') then 'Decommissioned'
            when
                vehicle_status in ('CREATED')
                and vehicle_record_status in ('ACTIVE')
                then 'Liquid'
            else 'Investigate'
        end as production_zone,
        case
            when vin is not null then 'vin_created'
            else 'vin_not_created'
        end as vin_status
    from commercial.demand_planning.rep_vehicles rv
    left join charging_port_table cp on rv.vehicle_id = cp.cp_vehicle_id
    where
        model in ('R1T', 'R1S')
        and vehicle_generation in ('GEN-2')
),

core_vehicle_tbl as (
    select
        vehicle_id,
        metadata_source_order,
        case
            when lower(metadata_source_order) like '%custom%' then 'Custom'
            else 'Inventory'
        end as custom_v_inventory_flag,
        split(metadata_source_order, '_')[0] as intended_order,
        split(metadata_source_order, '_')[1] as custom_v_inventory_split,
        to_date(split(metadata_source_order, '_')[2], 'yyyyMMdd') as freeze_date
    from
        commercial.staging.stg_dc_commercial__coresvcs_vehicle
)

select
    *,
    -- expected date for when a vehicle starts production 
    case
        when
            production_zone in ('Frozen', 'Firm/ VINned')
            then expected_ga_in_date
        when
            vehicle_status in (
                'GENERAL_ASSEMBLY_COMPLETED', 'FACTORY_OK', 'PRODUCTION_STARTED'
            )
            -- then expected_ga_in_date
            then actual_ga_in_date
        when
            production_zone in ('Already Built/ WIP')
            -- then actual_factory_gate_date
            then actual_ga_in_date
        when production_zone in ('Liquid') then expected_build_date_ip
    else current_date()
    end as sequence_date
from vehicle_table as vt
left join core_vehicle_tbl as cv
    on vt.vo_vehicle_id = cv.vehicle_id
where production_zone = 'Liquid'
-- expected ga start limitation filter
                   """
).toPandas()

display(liquid_zone_df)

# COMMAND ----------

# DBTITLE 1,Exact Match Engine
import pandas as pd

def assign_vehicle_ids(df1: pd.DataFrame, df2: pd.DataFrame):
    """Assigns vehicle IDs from file2 to file1 based on matching SKU strings, avoiding reuse.
    If an exact matching vehicle doesn't exist, sets vo_vehicle_id to "exact matching vehicle not found".
    """

    used_ids = set()
    assigned_ids = []

    for sku in df1['config_string']:
        match_found = False
        for index, row in df2[df2['config_string'] == sku].iterrows():
            vehicle_id = row['vo_vehicle_id']
            if vehicle_id not in used_ids:
                assigned_ids.append(vehicle_id)
                used_ids.add(vehicle_id)
                match_found = True
                break
        if not match_found:
            assigned_ids.append("exact matching vehicle not found")

    df1_copy = df1.copy()
    df1_copy['vo_vehicle_id'] = assigned_ids
    return df1_copy


result = assign_vehicle_ids(expanded_df, liquid_zone_df)
display(result)

# COMMAND ----------

# DBTITLE 1,Ingest Liquid Zone 2
compliant_liquid_zone_df = spark.sql(
    f""" with charging_port_table as (
select vehicle_id as cp_vehicle_id, charging_port_option_id
from commercial.digital_commerce.fct_vehicles
),
    vehicle_table as (
    select
        vehicle_id as vo_vehicle_id,
        vin,
    CASE
        WHEN cp.charging_port_option_id = 'CHRG-NACS01'
        THEN replace(rv.config_string, '2025', '2026')
        ELSE rv.config_string
    END as config_string,
    CASE
        WHEN cp.charging_port_option_id = 'CHRG-NACS01'
        THEN replace(rv.model_year, '2025', '2026')
        ELSE rv.model_year
    END as model_year,
        model as build_line_items_sku,
        model_powertrain,
        motor_id as build_items_mot_drivesystem_id,
        battery_id as build_items_bat_range_id,
        paint_id as build_items_exp_exterior_id,
        interior_id as build_items_int_interior_id,
        wheels_id as build_items_whl_wheels_id,
        tonneau_id as build_items_accessories_tonneau_cover_id,
        accents_and_badging_id as build_items_badging_id,
        roof_id as build_items_roof_glass_id,
        audio_id as build_items_audio_id,
        utility_panel_id as build_items_utl_utility_panel_id,
        camp_speaker_id as build_camp_speaker_id,
        distribution_center,
        vehicle_generation,
        match_status,
        vehicle_usage,
        vehicle_status,
        vehicle_record_status,
        expected_ga_in_date,
        actual_ga_in_date,
        expected_ga_in_date,
        actual_factory_gate_date,
        expected_build_date_ip,
        case
            when
                vehicle_status in ('PRODUCTION_CONTROL_FROZEN') and vin is null
                then 'Frozen'
            when
                vehicle_status in ('PRODUCTION_CONTROL_FROZEN')
                and vin is not null
                then 'Firm/ VINned'
            when vehicle_status in ('DELIVERED') then 'Delivered'
            when vehicle_status in (
                'PRODUCTION_STARTED',
                'ARRIVED_AT_DISTRIBUTION_CENTER',
                'FACTORY_GATED',
                'FACTORY_OK',
                'GENERAL_ASSEMBLY_COMPLETED',
                'IN_TRANSIT_TO_DISTRIBUTION_CENTER',
                'INTERNAL_RECEIVED',
                'PDI_COMPLETED',
                'PDI_STARTED',
                'RETURNED',
                'STAGED_FOR_PDI',
                'VEHICLE_PREPPED'
            ) and vin is not null then 'Already Built/ WIP'
            when vehicle_status in ('DECOMMISSIONED') then 'Decommissioned'
            when
                vehicle_status in ('CREATED')
                and vehicle_record_status in ('ACTIVE')
                then 'Liquid'
            else 'Investigate'
        end as production_zone,
        case
            when vin is not null then 'vin_created'
            else 'vin_not_created'
        end as vin_status
    from commercial.demand_planning.rep_vehicles rv
    left join charging_port_table cp on rv.vehicle_id = cp.cp_vehicle_id
    where
        model in ('R1T', 'R1S')
        and vehicle_generation in ('GEN-2')
),

core_vehicle_tbl as (
    select
        vehicle_id,
        metadata_source_order,
        case
            when lower(metadata_source_order) like '%custom%' then 'Custom'
            else 'Inventory'
        end as custom_v_inventory_flag,
        split(metadata_source_order, '_')[0] as intended_order,
        split(metadata_source_order, '_')[1] as custom_v_inventory_split,
        to_date(split(metadata_source_order, '_')[2], 'yyyyMMdd') as freeze_date
    from
        commercial.staging.stg_dc_commercial__coresvcs_vehicle
)
select
    *,
    -- expected date for when a vehicle starts production 
    case
        when
            production_zone in ('Frozen', 'Firm/ VINned')
            then expected_ga_in_date
        when
            vehicle_status in (
                'GENERAL_ASSEMBLY_COMPLETED', 'FACTORY_OK', 'PRODUCTION_STARTED'
            )
            -- then expected_ga_in_date
            then actual_ga_in_date
        when
            production_zone in ('Already Built/ WIP')
            -- then actual_factory_gate_date
            then actual_ga_in_date
        when production_zone in ('Liquid') then expected_build_date_ip
    else current_date()
    end as sequence_date
from vehicle_table as vt
left join core_vehicle_tbl as cv
    on vt.vo_vehicle_id = cv.vehicle_id
where production_zone = 'Liquid'
                   """
).toPandas()

display(compliant_liquid_zone_df)

# COMMAND ----------

# DBTITLE 1,Inventory Policy Compliance
inventory_policy_df = spark.sql(
    f""" select
    config_string, 
    case
    when is_inventory_policy_eligible = 1 then 'Inventory Policy Compliant'
    else 'Not Compliant'
    end as inventory_policy_compliance
    from commercial.demand_planning.rep_item_master 
""" ).toPandas()

display(inventory_policy_df)

# COMMAND ----------

compliant_liquid_zone_df = compliant_liquid_zone_df.merge(inventory_policy_df, how='left', left_on='config_string', right_on='config_string')
display(compliant_liquid_zone_df)

# COMMAND ----------

# DBTITLE 1,Merge Compliance?
# compliant_liquid_zone_df = compliant_liquid_zone_df.merge(inventory_policy_df, how='left', left_on='config_string', right_on='config_string')
compliant_liquid_zone_df['inventory_policy_compliance'].fillna('Not Compliant', inplace=True)
compliant_liquid_zone_df = compliant_liquid_zone_df.loc[compliant_liquid_zone_df['inventory_policy_compliance'].isin(['Inventory Policy Compliant'])]
display(compliant_liquid_zone_df)

# COMMAND ----------

# DBTITLE 1,Similar Match Engine
import pandas as pd
from fuzzywuzzy import process, fuzz

def extract_vehicle_prefix(sku_string):
    parts = sku_string.split("_", 3)
    return parts[0] if parts else sku_string

def assign_vehicle_ids_similar_optimized(df1: pd.DataFrame, df2: pd.DataFrame):
    """Optimized version using apply and process.extractOne with fixed vehicle prefix check."""

    # Pre-compute prefixes and suffixes
    df1['prefix'] = df1['config_string'].apply(extract_vehicle_prefix)
    df1['suffix'] = df1.apply(lambda row: row['config_string'][len(row['prefix']):].strip(), axis=1)
    df2['prefix'] = df2['config_string'].apply(extract_vehicle_prefix)
    df2['suffix'] = df2.apply(lambda row: row['config_string'][len(row['prefix']):].strip(), axis=1)

    used_ids = set(df1.dropna(subset=['vo_vehicle_id'])['vo_vehicle_id'].unique())
    available_df2 = df2[~df2['vo_vehicle_id'].isin(used_ids)]

    results = []
    for _, row in df1.iterrows():
        if row['vo_vehicle_id'] == 'exact matching vehicle not found':
            prefix_matches = available_df2[available_df2['prefix'] == row['prefix']]
            if not prefix_matches.empty:
                best_match = process.extractOne(row['suffix'], prefix_matches['suffix'].tolist(), scorer=fuzz.ratio)
                if best_match:
                    best_match_suffix = best_match[0]
                    best_match_row = prefix_matches[prefix_matches['suffix'] == best_match_suffix].iloc[0]
                    results.append((best_match_row['vo_vehicle_id'], best_match_row['config_string']))
                    used_ids.add(best_match_row['vo_vehicle_id']) #add the used ID.
                    available_df2 = available_df2[available_df2['vo_vehicle_id'] != best_match_row['vo_vehicle_id']] #remove the used ID.
                else:
                    results.append((None, None))
            else:
                results.append((None, None))
        else:
            results.append((None, None))

    df1['best_match'] = [result[0] for result in results]
    df1['best_matching_sku_string'] = [result[1] for result in results]

    df1.drop(columns=['prefix', 'suffix'], inplace=True) #remove the extra columns.
    df2.drop(columns=['prefix', 'suffix'], inplace=True) #remove the extra columns.

    return df1
result_df = assign_vehicle_ids_similar_optimized(result, compliant_liquid_zone_df)
display(result_df)

# COMMAND ----------

# DBTITLE 1,Pre-Final Inventory Freeze Load Output
import pandas as pd

prefix_to_column = {
    'EXP': 'to_exterior',
    'INT': 'to_interior',
    'WHL': 'to_wheel',
    'TON': 'to_tonneau',
    'ACTBDG': 'to_badging',
    'ROOF': 'to_roof',
    'AUD': 'to_audio',
    'UTL': 'to_utility',
    'SPKR': 'to_camp_speaker',
}

# Function to process each row
def assign_columns(row):
    # Initialize a dictionary to hold the assigned columns
    assigned_columns = {col: None for col in prefix_to_column.values()}
    
    # Check if the best_matching_sku_string is not None
    if row['best_matching_sku_string'] is not None:
        # Split the best_match column by " - "
        matches = row['best_matching_sku_string'].split(' - ')
        # matches = row['best_matching_sku_string'].split('_')

        
        # Assign values to the appropriate columns based on the prefix
        for match in matches:
            prefix = match.split('-')[0]
            if prefix in prefix_to_column:
                assigned_columns[prefix_to_column[prefix]] = match
    
    return pd.Series(assigned_columns)

# Apply the function to each row and concatenate the results with the original DataFrame
result = pd.concat([result_df, result_df.apply(assign_columns, axis=1)], axis=1)

display(result)

# COMMAND ----------

# DBTITLE 1,DOWNLOAD REVISE OUTPUT 6/23
import pandas as pd
import re # Make sure 're' is imported for regex if needed elsewhere, though not explicitly used in this split function.

def revise_result_output(df: pd.DataFrame):
  df_copy = df.copy()

  df_copy['vehicle_id'] = df_copy['vo_vehicle_id']
  df_copy['action'] = 'Freeze'
  df_copy['configuration_string'] = df_copy['config_string']

  not_found_mask = (df_copy['vo_vehicle_id'] == 'exact matching vehicle not found')

  df_copy.loc[not_found_mask, 'action'] = 'Reconfigure then freeze'
  df_copy.loc[not_found_mask, 'vehicle_id'] = df_copy.loc[not_found_mask, 'best_match']

  df_copy = df_copy[['vehicle_id', 'action', 'configuration_string']]

  return df_copy


def split_config_string_to_attributes(df: pd.DataFrame):
    attribute_prefix_map = {
        'EXP': 'build_exterior',
        'INT': 'build_interior',
        'WHL': 'build_wheels',
        'AUD': 'build_audio',
        'ROOF': 'build_roof',
        'TON': 'build_tonneau',
        'ACTBDG': 'build_badging',
        'UTL': 'build_utility',
    }

    final_column_order = [
        'build_model_year',
        'build_country',
        'build_model_powertrain',
        'build_exterior',
        'build_interior',
        'build_wheels',
        'build_audio',
        'build_roof',
        'build_tonneau',
        'build_badging',
        'build_utility'
    ]

    for col_name in final_column_order:
        df[col_name] = pd.NA

    # Process each row
    for index, row in df.iterrows():
        # Ensure we're reading from the 'configuration_string' column created by revise_result_output
        config_string = row['configuration_string'] 

        if isinstance(config_string, str) and config_string.strip():
            parts = config_string.split('_')

            if len(parts) > 0:
                df.loc[index, 'build_model_year'] = parts[0]

            if len(parts) > 1:
                country_code = parts[1]
                if country_code == 'US':
                    df.loc[index, 'build_country'] = 'US'
                elif country_code == 'CA':
                    df.loc[index, 'build_country'] = 'CA'
                else:
                    df.loc[index, 'build_country'] = country_code

            if len(parts) > 2:
                df.loc[index, 'build_model_powertrain'] = parts[2]

            for part in parts[3:]:
                for prefix, column_name in attribute_prefix_map.items():
                    if part.startswith(prefix + '-'):
                        df.loc[index, column_name] = part
                        break

    return df # Return df with new columns


result_with_basic_info = revise_result_output(result.copy()) 


#join the software upgrade and build option and charger from rep_vehicles
software_upgrade_build_id_charger_details = spark.sql(f"""
select vehicle_id as rv_vehicle_id, 
case when vehicle_id is not null then 'SWUP-NULL' else 'SWUP-NULL' end as build_software_upgrade_id,	
model as build_model,
powertrain as build_powertrain,
build_id as build_option_id,
charging_port_id as build_charger_id
from commercial.demand_planning.rep_vehicles
""").toPandas()




# 2. Then, pass the output of the first function to the string splitting engine
final_processed_df = split_config_string_to_attributes(result_with_basic_info.copy()) # Pass a copy if you want to keep result_with_basic_info separate

final_processed_df_merged = final_processed_df.merge(software_upgrade_build_id_charger_details, how='left', left_on = 'vehicle_id', right_on='rv_vehicle_id')

final_processed_df_merged = final_processed_df_merged[[
'vehicle_id',
'action',
'configuration_string',
'build_model_powertrain',
'build_model',
'build_powertrain',
'build_country',
'build_exterior',
'build_interior',
'build_wheels',
'build_tonneau',
'build_badging',
'build_roof',
'build_audio',
'build_utility',
'build_software_upgrade_id',
'build_option_id',
'build_charger_id'
]]

# Display the final DataFrame with all extracted attributes
display(final_processed_df_merged)

# COMMAND ----------

# DBTITLE 1,QTY to Freeze Summary
# Group by 'config_string' and count the occurrences of 'vehicle_id'
result['vehicle_id'] = ""
result.loc[result['vo_vehicle_id'] == 'exact matching vehicle not found', 'vehicle_id'] = result['best_match']
result.loc[result['vo_vehicle_id'] != 'exact matching vehicle not found', 'vehicle_id'] = result['vo_vehicle_id']

grouped_df = result.groupby(
    ['model_powertrain', 'config_string']
)['vehicle_id'].count().reset_index(name='vehicle_id_count')
grouped_df.sort_values(['model_powertrain','vehicle_id_count'], ascending=[False, False], inplace=True)
display(grouped_df)

# COMMAND ----------

# DBTITLE 1,DOWNLOAD: Current Supply
# how much supply of each config_string exists between as_of_date and inventory_target_date (user inputs)
#based off of actual_factory_gate_date and expected_factory_gate_date

current_supply_config_list = spark.sql(f"""
with charging_port_table as (
select vehicle_id as cp_vehicle_id, charging_port_option_id
from commercial.digital_commerce.fct_vehicles
)

select vehicle_id,
    case
      when
        rv.vehicle_status in ('PRODUCTION_CONTROL_FROZEN') and rv.vin is null
        then 'Firm'
      when
        rv.vehicle_status in ('PRODUCTION_CONTROL_FROZEN')
        and rv.vin is not null
        then 'Frozen/VINned'
      when rv.vehicle_status in ('DELIVERED') then 'Delivered'
      when rv.vehicle_status in (
          'PRODUCTION_STARTED',
          'ARRIVED_AT_DISTRIBUTION_CENTER',
          'FACTORY_GATED',
          'FACTORY_OK',
          'GENERAL_ASSEMBLY_COMPLETED',
          'IN_TRANSIT_TO_DISTRIBUTION_CENTER',
          'INTERNAL_RECEIVED',
          'PDI_COMPLETED',
          'PDI_STARTED',
          'RETURNED',
          'STAGED_FOR_PDI',
          'VEHICLE_PREPPED'
        ) and rv.vin is not null
        and rv.distribution_center = 8101
        then 'Already Built/ WIP at Normal'
      when rv.vehicle_status in (
          'PRODUCTION_STARTED',
          'ARRIVED_AT_DISTRIBUTION_CENTER',
          'FACTORY_GATED',
          'FACTORY_OK',
          'GENERAL_ASSEMBLY_COMPLETED',
          'IN_TRANSIT_TO_DISTRIBUTION_CENTER',
          'INTERNAL_RECEIVED',
          'PDI_COMPLETED',
          'PDI_STARTED',
          'RETURNED',
          'STAGED_FOR_PDI',
          'VEHICLE_PREPPED'
        ) and rv.vin is not null
        and rv.distribution_center <> 8101
        then 'Already Built/ WIP in Field'
      when rv.vehicle_status in ('DECOMMISSIONED') then 'Decommissioned'
      when
        rv.vehicle_status in ('CREATED')
        and rv.vehicle_record_status in ('ACTIVE')
        then 'Liquid'
      else 'Investigate'
    end as production_zone,
CASE
    WHEN cp.charging_port_option_id = 'CHRG-NACS01'
    THEN replace(rv.config_string, '2025', '2026')
    ELSE rv.config_string
END as config_string,
rv.model_powertrain,
rv.model,
rv.powertrain,
rv.country,
rv.paint_id,
rv.interior_id,
rv.wheels_id,
rv.tonneau_id,
rv.accents_and_badging_id,
rv.roof_id,
rv.audio_id,
rv.utility_panel_id
from commercial.demand_planning.rep_vehicles rv
left join charging_port_table cp on rv.vehicle_id = cp.cp_vehicle_id
where CASE
        WHEN cp.charging_port_option_id = 'CHRG-NACS01'
        THEN replace(rv.model_year, '2025', '2026')
        ELSE rv.model_year
    END = 2026
  and rv.match_status is null --unmatched vehicles
  and rv.is_delivered = 0 -- not delivered
  and rv.vin_salability <> 'NON_SALEABLE' --saleable vehicles
  and rv.vehicle_status not in ('CREATED','DECOMMISSIONED','RETURNED','INTERNAL_RECEIVED', 'DELIVERED') -- not liquid, not delivered, not internal
  and (rv.actual_factory_gate_date is not null or rv.expected_factory_gate_date is not null)
  and coalesce(rv.actual_factory_gate_date, rv.expected_factory_gate_date) < '{inventory_target_date}' 
  and rv.model_powertrain in ({', '.join(f"'{item}'" for item in freeze_model_powertrain_list)})
  and rv.vehicle_usage in ('CUSTOMER_DELIVERY')
  -- and rv.is_eligible_for_vehicle_shop = 1 
  -- and rv.vehicle_record_status = 'ACTIVE'
  order by rv.model_powertrain desc, config_string desc
""").toPandas()

display(current_supply_config_list)