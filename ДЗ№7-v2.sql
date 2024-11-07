with a_d as 
(
select
		fabd.ad_date,
		coalesce (fc.campaign_name,
	'N/A') as campaign_name,
		coalesce (fa.adset_name,
	'N/A') as adset_name,
		coalesce (fabd.url_parameters,
	'N/A') as url_parameters,
		coalesce (fabd.spend::numeric,
	0) as spend,
		coalesce (fabd.impressions::numeric,
	0) as impressions,
		coalesce (fabd.reach::numeric,
	0) as reach,
		coalesce (fabd.clicks::numeric,
	0) as clicks,
		coalesce (fabd.leads::numeric,
	0) as leads,
		coalesce (fabd.value::numeric,
	0) as value,
		case
		when lower (substring(url_parameters,
		'utm_campaign=([^$#&]+)'))= 'nan'
			then null
		else lower (substring(url_parameters,
		'utm_campaign=([^$#&]+)'))
	end as utm_campaign
from
		facebook_ads_basic_daily fabd
left join facebook_adset fa on
		fa.adset_id = fabd.adset_id
left join facebook_campaign fc on
		fc.campaign_id = fabd.campaign_id
union all
select
				gabd.ad_date,
			coalesce (gabd.campaign_name,
	'N/A') as campaign_name,
			coalesce (gabd.adset_name,
	'N/A') as adset_name,
			coalesce (gabd.url_parameters,
	'N/A') as url_parameters,
			coalesce (gabd.spend,
	0) as spend,
			coalesce (gabd.impressions,
	0) as impressions,
			coalesce (gabd.reach,
	0) as reach,
			coalesce (gabd.clicks,
	0) as clicks,
			coalesce (gabd.leads,
	0) as leads,
			coalesce (gabd.value,
	0) as value,
			case
		when lower (substring(url_parameters,
		'utm_campaign=([^$#&]+)'))= 'nan'
				then null
		else lower (substring(url_parameters,
		'utm_campaign=([^$#&]+)'))
	end as utm_campaign
from
			google_ads_basic_daily gabd
),
a_d_by_month as (
select 
	date (date_trunc('month',
	a_d.ad_date)) as ad_month,
	a_d.utm_campaign,
	sum (a_d.spend) as spend,
	sum (a_d.impressions) as impressions,
	sum (a_d.clicks) as clicks,
	sum (a_d.value) as value,
	case
		when sum (a_d.impressions)>0 
		then sum (a_d.clicks)/ sum (a_d.impressions)
	end as CTR,
	case
		when sum(a_d.clicks)>0 
		then sum(a_d.spend)/ sum (a_d.clicks)
	end as CPC,
	case
		when sum (a_d.impressions)>0 
		then (sum (a_d.spend)/ sum (a_d.impressions))* 1000
	end as CPM,
	case
		when sum (a_d.spend)>0 
		then sum (a_d.value)/ sum (a_d.spend)-1
	end as ROMI
from
	a_d
group by
	a_d.ad_date,
	a_d.utm_campaign)
select 
	adm.utm_campaign,
	adm.ad_month,
	adm.CPM,
	lag(adm.CPM) over (partition by adm.utm_campaign
order by
	adm.ad_month) as CPM_prev_month,
	(adm.CPM - lag(adm.CPM) over (partition by adm.utm_campaign
order by
	adm.ad_month)) / nullif
  (lag(adm.CPM) over (partition by adm.utm_campaign
order by
	adm.ad_month),
	0) * 100 as CPM_change_percent,
	adm.CTR,
	lag(adm.CTR) over (partition by adm.utm_campaign
order by
	adm.ad_month) as CTR_prev_month,
	(adm.CTR - lag(adm.CTR) over (partition by adm.utm_campaign
order by
	adm.ad_month)) / nullif
 	(lag(adm.CTR) over (partition by adm.utm_campaign
order by
	adm.ad_month),
	0) * 100 as CTR_change_percent,
	adm.ROMI,
	lag(adm.ROMI) over (partition by adm.utm_campaign
order by
	adm.ad_month) as ROMI_prev_month,
	(adm.ROMI - lag(adm.ROMI) over (partition by adm.utm_campaign
order by
	adm.ad_month)) / nullif
  	(lag(adm.ROMI) over (partition by adm.utm_campaign
order by
	adm.ad_month),
	0) * 100 as ROMI_change_percent
from
	a_d_by_month adm
order by
	adm.ad_month
;