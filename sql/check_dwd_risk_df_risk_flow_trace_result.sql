set hive.strict.checks.type.safety=false;


-- 表单A和表单B的交集差异明细
select single_diff,
       a_single_detail,
       b_single_detail,
       a_trace_id,
       b_trace_id,
       a_loan_account_id,
       b_loan_account_id,
       a_risk_flow_id,
       b_risk_flow_id,
       a_flow_name,
       b_flow_name,
       a_scope,
       b_scope,
       a_scope_name,
       b_scope_name,
       a_scope_desc,
       b_scope_desc,
       a_risk_type,
       b_risk_type,
       a_event_id,
       b_event_id,
       a_event_name,
       b_event_name,
       a_order_id,
       b_order_id,
       a_sdk_type,
       b_sdk_type,
       a_flow_tag,
       b_flow_tag,
       a_decision_tag,
       b_decision_tag,
       a_credit_rank,
       b_credit_rank,
       a_approve,
       b_approve,
       a_credits_denied_reason,
       b_credits_denied_reason,
       a_total_credits,
       b_total_credits,
       a_available_credits,
       b_available_credits,
       a_dynamic_credit,
       b_dynamic_credit,
       a_dynamic_total_credits,
       b_dynamic_total_credits,
       a_risk_group,
       b_risk_group,
       a_available_product,
       b_available_product,
       a_adjust_available_credit,
       b_adjust_available_credit,
       a_adjust_dynamic_credit,
       b_adjust_dynamic_credit,
       a_trace_ts,
       b_created_ts,
       a_updated_ts,
       b_updated_ts,
       a_credits_status,
       b_credits_status,
       a_retrieve_drools_id,
       b_retrieve_drools_id,
       a_callback_tag,
       b_callback_tag,
       a_dongzhi_score,
       b_dongzhi_score,
       a_if_offline_review,
       b_if_offline_review,
       a_credit_change_tag,
       b_credit_change_tag,
       a_offline_type,
       b_offline_type,
       a_trace_run_type,
       b_trace_run_type,
       rowNum
from (
         select split(single_diff, '&#')[0]                                  as single_diff,
                split(single_diff, '&#')[1]                                  as a_single_detail,
                split(single_diff, '&#')[2]                                  as b_single_detail,
                a_trace_id,
                b_trace_id,
                a_loan_account_id,
                b_loan_account_id,
                a_risk_flow_id,
                b_risk_flow_id,
                a_flow_name,
                b_flow_name,
                a_scope,
                b_scope,
                a_scope_name,
                b_scope_name,
                a_scope_desc,
                b_scope_desc,
                a_risk_type,
                b_risk_type,
                a_event_id,
                b_event_id,
                a_event_name,
                b_event_name,
                a_order_id,
                b_order_id,
                a_sdk_type,
                b_sdk_type,
                a_flow_tag,
                b_flow_tag,
                a_decision_tag,
                b_decision_tag,
                a_credit_rank,
                b_credit_rank,
                a_approve,
                b_approve,
                a_credits_denied_reason,
                b_credits_denied_reason,
                a_total_credits,
                b_total_credits,
                a_available_credits,
                b_available_credits,
                a_dynamic_credit,
                b_dynamic_credit,
                a_dynamic_total_credits,
                b_dynamic_total_credits,
                a_risk_group,
                b_risk_group,
                a_available_product,
                b_available_product,
                a_adjust_available_credit,
                b_adjust_available_credit,
                a_adjust_dynamic_credit,
                b_adjust_dynamic_credit,
                a_trace_ts,
                b_created_ts,
                a_updated_ts,
                b_updated_ts,
                a_credits_status,
                b_credits_status,
                a_retrieve_drools_id,
                b_retrieve_drools_id,
                a_callback_tag,
                b_callback_tag,
                a_dongzhi_score,
                b_dongzhi_score,
                a_if_offline_review,
                b_if_offline_review,
                a_credit_change_tag,
                b_credit_change_tag,
                a_offline_type,
                b_offline_type,
                a_trace_run_type,
                b_trace_run_type,
                row_number() over (partition by split(single_diff, '&#')[0]) as rowNum
         from (
                  select a_trace_id,
                         b_trace_id,
                         a_loan_account_id,
                         b_loan_account_id,
                         a_risk_flow_id,
                         b_risk_flow_id,
                         a_flow_name,
                         b_flow_name,
                         a_scope,
                         b_scope,
                         a_scope_name,
                         b_scope_name,
                         a_scope_desc,
                         b_scope_desc,
                         a_risk_type,
                         b_risk_type,
                         a_event_id,
                         b_event_id,
                         a_event_name,
                         b_event_name,
                         a_order_id,
                         b_order_id,
                         a_sdk_type,
                         b_sdk_type,
                         a_flow_tag,
                         b_flow_tag,
                         a_decision_tag,
                         b_decision_tag,
                         a_credit_rank,
                         b_credit_rank,
                         a_approve,
                         b_approve,
                         a_credits_denied_reason,
                         b_credits_denied_reason,
                         a_total_credits,
                         b_total_credits,
                         a_available_credits,
                         b_available_credits,
                         a_dynamic_credit,
                         b_dynamic_credit,
                         a_dynamic_total_credits,
                         b_dynamic_total_credits,
                         a_risk_group,
                         b_risk_group,
                         a_available_product,
                         b_available_product,
                         a_adjust_available_credit,
                         b_adjust_available_credit,
                         a_adjust_dynamic_credit,
                         b_adjust_dynamic_credit,
                         a_trace_ts,
                         b_created_ts,
                         a_updated_ts,
                         b_updated_ts,
                         a_credits_status,
                         b_credits_status,
                         a_retrieve_drools_id,
                         b_retrieve_drools_id,
                         a_callback_tag,
                         b_callback_tag,
                         a_dongzhi_score,
                         b_dongzhi_score,
                         a_if_offline_review,
                         b_if_offline_review,
                         a_credit_change_tag,
                         b_credit_change_tag,
                         a_offline_type,
                         b_offline_type,
                         a_trace_run_type,
                         b_trace_run_type,
                         single_diff
                  from (
                           select a.trace_id                                                    as a_trace_id,
                                  b.trace_id                                                    as b_trace_id,
                                  a.loan_account_id                                             as a_loan_account_id,
                                  b.loan_account_id                                             as b_loan_account_id,
                                  a.risk_flow_id                                                as a_risk_flow_id,
                                  b.risk_flow_id                                                as b_risk_flow_id,
                                  a.flow_name                                                   as a_flow_name,
                                  b.flow_name                                                   as b_flow_name,
                                  a.scope                                                       as a_scope,
                                  b.scope                                                       as b_scope,
                                  a.scope_name                                                  as a_scope_name,
                                  b.scope_name                                                  as b_scope_name,
                                  a.scope_desc                                                  as a_scope_desc,
                                  b.scope_desc                                                  as b_scope_desc,
                                  a.risk_type                                                   as a_risk_type,
                                  b.risk_type                                                   as b_risk_type,
                                  a.event_id                                                    as a_event_id,
                                  b.event_id                                                    as b_event_id,
                                  a.event_name                                                  as a_event_name,
                                  b.event_name                                                  as b_event_name,
                                  a.order_id                                                    as a_order_id,
                                  b.order_id                                                    as b_order_id,
                                  a.sdk_type                                                    as a_sdk_type,
                                  b.sdk_type                                                    as b_sdk_type,
                                  a.flow_tag                                                    as a_flow_tag,
                                  b.flow_tag                                                    as b_flow_tag,
                                  a.decision_tag                                                as a_decision_tag,
                                  b.decision_tag                                                as b_decision_tag,
                                  a.credit_rank                                                 as a_credit_rank,
                                  b.credit_rank                                                 as b_credit_rank,
                                  a.approve                                                     as a_approve,
                                  b.approve                                                     as b_approve,
                                  a.credits_denied_reason                                       as a_credits_denied_reason,
                                  b.credits_denied_reason                                       as b_credits_denied_reason,
                                  a.total_credits                                               as a_total_credits,
                                  b.total_credits                                               as b_total_credits,
                                  a.available_credits                                           as a_available_credits,
                                  b.available_credits                                           as b_available_credits,
                                  a.dynamic_credit                                              as a_dynamic_credit,
                                  b.dynamic_credit                                              as b_dynamic_credit,
                                  a.dynamic_total_credits                                       as a_dynamic_total_credits,
                                  b.dynamic_total_credits                                       as b_dynamic_total_credits,
                                  a.risk_group                                                  as a_risk_group,
                                  b.risk_group                                                  as b_risk_group,
                                  a.available_product                                           as a_available_product,
                                  b.available_product                                           as b_available_product,
                                  a.adjust_available_credit                                     as a_adjust_available_credit,
                                  b.adjust_available_credit                                     as b_adjust_available_credit,
                                  a.adjust_dynamic_credit                                       as a_adjust_dynamic_credit,
                                  b.adjust_dynamic_credit                                       as b_adjust_dynamic_credit,
                                  a.trace_ts                                                    as a_trace_ts,
                                  b.created_ts                                                  as b_created_ts,
                                  a.updated_ts                                                  as a_updated_ts,
                                  b.updated_ts                                                  as b_updated_ts,
                                  a.credits_status                                              as a_credits_status,
                                  b.credits_status                                              as b_credits_status,
                                  a.retrieve_drools_id                                          as a_retrieve_drools_id,
                                  b.retrieve_drools_id                                          as b_retrieve_drools_id,
                                  a.callback_tag                                                as a_callback_tag,
                                  b.callback_tag                                                as b_callback_tag,
                                  a.dongzhi_score                                               as a_dongzhi_score,
                                  b.dongzhi_score                                               as b_dongzhi_score,
                                  a.if_offline_review                                           as a_if_offline_review,
                                  b.if_offline_review                                           as b_if_offline_review,
                                  a.credit_change_tag                                           as a_credit_change_tag,
                                  b.credit_change_tag                                           as b_credit_change_tag,
                                  a.offline_type                                                as a_offline_type,
                                  b.offline_type                                                as b_offline_type,
                                  a.trace_run_type                                              as a_trace_run_type,
                                  b.trace_run_type                                              as b_trace_run_type,
                                  concat_ws('&!', if(case when a.trace_id is null and b.trace_id is null then false
                                                          else nvl(a.trace_id != b.trace_id, True) end,
                                                     concat_ws('&#', 'a_trace_id', nvl(string(a.trace_id), '值为null'),
                                                               string(b.trace_id)), null), if(
                                                    case when a.loan_account_id is null and b.loan_account_id is null
                                                             then false
                                                         else nvl(a.loan_account_id != b.loan_account_id, True) end,
                                                    concat_ws('&#', 'a_loan_account_id',
                                                              nvl(string(a.loan_account_id), '值为null'),
                                                              string(b.loan_account_id)), null),
                                            if(case when a.risk_flow_id is null and b.risk_flow_id is null then false
                                                    else nvl(a.risk_flow_id != b.risk_flow_id, True) end,
                                               concat_ws('&#', 'a_risk_flow_id', nvl(string(a.risk_flow_id), '值为null'),
                                                         string(b.risk_flow_id)), null), if(
                                                    case when a.flow_name is null and b.flow_name is null then false
                                                         else nvl(a.flow_name != b.flow_name, True) end,
                                                    concat_ws('&#', 'a_flow_name', nvl(string(a.flow_name), '值为null'),
                                                              string(b.flow_name)), null), if(
                                                    case when a.scope is null and b.scope is null then false
                                                         else nvl(a.scope != b.scope, True) end,
                                                    concat_ws('&#', 'a_scope', nvl(string(a.scope), '值为null'),
                                                              string(b.scope)), null), if(
                                                    case when a.scope_name is null and b.scope_name is null then false
                                                         else nvl(a.scope_name != b.scope_name, True) end,
                                                    concat_ws('&#', 'a_scope_name', nvl(string(a.scope_name), '值为null'),
                                                              string(b.scope_name)), null), if(
                                                    case when a.scope_desc is null and b.scope_desc is null then false
                                                         else nvl(a.scope_desc != b.scope_desc, True) end,
                                                    concat_ws('&#', 'a_scope_desc', nvl(string(a.scope_desc), '值为null'),
                                                              string(b.scope_desc)), null), if(
                                                    case when a.risk_type is null and b.risk_type is null then false
                                                         else nvl(a.risk_type != b.risk_type, True) end,
                                                    concat_ws('&#', 'a_risk_type', nvl(string(a.risk_type), '值为null'),
                                                              string(b.risk_type)), null), if(
                                                    case when a.event_id is null and b.event_id is null then false
                                                         else nvl(a.event_id != b.event_id, True) end,
                                                    concat_ws('&#', 'a_event_id', nvl(string(a.event_id), '值为null'),
                                                              string(b.event_id)), null), if(
                                                    case when a.event_name is null and b.event_name is null then false
                                                         else nvl(a.event_name != b.event_name, True) end,
                                                    concat_ws('&#', 'a_event_name', nvl(string(a.event_name), '值为null'),
                                                              string(b.event_name)), null), if(
                                                    case when a.order_id is null and b.order_id is null then false
                                                         else nvl(a.order_id != b.order_id, True) end,
                                                    concat_ws('&#', 'a_order_id', nvl(string(a.order_id), '值为null'),
                                                              string(b.order_id)), null), if(
                                                    case when a.sdk_type is null and b.sdk_type is null then false
                                                         else nvl(a.sdk_type != b.sdk_type, True) end,
                                                    concat_ws('&#', 'a_sdk_type', nvl(string(a.sdk_type), '值为null'),
                                                              string(b.sdk_type)), null), if(
                                                    case when a.flow_tag is null and b.flow_tag is null then false
                                                         else nvl(a.flow_tag != b.flow_tag, True) end,
                                                    concat_ws('&#', 'a_flow_tag', nvl(string(a.flow_tag), '值为null'),
                                                              string(b.flow_tag)), null),
                                            if(case when a.decision_tag is null and b.decision_tag is null then false
                                                    else nvl(a.decision_tag != b.decision_tag, True) end,
                                               concat_ws('&#', 'a_decision_tag', nvl(string(a.decision_tag), '值为null'),
                                                         string(b.decision_tag)), null),
                                            if(case when a.credit_rank is null and b.credit_rank is null then false
                                                    else nvl(a.credit_rank != b.credit_rank, True) end,
                                               concat_ws('&#', 'a_credit_rank', nvl(string(a.credit_rank), '值为null'),
                                                         string(b.credit_rank)), null), if(
                                                    case when a.approve is null and b.approve is null then false
                                                         else nvl(a.approve != b.approve, True) end,
                                                    concat_ws('&#', 'a_approve', nvl(string(a.approve), '值为null'),
                                                              string(b.approve)), null), if(
                                                    case when a.credits_denied_reason is null and b.credits_denied_reason is null
                                                             then false
                                                         else nvl(a.credits_denied_reason != b.credits_denied_reason, True) end,
                                                    concat_ws('&#', 'a_credits_denied_reason',
                                                              nvl(string(a.credits_denied_reason), '值为null'),
                                                              string(b.credits_denied_reason)), null),
                                            if(case when a.total_credits is null and b.total_credits is null then false
                                                    else nvl(a.total_credits != b.total_credits, True) end,
                                               concat_ws('&#', 'a_total_credits',
                                                         nvl(string(a.total_credits), '值为null'),
                                                         string(b.total_credits)), null), if(
                                                    case when a.available_credits is null and b.available_credits is null
                                                             then false
                                                         else nvl(a.available_credits != b.available_credits, True) end,
                                                    concat_ws('&#', 'a_available_credits',
                                                              nvl(string(a.available_credits), '值为null'),
                                                              string(b.available_credits)), null), if(
                                                    case when a.dynamic_credit is null and b.dynamic_credit is null
                                                             then false
                                                         else nvl(a.dynamic_credit != b.dynamic_credit, True) end,
                                                    concat_ws('&#', 'a_dynamic_credit',
                                                              nvl(string(a.dynamic_credit), '值为null'),
                                                              string(b.dynamic_credit)), null), if(
                                                    case when a.dynamic_total_credits is null and b.dynamic_total_credits is null
                                                             then false
                                                         else nvl(a.dynamic_total_credits != b.dynamic_total_credits, True) end,
                                                    concat_ws('&#', 'a_dynamic_total_credits',
                                                              nvl(string(a.dynamic_total_credits), '值为null'),
                                                              string(b.dynamic_total_credits)), null), if(
                                                    case when a.risk_group is null and b.risk_group is null then false
                                                         else nvl(a.risk_group != b.risk_group, True) end,
                                                    concat_ws('&#', 'a_risk_group', nvl(string(a.risk_group), '值为null'),
                                                              string(b.risk_group)), null), if(
                                                    case when a.available_product is null and b.available_product is null
                                                             then false
                                                         else nvl(a.available_product != b.available_product, True) end,
                                                    concat_ws('&#', 'a_available_product',
                                                              nvl(string(a.available_product), '值为null'),
                                                              string(b.available_product)), null), if(
                                                    case when a.adjust_available_credit is null and b.adjust_available_credit is null
                                                             then false
                                                         else nvl(a.adjust_available_credit != b.adjust_available_credit, True) end,
                                                    concat_ws('&#', 'a_adjust_available_credit',
                                                              nvl(string(a.adjust_available_credit), '值为null'),
                                                              string(b.adjust_available_credit)), null), if(
                                                    case when a.adjust_dynamic_credit is null and b.adjust_dynamic_credit is null
                                                             then false
                                                         else nvl(a.adjust_dynamic_credit != b.adjust_dynamic_credit, True) end,
                                                    concat_ws('&#', 'a_adjust_dynamic_credit',
                                                              nvl(string(a.adjust_dynamic_credit), '值为null'),
                                                              string(b.adjust_dynamic_credit)), null), if(
                                                    case when a.trace_ts is null and b.created_ts is null then false
                                                         else nvl(a.trace_ts != b.created_ts, True) end,
                                                    concat_ws('&#', 'a_trace_ts', nvl(string(a.trace_ts), '值为null'),
                                                              string(b.created_ts)), null), if(
                                                    case when a.updated_ts is null and b.updated_ts is null then false
                                                         else nvl(a.updated_ts != b.updated_ts, True) end,
                                                    concat_ws('&#', 'a_updated_ts', nvl(string(a.updated_ts), '值为null'),
                                                              string(b.updated_ts)), null), if(
                                                    case when a.credits_status is null and b.credits_status is null
                                                             then false
                                                         else nvl(a.credits_status != b.credits_status, True) end,
                                                    concat_ws('&#', 'a_credits_status',
                                                              nvl(string(a.credits_status), '值为null'),
                                                              string(b.credits_status)), null), if(
                                                    case when a.retrieve_drools_id is null and b.retrieve_drools_id is null
                                                             then false
                                                         else nvl(a.retrieve_drools_id != b.retrieve_drools_id, True) end,
                                                    concat_ws('&#', 'a_retrieve_drools_id',
                                                              nvl(string(a.retrieve_drools_id), '值为null'),
                                                              string(b.retrieve_drools_id)), null),
                                            if(case when a.callback_tag is null and b.callback_tag is null then false
                                                    else nvl(a.callback_tag != b.callback_tag, True) end,
                                               concat_ws('&#', 'a_callback_tag', nvl(string(a.callback_tag), '值为null'),
                                                         string(b.callback_tag)), null),
                                            if(case when a.dongzhi_score is null and b.dongzhi_score is null then false
                                                    else nvl(a.dongzhi_score != b.dongzhi_score, True) end,
                                               concat_ws('&#', 'a_dongzhi_score',
                                                         nvl(string(a.dongzhi_score), '值为null'),
                                                         string(b.dongzhi_score)), null), if(
                                                    case when a.if_offline_review is null and b.if_offline_review is null
                                                             then false
                                                         else nvl(a.if_offline_review != b.if_offline_review, True) end,
                                                    concat_ws('&#', 'a_if_offline_review',
                                                              nvl(string(a.if_offline_review), '值为null'),
                                                              string(b.if_offline_review)), null), if(
                                                    case when a.credit_change_tag is null and b.credit_change_tag is null
                                                             then false
                                                         else nvl(a.credit_change_tag != b.credit_change_tag, True) end,
                                                    concat_ws('&#', 'a_credit_change_tag',
                                                              nvl(string(a.credit_change_tag), '值为null'),
                                                              string(b.credit_change_tag)), null),
                                            if(case when a.offline_type is null and b.offline_type is null then false
                                                    else nvl(a.offline_type != b.offline_type, True) end,
                                               concat_ws('&#', 'a_offline_type', nvl(string(a.offline_type), '值为null'),
                                                         string(b.offline_type)), null), if(
                                                    case when a.trace_run_type is null and b.trace_run_type is null
                                                             then false
                                                         else nvl(a.trace_run_type != b.trace_run_type, True) end,
                                                    concat_ws('&#', 'a_trace_run_type',
                                                              nvl(string(a.trace_run_type), '值为null'),
                                                              string(b.trace_run_type)), null)) as diff
                           from dwd.dwd_risk_df_risk_flow_trace_result as a
                           inner join test.dwd_risk_df_risk_old_flow_decision as b
                               on a.trace_id = b.trace_id
                           where 1 = 1 and
                                 (case when a.trace_id is null and b.trace_id is null then false
                                       else nvl(a.trace_id != b.trace_id, True) end or
                                  case when a.loan_account_id is null and b.loan_account_id is null then false
                                       else nvl(a.loan_account_id != b.loan_account_id, True) end or
                                  case when a.risk_flow_id is null and b.risk_flow_id is null then false
                                       else nvl(a.risk_flow_id != b.risk_flow_id, True) end or
                                  case when a.flow_name is null and b.flow_name is null then false
                                       else nvl(a.flow_name != b.flow_name, True) end or
                                  case when a.scope is null and b.scope is null then false
                                       else nvl(a.scope != b.scope, True) end or
                                  case when a.scope_name is null and b.scope_name is null then false
                                       else nvl(a.scope_name != b.scope_name, True) end or
                                  case when a.scope_desc is null and b.scope_desc is null then false
                                       else nvl(a.scope_desc != b.scope_desc, True) end or
                                  case when a.risk_type is null and b.risk_type is null then false
                                       else nvl(a.risk_type != b.risk_type, True) end or
                                  case when a.event_id is null and b.event_id is null then false
                                       else nvl(a.event_id != b.event_id, True) end or
                                  case when a.event_name is null and b.event_name is null then false
                                       else nvl(a.event_name != b.event_name, True) end or
                                  case when a.order_id is null and b.order_id is null then false
                                       else nvl(a.order_id != b.order_id, True) end or
                                  case when a.sdk_type is null and b.sdk_type is null then false
                                       else nvl(a.sdk_type != b.sdk_type, True) end or
                                  case when a.flow_tag is null and b.flow_tag is null then false
                                       else nvl(a.flow_tag != b.flow_tag, True) end or
                                  case when a.decision_tag is null and b.decision_tag is null then false
                                       else nvl(a.decision_tag != b.decision_tag, True) end or
                                  case when a.credit_rank is null and b.credit_rank is null then false
                                       else nvl(a.credit_rank != b.credit_rank, True) end or
                                  case when a.approve is null and b.approve is null then false
                                       else nvl(a.approve != b.approve, True) end or
                                  case when a.credits_denied_reason is null and b.credits_denied_reason is null
                                           then false
                                       else nvl(a.credits_denied_reason != b.credits_denied_reason, True) end or
                                  case when a.total_credits is null and b.total_credits is null then false
                                       else nvl(a.total_credits != b.total_credits, True) end or
                                  case when a.available_credits is null and b.available_credits is null then false
                                       else nvl(a.available_credits != b.available_credits, True) end or
                                  case when a.dynamic_credit is null and b.dynamic_credit is null then false
                                       else nvl(a.dynamic_credit != b.dynamic_credit, True) end or
                                  case when a.dynamic_total_credits is null and b.dynamic_total_credits is null
                                           then false
                                       else nvl(a.dynamic_total_credits != b.dynamic_total_credits, True) end or
                                  case when a.risk_group is null and b.risk_group is null then false
                                       else nvl(a.risk_group != b.risk_group, True) end or
                                  case when a.available_product is null and b.available_product is null then false
                                       else nvl(a.available_product != b.available_product, True) end or
                                  case when a.adjust_available_credit is null and b.adjust_available_credit is null
                                           then false
                                       else nvl(a.adjust_available_credit != b.adjust_available_credit, True) end or
                                  case when a.adjust_dynamic_credit is null and b.adjust_dynamic_credit is null
                                           then false
                                       else nvl(a.adjust_dynamic_credit != b.adjust_dynamic_credit, True) end or
                                  case when a.trace_ts is null and b.created_ts is null then false
                                       else nvl(a.trace_ts != b.created_ts, True) end or
                                  case when a.updated_ts is null and b.updated_ts is null then false
                                       else nvl(a.updated_ts != b.updated_ts, True) end or
                                  case when a.credits_status is null and b.credits_status is null then false
                                       else nvl(a.credits_status != b.credits_status, True) end or
                                  case when a.retrieve_drools_id is null and b.retrieve_drools_id is null then false
                                       else nvl(a.retrieve_drools_id != b.retrieve_drools_id, True) end or
                                  case when a.callback_tag is null and b.callback_tag is null then false
                                       else nvl(a.callback_tag != b.callback_tag, True) end or
                                  case when a.dongzhi_score is null and b.dongzhi_score is null then false
                                       else nvl(a.dongzhi_score != b.dongzhi_score, True) end or
                                  case when a.if_offline_review is null and b.if_offline_review is null then false
                                       else nvl(a.if_offline_review != b.if_offline_review, True) end or
                                  case when a.credit_change_tag is null and b.credit_change_tag is null then false
                                       else nvl(a.credit_change_tag != b.credit_change_tag, True) end or
                                  case when a.offline_type is null and b.offline_type is null then false
                                       else nvl(a.offline_type != b.offline_type, True) end or
                                  case when a.trace_run_type is null and b.trace_run_type is null then false
                                       else nvl(a.trace_run_type != b.trace_run_type, True) end) and
                                 a.dt = '20221009' and
                                 1 = 1
                       ) t lateral VIEW explode(split(diff, '&!')) tmp_diff as single_diff
              ) t
     ) t
where rowNum <= 50
limit 5000;


-- 表单A和表单B的交集差异数量统计
select t.single_diff,count(*) as numbers from (select a_trace_id,b_trace_id,a_loan_account_id,b_loan_account_id,a_risk_flow_id,b_risk_flow_id,a_flow_name,b_flow_name,a_scope,b_scope,a_scope_name,b_scope_name,a_scope_desc,b_scope_desc,a_risk_type,b_risk_type,a_event_id,b_event_id,a_event_name,b_event_name,a_order_id,b_order_id,a_sdk_type,b_sdk_type,a_flow_tag,b_flow_tag,a_decision_tag,b_decision_tag,a_credit_rank,b_credit_rank,a_approve,b_approve,a_credits_denied_reason,b_credits_denied_reason,a_total_credits,b_total_credits,a_available_credits,b_available_credits,a_dynamic_credit,b_dynamic_credit,a_dynamic_total_credits,b_dynamic_total_credits,a_risk_group,b_risk_group,a_available_product,b_available_product,a_adjust_available_credit,b_adjust_available_credit,a_adjust_dynamic_credit,b_adjust_dynamic_credit,a_trace_ts,b_created_ts,a_updated_ts,b_updated_ts,a_credits_status,b_credits_status,a_retrieve_drools_id,b_retrieve_drools_id,a_callback_tag,b_callback_tag,a_dongzhi_score,b_dongzhi_score,a_if_offline_review,b_if_offline_review,a_credit_change_tag,b_credit_change_tag,a_offline_type,b_offline_type,a_trace_run_type,b_trace_run_type,single_diff from (select a.trace_id as a_trace_id,b.trace_id as b_trace_id,a.loan_account_id as a_loan_account_id,b.loan_account_id as b_loan_account_id,a.risk_flow_id as a_risk_flow_id,b.risk_flow_id as b_risk_flow_id,a.flow_name as a_flow_name,b.flow_name as b_flow_name,a.scope as a_scope,b.scope as b_scope,a.scope_name as a_scope_name,b.scope_name as b_scope_name,a.scope_desc as a_scope_desc,b.scope_desc as b_scope_desc,a.risk_type as a_risk_type,b.risk_type as b_risk_type,a.event_id as a_event_id,b.event_id as b_event_id,a.event_name as a_event_name,b.event_name as b_event_name,a.order_id as a_order_id,b.order_id as b_order_id,a.sdk_type as a_sdk_type,b.sdk_type as b_sdk_type,a.flow_tag as a_flow_tag,b.flow_tag as b_flow_tag,a.decision_tag as a_decision_tag,b.decision_tag as b_decision_tag,a.credit_rank as a_credit_rank,b.credit_rank as b_credit_rank,a.approve as a_approve,b.approve as b_approve,a.credits_denied_reason as a_credits_denied_reason,b.credits_denied_reason as b_credits_denied_reason,a.total_credits as a_total_credits,b.total_credits as b_total_credits,a.available_credits as a_available_credits,b.available_credits as b_available_credits,a.dynamic_credit as a_dynamic_credit,b.dynamic_credit as b_dynamic_credit,a.dynamic_total_credits as a_dynamic_total_credits,b.dynamic_total_credits as b_dynamic_total_credits,a.risk_group as a_risk_group,b.risk_group as b_risk_group,a.available_product as a_available_product,b.available_product as b_available_product,a.adjust_available_credit as a_adjust_available_credit,b.adjust_available_credit as b_adjust_available_credit,a.adjust_dynamic_credit as a_adjust_dynamic_credit,b.adjust_dynamic_credit as b_adjust_dynamic_credit,a.trace_ts as a_trace_ts,b.created_ts as b_created_ts,a.updated_ts as a_updated_ts,b.updated_ts as b_updated_ts,a.credits_status as a_credits_status,b.credits_status as b_credits_status,a.retrieve_drools_id as a_retrieve_drools_id,b.retrieve_drools_id as b_retrieve_drools_id,a.callback_tag as a_callback_tag,b.callback_tag as b_callback_tag,a.dongzhi_score as a_dongzhi_score,b.dongzhi_score as b_dongzhi_score,a.if_offline_review as a_if_offline_review,b.if_offline_review as b_if_offline_review,a.credit_change_tag as a_credit_change_tag,b.credit_change_tag as b_credit_change_tag,a.offline_type as a_offline_type,b.offline_type as b_offline_type,a.trace_run_type as a_trace_run_type,b.trace_run_type as b_trace_run_type, concat_ws('&!',if(case when a.trace_id is null and b.trace_id is null then false else nvl(a.trace_id != b.trace_id, True) end,concat_ws('&#','a_trace_id',nvl(string(a.trace_id),'值为null'),string(b.trace_id)),null),if(case when a.loan_account_id is null and b.loan_account_id is null then false else nvl(a.loan_account_id != b.loan_account_id, True) end,concat_ws('&#','a_loan_account_id',nvl(string(a.loan_account_id),'值为null'),string(b.loan_account_id)),null),if(case when a.risk_flow_id is null and b.risk_flow_id is null then false else nvl(a.risk_flow_id != b.risk_flow_id, True) end,concat_ws('&#','a_risk_flow_id',nvl(string(a.risk_flow_id),'值为null'),string(b.risk_flow_id)),null),if(case when a.flow_name is null and b.flow_name is null then false else nvl(a.flow_name != b.flow_name, True) end,concat_ws('&#','a_flow_name',nvl(string(a.flow_name),'值为null'),string(b.flow_name)),null),if(case when a.scope is null and b.scope is null then false else nvl(a.scope != b.scope, True) end,concat_ws('&#','a_scope',nvl(string(a.scope),'值为null'),string(b.scope)),null),if(case when a.scope_name is null and b.scope_name is null then false else nvl(a.scope_name != b.scope_name, True) end,concat_ws('&#','a_scope_name',nvl(string(a.scope_name),'值为null'),string(b.scope_name)),null),if(case when a.scope_desc is null and b.scope_desc is null then false else nvl(a.scope_desc != b.scope_desc, True) end,concat_ws('&#','a_scope_desc',nvl(string(a.scope_desc),'值为null'),string(b.scope_desc)),null),if(case when a.risk_type is null and b.risk_type is null then false else nvl(a.risk_type != b.risk_type, True) end,concat_ws('&#','a_risk_type',nvl(string(a.risk_type),'值为null'),string(b.risk_type)),null),if(case when a.event_id is null and b.event_id is null then false else nvl(a.event_id != b.event_id, True) end,concat_ws('&#','a_event_id',nvl(string(a.event_id),'值为null'),string(b.event_id)),null),if(case when a.event_name is null and b.event_name is null then false else nvl(a.event_name != b.event_name, True) end,concat_ws('&#','a_event_name',nvl(string(a.event_name),'值为null'),string(b.event_name)),null),if(case when a.order_id is null and b.order_id is null then false else nvl(a.order_id != b.order_id, True) end,concat_ws('&#','a_order_id',nvl(string(a.order_id),'值为null'),string(b.order_id)),null),if(case when a.sdk_type is null and b.sdk_type is null then false else nvl(a.sdk_type != b.sdk_type, True) end,concat_ws('&#','a_sdk_type',nvl(string(a.sdk_type),'值为null'),string(b.sdk_type)),null),if(case when a.flow_tag is null and b.flow_tag is null then false else nvl(a.flow_tag != b.flow_tag, True) end,concat_ws('&#','a_flow_tag',nvl(string(a.flow_tag),'值为null'),string(b.flow_tag)),null),if(case when a.decision_tag is null and b.decision_tag is null then false else nvl(a.decision_tag != b.decision_tag, True) end,concat_ws('&#','a_decision_tag',nvl(string(a.decision_tag),'值为null'),string(b.decision_tag)),null),if(case when a.credit_rank is null and b.credit_rank is null then false else nvl(a.credit_rank != b.credit_rank, True) end,concat_ws('&#','a_credit_rank',nvl(string(a.credit_rank),'值为null'),string(b.credit_rank)),null),if(case when a.approve is null and b.approve is null then false else nvl(a.approve != b.approve, True) end,concat_ws('&#','a_approve',nvl(string(a.approve),'值为null'),string(b.approve)),null),if(case when a.credits_denied_reason is null and b.credits_denied_reason is null then false else nvl(a.credits_denied_reason != b.credits_denied_reason, True) end,concat_ws('&#','a_credits_denied_reason',nvl(string(a.credits_denied_reason),'值为null'),string(b.credits_denied_reason)),null),if(case when a.total_credits is null and b.total_credits is null then false else nvl(a.total_credits != b.total_credits, True) end,concat_ws('&#','a_total_credits',nvl(string(a.total_credits),'值为null'),string(b.total_credits)),null),if(case when a.available_credits is null and b.available_credits is null then false else nvl(a.available_credits != b.available_credits, True) end,concat_ws('&#','a_available_credits',nvl(string(a.available_credits),'值为null'),string(b.available_credits)),null),if(case when a.dynamic_credit is null and b.dynamic_credit is null then false else nvl(a.dynamic_credit != b.dynamic_credit, True) end,concat_ws('&#','a_dynamic_credit',nvl(string(a.dynamic_credit),'值为null'),string(b.dynamic_credit)),null),if(case when a.dynamic_total_credits is null and b.dynamic_total_credits is null then false else nvl(a.dynamic_total_credits != b.dynamic_total_credits, True) end,concat_ws('&#','a_dynamic_total_credits',nvl(string(a.dynamic_total_credits),'值为null'),string(b.dynamic_total_credits)),null),if(case when a.risk_group is null and b.risk_group is null then false else nvl(a.risk_group != b.risk_group, True) end,concat_ws('&#','a_risk_group',nvl(string(a.risk_group),'值为null'),string(b.risk_group)),null),if(case when a.available_product is null and b.available_product is null then false else nvl(a.available_product != b.available_product, True) end,concat_ws('&#','a_available_product',nvl(string(a.available_product),'值为null'),string(b.available_product)),null),if(case when a.adjust_available_credit is null and b.adjust_available_credit is null then false else nvl(a.adjust_available_credit != b.adjust_available_credit, True) end,concat_ws('&#','a_adjust_available_credit',nvl(string(a.adjust_available_credit),'值为null'),string(b.adjust_available_credit)),null),if(case when a.adjust_dynamic_credit is null and b.adjust_dynamic_credit is null then false else nvl(a.adjust_dynamic_credit != b.adjust_dynamic_credit, True) end,concat_ws('&#','a_adjust_dynamic_credit',nvl(string(a.adjust_dynamic_credit),'值为null'),string(b.adjust_dynamic_credit)),null),if(case when a.trace_ts is null and b.created_ts is null then false else nvl(a.trace_ts != b.created_ts, True) end,concat_ws('&#','a_trace_ts',nvl(string(a.trace_ts),'值为null'),string(b.created_ts)),null),if(case when a.updated_ts is null and b.updated_ts is null then false else nvl(a.updated_ts != b.updated_ts, True) end,concat_ws('&#','a_updated_ts',nvl(string(a.updated_ts),'值为null'),string(b.updated_ts)),null),if(case when a.credits_status is null and b.credits_status is null then false else nvl(a.credits_status != b.credits_status, True) end,concat_ws('&#','a_credits_status',nvl(string(a.credits_status),'值为null'),string(b.credits_status)),null),if(case when a.retrieve_drools_id is null and b.retrieve_drools_id is null then false else nvl(a.retrieve_drools_id != b.retrieve_drools_id, True) end,concat_ws('&#','a_retrieve_drools_id',nvl(string(a.retrieve_drools_id),'值为null'),string(b.retrieve_drools_id)),null),if(case when a.callback_tag is null and b.callback_tag is null then false else nvl(a.callback_tag != b.callback_tag, True) end,concat_ws('&#','a_callback_tag',nvl(string(a.callback_tag),'值为null'),string(b.callback_tag)),null),if(case when a.dongzhi_score is null and b.dongzhi_score is null then false else nvl(a.dongzhi_score != b.dongzhi_score, True) end,concat_ws('&#','a_dongzhi_score',nvl(string(a.dongzhi_score),'值为null'),string(b.dongzhi_score)),null),if(case when a.if_offline_review is null and b.if_offline_review is null then false else nvl(a.if_offline_review != b.if_offline_review, True) end,concat_ws('&#','a_if_offline_review',nvl(string(a.if_offline_review),'值为null'),string(b.if_offline_review)),null),if(case when a.credit_change_tag is null and b.credit_change_tag is null then false else nvl(a.credit_change_tag != b.credit_change_tag, True) end,concat_ws('&#','a_credit_change_tag',nvl(string(a.credit_change_tag),'值为null'),string(b.credit_change_tag)),null),if(case when a.offline_type is null and b.offline_type is null then false else nvl(a.offline_type != b.offline_type, True) end,concat_ws('&#','a_offline_type',nvl(string(a.offline_type),'值为null'),string(b.offline_type)),null),if(case when a.trace_run_type is null and b.trace_run_type is null then false else nvl(a.trace_run_type != b.trace_run_type, True) end,concat_ws('&#','a_trace_run_type',nvl(string(a.trace_run_type),'值为null'),string(b.trace_run_type)),null)) as diff from dwd.dwd_risk_df_risk_flow_trace_result as a inner join test.dwd_risk_df_risk_old_flow_decision as b on a.trace_id=b.trace_id where 1=1 and (case when a.trace_id is null and b.trace_id is null then false else nvl(a.trace_id != b.trace_id, True) end or case when a.loan_account_id is null and b.loan_account_id is null then false else nvl(a.loan_account_id != b.loan_account_id, True) end or case when a.risk_flow_id is null and b.risk_flow_id is null then false else nvl(a.risk_flow_id != b.risk_flow_id, True) end or case when a.flow_name is null and b.flow_name is null then false else nvl(a.flow_name != b.flow_name, True) end or case when a.scope is null and b.scope is null then false else nvl(a.scope != b.scope, True) end or case when a.scope_name is null and b.scope_name is null then false else nvl(a.scope_name != b.scope_name, True) end or case when a.scope_desc is null and b.scope_desc is null then false else nvl(a.scope_desc != b.scope_desc, True) end or case when a.risk_type is null and b.risk_type is null then false else nvl(a.risk_type != b.risk_type, True) end or case when a.event_id is null and b.event_id is null then false else nvl(a.event_id != b.event_id, True) end or case when a.event_name is null and b.event_name is null then false else nvl(a.event_name != b.event_name, True) end or case when a.order_id is null and b.order_id is null then false else nvl(a.order_id != b.order_id, True) end or case when a.sdk_type is null and b.sdk_type is null then false else nvl(a.sdk_type != b.sdk_type, True) end or case when a.flow_tag is null and b.flow_tag is null then false else nvl(a.flow_tag != b.flow_tag, True) end or case when a.decision_tag is null and b.decision_tag is null then false else nvl(a.decision_tag != b.decision_tag, True) end or case when a.credit_rank is null and b.credit_rank is null then false else nvl(a.credit_rank != b.credit_rank, True) end or case when a.approve is null and b.approve is null then false else nvl(a.approve != b.approve, True) end or case when a.credits_denied_reason is null and b.credits_denied_reason is null then false else nvl(a.credits_denied_reason != b.credits_denied_reason, True) end or case when a.total_credits is null and b.total_credits is null then false else nvl(a.total_credits != b.total_credits, True) end or case when a.available_credits is null and b.available_credits is null then false else nvl(a.available_credits != b.available_credits, True) end or case when a.dynamic_credit is null and b.dynamic_credit is null then false else nvl(a.dynamic_credit != b.dynamic_credit, True) end or case when a.dynamic_total_credits is null and b.dynamic_total_credits is null then false else nvl(a.dynamic_total_credits != b.dynamic_total_credits, True) end or case when a.risk_group is null and b.risk_group is null then false else nvl(a.risk_group != b.risk_group, True) end or case when a.available_product is null and b.available_product is null then false else nvl(a.available_product != b.available_product, True) end or case when a.adjust_available_credit is null and b.adjust_available_credit is null then false else nvl(a.adjust_available_credit != b.adjust_available_credit, True) end or case when a.adjust_dynamic_credit is null and b.adjust_dynamic_credit is null then false else nvl(a.adjust_dynamic_credit != b.adjust_dynamic_credit, True) end or case when a.trace_ts is null and b.created_ts is null then false else nvl(a.trace_ts != b.created_ts, True) end or case when a.updated_ts is null and b.updated_ts is null then false else nvl(a.updated_ts != b.updated_ts, True) end or case when a.credits_status is null and b.credits_status is null then false else nvl(a.credits_status != b.credits_status, True) end or case when a.retrieve_drools_id is null and b.retrieve_drools_id is null then false else nvl(a.retrieve_drools_id != b.retrieve_drools_id, True) end or case when a.callback_tag is null and b.callback_tag is null then false else nvl(a.callback_tag != b.callback_tag, True) end or case when a.dongzhi_score is null and b.dongzhi_score is null then false else nvl(a.dongzhi_score != b.dongzhi_score, True) end or case when a.if_offline_review is null and b.if_offline_review is null then false else nvl(a.if_offline_review != b.if_offline_review, True) end or case when a.credit_change_tag is null and b.credit_change_tag is null then false else nvl(a.credit_change_tag != b.credit_change_tag, True) end or case when a.offline_type is null and b.offline_type is null then false else nvl(a.offline_type != b.offline_type, True) end or case when a.trace_run_type is null and b.trace_run_type is null then false else nvl(a.trace_run_type != b.trace_run_type, True) end) and a.dt = '20221009' and 1=1) t lateral VIEW explode(split(diff, '&!')) tmp_diff as single_diff) as t group by t.single_diff;


-- 表单A存在表单B不存在明细
select a.trace_id as a_trace_id,a.loan_account_id as a_loan_account_id,a.risk_flow_id as a_risk_flow_id,a.flow_name as a_flow_name,a.scope as a_scope,a.scope_name as a_scope_name,a.scope_desc as a_scope_desc,a.risk_type as a_risk_type,a.event_id as a_event_id,a.event_name as a_event_name,a.order_id as a_order_id,a.sdk_type as a_sdk_type,a.flow_tag as a_flow_tag,a.decision_tag as a_decision_tag,a.credit_rank as a_credit_rank,a.approve as a_approve,a.credits_denied_reason as a_credits_denied_reason,a.total_credits as a_total_credits,a.available_credits as a_available_credits,a.dynamic_credit as a_dynamic_credit,a.dynamic_total_credits as a_dynamic_total_credits,a.risk_group as a_risk_group,a.available_product as a_available_product,a.adjust_available_credit as a_adjust_available_credit,a.adjust_dynamic_credit as a_adjust_dynamic_credit,a.trace_ts as a_trace_ts,a.updated_ts as a_updated_ts,a.credits_status as a_credits_status,a.retrieve_drools_id as a_retrieve_drools_id,a.callback_tag as a_callback_tag,a.dongzhi_score as a_dongzhi_score,a.if_offline_review as a_if_offline_review,a.credit_change_tag as a_credit_change_tag,a.offline_type as a_offline_type,a.trace_run_type as a_trace_run_type from dwd.dwd_risk_df_risk_flow_trace_result as a where not exists (select 1 from test.dwd_risk_df_risk_old_flow_decision as b where a.trace_id=b.trace_id and 1=1) and a.dt = '20221009';


-- 表单B存在表单A不存在明细
select b.trace_id as b_trace_id,b.loan_account_id as b_loan_account_id,b.risk_flow_id as b_risk_flow_id,b.flow_name as b_flow_name,b.scope as b_scope,b.scope_name as b_scope_name,b.scope_desc as b_scope_desc,b.risk_type as b_risk_type,b.event_id as b_event_id,b.event_name as b_event_name,b.order_id as b_order_id,b.sdk_type as b_sdk_type,b.flow_tag as b_flow_tag,b.decision_tag as b_decision_tag,b.credit_rank as b_credit_rank,b.approve as b_approve,b.credits_denied_reason as b_credits_denied_reason,b.total_credits as b_total_credits,b.available_credits as b_available_credits,b.dynamic_credit as b_dynamic_credit,b.dynamic_total_credits as b_dynamic_total_credits,b.risk_group as b_risk_group,b.available_product as b_available_product,b.adjust_available_credit as b_adjust_available_credit,b.adjust_dynamic_credit as b_adjust_dynamic_credit,b.created_ts as b_created_ts,b.updated_ts as b_updated_ts,b.credits_status as b_credits_status,b.retrieve_drools_id as b_retrieve_drools_id,b.callback_tag as b_callback_tag,b.dongzhi_score as b_dongzhi_score,b.if_offline_review as b_if_offline_review,b.credit_change_tag as b_credit_change_tag,b.offline_type as b_offline_type,b.trace_run_type as b_trace_run_type from test.dwd_risk_df_risk_old_flow_decision as b where not exists (select 1 from dwd.dwd_risk_df_risk_flow_trace_result as a where a.trace_id=b.trace_id and a.dt = '20221009') and 1=1;
