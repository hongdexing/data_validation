-- 表单A和表单B的交集差异明细
select single_diff,
       a_single_detail,
       b_single_detail,
       a_trace_id,
       b_trace_id,
       a_loan_account_id,
       b_loan_account_id,
       a_aa,
       b_aa,
       rowNum
from (
         select split(single_diff, '&^')[0]                                  as single_diff,
                split(single_diff, '&^')[1]                                  as a_single_detail,
                split(single_diff, '&^')[2]                                  as b_single_detail,
                a_trace_id,
                b_trace_id,
                a_loan_account_id,
                b_loan_account_id,
                a_aa,
                b_aa,
                row_number() over (partition by split(single_diff, '&^')[0]) as rowNum
         from (
                  select a_trace_id, b_trace_id, a_loan_account_id, b_loan_account_id, a_aa, b_aa, single_diff
                  from (
                           select a.trace_id                                                    as a_trace_id,
                                  b.trace_id                                                    as b_trace_id,
                                  a.loan_account_id                                             as a_loan_account_id,
                                  b.loan_account_id                                             as b_loan_account_id,
                                  a.aa                                                          as a_aa,
                                  b.aa                                                          as b_aa,
                                  concat_ws('&!', if(case when a.trace_id is null and b.trace_id is null then false
                                                          else nvl(a.trace_id != b.trace_id, True) end,
                                                     concat_ws('&^', 'a_trace_id', a.trace_id, b.trace_id), null), if(
                                                    case when a.loan_account_id is null and b.loan_account_id is null
                                                             then false
                                                         else nvl(a.loan_account_id != b.loan_account_id, True) end,
                                                    concat_ws('&^', 'a_loan_account_id', a.loan_account_id,
                                                              b.loan_account_id), null), if(
                                                    case when a.aa is null and b.aa is null then false
                                                         else nvl(a.aa != b.aa, True) end,
                                                    concat_ws('&^', 'a_aa', a.aa, b.aa), null)) as diff
                           from dwd.dwd_risk_df_risk_flow_trace_result as a
                           inner join test.dwd_risk_df_risk_old_flow_decision as b
                               on a.trace_id = b.trace_id
                           where 1 = 1 and
                                 (case when a.trace_id is null and b.trace_id is null then false
                                       else nvl(a.trace_id != b.trace_id, True) end or
                                  case when a.loan_account_id is null and b.loan_account_id is null then false
                                       else nvl(a.loan_account_id != b.loan_account_id, True) end or
                                  case when a.aa is null and b.aa is null then false
                                       else nvl(a.aa != b.aa, True) end) and
                                 a.dt = '20221009' and
                                 1 = 1
                       ) t lateral VIEW explode(split(diff, '&!')) tmp_diff as single_diff
              ) t
     ) t
where rowNum <= 50
limit 5000;


-- 表单A和表单B的交集差异数量统计
select t.single_diff, count(*) as numbers
from (
         select a_trace_id, b_trace_id, a_loan_account_id, b_loan_account_id, a_aa, b_aa, single_diff
         from (
                  select a.trace_id          as a_trace_id,
                         b.trace_id          as b_trace_id,
                         a.loan_account_id   as a_loan_account_id,
                         b.loan_account_id   as b_loan_account_id,
                         a.aa                as a_aa,
                         b.aa                as b_aa,
                         concat_ws('&!', if(case when a.trace_id is null and b.trace_id is null then false
                                                 else nvl(a.trace_id != b.trace_id, True) end,
                                            concat_ws('&^', 'a_trace_id', a.trace_id, b.trace_id), null),
                                   if(case when a.loan_account_id is null and b.loan_account_id is null then false
                                           else nvl(a.loan_account_id != b.loan_account_id, True) end,
                                      concat_ws('&^', 'a_loan_account_id', a.loan_account_id, b.loan_account_id), null),
                                   if(case when a.aa is null and b.aa is null then false
                                           else nvl(a.aa != b.aa, True) end, concat_ws('&^', 'a_aa', a.aa, b.aa),
                                      null)) as diff
                  from dwd.dwd_risk_df_risk_flow_trace_result as a
                  inner join test.dwd_risk_df_risk_old_flow_decision as b
                      on a.trace_id = b.trace_id
                  where 1 = 1 and
                        (case when a.trace_id is null and b.trace_id is null then false
                              else nvl(a.trace_id != b.trace_id, True) end or
                         case when a.loan_account_id is null and b.loan_account_id is null then false
                              else nvl(a.loan_account_id != b.loan_account_id, True) end or
                         case when a.aa is null and b.aa is null then false else nvl(a.aa != b.aa, True) end) and
                        a.dt = '20221009' and
                        1 = 1
              ) t lateral VIEW explode(split(diff, '&!')) tmp_diff as single_diff
     ) as t
group by t.single_diff;


-- 表单A存在表单B不存在明细
select a.trace_id as a_trace_id,a.loan_account_id as a_loan_account_id,a.aa as a_aa from dwd.dwd_risk_df_risk_flow_trace_result as a where not exists (select 1 from test.dwd_risk_df_risk_old_flow_decision as b where a.trace_id=b.trace_id and 1=1) and a.dt = '20221009';


-- 表单B存在表单A不存在明细
select b.trace_id as b_trace_id,b.loan_account_id as b_loan_account_id,b.aa as b_aa from test.dwd_risk_df_risk_old_flow_decision as b where not exists (select 1 from dwd.dwd_risk_df_risk_flow_trace_result as a where a.trace_id=b.trace_id and a.dt = '20221009') and 1=1;

