/* TASK 1 */

-- Grant permissions to planadmin role
GRANT SELECT, UPDATE, INSERT, DELETE ON plan_data TO planadmin;
GRANT SELECT, UPDATE, INSERT, DELETE ON plan_status TO planadmin;
GRANT SELECT, UPDATE, INSERT, DELETE ON country_managers TO planadmin;

GRANT SELECT ON v_plan_edit TO planadmin;
GRANT SELECT ON v_plan TO planadmin;

-- Grant select permission on all other tables to planadmin
GRANT USAGE ON SCHEMA public TO planadmin;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO planadmin;

-- Grant permissions to planmanager role
GRANT SELECT, UPDATE, INSERT, DELETE ON plan_data TO planmanager;
GRANT SELECT, UPDATE ON plan_status TO planmanager;
GRANT SELECT ON country_managers TO planmanager;

GRANT SELECT, UPDATE ON v_plan_edit TO planmanager;
GRANT SELECT ON v_plan TO planmanager;

-- Grant select permission on all other tables to planmanager
GRANT USAGE ON SCHEMA public TO planmanager;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO planmanager;

CREATE USER ivan WITH PASSWORD 'ivan';
GRANT planadmin TO ivan;

CREATE USER sophie WITH PASSWORD 'sophie';
GRANT planmanager TO sophie;

CREATE USER kirill WITH PASSWORD 'kirill';
GRANT planmanager TO kirill;

insert into country_managers (username, country) values
    ('sophie', 'US'),
    ('sophie', 'CA'),
    ('kirill', 'FR'),
    ('kirill', 'GB'),
    ('kirill', 'DE'),
    ('kirill', 'AU')
;

/* TASK 2 */

create materialized view product2 as
    select
        pc.productcategoryid pcid,
        p.productid,
        pc."name" pcname,
        p."name" pname
    from product p
    join productsubcategory ps
        on p.productsubcategoryid = ps.productsubcategoryid
    join productcategory pc
        on ps.productcategoryid = pc.productcategoryid
;
GRANT SELECT ON product2 TO planmanager, planadmin;

create materialized view country2 as
    select distinct a.countryregioncode countrycode
    from address a
    join customeraddress ca
        on a.addressid = ca.addressid
    where ca.addresstype = 'Main Office'
;
GRANT SELECT ON country2 TO planmanager, planadmin;

/* TASK 3 */

insert into company (cname, countrycode, city)
select
    c.companyname,
    a.countryregioncode,
    a.city
from customer c
join customeraddress ca
    on c.customerid = ca.customerid
    and ca.addresstype = 'Main Office'
join address a
    on a.addressid = ca.addressid
;

/* TASK 4 */

insert into company_abc (cid, salestotal, cls, "year")
with year_total as (
    select
        extract('year' from s.orderdate) as "year",
        c.companyname,
        sum(s.subtotal) as salestotal
    from customer c
    join salesorderheader s
        on c.customerid = s.customerid
    where 1=1
        and c.companyname is not null
        and s.orderdate::date between '2012-01-01' and '2013-12-31'
    group by 1, 2
    order by 1 asc, 3 desc
),
running_total as (
    select
        "year",
        companyname,
        salestotal,
        sum(salestotal) over (partition by "year" order by salestotal desc
                              rows between unbounded preceding and current row) SRT_i,
        sum(salestotal) over (partition by "year") * 0.8 S_a,
        sum(salestotal) over (partition by "year") * 0.95 S_b
    from year_total
)
select
    com.id cid,
    salestotal,
    case
        when SRT_i <= S_a then 'A'
        when SRT_i <= S_b then 'B'
        else 'C'
    end as cls,
    "year"
from running_total r
join company com
    on r.companyname = com.cname
;

/* TASK 5 */

insert into company_sales (cid, salesamt, "year", quarter_yr, qr, categoryid, ccls)
with quarter_sales as (
    select
        c.companyname,
        extract('year' from s.orderdate) "year",
        extract('quarter' from s.orderdate) as quarter_yr,
        p2.pcid categoryid,
        sum(sd.linetotal) salesamt
    from customer c
    join salesorderheader s
        on c.customerid = s.customerid
        and s.orderdate::date between '2012-01-01' and '2013-12-31'  -- здесь изменил даты на '2014-01-01' and '2014-03-31'
    join salesorderdetail sd
        on sd.salesorderid =  s.salesorderid
    join product2 p2
        on p2.productid = sd.productid
    where c.companyname is not null
    group by 1, 2, 3, 4
)
select
    com.id cid,
    q.salesamt,
    q."year",
    q.quarter_yr,
    q."year" || '.' || q.quarter_yr qr,
    q.categoryid,
    ca.cls ccls
from company com
join quarter_sales q
    on com.cname = q.companyname
join company_abc ca
    on ca.cid = com.id
    and ca."year" = q."year"  -- сюда добавил -1 для выбора классификации 2013 года
;

/* TASK 6 */
select
    'N' versionid,
    country,
    '2014.1' quarterid,
    pcid,
    avg(salesamt) salesamt
from (
    select
        cs.qr,
        c.countrycode country,
        cs.categoryid pcid,
        sum(cs.salesamt) salesamt
    from
        company_sales cs
        join company c
            on cs.cid = c.id
    where 1=1
        and cs.ccls in ('A', 'B')
        and cs."year" in (2014 - 1, 2014 - 2)
        and cs.quarter_yr = 1
    group by 1, 2, 3
) subq
group by 1, 2, 3, 4
;

/* TASK 9 */
drop materialized view mv_plan_fact_2014_q1;
create materialized view mv_plan_fact_2014_q1 as
with country_fact as (
    select
        com.countrycode country,
        cs.categoryid pcid,
        cs.qr,
        sum(salesamt) as salesamt
    from
        company com
        join company_sales cs
            on com.id = cs.cid
            and cs.ccls in ('A', 'B')
    group by 1, 2, 3
)
select
    p.quarterid "Quarter",
    p.country "Country",
    pc."name" "Category name",
    p.salesamt - f.salesamt "Dev.",
    (p.salesamt - f.salesamt) / p.salesamt "Dev., %"
from
    plan_data p
    left join country_fact f
        on p.country = f.country
        and p.pcid = f.pcid
        and p.quarterid = f.qr
    left join productcategory pc
        on p.pcid = pc.productcategoryid
where p.versionid = 'A'
;