from psycopg2 import connect


def start_planning(year: int, quarter: int, user: str, pwd: str):
    conn = connect(dbname='2023_plan_Dontsov', user=user, password=pwd, host='localhost')
    quarter_id = str(year) + '.' + str(quarter)

    # 1 Delete plan data from the plan_data table related to the target year and quarter
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM plan_data WHERE quarterid = {quarter_id}::varchar;")

    # 2 Delete all records related to the target quarter from the plan_status table
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM plan_status WHERE quarterid = {quarter_id}::varchar;")

    # 3 Create the necessary planning status records in the plan_status table for the selected quarter.
    # The number of records added should be equal to the number of countries in which customer-companies are situated.
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO plan_status (quarterid, status, country)
            SELECT DISTINCT
                {quarter_id}::varchar quarterid,
                'R' status,
                c.countrycode country
            FROM
                company c;
        """)

    # 4 Generate a version N of planning data in the plan_data table.
    # Use the calculation algorithm as described in section 1.4.
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO plan_data (versionid, country, quarterid, pcid, salesamt)
            WITH fact as (
                SELECT
                    cs.qr,
                    c.countrycode country,
                    cs.categoryid pcid,
                    sum(cs.salesamt) salesamt
                FROM
                    company_sales cs
                    JOIN company c on cs.cid = c.id
                WHERE 1=1
                    and cs.ccls in ('A', 'B')
                    and cs.year in ({year} - 1, {year} - 2)
                    and cs.quarter_yr = {quarter}
                GROUP BY 1, 2, 3
            ),
            plan as (
                SELECT
                    country,
                    {quarter_id}::varchar quarterid,
                    pcid,
                    avg(salesamt) salesamt
                FROM fact
                GROUP BY 1, 2, 3
            )
            SELECT
                'N' versionid,
                ps.country,
                ps.quarterid,
                p.pcid,
                coalesce(p.salesamt, 0) salesamt
            FROM
                plan_status ps
                LEFT JOIN plan p
                    on ps.quarterid = p.quarterid
                    and ps.country = p.country;
        """)

    # 5 Copy the data from the version N and insert it further to the plan_data table changing the version to P
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO plan_data (versionid, country, quarterid, pcid, salesamt)
            SELECT
                'P' versionid,
                country,
                quarterid,
                pcid,
                salesamt
            FROM plan_data
            WHERE versionid = 'N' and quarterid = {quarter_id}::varchar;
        """)

    # 6 Store the name of the user who called the function, in the records of the plan_status table
    with conn.cursor() as cur:
        cur.execute(f"UPDATE plan_status SET author = '{user}' WHERE quarterid = {quarter_id}::varchar and status = 'R'")

    conn.commit()


if __name__ == '__main__':
    start_planning(2014, 1, 'ivan', 'ivan')
