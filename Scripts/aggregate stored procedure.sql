CREATE OR REPLACE PROCEDURE stg.GenerateAggregateTables()
LANGUAGE plpgsql
AS $$
BEGIN
    -- 1. Analyze total salary, overtime, and other payments by fiscal year, agency, and title
    DROP TABLE IF EXISTS dev.payroll_aggregates;
    CREATE TABLE dev.payroll_aggregates AS
    SELECT
        fiscalyear,
        agencyid,
        titlecode,
        ROUND(SUM(BaseSalary)::numeric, 2) AS total_base_salary,
        ROUND(SUM(RegularGrossPaid)::numeric, 2) AS total_regular_paid,
        SUM(OTHours) AS total_overtime_hours,
        ROUND(SUM(TotalOTPaid)::numeric, 2) AS total_overtime_paid,
        ROUND(SUM(TotalOtherPay)::numeric, 2) AS total_other_pay,
        COUNT(EmployeeID) AS total_employees
    FROM stg.Payroll
    GROUP BY fiscalyear, agencyid, titlecode;

    -- 2. Salary Analysis
    DROP TABLE IF EXISTS dev.salary_analysis;
    CREATE TABLE dev.salary_analysis AS
    SELECT
        fiscalyear,
        agencyid,
        titlecode,
        ROUND(AVG(BaseSalary)::numeric, 2) AS avg_base_salary,
        ROUND(AVG(RegularGrossPaid)::numeric, 2) AS avg_regular_paid,
        ROUND(AVG(TotalOTPaid)::numeric, 2) AS avg_overtime_paid,
        ROUND(AVG(TotalOtherPay)::numeric, 2) AS avg_other_pay,
        ROUND(SUM(BaseSalary + RegularGrossPaid + TotalOTPaid + TotalOtherPay)::numeric, 2) AS total_compensation
    FROM stg.Payroll
    GROUP BY fiscalyear, agencyid, titlecode;

    -- 3. Workforce Distribution
    DROP TABLE IF EXISTS dev.workforce_distribution;
    CREATE TABLE dev.workforce_distribution AS
    SELECT
        fiscalyear,
        agencyid,
        worklocationborough,
        titlecode,
        COUNT(EmployeeID) AS employee_count
    FROM stg.Payroll
    GROUP BY fiscalyear, agencyid, worklocationborough, titlecode;

    -- 4. Leave Patterns
    DROP TABLE IF EXISTS dev.leave_patterns;
    CREATE TABLE dev.leave_patterns AS
    SELECT
        fiscalyear,
        agencyid,
        titlecode,
        COUNT(EmployeeID) AS total_employees,
        SUM(CASE WHEN leavestatusasofjune30 = 'On Leave' THEN 1 ELSE 0 END) AS employees_on_leave,
        ROUND(100.0 * SUM(CASE WHEN leavestatusasofjune30 = 'On Leave' THEN 1 ELSE 0 END)::numeric / COUNT(EmployeeID), 2) AS leave_percentage
    FROM stg.Payroll
    GROUP BY fiscalyear, agencyid, titlecode;

    -- 5. Overtime Analysis
    DROP TABLE IF EXISTS dev.overtime_analysis;
    CREATE TABLE dev.overtime_analysis AS
    SELECT
        fiscalyear,
        agencyid,
        titlecode,
        SUM(OTHours) AS total_overtime_hours,
        ROUND(AVG(CASE WHEN OTHours > 0 THEN TotalOTPaid / OTHours ELSE 0 END)::numeric, 2) AS avg_overtime_pay_per_hour,
        ROUND(SUM(TotalOTPaid)::numeric, 2) AS total_overtime_cost
    FROM stg.Payroll
    GROUP BY fiscalyear, agencyid, titlecode;

    -- 6. Understand hiring trends over time for each agency
    DROP TABLE IF EXISTS dev.employee_hiring_aggregates;
    CREATE TABLE dev.employee_hiring_aggregates AS
    SELECT
        EXTRACT(YEAR FROM AgencyStartDate) AS hiring_year,
        agencyid,
        COUNT(EmployeeID) AS total_hires
    FROM stg.Payroll
    WHERE AgencyStartDate IS NOT NULL
    GROUP BY EXTRACT(YEAR FROM AgencyStartDate), agencyid;

    -- 7. Assess overtime usage and costs by agency and title
    DROP TABLE IF EXISTS dev.overtime_aggregates;
    CREATE TABLE dev.overtime_aggregates AS
    SELECT
        fiscalyear,
        agencyid,
        titlecode,
        SUM(OTHours) AS total_overtime_hours,
        ROUND(SUM(TotalOTPaid)::numeric, 2) AS total_overtime_paid
    FROM stg.Payroll
    WHERE OTHours > 0
    GROUP BY fiscalyear, agencyid, titlecode;

    -- 8. Compare compensation across agencies and fiscal years
    DROP TABLE IF EXISTS dev.agency_compensation_summary;
    CREATE TABLE dev.agency_compensation_summary AS
    SELECT
        fiscalyear,
        agencyid,
        ROUND(SUM(BaseSalary + RegularGrossPaid + TotalOTPaid + TotalOtherPay)::numeric, 2) AS total_compensation,
        COUNT(DISTINCT EmployeeID) AS total_employees
    FROM stg.Payroll
    GROUP BY fiscalyear, agencyid;
END;
$$;
