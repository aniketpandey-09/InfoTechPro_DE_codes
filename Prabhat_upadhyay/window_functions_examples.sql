-- Use ROW_NUMBER() to generate row numbers within each department
SELECT
    id,
    name,
    department,
    salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS row_num
FROM
    employees;

-- Use RANK() to rank employees within each department by salary
SELECT
    id,
    name,
    department,
    salary,
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank
FROM
    employees;

-- Calculate a cumulative sum of salary for each department
SELECT
    id,
    name,
    department,
    salary,
    SUM(salary) OVER (PARTITION BY department ORDER BY hire_date) AS cumulative_salary
FROM
    employees;

-- Use LAG() to show each employee's hire date and the hire date of the previous employee
SELECT
    id,
    name,
    hire_date,
    LAG(hire_date, 1) OVER (ORDER BY hire_date) AS previous_hire_date
FROM
    employees;

-- Calculate a moving average of salary over the current row and the two preceding rows
SELECT
    id,
    name,
    hire_date,
    salary,
    AVG(salary) OVER (ORDER BY hire_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg_salary
FROM
    employees
ORDER BY
    hire_date;

-- Use DENSE_RANK() to rank employees within each department by salary
SELECT
    id,
    name,
    department,
    salary,
    DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dense_rank
FROM
    employees;

-- Divide employees into 4 buckets based on their salary within each department
SELECT
    id,
    name,
    department,
    salary,
    NTILE(4) OVER (PARTITION BY department ORDER BY salary DESC) AS salary_bucket
FROM
    employees;

-- Find the first and last hired employee's salary within each department
SELECT
    id,
    name,
    department,
    salary,
    FIRST_VALUE(salary) OVER (PARTITION BY department ORDER BY hire_date) AS first_salary,
    LAST_VALUE(salary) OVER (PARTITION BY department ORDER BY hire_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_salary
FROM
    employees;

-- Calculate the relative rank of each employee's salary within their department
SELECT
    id,
    name,
    department,
    salary,
    PERCENT_RANK() OVER (PARTITION BY department ORDER BY salary) AS percent_rank
FROM
    employees;

-- Calculate the cumulative distribution of salaries within each department
SELECT
    id,
    name,
    department,
    salary,
    CUME_DIST() OVER (PARTITION BY department ORDER BY salary) AS cumulative_distribution
FROM
    employees;
