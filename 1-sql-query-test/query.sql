-- Retrieve the names of employees who have been working for more than 5 years.
SELECT name, (NOW()::DATE - hire_date::DATE)/365 AS working_years
FROM employees 
WHERE (NOW()::DATE - hire_date::DATE)/365 > 5;

-- Find the departments where the average salary is higher than the overall average salary of all employees.
WITH current_salaries AS (
	SELECT DISTINCT ON (employee_id)
	       employee_id, 
	       salary, 
	       effective_date 
	FROM salaries
	ORDER BY employee_id, effective_date DESC
), avg_department_salaries AS(
	SELECT 
		e.department_id AS department_id,
		AVG(c.salary) AS avg_salary
	FROM current_salaries AS c, employees AS e
	WHERE e.id = c.employee_id
	GROUP BY department_id
)
SELECT a.department_id, d.name, a.avg_salary
FROM avg_department_salaries AS a, departments AS d
WHERE a.department_id = d.id AND avg_salary > (SELECT AVG(salary) FROM current_salaries);

-- Identify the top 5 highest-paid employees along with their department names.
WITH current_salaries AS (
	SELECT DISTINCT ON (employee_id)
	       employee_id, 
	       salary, 
	       effective_date 
	FROM salaries
	ORDER BY employee_id, effective_date DESC
)
SELECT c.employee_id, e.name AS employee_name, d.name AS derpatment_name, c.salary
FROM current_salaries AS c
JOIN employees AS e ON c.employee_id = e.id
JOIN departments AS d ON e.department_id = d.id
ORDER BY salary DESC LIMIT 5;

-- Calculate the percentage increase in salary for each employee from their previous salary to the current salary.
WITH history_salaries AS (
    SELECT 
        employee_id, 
        salary, 
        effective_date, 
        ROW_NUMBER() OVER (
            PARTITION BY employee_id 
            ORDER BY effective_date DESC
        ) AS rank
    FROM salaries
), current_salaries AS (
	SELECT employee_id, salary
	FROM history_salaries
	WHERE "rank" = 1
), previous_salaries AS (
	SELECT employee_id, salary
	FROM history_salaries
	WHERE "rank" = 2
)
SELECT e.name, c.employee_id, ((c.salary - p.salary)*1.0/p.salary) *100 AS increase_pct
FROM previous_salaries AS p
JOIN current_salaries AS c ON c.employee_id = p.employee_id
JOIN employees AS e ON e.id = p.employee_id;