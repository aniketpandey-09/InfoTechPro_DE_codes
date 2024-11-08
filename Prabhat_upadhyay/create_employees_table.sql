-- Create the sample employees table
CREATE TABLE employees (
    id INT,
    name STRING,
    salary DECIMAL(10, 2),
    department STRING,
    gender STRING,
    hire_date DATE
);

-- Insert sample data into the employees table
INSERT INTO employees VALUES
(1, 'Alice', 70000, 'HR', 'F', '2020-01-15'),
(2, 'Bob', 80000, 'Finance', 'M', '2019-11-10'),
(3, 'Charlie', 90000, 'IT', 'M', '2021-06-05'),
(4, 'Diana', 85000, 'HR', 'F', '2018-03-22'),
(5, 'Evan', 75000, 'Finance', 'M', '2020-07-12'),
(6, 'Fay', 95000, 'IT', 'F', '2021-08-19'),
(7, 'George', 72000, 'HR', 'M', '2020-10-29'),
(8, 'Hannah', 68000, 'Finance', 'F', '2019-09-15'),
(9, 'Ian', 56000, 'IT', 'M', '2021-01-01'),
(10, 'Jane', 79000, 'HR', 'F', '2020-02-25'),
(11, 'Kyle', 82000, 'Finance', 'M', '2020-04-30'),
(12, 'Liam', 91000, 'IT', 'M', '2021-07-07'),
(13, 'Mia', 86000, 'HR', 'F', '2019-08-20'),
(14, 'Nina', 72000, 'Finance', 'F', '2018-05-30'),
(15, 'Oscar', 67000, 'IT', 'M', '2021-02-10'),
(16, 'Paula', 70000, 'HR', 'F', '2021-03-15'),
(17, 'Quincy', 82000, 'Finance', 'M', '2019-12-20'),
(18, 'Rita', 94000, 'IT', 'F', '2020-09-05'),
(19, 'Sam', 89000, 'HR', 'M', '2020-11-18'),
(20, 'Tina', 73000, 'Finance', 'F', '2020-12-25'),
(21, 'Uma', 69000, 'IT', 'F', '2021-05-14'),
(22, 'Victor', 61000, 'HR', 'M', '2018-07-22'),
(23, 'Wendy', 93000, 'Finance', 'F', '2020-06-18'),
(24, 'Xander', 75000, 'IT', 'M', '2019-04-14'),
(25, 'Yara', 87000, 'HR', 'F', '2021-09-01');
