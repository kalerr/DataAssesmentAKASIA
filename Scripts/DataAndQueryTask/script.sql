--1. Create script to create table for each object
--	a. Employee
--	b. PositionHistory

CREATE TABLE master.dbo.Employee (
	Id int IDENTITY(0,1) NOT NULL,
	EmployeeId varchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	FullName varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	BirthDate date NOT NULL,
	Address varchar(500) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	CONSTRAINT Employee_PK PRIMARY KEY (Id),
	CONSTRAINT Employee_UNIQUE UNIQUE (EmployeeId)
);

CREATE TABLE master.dbo.PositionHistory (
	Id int IDENTITY(0,1) NOT NULL,
	PosId varchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	PosTitle varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	EmployeeId varchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	StartDate date NOT NULL,
	EndDate date NOT NULL,
	CONSTRAINT PositionHistory_pk PRIMARY KEY (Id)
);

--2. Create insert script to inserting data into each table (Employee and PositionHistory)

SET IDENTITY_INSERT master.dbo.Employee ON;

INSERT INTO master.dbo.Employee 
(Id, EmployeeId, FullName, BirthDate, Address) VALUES 
(1, 10105001, 'Ali Anton', '1982-08-19', 'Jakarta Utara'),
(2, 10105002, 'Rara Siva', '1982-01-01', 'Mandalika'),
(3, 10105003, 'Rini Aini', '1982-02-20', 'Sumbawa Besar'),
(4, 10105004, 'Budi', '1982-02-22', 'Mataram Kota');

SET IDENTITY_INSERT master.dbo.Employee OFF;

SET IDENTITY_INSERT master.dbo.PositionHistory ON;

INSERT INTO master.dbo.PositionHistory
(Id, PosId, PosTitle, EmployeeId, StartDate, EndDate) VALUES
(1, 5000, 'IT Manager', '10105001', '2022-06-01', '2022-02-28'),
(2, 5001, 'IT Sr. Manager', '10105001', '2022-03-01', '2022-12-31'),
(3, 5002, 'Programmer Analyst', '10105002', '2022-01-01', '2022-02-28'),
(4, 5003, 'Sr. Programmer Analyst', '10105002', '2022-03-01', '2022-12-31'),
(5, 5004, 'IT Admin', '10105003', '2022-06-01', '2022-02-28'),
(6, 5005, 'IT Secretary', '10105003', '2022-03-01', '2022-12-31');

SET IDENTITY_INSERT master.dbo.PositionHistory OFF;

--3. Create query to display all employee (EmployeeId, FullName, BirthDate, Address) data with their
--   current position information (PosId, PosTitle, EmployeeId, StartDate, EndDate)

-- This will display current position for all the employee. 
-- It will return empty since the end date of position history is at 2022
SELECT e.EmployeeId, e.FullName, e.BirthDate, e.Address,
	ph.PosId, ph.PosTitle, ph.EmployeeId, ph.StartDate, ph.EndDate
from master.dbo.Employee e 
left join master.dbo.PositionHistory ph 
	on e.EmployeeId = ph.EmployeeId 
where cast(GETDATE() as date) between ph.StartDate and ph.EndDate;

-- This will display the latest position for all the employee. 
with cte as (
SELECT e.EmployeeId, e.FullName, e.BirthDate, e.Address,
	ph.PosId, ph.PosTitle, ph.EmployeeId as PhEmployeeId, ph.StartDate, ph.EndDate,
	ROW_NUMBER() over (PARTITION by ph.EmployeeId order by ph.StartDate desc) as rn
from master.dbo.Employee e 
left join master.dbo.PositionHistory ph 
	on e.EmployeeId = ph.EmployeeId
)
select EmployeeId, FullName, BirthDate, Address,
	PosId, PosTitle, PhEmployeeId, StartDate, EndDate
from cte 
where rn = 1;