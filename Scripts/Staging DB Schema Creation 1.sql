CREATE TABLE stg.Agency (
    AgencyID INT PRIMARY KEY,
    AgencyName TEXT NOT NULL
);

CREATE TABLE stg.Employee (
    EmployeeID TEXT PRIMARY KEY,
    LastName TEXT NOT NULL,
    FirstName TEXT NOT NULL
);

CREATE TABLE stg.Title (
    TitleCode TEXT PRIMARY KEY,
    TitleDescription TEXT NOT NULL
);


-- Payroll Valid Table
CREATE TABLE stg.Payroll (
    FiscalYear INT NOT NULL,
    PayrollNumber INT NOT NULL,
    AgencyID INT NOT NULL,
    AgencyName VARCHAR(255) NOT NULL,
    EmployeeID VARCHAR(50) NOT NULL,
    LastName VARCHAR(100) NOT NULL,
    FirstName VARCHAR(100) NOT NULL,
    AgencyStartDate DATE NOT NULL,
    WorkLocationBorough VARCHAR(100),
    TitleCode VARCHAR(50),
    TitleDescription VARCHAR(255),
    LeaveStatusAsOfJune30 VARCHAR(50),
    BaseSalary NUMERIC(10, 2),
    PayBasis VARCHAR(50),
    RegularHours NUMERIC(10, 2),
    RegularGrossPaid NUMERIC(10, 2),
    OTHours NUMERIC(10, 2),
    TotalOTPaid NUMERIC(10, 2),
    TotalOtherPay NUMERIC(10, 2),
    PRIMARY KEY (FiscalYear, PayrollNumber, AgencyID, EmployeeID),
	FOREIGN KEY (AgencyID) REFERENCES stg.Agency (AgencyID) ON DELETE CASCADE,
    FOREIGN KEY (EmployeeID) REFERENCES stg.Employee (EmployeeID) ON DELETE CASCADE,
    FOREIGN KEY (TitleCode) REFERENCES stg.Title (TitleCode) ON DELETE CASCADE,
    CONSTRAINT chk_BaseSalary CHECK (BaseSalary >= 0),
    CONSTRAINT chk_RegularHours CHECK (RegularHours >= 0),
    CONSTRAINT chk_RegularGrossPaid CHECK (RegularGrossPaid >= 0)
);

-- Payroll Issue Table
CREATE TABLE stg.payroll_data_issues (
    FiscalYear INT,
    PayrollNumber INT,
    AgencyID INT,
    AgencyName TEXT,
    EmployeeID TEXT,
    LastName TEXT,
    FirstName TEXT,
    AgencyStartDate DATE,
    WorkLocationBorough TEXT,
    TitleCode TEXT,
    TitleDescription TEXT,
    LeaveStatusasofJune30 TEXT,
    BaseSalary NUMERIC,
    PayBasis TEXT,
    RegularHours NUMERIC,
    RegularGrossPaid NUMERIC,
    OTHours NUMERIC,
    TotalOTPaid NUMERIC,
    TotalOtherPay NUMERIC,
    AgencyCode TEXT,
    issue_reason TEXT,
    reviewed_status TEXT DEFAULT 'Pending'
);
