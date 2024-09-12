import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from test.TestUtils import TestUtils
from empspark import *  # Ensure the import path is correct

class EmployeeSalaryAnalysisTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize Spark Session
        cls.spark = SparkSession.builder.appName("Employee Salary Analysis Test").getOrCreate()

        # Sample DataFrame
        data = [
            ('John Doe', '1990-05-15', '2010-01-01', 50000, 'Engineering'),
            ('Jane Smith', '1985-07-20', '2008-03-15', 75000, 'HR'),
            ('Mike Johnson', '1970-10-10', '1995-10-20', 95000, 'Finance'),
            ('Emily Davis', '1995-12-25', '2020-06-10', 60000, 'Engineering'),
            ('Robert Brown', '1980-11-05', '2005-07-25', 85000, 'Finance')
        ]

        columns = ['name', 'date_of_birth', 'date_of_joining', 'salary', 'department']
        cls.df = cls.spark.createDataFrame(data, columns)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_get_max_salary_employee(self):
        try:
            max_salary_employee = get_max_salary_employee(self.df)
            expected_name = 'Mike Johnson'

            if max_salary_employee and max_salary_employee.collect()[0]['name'] == expected_name:
                TestUtils.yakshaAssert("test_get_max_salary_employee", True, "functional")
                print("test_get_max_salary_employee = Passed")
            else:
                TestUtils.yakshaAssert("test_get_max_salary_employee", False, "functional")
                print("test_get_max_salary_employee = Failed")
        except Exception as e:
            TestUtils.yakshaAssert("test_get_max_salary_employee", False, "functional")
            print(f"test_get_max_salary_employee = Failed with error: {str(e)}")

    def test_get_min_salary_employee(self):
        try:
            min_salary_employee = get_min_salary_employee(self.df)
            expected_name = 'John Doe'

            if min_salary_employee and min_salary_employee.collect()[0]['name'] == expected_name:
                TestUtils.yakshaAssert("test_get_min_salary_employee", True, "functional")
                print("test_get_min_salary_employee = Passed")
            else:
                TestUtils.yakshaAssert("test_get_min_salary_employee", False, "functional")
                print("test_get_min_salary_employee = Failed")
        except Exception as e:
            TestUtils.yakshaAssert("test_get_min_salary_employee", False, "functional")
            print(f"test_get_min_salary_employee = Failed with error: {str(e)}")

    def test_get_youngest_employee(self):
        try:
            youngest_employee = get_youngest_employee(self.df)
            expected_name = 'Emily Davis'

            if youngest_employee and youngest_employee.collect()[0]['name'] == expected_name:
                TestUtils.yakshaAssert("test_get_youngest_employee", True, "functional")
                print("test_get_youngest_employee = Passed")
            else:
                TestUtils.yakshaAssert("test_get_youngest_employee", False, "functional")
                print("test_get_youngest_employee = Failed")
        except Exception as e:
            TestUtils.yakshaAssert("test_get_youngest_employee", False, "functional")
            print(f"test_get_youngest_employee = Failed with error: {str(e)}")

    def test_get_senior_employee(self):
        try:
            senior_employee = get_senior_employee(self.df)
            expected_name = 'Mike Johnson'

            if senior_employee and senior_employee.collect()[0]['name'] == expected_name:
                TestUtils.yakshaAssert("test_get_senior_employee", True, "functional")
                print("test_get_senior_employee = Passed")
            else:
                TestUtils.yakshaAssert("test_get_senior_employee", False, "functional")
                print("test_get_senior_employee = Failed")
        except Exception as e:
            TestUtils.yakshaAssert("test_get_senior_employee", False, "functional")
            print(f"test_get_senior_employee = Failed with error: {str(e)}")

    def test_get_department_with_highest_avg_salary(self):
        try:
            department_with_highest_avg_salary = get_department_with_highest_avg_salary(self.df)
            expected_department = 'Finance'

            if department_with_highest_avg_salary and department_with_highest_avg_salary.collect()[0]['department'] == expected_department:
                TestUtils.yakshaAssert("test_get_department_with_highest_avg_salary", True, "functional")
                print("test_get_department_with_highest_avg_salary = Passed")
            else:
                TestUtils.yakshaAssert("test_get_department_with_highest_avg_salary", False, "functional")
                print("test_get_department_with_highest_avg_salary = Failed")
        except Exception as e:
            TestUtils.yakshaAssert("test_get_department_with_highest_avg_salary", False, "functional")
            print(f"test_get_department_with_highest_avg_salary = Failed with error: {str(e)}")

if __name__ == '__main__':
    unittest.main()
